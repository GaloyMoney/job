mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, JobCompletion, JobId, JobInitializer, JobOutcomes, JobPollerConfig, JobRunner,
    JobSpawner, JobSpec, JobSvcConfig, JobTerminalState, JobType, Jobs,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

struct NoopRunner;

#[async_trait]
impl JobRunner for NoopRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        Ok(JobCompletion::Complete)
    }
}

struct NoopInitializer {
    job_type: JobType,
}

impl JobInitializer for NoopInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _job: &job::Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(NoopRunner))
    }
}

async fn xact_count(pool: &sqlx::PgPool) -> anyhow::Result<i64> {
    let row: (Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT xact_commit, xact_rollback FROM pg_stat_database \
         WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await?;
    Ok(row.0.unwrap_or(0) + row.1.unwrap_or(0))
}

async fn wait_until(
    mut f: impl AsyncFnMut() -> anyhow::Result<bool>,
    attempts: usize,
    period: Duration,
    what: &str,
) -> anyhow::Result<()> {
    for _ in 0..attempts {
        if f().await? {
            return Ok(());
        }
        tokio::time::sleep(period).await;
    }
    anyhow::bail!("timed out waiting for: {what}");
}

async fn remaining_executions(pool: &sqlx::PgPool, job_type: &str) -> anyhow::Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM job_executions WHERE job_type = $1")
            .bind(job_type)
            .fetch_one(pool)
            .await?,
    )
}

#[tokio::test]
async fn await_all_over_a_wide_fanout_does_not_exhaust_the_pool() -> anyhow::Result<()> {
    let pg_con = std::env::var("PG_CON").unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(40)
        .acquire_timeout(Duration::from_millis(200))
        .connect(&pg_con)
        .await?;

    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .poller_config(JobPollerConfig {
            max_jobs_per_process: 150,
            min_jobs_per_process: 100,
            ..Default::default()
        })
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("await-all-wide-fanout");
    let spawner = jobs.add_initializer(NoopInitializer {
        job_type: job_type.clone(),
    });
    jobs.start_poll().await?;

    const N: usize = 20_000;
    let ids: Vec<JobId> = (0..N).map(|_| JobId::new()).collect();

    for page in ids.chunks(1_000) {
        let specs: Vec<JobSpec<Cfg>> = page.iter().map(|id| JobSpec::new(*id, Cfg)).collect();
        spawner.spawn_all(specs).await?;
    }

    wait_until(
        async || Ok(remaining_executions(&pool, job_type.as_str()).await? == 0),
        1200,
        Duration::from_millis(100),
        "every spawned child to reach terminal state",
    )
    .await?;

    let handles = jobs.handles(ids.clone());

    let before = xact_count(&pool).await?;
    let outcomes = handles.await_all(Duration::from_secs(60)).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = xact_count(&pool).await?;

    assert_eq!(outcomes.len(), N);
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "every job must have completed"
    );
    assert!(outcomes.all_succeeded());

    let delta = after - before;
    let budget = (N / 2) as i64;
    assert!(
        delta < budget,
        "await_all issued {delta} statements resolving {N} already-terminal handles -- \
         stock code issues roughly 2 per handle (~{}), the fix should issue far fewer; \
         budget is N/2 = {budget}",
        2 * N,
    );

    jobs.shutdown().await?;
    Ok(())
}
