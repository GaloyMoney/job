mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, JobCompletion, JobId, JobInitializer, JobPollerConfig, JobRunner, JobSpawner,
    JobSvcConfig, JobType, Jobs,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::watch;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg {
    index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct MyResult {
    value: u64,
}

struct ResultRunner {
    index: u64,
    release: Option<watch::Receiver<bool>>,
}

#[async_trait]
impl JobRunner for ResultRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        if let Some(release) = &self.release {
            let mut release = release.clone();
            while !*release.borrow() {
                release.changed().await.ok();
            }
        }
        current_job
            .set_result(&MyResult { value: self.index })
            .await?;
        Ok(JobCompletion::Complete)
    }
}

struct ResultInitializer {
    job_type: JobType,
    release: Option<watch::Receiver<bool>>,
}

impl JobInitializer for ResultInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        job: &job::Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: Cfg = job.config()?;
        Ok(Box::new(ResultRunner {
            index: config.index,
            release: self.release.clone(),
        }))
    }
}

#[tokio::test]
async fn await_all_resolves_jobs_completed_by_another_process() -> anyhow::Result<()> {
    let pg_con = std::env::var("PG_CON").unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(20)
        .connect(&pg_con)
        .await?;

    let job_type = helpers::job_type("await-all-cross-process-notify");

    let config_a = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig (A)");
    let mut jobs_a = Jobs::init(config_a).await?;
    jobs_a.start_poll().await?;

    let config_b = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig (B)");
    let mut jobs_b = Jobs::init(config_b).await?;
    let spawner_b = jobs_b.add_initializer(ResultInitializer {
        job_type: job_type.clone(),
        release: None,
    });
    jobs_b.start_poll().await?;

    const N: u64 = 200;
    let ids: Vec<JobId> = (0..N).map(|_| JobId::new()).collect();
    for (i, id) in ids.iter().enumerate() {
        spawner_b.spawn(*id, Cfg { index: i as u64 }).await?;
    }

    let outcomes = jobs_a
        .handles(ids.clone())
        .await_all(Duration::from_secs(10))
        .await?;

    assert_eq!(outcomes.len(), N as usize);
    for (i, outcome) in outcomes.iter().enumerate() {
        assert!(outcome.is_completed());
        let result: MyResult = outcome
            .result()?
            .expect("outcome must carry the runner's return value");
        assert_eq!(
            result.value, i as u64,
            "outcomes[{i}] must belong to ids[{i}] (order preservation, contract 2)"
        );
    }

    jobs_a.shutdown().await?;
    jobs_b.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn await_all_resolves_via_sweep_when_notify_buffer_overflows() -> anyhow::Result<()> {
    let pg_con = std::env::var("PG_CON").unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(20)
        .connect(&pg_con)
        .await?;

    let job_type = helpers::job_type("await-all-cross-process-sweep");

    let config_a = JobSvcConfig::builder()
        .pool(pool.clone())
        .poller_config(JobPollerConfig {
            terminal_channel_size: 1,
            sweep_interval: Duration::from_millis(500),
            ..Default::default()
        })
        .build()
        .expect("Failed to build JobsConfig (A)");
    let mut jobs_a = Jobs::init(config_a).await?;
    jobs_a.start_poll().await?;

    let config_b = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig (B)");
    let mut jobs_b = Jobs::init(config_b).await?;
    let (release_tx, release_rx) = watch::channel(false);
    let spawner_b = jobs_b.add_initializer(ResultInitializer {
        job_type: job_type.clone(),
        release: Some(release_rx),
    });
    jobs_b.start_poll().await?;

    const N: u64 = 200;
    let ids: Vec<JobId> = (0..N).map(|_| JobId::new()).collect();
    for (i, id) in ids.iter().enumerate() {
        spawner_b.spawn(*id, Cfg { index: i as u64 }).await?;
    }

    let handles_a = jobs_a.handles(ids.clone());
    let await_all_task =
        tokio::spawn(async move { handles_a.await_all(Duration::from_secs(15)).await });

    release_tx.send(true)?;

    let outcomes = await_all_task.await??;

    assert_eq!(outcomes.len(), N as usize);
    for (i, outcome) in outcomes.iter().enumerate() {
        assert!(outcome.is_completed());
        let result: MyResult = outcome
            .result()?
            .expect("outcome must carry the runner's return value");
        assert_eq!(
            result.value, i as u64,
            "outcomes[{i}] must belong to ids[{i}]"
        );
    }

    jobs_a.shutdown().await?;
    jobs_b.shutdown().await?;
    Ok(())
}
