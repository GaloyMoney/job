//! Live-PG coverage for `JobDispatcher::fail_job`/`Finalizer::finalize`
//! routing terminal writes to the internal pool when the shared one is
//! under pressure, on the single-job (non-batched) path.
//!
//! Deliberately its own file, same reason as `pool_terminal_write_safety.rs`
//! (its batched counterpart): this test deliberately exhausts the shared
//! pool it hands to `Jobs`, which would starve any other test sharing the
//! same `Jobs`/pool instance.

mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSvcConfig, JobType,
    Jobs, RetrySettings,
};
use serde::{Deserialize, Serialize};
use sqlx::Postgres;
use sqlx::pool::PoolConnection;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg {
    label: String,
}

/// Drains the shared pool down to whatever it can truly give up from INSIDE
/// `run`, then fails -- so by the time `fail_job` needs a connection to
/// record that failure, the shared pool has none left to give. See
/// `pool_terminal_write_safety.rs`'s `StarvePoolThenFailRunner` for why this
/// drains via a bounded-timeout loop rather than a fixed connection count
/// (the permanently-held `LISTEN` connection).
struct StarvePoolThenFailRunner {
    held: Arc<Mutex<Vec<PoolConnection<Postgres>>>>,
}

#[async_trait]
impl JobRunner for StarvePoolThenFailRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let pool = current_job.pool().clone();
        let mut held = self.held.lock().await;
        loop {
            match tokio::time::timeout(Duration::from_millis(200), pool.acquire()).await {
                Ok(Ok(conn)) => held.push(conn),
                Ok(Err(e)) => return Err(Box::new(e)),
                // No connection became available within the timeout: the
                // pool has given up everything it's going to give up.
                Err(_) => break,
            }
        }
        Err("real failure, shared pool now fully starved".into())
    }
}

struct StarvePoolThenFailInitializer {
    job_type: JobType,
    held: Arc<Mutex<Vec<PoolConnection<Postgres>>>>,
}

impl JobInitializer for StarvePoolThenFailInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        // `n_attempts: 1` so the single failure is immediately terminal --
        // no need to wait through a backoff/retry cycle for this test's one
        // assertion.
        RetrySettings {
            n_attempts: Some(1),
            ..Default::default()
        }
    }

    fn init(
        &self,
        _job: &job::Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(StarvePoolThenFailRunner {
            held: Arc::clone(&self.held),
        }))
    }
}

/// Poll `job_executions` for `id` going away via a connection OUTSIDE the
/// pool this test starves. See
/// `pool_terminal_write_safety.rs`'s `wait_for_execution_row_deleted` for why
/// this can't use `Jobs`' own `handles().await_all()`.
async fn wait_for_execution_row_deleted(
    observer_pool: &sqlx::PgPool,
    id: JobId,
) -> anyhow::Result<()> {
    for _ in 0..150 {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM job_executions WHERE id = $1")
            .bind(uuid::Uuid::from(id))
            .fetch_one(observer_pool)
            .await?;
        if count == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!(
        "job_executions row for {id} was never deleted -- the failure write \
         never landed"
    );
}

/// A solo job's failure write must land even when the shared pool is fully
/// exhausted at the moment it's recorded -- the single-job counterpart of
/// `pool_terminal_write_safety.rs`'s `terminal_write_survives_shared_pool_exhaustion`,
/// closing the gap this feature's PR named (`dispatcher.rs` had the
/// identical shared-pool pattern with no rescue fallback). Revert
/// `Finalizer::begin_op` to always begin on the shared pool to see this
/// test fail for the right reason (`wait_for_execution_row_deleted` times
/// out).
#[tokio::test]
async fn terminal_write_survives_shared_pool_exhaustion() -> anyhow::Result<()> {
    // A separate connection this test's own exhaustion can never touch --
    // see `wait_for_execution_row_deleted`.
    let observer_pool = helpers::init_pool().await?;

    let pg_con = std::env::var("PG_CON").unwrap();
    // Small and known on purpose: the runner drains it to whatever it can
    // truly give up, and the test asserts on that afterward.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&pg_con)
        .await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let held = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(StarvePoolThenFailInitializer {
        job_type: helpers::job_type("pool-terminal-write-safety-solo"),
        held: Arc::clone(&held),
    });

    let id = JobId::new();
    spawner
        .spawn(
            id,
            Cfg {
                label: "starve".to_string(),
            },
        )
        .await?;

    jobs.start_poll().await?;

    // If `fail_job` still opened its op on the shared pool (the pre-fix
    // behaviour), this would time out: the write could never acquire a
    // connection, and the row would strand instead of being deleted here.
    wait_for_execution_row_deleted(&observer_pool, id).await?;

    // The exhaustion was real when the write had to happen: every
    // connection the shared pool could give the runner is still held right
    // now, well after the write already landed via the internal pool.
    assert_eq!(
        pool.num_idle(),
        0,
        "every shared-pool connection the runner could get should still be held"
    );

    held.lock().await.clear();
    Ok(())
}
