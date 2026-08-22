//! Live-PG coverage for `BatchDispatcher::fail_batch`/`terminal_write_repo`
//! routing terminal writes through the internal pool instead of the shared
//! one (`handoff-pool-aware-claiming-and-fail-path.md` §5).
//!
//! Deliberately its own file: this is the one test in the suite that
//! deliberately exhausts the shared pool it hands to `Jobs`, which would
//! starve any other test sharing the same `Jobs`/pool instance.

mod helpers;

use async_trait::async_trait;
use job::{
    BatchedJobInitializer, BatchedJobRunner, CurrentBatchedJob, JobBatchCompletion, JobId,
    JobSpawner, JobSpec, JobSvcConfig, JobTerminalState, JobType, Jobs, RetrySettings,
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

/// Drains the shared pool down to zero idle connections from INSIDE
/// `run_batch`, then fails -- so by the time `fail_batch` needs a
/// connection to record that failure, the shared pool has none left to
/// give. Held connections live in `held`, owned by the test, not dropped
/// until the test's own assertions are done: `fail_batch` has to succeed
/// while the exhaustion is still real, not merely at some point after.
struct StarvePoolThenFailRunner {
    held: Arc<Mutex<Vec<PoolConnection<Postgres>>>>,
}

#[async_trait]
impl BatchedJobRunner for StarvePoolThenFailRunner {
    type Config = Cfg;

    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<Cfg>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        let pool = current_batch.pool().clone();
        let max = pool.options().get_max_connections();
        let mut held = self.held.lock().await;
        for _ in 0..max {
            held.push(pool.acquire().await?);
        }
        Err("real failure, shared pool now fully starved".into())
    }
}

struct StarvePoolThenFailInitializer {
    job_type: JobType,
    held: Arc<Mutex<Vec<PoolConnection<Postgres>>>>,
}

impl BatchedJobInitializer for StarvePoolThenFailInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        // `n_attempts: 1` so the single failure is immediately terminal --
        // no need to wait through a backoff/retry cycle for this test's
        // one assertion.
        RetrySettings {
            n_attempts: Some(1),
            ..Default::default()
        }
    }

    fn init(
        &self,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(StarvePoolThenFailRunner {
            held: Arc::clone(&self.held),
        }))
    }
}

/// A batch's failure write must land even when the shared pool is fully
/// exhausted at the moment it's recorded -- the actual lost-job fix
/// (`handoff-pool-aware-claiming-and-fail-path.md` §5, work item C). Before
/// that fix, both `fail_batch` and its `rescue_claimed_rows` fallback opened
/// their op on the same shared pool this test starves, so under this exact
/// condition the row would strand `running` until the lost-handler swept it
/// minutes later. Revert `terminal_write_repo` to always return
/// `(*self.repo).clone()` (i.e. undo the internal-pool routing) to see this
/// test time out for the right reason.
#[tokio::test]
async fn terminal_write_survives_shared_pool_exhaustion() -> anyhow::Result<()> {
    let pg_con = std::env::var("PG_CON").unwrap();
    // Small and known on purpose: the runner drains exactly this many
    // connections, so the test can assert the pool was truly at zero idle
    // when the write had to happen.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&pg_con)
        .await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let held = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_batched_initializer(StarvePoolThenFailInitializer {
        job_type: helpers::job_type("pool-terminal-write-safety"),
        held: Arc::clone(&held),
    });

    let id = JobId::new();
    spawner
        .spawn_all(vec![JobSpec::new(
            id,
            Cfg {
                label: "starve".to_string(),
            },
        )])
        .await?;

    jobs.start_poll().await?;

    // If `fail_batch` still opened its op on the shared pool (the pre-fix
    // behaviour), this would time out: the write could never acquire a
    // connection, and the row would strand exactly as sb-max11 observed
    // instead of reaching `Errored` here.
    let outcomes = jobs
        .handles(vec![id])
        .await_all(Duration::from_secs(15))
        .await?;
    assert_eq!(
        outcomes[0].state(),
        JobTerminalState::Errored,
        "the failure write must land (terminating the job, n_attempts=1) even \
         though the shared pool was fully exhausted when it had to be written"
    );

    // The exhaustion was real and sustained throughout, not just before the
    // runner started.
    assert_eq!(
        pool.size(),
        2,
        "the runner should have grown the pool to its cap"
    );
    assert_eq!(
        pool.num_idle(),
        0,
        "every shared-pool connection should still be held at this point"
    );

    held.lock().await.clear();
    Ok(())
}
