//! Live-PG coverage for `BatchDispatcher::fail_batch`/
//! `CongestionHandler::begin_terminal_write_op` routing terminal writes
//! through the internal pool instead of the shared one
//! (`handoff-pool-aware-claiming-and-fail-path.md` §5).
//!
//! Deliberately its own file: this is the one test in the suite that
//! deliberately exhausts the shared pool it hands to `Jobs`, which would
//! starve any other test sharing the same `Jobs`/pool instance.

mod helpers;

use async_trait::async_trait;
use job::{
    BatchedJobInitializer, BatchedJobRunner, CurrentBatchedJob, JobBatchCompletion, JobId,
    JobSpawner, JobSpec, JobSvcConfig, JobType, Jobs, RetrySettings,
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
///
/// Drains by repeatedly acquiring under a short per-attempt timeout rather
/// than looping `max_connections` times: `Jobs`' own notification router
/// permanently checks out one connection for its `LISTEN` for as long as
/// `Jobs` is running (`PgListener::connect_with` acquires and never
/// releases), so fewer than `max_connections` are ever actually
/// obtainable. A bounded acquire that gives up once nothing is available
/// -- rather than a fixed count -- drains whatever the pool can truly give
/// up, regardless of what else is ambiently holding a connection.
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

/// Poll `job_executions` for `id` going away via a connection OUTSIDE the
/// pool this test starves.
///
/// `Jobs`' own await-completion path (`handles().await_all()`) reads job
/// state back through the same shared pool the runner just drained to
/// zero -- so awaiting completion that way would itself starve on this
/// test's own exhaustion, independent of whether `fail_batch` used the
/// shared or the internal pool. `observer_pool` is a wholly separate
/// connection (from `helpers::init_pool`, never touched by
/// `StarvePoolThenFailRunner`), so this observes the actual database
/// outcome without depending on the thing under test.
///
/// A deleted row is `fail_in_op`'s terminal branch's signature (see
/// `batch_dispatcher.rs`'s terminal `DELETE FROM job_executions`): with
/// `n_attempts: 1` this is the only way this test's job execution row can
/// disappear.
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
         never landed (the row stranded exactly as sb-max11 observed)"
    );
}

/// A batch's failure write must land even when the shared pool is fully
/// exhausted at the moment it's recorded -- the actual lost-job fix
/// (`handoff-pool-aware-claiming-and-fail-path.md` §5, work item C). Before
/// that fix, both `fail_batch` and its `rescue_claimed_rows` fallback opened
/// their op on the same shared pool this test starves, so under this exact
/// condition the row would strand `running` until the lost-handler swept it
/// minutes later. Revert `CongestionHandler::begin_terminal_write_op` to
/// always begin on `(*self.repo).clone()` (i.e. undo the internal-pool
/// routing) to see this test fail for the right reason
/// (`wait_for_execution_row_deleted` times out).
#[tokio::test]
async fn terminal_write_survives_shared_pool_exhaustion() -> anyhow::Result<()> {
    // A separate connection this test's own exhaustion can never touch --
    // see `wait_for_execution_row_deleted`.
    let observer_pool = helpers::init_pool().await?;

    let pg_con = std::env::var("PG_CON").unwrap();
    // Small and known on purpose: the runner drains it to whatever it can
    // truly give up (see `StarvePoolThenFailRunner`'s doc comment), and the
    // test asserts on that afterward.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
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
    // instead of being deleted here.
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
