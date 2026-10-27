//! Live-PG coverage for the lane-banded retry model: a runner's `Err(_)` is
//! always retried on the band its lane selects and never ends the job, a
//! deployed fix is picked up with no operator action, and only the job's own
//! `JobCompletion::Fail` is terminal.
//!
//! The retry tests run on a manual clock so the fatal band's multi-minute
//! delays are crossed by advancing it, not by waiting.

mod helpers;

use async_trait::async_trait;
use es_entity::clock::ClockHandle;
use es_entity::errlanes::Fatal;
use job::{
    Backoff, CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner,
    JobStatus, JobSvcConfig, JobTerminalState, JobType, Jobs, RetryBand, RetrySettings,
    error::{JobError, JobRejection},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

const POLL_TIMEOUT: Duration = Duration::from_secs(30);

fn deterministic_retry() -> RetrySettings {
    RetrySettings {
        backoff: Backoff::new(
            Duration::from_secs(1),
            Duration::from_secs(512),
            Duration::from_secs(6 * 3600),
            0,
        ),
        attempt_reset_after_healthy_run: None,
    }
}

/// Fails with `Fatal(Invariant)` until `fixed` is set, then completes.
struct FixableRunner {
    fixed: Arc<AtomicBool>,
    runs: Arc<AtomicUsize>,
}

#[async_trait]
impl JobRunner for FixableRunner {
    async fn run(
        &self,
        _: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        if self.fixed.load(Ordering::SeqCst) {
            Ok(JobCompletion::Complete)
        } else {
            Err(Box::new(Fatal::invariant("deterministic failure")))
        }
    }
}

struct FixableInitializer {
    job_type: JobType,
    fixed: Arc<AtomicBool>,
    runs: Arc<AtomicUsize>,
}

impl JobInitializer for FixableInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        deterministic_retry()
    }

    fn init(
        &self,
        _: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(FixableRunner {
            fixed: Arc::clone(&self.fixed),
            runs: Arc::clone(&self.runs),
        }))
    }
}

async fn execution_row(
    pool: &sqlx::PgPool,
    id: JobId,
) -> anyhow::Result<Option<(i32, String, Option<chrono::DateTime<chrono::Utc>>)>> {
    Ok(sqlx::query_as(
        "SELECT attempt_index, state::text, execute_at FROM job_executions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?)
}

/// State-polls until `id` is `pending` at `attempt_index = attempt`, and
/// returns the row's `execute_at`.
async fn await_pending_at(
    pool: &sqlx::PgPool,
    id: JobId,
    attempt: i32,
) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    loop {
        if let Some((got, state, Some(execute_at))) = execution_row(pool, id).await?
            && got == attempt
            && state == "pending"
        {
            return Ok(execute_at);
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {id} to be pending at attempt {attempt}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn await_state(
    jobs: &Jobs,
    id: JobId,
    what: &str,
    predicate: impl Fn(&JobStatus) -> bool,
) -> anyhow::Result<JobStatus> {
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    loop {
        let status = jobs.handle(id).load().await?.state();
        if predicate(&status) {
            return Ok(status);
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}; last status {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A runner that always returns `Err(Fatal)` is never ended by retrying: the
/// row stays `pending`, `attempt_index` climbs by one per dispatch, and each
/// retry lands on the fatal band's doubling schedule (512s, 1024s, 2048s).
/// The span-level half (`error.level = ERROR`, `retry.band = fatal`) is in
/// `tests/lanes_denied_span.rs`.
#[tokio::test]
async fn a_deterministic_failer_never_dies_and_retries_on_the_fatal_band() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (clock, controller) = ClockHandle::manual();
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .clock(clock.clone())
        .build()
        .unwrap();
    let mut jobs = Jobs::init(config).await?;
    let runs = Arc::new(AtomicUsize::new(0));
    let spawner = jobs.add_initializer(FixableInitializer {
        job_type: helpers::job_type("lane-banded-never-dies"),
        fixed: Arc::new(AtomicBool::new(false)),
        runs: Arc::clone(&runs),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;

    for n in 1..=3i32 {
        let execute_at = await_pending_at(&pool, id, n + 1).await?;
        let expected = Duration::from_secs(512 * (1 << (n - 1)));
        let delay = execute_at - clock.now();
        assert!(
            (delay.num_milliseconds() - expected.as_millis() as i64).abs() <= 1,
            "failure {n} must retry {expected:?} out on the fatal band, got {delay}"
        );

        let status = jobs.handle(id).load().await?.state();
        assert!(
            matches!(
                status,
                JobStatus::Pending {
                    band: RetryBand::Fatal,
                    last_error: Some(_),
                    ..
                }
            ),
            "still alive and in the fatal band, got {status:?}"
        );

        controller.advance(expected).await;
    }

    await_pending_at(&pool, id, 5).await?;
    assert_eq!(runs.load(Ordering::SeqCst), 4);

    jobs.shutdown().await?;
    Ok(())
}

/// The same failing job, fixed by flipping a flag the runner checks: once the
/// clock passes `execute_at` the next dispatch completes it, with no API
/// call in between.
#[tokio::test]
async fn a_deployed_hotfix_is_picked_up_by_the_next_dispatch() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (clock, controller) = ClockHandle::manual();
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .clock(clock.clone())
        .build()
        .unwrap();
    let mut jobs = Jobs::init(config).await?;
    let fixed = Arc::new(AtomicBool::new(false));
    let spawner = jobs.add_initializer(FixableInitializer {
        job_type: helpers::job_type("lane-banded-hotfix"),
        fixed: Arc::clone(&fixed),
        runs: Arc::new(AtomicUsize::new(0)),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let execute_at = await_pending_at(&pool, id, 2).await?;

    fixed.store(true, Ordering::SeqCst);
    controller
        .advance((execute_at - clock.now()).to_std()?)
        .await;

    let status = await_state(&jobs, id, "the fixed job to complete", |s| {
        matches!(s, JobStatus::Completed { .. })
    })
    .await?;
    assert!(status.is_terminal());

    jobs.shutdown().await?;
    Ok(())
}

struct FailVerdictRunner;

#[async_trait]
impl JobRunner for FailVerdictRunner {
    async fn run(
        &self,
        _: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        Ok(JobCompletion::Fail("nope".into()))
    }
}

struct FailVerdictInitializer {
    job_type: JobType,
}

impl JobInitializer for FailVerdictInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(FailVerdictRunner))
    }
}

/// `Ok(JobCompletion::Fail)` is terminal and wakes the job's waiters. The
/// intended cost is that the id is spent: respawning the same id -- which is
/// what a `key()`ed job derives -- is `DuplicateId`, not a fresh generation.
#[tokio::test]
async fn fail_is_terminal_wakes_waiters_and_respawning_the_same_id_is_duplicate_id()
-> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(FailVerdictInitializer {
        job_type: helpers::job_type("lane-banded-fail-verdict"),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;

    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert!(outcome.is_failed());
    assert_eq!(outcome.state(), JobTerminalState::Failed);
    assert_eq!(
        jobs.handle(id).load().await?.state(),
        JobStatus::Failed {
            reason: "nope".to_string(),
            queue_id: None,
        }
    );
    assert!(
        execution_row(&pool, id).await?.is_none(),
        "the execution row must be gone"
    );

    let respawn = spawner.spawn(id, Cfg).await;
    assert!(
        matches!(
            &respawn,
            Err(JobError::Rejected(JobRejection::DuplicateId(got))) if *got == id
        ),
        "respawning a failed job's id must be DuplicateId, got an error of another kind or Ok"
    );

    jobs.shutdown().await?;
    Ok(())
}

#[cfg(feature = "es-entity")]
mod with_op {
    use super::*;
    use es_entity::AtomicOperation;

    const TABLE: &str = "lane_banded_fail_with_op";

    struct FailWithOpRunner;

    #[async_trait]
    impl JobRunner for FailWithOpRunner {
        async fn run(
            &self,
            current_job: CurrentJob,
        ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
            let mut op = current_job.begin_op().await?;
            sqlx::query(&format!("INSERT INTO {TABLE} (id) VALUES ($1)"))
                .bind(uuid::Uuid::from(*current_job.id()))
                .execute(op.as_executor())
                .await?;
            Ok(JobCompletion::FailWithOp(op, "nope".into()))
        }
    }

    struct FailWithOpInitializer {
        job_type: JobType,
    }

    impl JobInitializer for FailWithOpInitializer {
        type Config = Cfg;

        fn job_type(&self) -> JobType {
            self.job_type.clone()
        }

        fn init(
            &self,
            _: &Job,
            _: JobSpawner<Self::Config>,
        ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Box::new(FailWithOpRunner))
        }
    }

    /// `FailWithOp` commits the op the runner handed back along with the
    /// verdict.
    #[tokio::test]
    async fn fail_with_op_commits_the_op() -> anyhow::Result<()> {
        let pool = helpers::init_pool().await?;
        sqlx::query(&format!("DROP TABLE IF EXISTS {TABLE}"))
            .execute(&pool)
            .await?;
        sqlx::query(&format!("CREATE TABLE {TABLE} (id UUID PRIMARY KEY)"))
            .execute(&pool)
            .await?;

        let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
        let mut jobs = Jobs::init(config).await?;
        let spawner = jobs.add_initializer(FailWithOpInitializer {
            job_type: helpers::job_type("lane-banded-fail-with-op"),
        });
        jobs.start_poll().await?;

        let id = JobId::new();
        spawner.spawn(id, Cfg).await?;
        let outcome = jobs
            .handle(id)
            .await_completion(Duration::from_secs(10))
            .await?;
        assert!(outcome.is_failed());

        let (count,): (i64,) =
            sqlx::query_as(&format!("SELECT COUNT(*) FROM {TABLE} WHERE id = $1"))
                .bind(uuid::Uuid::from(id))
                .fetch_one(&pool)
                .await?;
        assert_eq!(count, 1, "the op handed back with the verdict must commit");

        jobs.shutdown().await?;
        Ok(())
    }
}
