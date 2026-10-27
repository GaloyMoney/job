//! Live-PG coverage for the lane-banded retry matrix (`finalizer.rs`'s
//! runner-failure classification, `JobDispatcher::fail_job`,
//! `Job::maybe_schedule_retry`): what a runner's classified error does to
//! the job. A runner error never ends a job; its lane picks the band the
//! retry is scheduled in, and `JobStatus::Pending` reports that band.
//!
//! One job type per test (`helpers::job_type` suffixes for uniqueness), and
//! every runner reports its own invocations' attempt numbers through a
//! shared `Vec<u32>` so a test can assert on the exact attempt-count
//! progression, not just the final `JobStatus` -- the same pattern
//! `tests/pool_congestion.rs` uses.

mod helpers;

use async_trait::async_trait;
use es_entity::errlanes::{Denied, Fatal, FatalKind, Transient, TransientKind};
use job::{
    Backoff, CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner,
    JobStatus, JobSvcConfig, JobTerminalState, JobType, Jobs, ResidentJobCompletion,
    ResidentJobInitializer, ResidentJobRunner, RetryBand, RetrySettings,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

/// A runner that always fails with whatever `build_error` produces, boxed
/// fresh on every call (errlanes payloads aren't `Clone` into a shared
/// closure-friendly form across calls in every case, so this takes a
/// factory rather than one pre-built error). Records every call's attempt
/// number.
struct AlwaysFailsRunner<F> {
    attempts: Arc<Mutex<Vec<u32>>>,
    build_error: F,
}

#[async_trait]
impl<F> JobRunner for AlwaysFailsRunner<F>
where
    F: Fn() -> Box<dyn std::error::Error + Send + Sync> + Send + Sync + 'static,
{
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        self.attempts.lock().await.push(current_job.attempt());
        Err((self.build_error)())
    }
}

struct AlwaysFailsInitializer<F> {
    job_type: JobType,
    retry: RetrySettings,
    attempts: Arc<Mutex<Vec<u32>>>,
    build_error: F,
}

impl<F> JobInitializer for AlwaysFailsInitializer<F>
where
    F: Fn() -> Box<dyn std::error::Error + Send + Sync> + Send + Sync + Clone + 'static,
{
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        self.retry.clone()
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(AlwaysFailsRunner {
            attempts: Arc::clone(&self.attempts),
            build_error: self.build_error.clone(),
        }))
    }
}

async fn start(label: &str) -> anyhow::Result<(Jobs, sqlx::PgPool, JobType)> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let jobs = Jobs::init(config).await?;
    Ok((jobs, pool, helpers::job_type(label)))
}

/// Polls the job's `JobStatus` until `predicate` holds, or fails the test.
async fn await_status(
    jobs: &Jobs,
    id: JobId,
    what: &str,
    predicate: impl Fn(&JobStatus) -> bool,
) -> anyhow::Result<JobStatus> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status = jobs.handle(id).load().await?.state();
        if predicate(&status) {
            return Ok(status);
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}; last status {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A job that has been retried at least three times: `Pending` at attempt 4
/// or later.
fn retried_at_least_thrice(status: &JobStatus) -> bool {
    matches!(status, JobStatus::Pending { attempt, .. } if *attempt >= 4)
}

/// A short curve whose transient band cannot cross `fatal_min` within the
/// handful of attempts these tests wait for, and whose fatal band is short
/// enough to wait through.
fn short_backoff() -> RetrySettings {
    RetrySettings {
        backoff: Backoff::new(
            Duration::from_millis(5),
            Duration::from_secs(1),
            Duration::from_secs(2),
            0,
        ),
        ..Default::default()
    }
}

fn short_fatal_backoff() -> RetrySettings {
    RetrySettings {
        backoff: Backoff::new(
            Duration::from_millis(2),
            Duration::from_millis(5),
            Duration::from_millis(20),
            0,
        ),
        ..Default::default()
    }
}

/// A `Transient` runner error (not `PoolTimeout`/`Congestion`, which take
/// the congestion-reschedule path instead) is retried in the transient band:
/// `attempt_index` climbs by one per retry and the job never ends.
#[tokio::test]
async fn transient_runner_error_retries_with_attempt_plus_one() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-transient-retries").await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: short_backoff(),
        attempts: Arc::clone(&attempts),
        build_error: || {
            Box::new(Transient::new(TransientKind::Other))
                as Box<dyn std::error::Error + Send + Sync>
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let status = await_status(&jobs, id, "a retry at attempt 4", retried_at_least_thrice).await?;
    assert!(
        matches!(
            status,
            JobStatus::Pending {
                band: RetryBand::Transient,
                last_error: Some(_),
                ..
            }
        ),
        "a transient failure must wait in the transient band, got {status:?}"
    );

    let seen = attempts.lock().await.clone();
    assert!(
        seen.starts_with(&[1, 2, 3]),
        "a transient failure must spend an ordinary retry attempt each time, got {seen:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A `Fatal` runner error is retried in the fatal band from its first
/// failure and the job never ends: errlanes' claim that it will not succeed
/// on retry selects the schedule and the log level, never death, so a
/// `Fatal` that is really transient costs time and nothing else.
#[tokio::test]
async fn fatal_runner_error_retries_in_the_fatal_band() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-fatal-retries").await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: short_fatal_backoff(),
        attempts: Arc::clone(&attempts),
        build_error: || {
            Box::new(Fatal::new(FatalKind::Invariant)) as Box<dyn std::error::Error + Send + Sync>
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let status = await_status(&jobs, id, "a retry at attempt 4", retried_at_least_thrice).await?;
    assert!(
        matches!(
            status,
            JobStatus::Pending {
                band: RetryBand::Fatal,
                ..
            }
        ),
        "a Fatal failure must wait in the fatal band, got {status:?}"
    );

    let seen = attempts.lock().await.clone();
    assert!(
        seen.starts_with(&[1, 2, 3]),
        "a Fatal error must be retried, got {seen:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A plain, unlaned string error (no errlanes payload anywhere in its
/// chain) gets `Fault::classify`'s `Fatal(Dependency)` default -- so it
/// lands in the fatal band, at `ERROR`, and is still retried forever.
#[tokio::test]
async fn unclassified_string_error_retries_in_the_fatal_band() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-unclassified-retries").await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: short_fatal_backoff(),
        attempts: Arc::clone(&attempts),
        build_error: || "plain unlaned failure".into(),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let status = await_status(&jobs, id, "a retry at attempt 4", retried_at_least_thrice).await?;
    assert!(
        matches!(
            status,
            JobStatus::Pending {
                band: RetryBand::Fatal,
                ..
            }
        ),
        "an unclassified error must wait in the fatal band, got {status:?}"
    );

    let seen = attempts.lock().await.clone();
    assert!(
        seen.starts_with(&[1, 2, 3]),
        "an unclassified error must be retried, got {seen:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A panic inside `run` is classified as `Fatal(FatalKind::Panic)` (see
/// `JobDispatcher::dispatch_job`), so the span says `error.lane = fatal`,
/// `error.code = panic` straight away and the retry waits in the fatal band.
/// The job stays alive for a hotfix to pick up.
#[tokio::test]
async fn panicking_runner_retries_in_the_fatal_band() -> anyhow::Result<()> {
    struct PanicInitializer {
        job_type: JobType,
    }
    impl JobInitializer for PanicInitializer {
        type Config = Cfg;
        fn job_type(&self) -> JobType {
            self.job_type.clone()
        }
        fn retry_on_error_settings(&self) -> RetrySettings {
            short_fatal_backoff()
        }
        fn init(
            &self,
            _job: &Job,
            _: JobSpawner<Self::Config>,
        ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Box::new(PanicRunner))
        }
    }
    struct PanicRunner;
    #[async_trait]
    impl JobRunner for PanicRunner {
        async fn run(
            &self,
            _current_job: CurrentJob,
        ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
            panic!("intentional test panic");
        }
    }

    let (mut jobs, _pool, job_type) = start("lanes-panic-retries").await?;
    let spawner = jobs.add_initializer(PanicInitializer { job_type });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let status = await_status(&jobs, id, "a retry at attempt 4", retried_at_least_thrice).await?;
    assert!(
        matches!(
            status,
            JobStatus::Pending {
                band: RetryBand::Fatal,
                ..
            }
        ),
        "a panic must wait in the fatal band, got {status:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A `Denied` runner error arrives as `Fatal(Denied)` (narrowed at the job
/// boundary, since job is not an authorization boundary) and is retried in
/// the fatal band from its first failure, like any other `Fatal`. The
/// span-assertion half of this (`job.fail_job`'s `error.lane`/`error.code`/
/// `retry.band`) lives in `tests/lanes_denied_span.rs`: `tracing`'s
/// per-callsite interest cache is process-wide, so a `set_default`
/// subscriber trick like `span_capture` is only reliable isolated in its
/// own test BINARY (a separate OS process), not merely its own test
/// function within this one.
#[tokio::test]
async fn denied_runner_error_retries_in_the_fatal_band() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-denied-retries").await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: short_fatal_backoff(),
        attempts: Arc::clone(&attempts),
        build_error: || Box::new(Denied::default()) as Box<dyn std::error::Error + Send + Sync>,
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let status = await_status(&jobs, id, "a retry at attempt 4", retried_at_least_thrice).await?;
    assert!(
        matches!(
            status,
            JobStatus::Pending {
                band: RetryBand::Fatal,
                ..
            }
        ),
        "Denied must wait in the fatal band, like Fatal, got {status:?}"
    );

    let seen = attempts.lock().await.clone();
    assert!(
        seen.starts_with(&[1, 2, 3]),
        "Denied must be retried, got {seen:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A resident job can never reach a terminal state (see
/// `ResidentJobCompletion`'s doc), so a `Fatal` runner error must be
/// rescheduled, not terminated: no failure ends any job by retrying.
/// `await_completion` times out -- the job never becomes terminal --
/// and the runner keeps being invoked.
#[tokio::test]
async fn resident_job_returning_fatal_is_rescheduled_not_terminated() -> anyhow::Result<()> {
    struct AlwaysFatalResidentInitializer {
        job_type: JobType,
        invocations: Arc<AtomicUsize>,
    }
    impl ResidentJobInitializer for AlwaysFatalResidentInitializer {
        type Config = Cfg;
        fn job_type(&self) -> JobType {
            self.job_type.clone()
        }
        fn retry_on_error_settings(&self) -> RetrySettings {
            short_fatal_backoff()
        }
        fn init(
            &self,
            _job: &Job,
        ) -> Result<Box<dyn ResidentJobRunner>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Box::new(AlwaysFatalResidentRunner {
                invocations: Arc::clone(&self.invocations),
            }))
        }
    }
    struct AlwaysFatalResidentRunner {
        invocations: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl ResidentJobRunner for AlwaysFatalResidentRunner {
        async fn run(
            &self,
            _current_job: CurrentJob,
        ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error + Send + Sync>> {
            self.invocations.fetch_add(1, Ordering::SeqCst);
            Err(Box::new(Fatal::new(FatalKind::Invariant)))
        }
    }

    let (mut jobs, pool, job_type) = start("lanes-resident-fatal-reschedules").await?;
    let invocations = Arc::new(AtomicUsize::new(0));
    let spawner = jobs.add_resident_initializer(AlwaysFatalResidentInitializer {
        job_type,
        invocations: Arc::clone(&invocations),
    });
    jobs.start_poll().await?;

    let handle = spawner.spawn(Cfg).await?;
    let id = handle.id();

    // Never terminal: await_completion must time out, not resolve.
    let timed_out = jobs
        .handle(id)
        .await_completion(Duration::from_millis(300))
        .await;
    assert!(
        timed_out.is_err(),
        "a resident job must never reach a terminal state, even after a Fatal error"
    );

    // The runner keeps being invoked (rescheduled), not abandoned.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if invocations.load(Ordering::SeqCst) >= 2 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("resident runner was not rescheduled after its Fatal error");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // And the row is still there, still pending/running -- never deleted.
    let still_pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM job_executions WHERE id = $1")
            .bind(uuid::Uuid::from(id))
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        still_pending, 1,
        "a resident job's execution row must never be deleted"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// Integration-level sibling of
/// `finalizer::run_failure_tests::classify_detects_congestion_from_a_bare_laned_transient_with_no_sqlx_source`:
/// a runner returning an already-laned `Transient::new(TransientKind::
/// PoolTimeout)` directly (no `sqlx::Error` anywhere in its chain -- the
/// shape any errlanes-based runner produces) must take the congestion-
/// reschedule path end to end, not just be labelled congestion by
/// `Fault::classify` in isolation: `attempt_index` stays unchanged
/// across the failure and the retry, and the job still completes.
#[tokio::test]
async fn laned_pool_timeout_with_no_sqlx_source_takes_the_congestion_path_end_to_end()
-> anyhow::Result<()> {
    struct CongestionOnceInitializer {
        job_type: JobType,
        attempts: Arc<Mutex<Vec<u32>>>,
        invocations: Arc<AtomicUsize>,
    }
    impl JobInitializer for CongestionOnceInitializer {
        type Config = Cfg;
        fn job_type(&self) -> JobType {
            self.job_type.clone()
        }
        fn retry_on_error_settings(&self) -> RetrySettings {
            // If congestion classification regressed and this went through
            // the ordinary retry policy instead, the job would burn an
            // attempt, which the `attempts` assertion below catches.
            short_fatal_backoff()
        }
        fn init(
            &self,
            _job: &Job,
            _: JobSpawner<Self::Config>,
        ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Box::new(CongestionOnceRunner {
                attempts: Arc::clone(&self.attempts),
                invocations: Arc::clone(&self.invocations),
            }))
        }
    }
    struct CongestionOnceRunner {
        attempts: Arc<Mutex<Vec<u32>>>,
        invocations: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl JobRunner for CongestionOnceRunner {
        async fn run(
            &self,
            current_job: CurrentJob,
        ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
            self.attempts.lock().await.push(current_job.attempt());
            if self.invocations.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(Box::new(Transient::new(TransientKind::PoolTimeout)));
            }
            Ok(JobCompletion::Complete)
        }
    }

    let (mut jobs, _pool, job_type) = start("lanes-congestion-end-to-end").await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let invocations = Arc::new(AtomicUsize::new(0));
    let spawner = jobs.add_initializer(CongestionOnceInitializer {
        job_type,
        attempts: Arc::clone(&attempts),
        invocations: Arc::clone(&invocations),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;

    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(20))
        .await?;
    assert_eq!(
        outcome.state(),
        JobTerminalState::Completed,
        "the job must complete after the congestion reschedule, not error out from a burned retry attempt"
    );

    let seen = attempts.lock().await.clone();
    assert_eq!(
        seen,
        vec![1, 1],
        "attempt_index must stay unchanged across the congestion reschedule, got {seen:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A `Fatal` failure is persisted as its own message with no wrapper, and
/// `JobStatus::Pending` carries it as `last_error`.
#[tokio::test]
async fn fatal_runner_error_is_persisted_as_its_own_message() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-fatal-persisted-message").await?;
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: short_fatal_backoff(),
        attempts: Arc::new(Mutex::new(Vec::new())),
        build_error: || {
            Box::new(Fatal::invariant("bad")) as Box<dyn std::error::Error + Send + Sync>
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let status = await_status(&jobs, id, "a retry at attempt 4", retried_at_least_thrice).await?;
    match status {
        JobStatus::Pending { last_error, .. } => {
            assert_eq!(last_error.as_deref(), Some("fatal(invariant): bad"));
        }
        other => panic!("expected Pending, got {other:?}"),
    }

    jobs.shutdown().await?;
    Ok(())
}
