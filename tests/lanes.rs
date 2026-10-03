//! Live-PG coverage for the lane-driven disposition matrix the errlanes
//! full-adoption refactor introduced (`finalizer.rs`'s runner-failure classification,
//! `JobDispatcher::fail_job`, `Job::maybe_schedule_retry`'s `terminal`
//! parameter): what a runner's classified error does to the job, per
//! `job-dev/handoff-errlanes-full-adoption.md` §3.7.
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
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobStatus,
    JobSvcConfig, JobTerminalState, JobType, Jobs, ResidentJobCompletion, ResidentJobInitializer,
    ResidentJobRunner, RetrySettings,
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

/// A `Transient` runner error (not `PoolTimeout`/`Congestion`, which take
/// the congestion-reschedule path instead) goes through the ordinary
/// `RetryPolicy`: each failure spends an attempt, `attempt_index` climbs by
/// one per retry.
#[tokio::test]
async fn transient_runner_error_retries_with_attempt_plus_one() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-transient-retries").await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: RetrySettings {
            n_attempts: Some(3),
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            ..Default::default()
        },
        attempts: Arc::clone(&attempts),
        build_error: || {
            Box::new(Transient::new(TransientKind::Other))
                as Box<dyn std::error::Error + Send + Sync>
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    let seen = attempts.lock().await.clone();
    assert_eq!(
        seen,
        vec![1, 2, 3],
        "a transient failure must spend an ordinary retry attempt each time, got {seen:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A `Fatal` runner error takes the ordinary attempt-count retry path by
/// default: `terminal_on_fatal` is `false`, so errlanes' claim that this
/// will not succeed on retry is *reported* on the span but not acted on.
/// Until there is live experience with how faithfully upstream crates lane
/// their errors, a `Fatal` that is really transient must not turn a blip
/// into a dead job.
#[tokio::test]
async fn fatal_runner_error_retries_by_default() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-fatal-retries").await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: RetrySettings {
            n_attempts: Some(2),
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            ..Default::default()
        },
        attempts: Arc::clone(&attempts),
        build_error: || {
            Box::new(Fatal::new(FatalKind::Invariant)) as Box<dyn std::error::Error + Send + Sync>
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    let seen = attempts.lock().await.clone();
    assert_eq!(
        seen,
        vec![1, 2],
        "a Fatal error must take the ordinary retry budget while `terminal_on_fatal` \
         is off, got {seen:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// The same always-`Fatal` runner with `terminal_on_fatal: true`: the type
/// opts into trusting the lane, and the job ends on the attempt that
/// produced it rather than spending its retry budget.
#[tokio::test]
async fn fatal_runner_error_goes_terminal_with_terminal_on_fatal() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-fatal-terminal").await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: RetrySettings {
            n_attempts: Some(30),
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            terminal_on_fatal: true,
            ..Default::default()
        },
        attempts: Arc::clone(&attempts),
        build_error: || {
            Box::new(Fatal::new(FatalKind::Invariant)) as Box<dyn std::error::Error + Send + Sync>
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    let seen = attempts.lock().await.clone();
    assert_eq!(
        seen,
        vec![1],
        "terminal_on_fatal: true must end the job on the attempt that produced the \
         Fatal, not burn the retry budget, got {seen:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A plain, unlaned string error (no errlanes payload anywhere in its
/// chain) gets `Fault::classify`'s `Fatal(Dependency)` default -- reported as
/// fatal on the span, so it is visible -- and still takes an ordinary
/// attempt-count retry, because `terminal_on_fatal` is off. Reporting the
/// lane and acting on it are separate.
#[tokio::test]
async fn unclassified_string_error_retries_per_policy() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-unclassified-retries").await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: RetrySettings {
            n_attempts: Some(3),
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            ..Default::default()
        },
        attempts: Arc::clone(&attempts),
        build_error: || "plain unlaned failure".into(),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    let seen = attempts.lock().await.clone();
    assert_eq!(
        seen,
        vec![1, 2, 3],
        "an unclassified error must retry on the ordinary attempt budget, got {seen:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A panic inside `run` is classified as `Fatal(FatalKind::Panic)` (see
/// `JobDispatcher::dispatch_job`), so the span says `error.lane = fatal`,
/// `error.code = panic` straight away -- but like any other `Fatal` it is
/// gated by `terminal_on_fatal`, which is off here. So the job spends its
/// retry budget and errors only when that runs out.
#[tokio::test]
async fn panicking_runner_errors_after_spending_its_retry_budget() -> anyhow::Result<()> {
    struct PanicInitializer {
        job_type: JobType,
    }
    impl JobInitializer for PanicInitializer {
        type Config = Cfg;
        fn job_type(&self) -> JobType {
            self.job_type.clone()
        }
        // Small budget with a short backoff: the panic is retried now, so
        // the default 30 attempts at a 1s floor would outlast the timeout
        // below. Two attempts is enough to show it retried AND errored.
        fn retry_on_error_settings(&self) -> RetrySettings {
            RetrySettings {
                n_attempts: Some(2),
                min_backoff: Duration::from_millis(5),
                max_backoff: Duration::from_millis(20),
                ..Default::default()
            }
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
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    jobs.shutdown().await?;
    Ok(())
}

/// A `Denied` runner error arrives as `Fatal(Denied)` (narrowed at the job
/// boundary, since job is not an authorization boundary) and takes the
/// ordinary retry path by default, same as any other `Fatal`:
/// `terminal_on_fatal` gates it. The span-assertion half of this
/// (`job.fail_job`'s `error.lane`/
/// `error.code`) lives in `tests/lanes_denied_span.rs`: `tracing`'s
/// per-callsite interest cache is process-wide, so a `set_default`
/// subscriber trick like `span_capture` is only reliable isolated in its
/// own test BINARY (a separate OS process), not merely its own test
/// function within this one.
#[tokio::test]
async fn denied_runner_error_retries_by_default() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-denied-retries").await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: RetrySettings {
            n_attempts: Some(2),
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            ..Default::default()
        },
        attempts: Arc::clone(&attempts),
        build_error: || Box::new(Denied::default()) as Box<dyn std::error::Error + Send + Sync>,
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    let seen = attempts.lock().await.clone();
    assert_eq!(
        seen,
        vec![1, 2],
        "Denied must take the ordinary retry budget while `terminal_on_fatal` is \
         off, like Fatal, got {seen:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// The same always-`Denied` runner with `terminal_on_fatal: true`: the
/// job ends on the attempt that produced it, exactly as for any other
/// `Fatal` (the narrowed `Denied` IS a `Fatal` by the time this gates it).
#[tokio::test]
async fn denied_runner_error_goes_terminal_with_terminal_on_fatal() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-denied-terminal").await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: RetrySettings {
            n_attempts: Some(30),
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            terminal_on_fatal: true,
            ..Default::default()
        },
        attempts: Arc::clone(&attempts),
        build_error: || Box::new(Denied::default()) as Box<dyn std::error::Error + Send + Sync>,
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    let seen = attempts.lock().await.clone();
    assert_eq!(
        seen,
        vec![1],
        "terminal_on_fatal: true must end the job on the attempt that produced the \
         Denied, got {seen:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A resident job can never reach a terminal state (see
/// `ResidentJobCompletion`'s doc), so a `Fatal` runner error must be
/// rescheduled, not terminated: `JobRegistry::add_resident_initializer`
/// forces `terminal_on_fatal: false` alongside `n_attempts: None` for exactly this
/// reason. `await_completion` times out -- the job never becomes terminal --
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
            RetrySettings {
                min_backoff: Duration::from_millis(5),
                max_backoff: Duration::from_millis(20),
                ..Default::default()
            }
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
            // A small cap: if congestion classification regressed and this
            // went through the ordinary retry policy instead, the job
            // would burn an attempt and this makes that failure mode
            // terminate (and be caught by the assertion below) instead of
            // quietly retrying its way to success.
            RetrySettings {
                n_attempts: Some(1),
                min_backoff: Duration::from_millis(5),
                max_backoff: Duration::from_millis(20),
                ..Default::default()
            }
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

/// On exhaustion the entity narrows the failure itself (`Job::
/// maybe_schedule_retry` -> `narrow_transient`) before persisting it: the
/// stored error says the job was retried to exhaustion, not just that it
/// last saw one more ordinary transient like every attempt before it.
#[tokio::test]
async fn exhausted_transient_is_persisted_as_fatal_exhausted() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-exhausted-transient").await?;
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: RetrySettings {
            n_attempts: Some(2),
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            ..Default::default()
        },
        attempts: Arc::new(Mutex::new(Vec::new())),
        build_error: || {
            Box::new(Transient::new(TransientKind::Other))
                as Box<dyn std::error::Error + Send + Sync>
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    match jobs.handle(id).load().await?.state() {
        JobStatus::Errored { error, .. } => {
            assert!(
                error.starts_with("fatal(exhausted): exhausted after 2 attempts: transient(other)"),
                "expected the exhausted wrapper naming the attempt count and the last \
                 transient, got {error:?}"
            );
        }
        other => panic!("expected Errored, got {other:?}"),
    }

    jobs.shutdown().await?;
    Ok(())
}

/// A `Fatal` that ends the job on its own attempt (`terminal_on_fatal:
/// true`) is persisted as its own message with no wrapper: `narrow_transient`
/// is only ever applied on the attempt-count exhaustion path, never here.
#[tokio::test]
async fn fatal_runner_error_is_persisted_as_its_own_message() -> anyhow::Result<()> {
    let (mut jobs, _pool, job_type) = start("lanes-fatal-persisted-message").await?;
    let spawner = jobs.add_initializer(AlwaysFailsInitializer {
        job_type,
        retry: RetrySettings {
            n_attempts: Some(30),
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            terminal_on_fatal: true,
            ..Default::default()
        },
        attempts: Arc::new(Mutex::new(Vec::new())),
        build_error: || {
            Box::new(Fatal::invariant("bad")) as Box<dyn std::error::Error + Send + Sync>
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    match jobs.handle(id).load().await?.state() {
        JobStatus::Errored { error, .. } => {
            assert_eq!(error, "fatal(invariant): bad");
        }
        other => panic!("expected Errored, got {other:?}"),
    }

    jobs.shutdown().await?;
    Ok(())
}
