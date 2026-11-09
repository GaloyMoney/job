//! Retries back off in real time, whatever clock the service was configured
//! with: a failure retry (and a pool-congestion reschedule) comes due even
//! while a manual application clock is never advanced. The one switch,
//! `JobSvcConfig::retry_schedule = RetrySchedule::AppClock`, puts retry
//! deadlines back on the application clock for tests that drive it.
//!
//! The motivating shape is an infrastructure job — an outbox relay mirroring
//! events to object storage, say — running under a simulated application
//! clock that a demo seed has stopped advancing. An app-clock retry deadline
//! written past the freeze can never come due: the row wedges `pending`
//! until some unrelated clock advance happens to cross it, and everything
//! fenced on the job's progress stalls behind it.

mod helpers;

use async_trait::async_trait;
use es_entity::clock::ClockHandle;
use es_entity::errlanes::Fatal;
use job::{
    Backoff, BatchedJobInitializer, BatchedJobRunner, CurrentBatchedJob, CurrentJob, Job,
    JobBatchCompletion, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSpec,
    JobStatus, JobSvcConfig, JobTerminalState, JobType, Jobs, RetrySchedule, RetrySettings,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

const POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Small enough that the wall-clock retry fires within the test's patience,
/// large enough not to flap: 50ms doubling, cap 200ms, no jitter.
fn quick_backoff() -> RetrySettings {
    RetrySettings {
        backoff: Backoff::new(
            Duration::from_millis(50),
            Duration::from_millis(100),
            Duration::from_millis(200),
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
            Err(Box::new(Fatal::invariant("transient upstream failure")))
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
        quick_backoff()
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

/// A batched runner whose FIRST invocation fails with `PoolTimedOut` -- the
/// client-side error `JobFault::is_congestion` classifies as pool
/// congestion -- and whose every later invocation completes.
struct CongestionOnceRunner {
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl BatchedJobRunner for CongestionOnceRunner {
    type Config = Cfg;

    async fn run_batch(
        &self,
        _: CurrentBatchedJob<Cfg>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error + Send + Sync>> {
        if self.invocations.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(Box::new(sqlx::Error::PoolTimedOut));
        }
        Ok(JobBatchCompletion::CompleteAll)
    }
}

struct CongestionOnceInitializer {
    job_type: JobType,
    invocations: Arc<AtomicUsize>,
}

impl BatchedJobInitializer for CongestionOnceInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: JobSpawner<Self::Config>,
    ) -> Result<
        Box<dyn BatchedJobRunner<Config = Self::Config>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        Ok(Box::new(CongestionOnceRunner {
            invocations: Arc::clone(&self.invocations),
        }))
    }
}

async fn execution_row(
    pool: &sqlx::PgPool,
    id: JobId,
) -> anyhow::Result<Option<(i32, String, Option<chrono::DateTime<chrono::Utc>>, String)>> {
    Ok(sqlx::query_as(
        "SELECT attempt_index, state::text, execute_at, retry_clock::text \
         FROM job_executions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?)
}

/// Wait for `id` to be parked `pending` at `attempt`; returns its deadline
/// and the clock the row says that deadline is on.
async fn await_pending_at(
    pool: &sqlx::PgPool,
    id: JobId,
    attempt: i32,
) -> anyhow::Result<(chrono::DateTime<chrono::Utc>, String)> {
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    loop {
        if let Some((got, state, Some(execute_at), retry_clock)) = execution_row(pool, id).await?
            && got == attempt
            && state == "pending"
        {
            return Ok((execute_at, retry_clock));
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {id} to be pending at attempt {attempt}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn await_complete(jobs: &Jobs, id: JobId) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    loop {
        if matches!(
            jobs.handle(id).load().await?.state(),
            JobStatus::Completed { .. }
        ) {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {id} to complete"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The default: a job fails once under a manual clock that is never
/// advanced, and its retry still fires -- the retry deadline was written
/// against real time, and the poller's wall wait arm crosses it -- so the
/// deployed fix (flipping `fixed`) is picked up with no clock advance at all.
#[tokio::test]
async fn a_retry_fires_in_real_time_without_advancing_the_clock() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (clock, _controller) = ClockHandle::manual();
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .clock(clock.clone())
        .build()
        .unwrap();
    let mut jobs = Jobs::init(config).await?;
    let fixed = Arc::new(AtomicBool::new(false));
    let runs = Arc::new(AtomicUsize::new(0));
    let before = chrono::Utc::now();
    let spawner = jobs.add_initializer(FixableInitializer {
        job_type: helpers::job_type("retry-clock-realtime"),
        fixed: Arc::clone(&fixed),
        runs: Arc::clone(&runs),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;

    // First dispatch fails; the retry must be scheduled ~50ms out on the
    // WALL clock -- ahead of the (frozen) application clock, whose freeze is
    // the whole point -- and the row must say so.
    let (execute_at, retry_clock) = await_pending_at(&pool, id, 2).await?;
    assert_eq!(retry_clock, "wall");
    let wall_delta = chrono::Utc::now() - before;
    let app_delta = execute_at - clock.now();
    assert!(
        app_delta > chrono::Duration::zero(),
        "the retry deadline must sit ahead of the frozen application clock, got {app_delta}"
    );
    assert!(
        execute_at > before && execute_at < before + wall_delta + chrono::Duration::seconds(1),
        "the retry deadline must be a wall-clock timestamp: {execute_at} should sit between \
         the test start ({before}) and roughly now ({})",
        chrono::Utc::now()
    );

    // The hotfix flips the flag; with no clock advance at all, the retry
    // fires in real time and completes the job.
    fixed.store(true, Ordering::SeqCst);
    await_complete(&jobs, id).await?;
    assert!(runs.load(Ordering::SeqCst) >= 2);

    jobs.shutdown().await?;
    Ok(())
}

/// The switch: under `RetrySchedule::AppClock` the same failure stays parked
/// on the frozen clock -- the retry must NOT fire in real time -- and
/// advances only when the clock does. Pins both that the switch works and
/// that the wall arm does not claim app-domain deadlines.
#[tokio::test]
async fn an_app_clock_retry_waits_for_the_clock() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (clock, controller) = ClockHandle::manual();
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .clock(clock.clone())
        .retry_schedule(RetrySchedule::AppClock)
        .build()
        .unwrap();
    let mut jobs = Jobs::init(config).await?;
    let fixed = Arc::new(AtomicBool::new(false));
    let runs = Arc::new(AtomicUsize::new(0));
    let spawner = jobs.add_initializer(FixableInitializer {
        job_type: helpers::job_type("retry-clock-app"),
        fixed: Arc::clone(&fixed),
        runs: Arc::clone(&runs),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;
    let (execute_at, retry_clock) = await_pending_at(&pool, id, 2).await?;
    assert_eq!(retry_clock, "app");

    // Real time elapses far past the 50ms backoff; the retry must stay
    // parked because its deadline is on the application clock.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (_, state, _, _) = execution_row(&pool, id)
        .await?
        .expect("the row exists while its retry is parked");
    assert_eq!(state, "pending", "an app-clock retry must not fire early");
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    // Crossing the deadline on the application clock fires it as always.
    fixed.store(true, Ordering::SeqCst);
    let delay_ms = (execute_at - clock.now()).num_milliseconds().max(0) + 1;
    controller
        .advance(Duration::from_millis(delay_ms as u64))
        .await;
    await_complete(&jobs, id).await?;
    assert!(runs.load(Ordering::SeqCst) >= 2);

    jobs.shutdown().await?;
    Ok(())
}

/// Pool congestion is operational too: a batch rescheduled for
/// `PoolTimedOut` under a never-advanced manual clock comes due in real time
/// (the ~2s congestion delay) and completes, with no clock advance.
#[tokio::test]
async fn a_congestion_reschedule_fires_in_real_time_without_advancing_the_clock()
-> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (clock, _controller) = ClockHandle::manual();
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .clock(clock)
        .build()
        .unwrap();
    let mut jobs = Jobs::init(config).await?;
    let invocations = Arc::new(AtomicUsize::new(0));
    let spawner = jobs.add_batched_initializer(CongestionOnceInitializer {
        job_type: helpers::job_type("retry-clock-congestion"),
        invocations: Arc::clone(&invocations),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn_all(vec![JobSpec::new(id, Cfg)]).await?;

    // The congestion park keeps attempt 1 and is on the wall clock.
    let (_, retry_clock) = await_pending_at(&pool, id, 1).await?;
    assert_eq!(retry_clock, "wall");

    let outcomes = jobs.handles(vec![id]).await_all(POLL_TIMEOUT).await?;
    assert_eq!(outcomes[0].state(), JobTerminalState::Completed);
    assert_eq!(invocations.load(Ordering::SeqCst), 2);

    jobs.shutdown().await?;
    Ok(())
}
