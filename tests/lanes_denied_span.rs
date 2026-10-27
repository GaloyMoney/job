//! Span-assertion half of the `Denied` disposition coverage (see
//! `tests/lanes.rs::denied_runner_error_retries_in_the_fatal_band` for the
//! disposition half): job narrows a `Denied` runner error to `Fatal(Denied)`
//! at the boundary (job is not an authorization boundary -- see the
//! handoff's §2.4), so `job.fail_job`'s span must record
//! `error.lane = "fatal"` / `error.code = "denied"`, not `"denied"`/
//! `"FORBIDDEN"`, and -- being fatal -- `retry.band = "fatal"` at `ERROR`
//! with `will_retry = true` on its first failure.
//!
//! Deliberately its own file/binary, not a test function alongside the rest
//! of `tests/lanes.rs`. `tracing`'s per-callsite "interest" cache is
//! process-wide: the compiled `#[instrument]` span-creation site inside
//! `fail_job` is a single fixed callsite, and under `cargo test`'s default
//! parallel execution another concurrently-running test function's thread
//! (with no subscriber installed, between `set_default` scopes) can get
//! that callsite cached as "never interested" before this test's own
//! `span_capture::install()` subscriber ever sees it -- silently dropping
//! every span/event here despite the thread-local override being active.
//! Separate test binaries run as separate OS processes and so never share
//! that cache; `tracing::callsite::rebuild_interest_cache()` alone was not
//! enough to recover from an already-cross-contaminated cache reliably.

mod helpers;
mod span_capture;

use async_trait::async_trait;
use es_entity::errlanes::Denied;
use job::{
    Backoff, CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner,
    JobSvcConfig, JobType, Jobs, RetrySettings,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

struct AlwaysDeniedRunner {
    attempts: Arc<Mutex<Vec<u32>>>,
}

#[async_trait]
impl JobRunner for AlwaysDeniedRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        self.attempts.lock().await.push(current_job.attempt());
        Err(Box::new(Denied::default()))
    }
}

struct AlwaysDeniedInitializer {
    job_type: JobType,
    attempts: Arc<Mutex<Vec<u32>>>,
}

impl JobInitializer for AlwaysDeniedInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
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

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(AlwaysDeniedRunner {
            attempts: Arc::clone(&self.attempts),
        }))
    }
}

#[tokio::test]
async fn denied_runner_error_is_recorded_as_fatal_denied_on_fail_job_span() -> anyhow::Result<()> {
    let (fields, _guard) = span_capture::install();

    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let attempts = Arc::new(Mutex::new(Vec::new()));
    let spawner = jobs.add_initializer(AlwaysDeniedInitializer {
        job_type: helpers::job_type("lanes-denied-span"),
        attempts: Arc::clone(&attempts),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while fields.get("job.fail_job", "retry.band").is_none() {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "no job.fail_job span recorded retry.band; captured: {}",
            fields.debug_dump()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        attempts.lock().await.starts_with(&[1]),
        "the denied runner must have run"
    );

    assert_eq!(
        fields.get("job.fail_job", "retry.band").as_deref(),
        Some("fatal"),
        "a narrowed Denied must land in the fatal band on its first failure; captured: {}",
        fields.debug_dump()
    );
    assert_eq!(
        fields.get("job.fail_job", "will_retry").as_deref(),
        Some("true"),
        "an Err is always retried"
    );
    assert!(
        fields.get("job.fail_job", "retry.fatal_streak").is_some(),
        "the fatal streak must be recorded; captured: {}",
        fields.debug_dump()
    );
    assert_eq!(
        fields.get("job.fail_job", "error.lane").as_deref(),
        Some("fatal"),
        "job.fail_job span should record error.lane = fatal (a Denied is narrowed at \
         the job boundary); captured: {}",
        fields.debug_dump()
    );
    assert_eq!(
        fields.get("job.fail_job", "error.code").as_deref(),
        Some("denied"),
        "job.fail_job span should record error.code = denied"
    );
    assert_eq!(
        fields.get("job.fail_job", "error.level").as_deref(),
        Some("ERROR"),
        "job.fail_job span should record error.level = ERROR"
    );
    let message = fields
        .get("job.fail_job", "exception.message")
        .unwrap_or_default();
    assert!(
        message.starts_with("fatal(denied): denied"),
        "job.fail_job span's exception.message should start with the narrowed Denied's \
         Display; captured: {message:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}
