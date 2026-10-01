//! Span-assertion half of the `Denied` disposition coverage (see
//! `tests/lanes.rs::denied_runner_error_goes_terminal_on_attempt_one` for
//! the disposition half): `job.fail_job`'s span must record `error.lane =
//! "denied"` / `error.code = "FORBIDDEN"` on a `Denied` runner error.
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
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSvcConfig,
    JobTerminalState, JobType, Jobs, RetrySettings,
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
            n_attempts: Some(30),
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
        Ok(Box::new(AlwaysDeniedRunner {
            attempts: Arc::clone(&self.attempts),
        }))
    }
}

#[tokio::test]
async fn denied_runner_error_records_error_lane_denied_on_fail_job_span() -> anyhow::Result<()> {
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
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);
    assert_eq!(attempts.lock().await.clone(), vec![1]);

    assert_eq!(
        fields.get("job.fail_job", "error.lane").as_deref(),
        Some("denied"),
        "job.fail_job span should record error.lane = denied; captured: {}",
        fields.debug_dump()
    );
    assert_eq!(
        fields.get("job.fail_job", "error.code").as_deref(),
        Some("FORBIDDEN"),
        "job.fail_job span should record error.code = FORBIDDEN"
    );

    jobs.shutdown().await?;
    Ok(())
}
