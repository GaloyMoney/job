//! Span-assertion half of the transient band: a `Transient` runner error is
//! retried in the transient band, so `job.fail_job` records
//! `retry.band = "transient"`, `error.level = "WARN"` and `will_retry = true`.
//!
//! Its own file/binary for the same reason as `tests/lanes_denied_span.rs`:
//! `tracing`'s per-callsite interest cache is process-wide, so span capture is
//! only reliable isolated in its own test binary.

mod helpers;
mod span_capture;

use async_trait::async_trait;
use es_entity::errlanes::{Transient, TransientKind};
use job::{
    Backoff, CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner,
    JobSvcConfig, JobType, Jobs, RetrySettings,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

struct AlwaysTransientRunner;

#[async_trait]
impl JobRunner for AlwaysTransientRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        Err(Box::new(Transient::new(TransientKind::Other)))
    }
}

struct AlwaysTransientInitializer {
    job_type: JobType,
}

impl JobInitializer for AlwaysTransientInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings {
            backoff: Backoff::new(
                Duration::from_millis(5),
                Duration::from_secs(60),
                Duration::from_secs(120),
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
        Ok(Box::new(AlwaysTransientRunner))
    }
}

#[tokio::test]
async fn transient_runner_error_is_recorded_in_the_transient_band_at_warn() -> anyhow::Result<()> {
    let (fields, _guard) = span_capture::install();

    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(AlwaysTransientInitializer {
        job_type: helpers::job_type("lanes-transient-span"),
    });
    jobs.start_poll().await?;

    spawner.spawn(JobId::new(), Cfg).await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while fields.get("job.fail_job", "retry.band").is_none() {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "no job.fail_job span recorded retry.band; captured: {}",
            fields.debug_dump()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        fields.get("job.fail_job", "retry.band").as_deref(),
        Some("transient"),
        "captured: {}",
        fields.debug_dump()
    );
    assert_eq!(
        fields.get("job.fail_job", "error.lane").as_deref(),
        Some("transient")
    );
    assert_eq!(
        fields.get("job.fail_job", "error.level").as_deref(),
        Some("WARN"),
        "the transient band logs at WARN"
    );
    assert_eq!(
        fields.get("job.fail_job", "will_retry").as_deref(),
        Some("true")
    );
    assert_eq!(
        fields.get("job.fail_job", "retry.fatal_streak").as_deref(),
        Some("0")
    );

    jobs.shutdown().await?;
    Ok(())
}
