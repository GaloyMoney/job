//! Traits and types used when defining job logic.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::{
    current::CurrentJob,
    entity::{Job, JobType, RetryPolicy},
};

/// Describes how to construct a [`JobRunner`] for a given job type.
pub trait JobInitializer: Send + Sync + 'static {
    /// Unique identifier for the job type.
    fn job_type() -> JobType
    where
        Self: Sized;

    /// Retry settings to use when the runner returns an error.
    fn retry_on_error_settings() -> RetrySettings
    where
        Self: Sized,
    {
        Default::default()
    }

    /// Produce a runner instance for the provided job.
    fn init(&self, job: &Job) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>>;
}

/// Associate a typed config payload with a [`JobInitializer`].
pub trait JobConfig: serde::Serialize {
    type Initializer: JobInitializer;
}

/// Result returned by [`JobRunner::run`] describing how to progress the job.
pub enum JobCompletion {
    /// Job finished successfully; mark the record as completed.
    Complete,
    #[cfg(feature = "es-entity")]
    /// Job finished and returns an `EsEntity` operation that the job service will commit.
    CompleteWithOp(es_entity::DbOp<'static>),
    /// Job finished and returns a transaction that the job service will commit.
    CompleteWithTx(sqlx::Transaction<'static, sqlx::Postgres>),
    /// Schedule a new run immediately.
    RescheduleNow,
    #[cfg(feature = "es-entity")]
    /// Schedule a new run immediately and return an `EsEntity` operation that the job service will commit.
    RescheduleNowWithOp(es_entity::DbOp<'static>),
    /// Schedule a new run immediately and return a transaction that the job service will commit.
    RescheduleNowWithTx(sqlx::Transaction<'static, sqlx::Postgres>),
    /// Schedule the next run after a delay.
    RescheduleIn(std::time::Duration),
    #[cfg(feature = "es-entity")]
    /// Schedule the next run after a delay and return an `EsEntity` operation that the job service will commit.
    RescheduleInWithOp(std::time::Duration, es_entity::DbOp<'static>),
    /// Schedule the next run after a delay and return a transaction that the job service will commit.
    RescheduleInWithTx(
        std::time::Duration,
        sqlx::Transaction<'static, sqlx::Postgres>,
    ),
    /// Schedule the next run at an exact timestamp.
    RescheduleAt(DateTime<Utc>),
    #[cfg(feature = "es-entity")]
    /// Schedule the next run at an exact timestamp and return an `EsEntity` operation that the job service will commit.
    RescheduleAtWithOp(DateTime<Utc>, es_entity::DbOp<'static>),
    /// Schedule the next run at an exact timestamp and return a transaction that the job service will commit.
    RescheduleAtWithTx(DateTime<Utc>, sqlx::Transaction<'static, sqlx::Postgres>),
}

#[async_trait]
/// Implemented by job executors that perform the actual work.
pub trait JobRunner: Send + Sync + 'static {
    /// Execute the job and return how it should be completed or retried.
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>>;
}

#[derive(Debug, Clone)]
/// Controls retry attempt limits, telemetry escalation thresholds, and exponential backoff behaviour.
/// Use [`RetrySettings::n_warn_attempts`] to decide how many failures remain `WARN` events before
/// escalation. Set it to `None` to keep every retry at `WARN`.
pub struct RetrySettings {
    /// Maximum number of consecutive attempts before the job is failed for good. `None` retries
    /// indefinitely.
    pub n_attempts: Option<u32>,
    /// Number of consecutive failures that can be emitted as `WARN` telemetry before the crate
    /// promotes subsequent failures to `ERROR`. `None` disables escalation and keeps every retry
    /// at `WARN`.
    pub n_warn_attempts: Option<u32>,
    /// Smallest backoff duration when rescheduling failures. Acts as the base for exponential
    /// backoff growth.
    pub min_backoff: std::time::Duration,
    /// Maximum backoff duration. Once the exponentially increasing delay reaches this value it will
    /// stop growing.
    pub max_backoff: std::time::Duration,
    /// Percentage (0-100) jitter applied to the computed backoff window to avoid thundering herds.
    pub backoff_jitter_pct: u8,
    /// Multiplier applied to the previous backoff window. Once the elapsed time since the last
    /// scheduled run exceeds `previous_backoff * attempt_reset_after_backoff_multiples`, the job is
    /// treated as healthy again and the attempt counter resets to `1`.
    pub attempt_reset_after_backoff_multiples: u32,
}

impl RetrySettings {
    pub fn repeat_indefinitely() -> Self {
        Self {
            n_attempts: None,
            ..Default::default()
        }
    }
}

impl Default for RetrySettings {
    fn default() -> Self {
        const SECS_IN_ONE_HOUR: u64 = 60 * 60;
        Self {
            n_attempts: Some(30),
            n_warn_attempts: Some(3),
            min_backoff: std::time::Duration::from_secs(1),
            max_backoff: std::time::Duration::from_secs(SECS_IN_ONE_HOUR),
            backoff_jitter_pct: 20,
            attempt_reset_after_backoff_multiples: 3,
        }
    }
}

impl From<&RetrySettings> for RetryPolicy {
    fn from(settings: &RetrySettings) -> Self {
        Self {
            max_attempts: settings.n_attempts,
            min_backoff: settings.min_backoff,
            max_backoff: settings.max_backoff,
            backoff_jitter_pct: settings.backoff_jitter_pct,
            attempt_reset_after_backoff_multiples: settings.attempt_reset_after_backoff_multiples,
        }
    }
}
