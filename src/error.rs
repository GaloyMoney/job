//! Error type returned by the job service and helpers.

use thiserror::Error;

use super::entity::JobType;
use super::repo::{JobCreateError, JobFindError, JobModifyError, JobQueryError};
use crate::JobId;

#[derive(Error, Debug)]
/// Exhaustive list of failures the job service can report.
pub enum JobError {
    #[error("JobError - Sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("JobError - Create: {0}")]
    Create(JobCreateError),
    #[error("JobError - Modify: {0}")]
    Modify(#[from] JobModifyError),
    #[error("JobError - Find: {0}")]
    Find(#[from] JobFindError),
    #[error("JobError - Query: {0}")]
    Query(#[from] JobQueryError),
    #[error("JobError - InvalidPollInterval: {0}")]
    InvalidPollInterval(String),
    #[error("JobError - InvalidJobType: expected '{0}' but initializer was '{1}'")]
    JobTypeMismatch(JobType, JobType),
    #[error("JobError - JobInitError: {0}")]
    JobInitError(String),
    #[error("JobError - BadState: {0}")]
    CouldNotSerializeExecutionState(serde_json::Error),
    #[error("JobError - BadState: {0}")]
    CouldNotDeserializeExecutionState(serde_json::Error),
    #[error("JobError - BadResult: {0}")]
    CouldNotSerializeResult(serde_json::Error),
    #[error("JobError - BadConfig: {0}")]
    CouldNotSerializeConfig(serde_json::Error),
    #[error("JobError - NoInitializerPresent")]
    NoInitializerPresent,
    #[error("JobError - JobExecutionError: {0}")]
    JobExecutionError(String),
    /// A batch runner error classified as pool congestion
    /// (`is_pool_congestion`) rather than a genuine failure -- distinct from
    /// [`Self::JobExecutionError`] so `BatchDispatcher::fail_batch` can route
    /// it to a reschedule that skips the retry policy's attempt escalation.
    /// See `BatchDispatcher::run_batch`'s `Ok(Err(e))` branch, the only
    /// place this is constructed.
    #[error("JobError - PoolCongestion: {0}")]
    PoolCongestion(String),
    #[error("JobError - BatchOutcomeMismatch: {0}")]
    BatchOutcomeMismatch(String),
    #[error("JobError - DuplicateId: {0:?}")]
    DuplicateId(Option<String>),
    /// Returned when a resident job type already has a live job (#170).
    #[error("JobError - DuplicateResident: {0:?}")]
    DuplicateResident(Option<String>),
    #[error("JobError - Config: {0}")]
    Config(String),
    #[error("JobError - Migration: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error(
        "JobError - AwaitCompletionShutdown: notification channel closed while awaiting job {0}"
    )]
    AwaitCompletionShutdown(JobId),
    #[error(
        "JobError - TimedOut: job {0} did not reach terminal state within the specified timeout"
    )]
    TimedOut(JobId),
    #[error("JobError - RouterNotStarted: await called before Jobs::start_poll")]
    RouterNotStarted,
    /// Practically unreachable; raised by
    /// [`KeyedJobSpawner::spawn`](crate::KeyedJobSpawner::spawn), which
    /// documents the race it exhausts.
    #[error(
        "JobError - KeyedSpawnRace: exhausted retries resolving a live-keyed conflict for job_type '{0}' key '{1}'"
    )]
    KeyedSpawnRace(JobType, String),
}

/// The SQLSTATE, if this error (or anything it wraps) is a Postgres abort that
/// is retryable by definition: `40P01` deadlock detected, `40001` serialization
/// failure. The victim did nothing wrong -- the server picked it to break a
/// cycle -- so the work is worth re-attempting rather than blaming on the job.
///
/// Walks the source chain rather than matching one variant: the same abort
/// surfaces as a bare [`sqlx::Error`] from raw statements, wrapped in a repo
/// error from es-entity's own writes, and wrapped again in whatever error type
/// a caller's closure returns.
pub(crate) fn retryable_conflict_code(
    err: &(dyn std::error::Error + 'static),
) -> Option<&'static str> {
    let mut source = Some(err);
    while let Some(e) = source {
        if let Some(db) = e
            .downcast_ref::<sqlx::Error>()
            .and_then(|e| e.as_database_error())
        {
            match db.code().as_deref() {
                Some("40P01") => return Some("40P01"),
                Some("40001") => return Some("40001"),
                _ => {}
            }
        }
        source = e.source();
    }
    None
}

/// [`retryable_conflict_code`] as a predicate.
pub(crate) fn is_retryable_conflict(err: &(dyn std::error::Error + 'static)) -> bool {
    retryable_conflict_code(err).is_some()
}

/// Whether this error (or anything it wraps) is `sqlx::Error::PoolTimedOut`
/// -- the shared pool had no connection to hand out within its acquire
/// timeout. This carries no evidence the job is broken: it says the pool was
/// busy, not that the work is wrong. Classified separately from a batch's
/// real failures so it can skip the retry-policy's attempt escalation (see
/// `BatchDispatcher::fail_batch`'s congestion branch) instead of walking a
/// perfectly good job toward `max_attempts` termination for congestion it
/// didn't cause.
///
/// Same source-chain walk as [`retryable_conflict_code`] and for the same
/// reason: a batched runner's error crosses an object-erasure boundary
/// (`BatchedJobRunner::run_batch` returns `Box<dyn std::error::Error>`)
/// before it reaches this crate's own error handling, so the check has to
/// happen on the *original* error there -- once it's been `.to_string()`'d
/// into `JobError::JobExecutionError`, the structure (and this function)
/// can no longer see it. See `BatchDispatcher::run_batch`'s `Ok(Err(e))`
/// branch, which is where this is actually called.
pub(crate) fn is_pool_congestion(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut source = Some(err);
    while let Some(e) = source {
        if let Some(sqlx::Error::PoolTimedOut) = e.downcast_ref::<sqlx::Error>() {
            return true;
        }
        source = e.source();
    }
    false
}

impl From<Box<dyn std::error::Error>> for JobError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        JobError::JobExecutionError(error.to_string())
    }
}

impl From<JobCreateError> for JobError {
    fn from(error: JobCreateError) -> Self {
        match error {
            JobCreateError::ConstraintViolation {
                column: Some(super::repo::JobColumn::Id),
                value,
                ..
            } => Self::DuplicateId(value),
            // `idx_jobs_job_type_resident` (the absolutely-unique
            // `ResidentJobSpawner::spawn` enforcement,
            // migrations/20250904065521_job_setup.sql) is a single-column
            // index on `job_type` — its partial predicate (`WHERE
            // resident`) isn't itself an indexed column, so es_entity
            // attributes the violation deterministically to `JobType`.
            JobCreateError::ConstraintViolation {
                column: Some(super::repo::JobColumn::JobType),
                value,
                ..
            } => Self::DuplicateResident(value),
            other => Self::Create(other),
        }
    }
}
