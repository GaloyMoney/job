//! Error type returned by the job service and helpers.

use thiserror::Error;

use super::entity::JobType;
use super::repo::{JobColumn, JobConstraintViolation};
use crate::JobId;

use es_entity::errlanes::{Fail, Fault};

#[derive(Error, Debug)]
/// Exhaustive list of failures the job service can report.
pub enum JobError {
    #[error("JobError - Sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// A repository write failure. Rejected violations are lifted to the
    /// duplicate variants below when they describe job-owned uniqueness.
    #[error("JobError - Repo: {0}")]
    Repo(Fail<JobConstraintViolation>),
    /// A repository read failure. Reads cannot produce a domain rejection.
    #[error("JobError - Read: {0}")]
    Read(#[from] Fault),
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
    /// A runner error classified as pool congestion rather than a genuine
    /// failure -- distinct from [`Self::JobExecutionError`] so the
    /// dispatchers' fail paths can route it to a reschedule that skips the
    /// retry policy's attempt escalation. Constructed only by
    /// `Finalizer::maybe_reclassify` (see `finalizer.rs`).
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
}

/// Total attempts a crate-owned bookkeeping transaction (batch seal / fail,
/// congestion reschedule) gets when Postgres keeps ABORTING it as a
/// deadlock victim or serialization failure --
/// transient aborts where the transaction lost to a concurrent partner and
/// is safe to simply re-run. Counted as attempts, not retries: `3` means
/// the original try plus two re-runs.
///
/// Small on purpose: these aborts are resolved by whichever partner
/// survives, so a re-attempt normally succeeds immediately. If three in a
/// row lose, something is wrong beyond ordinary contention and the work is
/// better off going through the rescue path than spinning here.
pub(crate) const TX_ABORT_MAX_ATTEMPTS: u32 = 3;

impl From<Box<dyn std::error::Error>> for JobError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        JobError::JobExecutionError(error.to_string())
    }
}

impl From<Fail<JobConstraintViolation>> for JobError {
    fn from(error: Fail<JobConstraintViolation>) -> Self {
        match &error {
            Fail::Rejected(cv) if cv.is_duplicate_of(JobColumn::Id) => {
                return Self::DuplicateId(cv.value().map(str::to_owned));
            }
            // `idx_jobs_job_type_resident` (the absolutely-unique
            // `ResidentJobSpawner::spawn` enforcement,
            // migrations/20250904065521_job_setup.sql) is a single-column
            // index on `job_type` — its partial predicate (`WHERE
            // resident`) isn't itself an indexed column, so es_entity
            // attributes the violation deterministically to `JobType`.
            Fail::Rejected(cv) if cv.is_duplicate_of(JobColumn::JobType) => {
                return Self::DuplicateResident(cv.value().map(str::to_owned));
            }
            _ => Self::Repo(error),
        }
    }
}
