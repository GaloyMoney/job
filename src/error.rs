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
    #[error("JobError - BadConfig: {0}")]
    CouldNotSerializeConfig(serde_json::Error),
    #[error("JobError - NoInitializerPresent")]
    NoInitializerPresent,
    #[error("JobError - JobExecutionError: {0}")]
    JobExecutionError(String),
    #[error("JobError - DuplicateId: {0:?}")]
    DuplicateId(Option<String>),
    #[error("JobError - DuplicateUniqueJobType: {0:?}")]
    DuplicateUniqueJobType(Option<String>),
    #[error(
        "JobError - CannotCancelJob: job is not in pending state (may be running or already completed)"
    )]
    CannotCancelJob,
    #[error("JobError - Config: {0}")]
    Config(String),
    #[error("JobError - Migration: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error(
        "JobError - AwaitCompletionShutdown: notification channel closed while awaiting job {0}"
    )]
    AwaitCompletionShutdown(JobId),
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
            JobCreateError::ConstraintViolation {
                column: Some(super::repo::JobColumn::JobType),
                value,
                ..
            } => Self::DuplicateUniqueJobType(value),
            other => Self::Create(other),
        }
    }
}
