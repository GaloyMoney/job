//! Error type returned by the job service and helpers.

use thiserror::Error;

use super::entity::JobType;
use super::repo::{JobCreateError, JobFindError, JobModifyError, JobQueryError};

#[derive(Error, Debug)]
/// Exhaustive list of failures the job service can report.
pub enum JobError {
    #[error("JobError - Sqlx: {0}")]
    Sqlx(sqlx::Error),
    #[error("JobError - EntityHydrationError: {0}")]
    EntityHydrationError(es_entity::EntityHydrationError),
    #[error("JobError - CursorDestructureError: {0}")]
    CursorDestructureError(#[from] es_entity::CursorDestructureError),
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
    #[error("JobError - DuplicateId")]
    DuplicateId,
    #[error("JobError - DuplicateUniqueJobType")]
    DuplicateUniqueJobType,
    #[error("JobError - Config: {0}")]
    Config(String),
    #[error("JobError - Migration: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
}

impl From<Box<dyn std::error::Error>> for JobError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        JobError::JobExecutionError(error.to_string())
    }
}

impl From<sqlx::Error> for JobError {
    fn from(error: sqlx::Error) -> Self {
        Self::Sqlx(error)
    }
}

impl From<JobCreateError> for JobError {
    fn from(error: JobCreateError) -> Self {
        match error {
            JobCreateError::ConstraintViolation { inner, .. } => {
                if let Some(err) = inner.as_database_error() {
                    if let Some(constraint) = err.constraint() {
                        if constraint.contains("type") {
                            return Self::DuplicateUniqueJobType;
                        }
                        if constraint.contains("jobs_pkey") {
                            return Self::DuplicateId;
                        }
                    }
                }
                Self::Sqlx(inner)
            }
            JobCreateError::Sqlx(e) | JobCreateError::PostPersistHookError(e) => Self::Sqlx(e),
            JobCreateError::HydrationError(e) => Self::EntityHydrationError(e),
            JobCreateError::ConcurrentModification => {
                Self::Sqlx(sqlx::Error::Protocol("ConcurrentModification".into()))
            }
        }
    }
}

impl From<JobModifyError> for JobError {
    fn from(error: JobModifyError) -> Self {
        match error {
            JobModifyError::Sqlx(e)
            | JobModifyError::ConstraintViolation { inner: e, .. }
            | JobModifyError::PostPersistHookError(e) => Self::Sqlx(e),
            JobModifyError::ConcurrentModification => {
                Self::Sqlx(sqlx::Error::Protocol("ConcurrentModification".into()))
            }
        }
    }
}

impl From<JobFindError> for JobError {
    fn from(error: JobFindError) -> Self {
        match error {
            JobFindError::Sqlx(e) => Self::Sqlx(e),
            JobFindError::NotFound { .. } => Self::Sqlx(sqlx::Error::RowNotFound),
            JobFindError::HydrationError(e) => Self::EntityHydrationError(e),
        }
    }
}

impl From<JobQueryError> for JobError {
    fn from(error: JobQueryError) -> Self {
        match error {
            JobQueryError::Sqlx(e) => Self::Sqlx(e),
            JobQueryError::HydrationError(e) => Self::EntityHydrationError(e),
            JobQueryError::CursorDestructureError(e) => Self::CursorDestructureError(e),
        }
    }
}
