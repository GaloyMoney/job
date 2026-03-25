//! Error type returned by the job service and helpers.

use thiserror::Error;

use super::entity::JobType;

#[derive(Error, Debug)]
/// Exhaustive list of failures the job service can report.
pub enum JobError {
    #[error("JobError - Sqlx: {0}")]
    Sqlx(sqlx::Error),
    #[error("JobError - EntityHydrationError: {0}")]
    EntityHydrationError(#[from] es_entity::EntityHydrationError),
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
        if let Some(err) = error.as_database_error()
            && let Some(constraint) = err.constraint()
        {
            if constraint.contains("type") {
                return Self::DuplicateUniqueJobType;
            }
            if constraint.contains("jobs_pkey") {
                return Self::DuplicateId;
            }
        }
        Self::Sqlx(error)
    }
}

impl From<super::repo::JobCreateError> for JobError {
    fn from(error: super::repo::JobCreateError) -> Self {
        use super::repo::{JobColumn, JobCreateError};
        match error {
            JobCreateError::ConstraintViolation { column, inner, .. } => match column {
                Some(JobColumn::UniquePerType) => Self::DuplicateUniqueJobType,
                Some(JobColumn::Id) => Self::DuplicateId,
                _ => Self::Sqlx(inner),
            },
            JobCreateError::Sqlx(e) => Self::Sqlx(e),
            JobCreateError::HydrationError(e) => Self::EntityHydrationError(e),
            JobCreateError::ConcurrentModification => {
                Self::Sqlx(sqlx::Error::Protocol("concurrent modification".into()))
            }
        }
    }
}

impl From<super::repo::JobFindError> for JobError {
    fn from(error: super::repo::JobFindError) -> Self {
        use super::repo::JobFindError;
        match error {
            JobFindError::Sqlx(e) => Self::Sqlx(e),
            JobFindError::HydrationError(e) => Self::EntityHydrationError(e),
            JobFindError::NotFound { .. } => Self::Sqlx(sqlx::Error::RowNotFound),
        }
    }
}

impl From<super::repo::JobModifyError> for JobError {
    fn from(error: super::repo::JobModifyError) -> Self {
        use super::repo::JobModifyError;
        match error {
            JobModifyError::Sqlx(e) => Self::Sqlx(e),
            JobModifyError::ConstraintViolation { inner, .. } => Self::Sqlx(inner),
            JobModifyError::ConcurrentModification => {
                Self::Sqlx(sqlx::Error::Protocol("concurrent modification".into()))
            }
        }
    }
}

impl From<super::repo::JobQueryError> for JobError {
    fn from(error: super::repo::JobQueryError) -> Self {
        use super::repo::JobQueryError;
        match error {
            JobQueryError::Sqlx(e) => Self::Sqlx(e),
            JobQueryError::HydrationError(e) => Self::EntityHydrationError(e),
            JobQueryError::CursorDestructureError(e) => Self::CursorDestructureError(e),
        }
    }
}
