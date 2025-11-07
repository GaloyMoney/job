//! Observer hooks for job execution attempts.

use chrono::{DateTime, Utc};

use std::time::Duration;

use crate::{JobId, JobType};

/// Context about a single job attempt, provided to observers.
#[derive(Debug, Clone)]
pub struct JobAttemptContext {
    pub job_id: JobId,
    pub job_type: JobType,
    pub attempt: u32,
    pub poller_instance_id: uuid::Uuid,
    pub started_at: DateTime<Utc>,
}

/// Outcome of an attempt, reported after the runner finishes.
#[derive(Debug, Clone)]
pub enum JobAttemptOutcome {
    Completed,
    Rescheduled(JobReschedule),
    Failed {
        error: String,
        retry_at: Option<DateTime<Utc>>,
        will_retry: bool,
    },
}

/// Kinds of reschedules a job can request.
#[derive(Debug, Clone)]
pub enum JobReschedule {
    Immediate,
    In(Duration),
    At(DateTime<Utc>),
}

/// Context about a job the poller marked as lost and rescheduled.
#[derive(Debug, Clone)]
pub struct LostJobContext {
    pub job_id: JobId,
    pub job_type: JobType,
    pub attempt: u32,
    pub detected_at: DateTime<Utc>,
}

/// Trait implemented by clients to observe job execution attempts.
pub trait JobObserver: Send + Sync + 'static {
    fn on_attempt_start(&self, _ctx: &JobAttemptContext) {}
    fn on_attempt_outcome(&self, _ctx: &JobAttemptContext, _outcome: &JobAttemptOutcome) {}
    fn on_lost_job(&self, _lost: &LostJobContext) {}
}

/// No-op observer used when callers do not supply their own hooks.
#[derive(Debug, Default)]
pub struct NoopJobObserver;

impl JobObserver for NoopJobObserver {}
