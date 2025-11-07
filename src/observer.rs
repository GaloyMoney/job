//! Observer hooks for job execution attempts.

use chrono::{DateTime, Utc};

use crate::error::JobError;
use crate::{Job, JobCompletion, JobId, JobType};

/// Trait implemented by clients to observe job execution attempts.
pub trait JobObserver: Send + Sync + 'static {
    fn on_attempt_start(&self, _job: &Job, _attempt: u32, _poller_id: uuid::Uuid) {}
    fn on_attempt_success(&self, _job: &Job, _attempt: u32, _completion: &JobCompletion) {}
    fn on_attempt_failure(
        &self,
        _job: &Job,
        _attempt: u32,
        _error: &JobError,
        _retry_at: Option<DateTime<Utc>>,
        _will_retry: bool,
    ) {
    }
    fn on_lost_job(&self, _job_id: JobId, _job_type: &JobType, _attempt: u32) {}
}

/// No-op observer used when callers do not supply their own hooks.
#[derive(Default)]
pub struct NoopJobObserver;

impl std::fmt::Debug for NoopJobObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NoopJobObserver")
    }
}

impl JobObserver for NoopJobObserver {}
