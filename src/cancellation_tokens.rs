//! Shared store of per-job cancellation tokens.

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

use crate::JobId;

/// Thread-safe store mapping running job IDs to their cancellation tokens.
///
/// Tokens are inserted when a job is dispatched and removed when it completes
/// (or is cancelled). The notification router calls [`cancel`] when a
/// `job_cancel` event arrives; the poller sweep also calls it as a safety net.
pub(crate) struct CancellationTokens {
    tokens: DashMap<JobId, CancellationToken>,
}

impl CancellationTokens {
    pub fn new() -> Self {
        Self {
            tokens: DashMap::new(),
        }
    }

    /// Insert a new token for `job_id` and return a clone the runner can observe.
    pub fn insert(&self, job_id: JobId) -> CancellationToken {
        let token = CancellationToken::new();
        self.tokens.insert(job_id, token.clone());
        token
    }

    /// Remove the token without cancelling it (used on normal completion).
    pub fn remove(&self, job_id: &JobId) {
        self.tokens.remove(job_id);
    }

    /// Cancel the token for `job_id`, signalling the running job to stop.
    pub fn cancel(&self, job_id: &JobId) {
        if let Some((_, token)) = self.tokens.remove(job_id) {
            token.cancel();
        }
    }
}
