//! Registry of currently running jobs and their cancellation tokens.

use dashmap::DashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::JobId;

/// Tracks running jobs and provides cooperative cancellation via [`CancellationToken`].
///
/// Each running job registers a token when dispatched. Cross-node cancel
/// signals (NOTIFY or keep-alive fallback) look up the token here and
/// trigger cancellation.
#[derive(Clone)]
pub(crate) struct RunningJobRegistry {
    inner: Arc<DashMap<JobId, CancellationToken>>,
}

impl RunningJobRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// Register a new running job and return its cancellation token.
    pub fn register(&self, id: JobId) -> CancellationToken {
        let token = CancellationToken::new();
        self.inner.insert(id, token.clone());
        token
    }

    /// Signal cancellation for a running job. Returns `true` if the job was found.
    pub fn cancel(&self, id: &JobId) -> bool {
        if let Some(entry) = self.inner.get(id) {
            entry.cancel();
            true
        } else {
            false
        }
    }

    /// Remove a job from the registry (called on completion/failure/abort).
    pub fn remove(&self, id: &JobId) {
        self.inner.remove(id);
    }
}
