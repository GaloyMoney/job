//! [`OwnedTaskHandle`] — an RAII wrapper around a background [`tokio`] task
//! that aborts the task when dropped, so a task's lifetime is tied to the
//! owner that spawned it (the poller and the notification router).

use tokio::task::JoinHandle;

/// Owns a spawned [`tokio`] task and aborts it on drop.
pub(crate) struct OwnedTaskHandle(Option<JoinHandle<()>>);

impl OwnedTaskHandle {
    pub fn new(inner: tokio::task::JoinHandle<()>) -> Self {
        Self(Some(inner))
    }

    #[allow(dead_code)]
    pub async fn stop(self) {
        let handle = self.into_inner();
        handle.abort();
        let _ = handle.await;
    }

    #[allow(dead_code)]
    fn into_inner(mut self) -> JoinHandle<()> {
        self.0.take().expect("Only consumed once")
    }
}

impl Drop for OwnedTaskHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}
