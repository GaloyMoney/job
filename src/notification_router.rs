use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::{PgListener, PgPool};
use tokio::sync::{Mutex, oneshot};

use crate::JobId;
use crate::entity::JobType;
use crate::handle::OwnedTaskHandle;
use crate::tracker::JobTracker;

/// Notification types from the unified `job_events` channel.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum JobNotification {
    ExecutionReady { job_type: String },
    JobTerminal { job_id: String },
}

pub(crate) struct JobNotificationRouter {
    waiters: Arc<Mutex<HashMap<JobId, oneshot::Sender<()>>>>,
}

impl JobNotificationRouter {
    pub fn new() -> Self {
        Self {
            waiters: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register interest in a job reaching terminal state.
    /// Returns a receiver that will fire when the job completes or is cancelled.
    #[allow(dead_code)]
    pub async fn register_waiter(&self, job_id: JobId) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().await.insert(job_id, tx);
        rx
    }

    /// Remove a waiter (e.g. if caller cancels before notification).
    #[allow(dead_code)]
    pub async fn deregister_waiter(&self, job_id: &JobId) {
        self.waiters.lock().await.remove(job_id);
    }

    /// Start the central listener task. Returns an OwnedTaskHandle that
    /// aborts the listener when dropped.
    pub async fn start(
        &self,
        pool: &PgPool,
        tracker: Arc<JobTracker>,
        job_types: Vec<JobType>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let mut listener = PgListener::connect_with(pool).await?;
        listener.listen("job_events").await?;

        let waiters = Arc::clone(&self.waiters);

        let handle = tokio::spawn(async move {
            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        let payload = notification.payload();
                        match serde_json::from_str::<JobNotification>(payload) {
                            Ok(JobNotification::ExecutionReady { job_type }) => {
                                if job_types.iter().any(|jt| jt.as_str() == job_type) {
                                    tracker.job_execution_inserted();
                                }
                            }
                            Ok(JobNotification::JobTerminal { job_id }) => {
                                if let Ok(uuid) = job_id.parse::<uuid::Uuid>() {
                                    let id = JobId::from(uuid);
                                    if let Some(tx) = waiters.lock().await.remove(&id) {
                                        let _ = tx.send(());
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    payload,
                                    "malformed job notification"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "job notification listener error");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        Ok(OwnedTaskHandle::new(handle))
    }

    /// Start a periodic sweep that checks all registered waiters against the DB.
    /// This is a safety net for notifications lost during PgListener reconnection.
    pub fn start_sweep(&self, pool: PgPool) -> OwnedTaskHandle {
        let waiters = Arc::clone(&self.waiters);

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;

                let waiter_ids: Vec<JobId> = {
                    let guard = waiters.lock().await;
                    if guard.is_empty() {
                        continue;
                    }
                    guard.keys().copied().collect()
                };

                // Check which of the waited-on jobs no longer have an execution row
                // (meaning they reached terminal state: completed or errored out).
                match sqlx::query_scalar::<_, uuid::Uuid>(
                    "SELECT id FROM UNNEST($1::uuid[]) AS t(id) WHERE NOT EXISTS (SELECT 1 FROM job_executions WHERE job_executions.id = t.id)",
                )
                .bind(&waiter_ids)
                .fetch_all(&pool)
                .await
                {
                    Ok(terminal_ids) => {
                        if !terminal_ids.is_empty() {
                            let mut guard = waiters.lock().await;
                            for uuid in terminal_ids {
                                let id = JobId::from(uuid);
                                if let Some(tx) = guard.remove(&id) {
                                    let _ = tx.send(());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "sweep: failed to check terminal jobs");
                    }
                }
            }
        });

        OwnedTaskHandle::new(handle)
    }
}
