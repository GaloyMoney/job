use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::{PgListener, PgPool};
use tokio::sync::broadcast;

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
    terminal_tx: broadcast::Sender<JobId>,
}

impl JobNotificationRouter {
    pub fn new() -> Self {
        let (terminal_tx, _) = broadcast::channel(256);
        Self { terminal_tx }
    }

    /// Subscribe to terminal events for a specific job.
    /// The returned future resolves when the job reaches terminal state.
    /// Drop the future to unsubscribe — no explicit deregister needed.
    #[allow(dead_code)]
    pub fn wait_for_terminal(&self, job_id: JobId) -> JobTerminalWaiter {
        JobTerminalWaiter {
            rx: self.terminal_tx.subscribe(),
            job_id,
        }
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

        let terminal_tx = self.terminal_tx.clone();

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
                                    let _ = terminal_tx.send(JobId::from(uuid));
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

    /// Start a periodic sweep that re-broadcasts terminal events for jobs
    /// whose execution rows have disappeared. Safety net for notifications
    /// lost during PgListener reconnection.
    pub fn start_sweep(&self, pool: PgPool) -> OwnedTaskHandle {
        let terminal_tx = self.terminal_tx.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;

                // If nobody is listening, skip the DB query entirely.
                if terminal_tx.receiver_count() == 0 {
                    continue;
                }

                // Find all jobs that have been completed but might have
                // had their notification lost. We query for jobs that
                // exist in the jobs table but NOT in job_executions.
                // However, we don't know which specific IDs waiters care
                // about. Instead, just broadcast all recently-terminal IDs
                // — waiters filter client-side anyway.
                //
                // For now, query recently completed jobs (no execution row).
                // This is intentionally broad; the broadcast is cheap.
                match sqlx::query_scalar::<_, uuid::Uuid>(
                    r#"SELECT j.id FROM jobs j
                       WHERE NOT EXISTS (
                           SELECT 1 FROM job_executions je WHERE je.id = j.id
                       )
                       AND j.created_at > NOW() - INTERVAL '5 minutes'"#,
                )
                .fetch_all(&pool)
                .await
                {
                    Ok(ids) => {
                        for uuid in ids {
                            let _ = terminal_tx.send(JobId::from(uuid));
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

/// A future that resolves when a specific job reaches terminal state.
/// Drop to unsubscribe.
#[allow(dead_code)]
pub(crate) struct JobTerminalWaiter {
    rx: broadcast::Receiver<JobId>,
    job_id: JobId,
}

#[allow(dead_code)]
impl JobTerminalWaiter {
    pub async fn wait(mut self) {
        loop {
            match self.rx.recv().await {
                Ok(id) if id == self.job_id => return,
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    }
}
