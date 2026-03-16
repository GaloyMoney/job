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
    pool: PgPool,
    terminal_tx: broadcast::Sender<JobId>,
}

impl JobNotificationRouter {
    pub fn new(pool: &PgPool) -> Self {
        let (terminal_tx, _) = broadcast::channel(256);
        Self {
            pool: pool.clone(),
            terminal_tx,
        }
    }

    /// Subscribe to terminal events for a specific job.
    /// The returned waiter resolves when the job reaches terminal state.
    /// It self-heals via periodic DB checks in case a notification was lost.
    /// Drop the waiter to unsubscribe.
    #[allow(dead_code)]
    pub fn wait_for_terminal(&self, job_id: JobId) -> JobTerminalWaiter {
        JobTerminalWaiter {
            rx: self.terminal_tx.subscribe(),
            pool: self.pool.clone(),
            job_id,
        }
    }

    /// Start the central listener task. Returns an OwnedTaskHandle that
    /// aborts the listener when dropped.
    pub async fn start(
        &self,
        tracker: Arc<JobTracker>,
        job_types: Vec<JobType>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
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
}

const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// A future that resolves when a specific job reaches terminal state.
/// Combines broadcast notifications with periodic DB polling as a safety net.
/// Drop to unsubscribe.
#[allow(dead_code)]
pub(crate) struct JobTerminalWaiter {
    rx: broadcast::Receiver<JobId>,
    pool: PgPool,
    job_id: JobId,
}

#[allow(dead_code)]
impl JobTerminalWaiter {
    pub async fn wait(mut self) {
        let sweep = tokio::time::interval(SWEEP_INTERVAL);
        tokio::pin!(sweep);

        loop {
            tokio::select! {
                biased;

                result = self.rx.recv() => {
                    match result {
                        Ok(id) if id == self.job_id => return,
                        Ok(_) => continue,
                        Err(broadcast::error::RecvError::Closed) => return,
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Missed messages — fall through to DB check
                            if self.is_terminal().await {
                                return;
                            }
                        }
                    }
                }

                _ = sweep.tick() => {
                    if self.is_terminal().await {
                        return;
                    }
                }
            }
        }
    }

    async fn is_terminal(&self) -> bool {
        matches!(
            sqlx::query_scalar::<_, bool>(
                "SELECT NOT EXISTS (SELECT 1 FROM job_executions WHERE id = $1)"
            )
            .bind(self.job_id)
            .fetch_one(&self.pool)
            .await,
            Ok(true)
        )
    }
}
