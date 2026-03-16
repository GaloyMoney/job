use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use sqlx::postgres::{PgListener, PgPool};
use tokio::sync::{broadcast, mpsc, oneshot};

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

type WaiterRegistration = (JobId, oneshot::Sender<()>);

pub(crate) struct JobNotificationRouter {
    pool: PgPool,
    terminal_tx: broadcast::Sender<JobId>,
    register_tx: OnceLock<mpsc::UnboundedSender<WaiterRegistration>>,
}

impl JobNotificationRouter {
    pub fn new(pool: &PgPool) -> Self {
        let (terminal_tx, _) = broadcast::channel(256);
        Self {
            pool: pool.clone(),
            terminal_tx,
            register_tx: OnceLock::new(),
        }
    }

    /// Register interest in a job reaching terminal state.
    /// Returns a oneshot receiver that fires when the job completes/errors/cancels.
    /// Drop the receiver to unsubscribe.
    pub fn wait_for_terminal(&self, job_id: JobId) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let register_tx = self.register_tx.get().expect("router not started");
        let _ = register_tx.send((job_id, tx));
        rx
    }

    /// Start the PG NOTIFY listener and waiter-manager tasks.
    /// Returns handles that abort both tasks when dropped.
    pub async fn start(
        &self,
        tracker: Arc<JobTracker>,
        job_types: Vec<JobType>,
    ) -> Result<(OwnedTaskHandle, OwnedTaskHandle), sqlx::Error> {
        let (register_tx, register_rx) = mpsc::unbounded_channel();
        self.register_tx
            .set(register_tx)
            .expect("router started more than once");

        let listener_handle = self.start_listener(tracker, job_types).await?;
        let waiter_handle = Self::start_waiter_manager(
            register_rx,
            self.terminal_tx.subscribe(),
            self.pool.clone(),
        );
        Ok((listener_handle, waiter_handle))
    }

    async fn start_listener(
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

    fn start_waiter_manager(
        mut register_rx: mpsc::UnboundedReceiver<WaiterRegistration>,
        mut terminal_rx: broadcast::Receiver<JobId>,
        pool: PgPool,
    ) -> OwnedTaskHandle {
        let handle = tokio::spawn(async move {
            let mut waiters: HashMap<JobId, Vec<oneshot::Sender<()>>> = HashMap::new();
            let mut sweep = tokio::time::interval(SWEEP_INTERVAL);

            loop {
                tokio::select! {
                    biased;

                    Some((job_id, tx)) = register_rx.recv() => {
                        waiters.entry(job_id).or_default().push(tx);
                    }

                    result = terminal_rx.recv() => {
                        match result {
                            Ok(job_id) => notify_waiters(&mut waiters, job_id),
                            Err(broadcast::error::RecvError::Closed) => break,
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                // Missed notifications — sweep will catch them
                            }
                        }
                    }

                    _ = sweep.tick() => {
                        // Prune dropped receivers
                        waiters.retain(|_, senders| {
                            senders.retain(|tx| !tx.is_closed());
                            !senders.is_empty()
                        });

                        if waiters.is_empty() {
                            continue;
                        }

                        let ids: Vec<JobId> = waiters.keys().copied().collect();
                        match sqlx::query_scalar::<_, uuid::Uuid>(
                            "SELECT id FROM UNNEST($1::uuid[]) AS t(id) \
                             WHERE NOT EXISTS (SELECT 1 FROM job_executions WHERE job_executions.id = t.id)",
                        )
                        .bind(&ids)
                        .fetch_all(&pool)
                        .await
                        {
                            Ok(terminal_ids) => {
                                for uuid in terminal_ids {
                                    notify_waiters(&mut waiters, JobId::from(uuid));
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "sweep: failed to check terminal jobs");
                            }
                        }
                    }
                }
            }
        });

        OwnedTaskHandle::new(handle)
    }
}

const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

fn notify_waiters(waiters: &mut HashMap<JobId, Vec<oneshot::Sender<()>>>, job_id: JobId) {
    if let Some(senders) = waiters.remove(&job_id) {
        for tx in senders {
            let _ = tx.send(());
        }
    }
}
