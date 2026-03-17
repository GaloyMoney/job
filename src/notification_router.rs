use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use sqlx::postgres::{PgListener, PgPool};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::JobId;
use crate::entity::{JobTerminalState, JobType};
use crate::handle::OwnedTaskHandle;
use crate::repo::JobRepo;
use crate::tracker::JobTracker;

/// Notification types from the unified `job_events` channel.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum JobNotification {
    ExecutionReady { job_type: String },
    JobTerminal { job_id: String },
}

type WaiterRegistration = (JobId, oneshot::Sender<JobTerminalState>);

pub(crate) struct JobNotificationRouter {
    pool: PgPool,
    repo: Arc<JobRepo>,
    terminal_tx: broadcast::Sender<JobId>,
    register_tx: OnceLock<mpsc::UnboundedSender<WaiterRegistration>>,
}

impl JobNotificationRouter {
    pub fn new(pool: &PgPool, repo: Arc<JobRepo>) -> Self {
        let (terminal_tx, _) = broadcast::channel(256);
        Self {
            pool: pool.clone(),
            repo,
            terminal_tx,
            register_tx: OnceLock::new(),
        }
    }

    /// Register interest in a job reaching terminal state.
    /// Returns a oneshot receiver that delivers the terminal state.
    /// Drop the receiver to unsubscribe.
    pub fn wait_for_terminal(&self, job_id: JobId) -> oneshot::Receiver<JobTerminalState> {
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
            self.repo.clone(),
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
        repo: Arc<JobRepo>,
    ) -> OwnedTaskHandle {
        let handle = tokio::spawn(async move {
            let mut waiters: HashMap<JobId, Vec<oneshot::Sender<JobTerminalState>>> =
                HashMap::new();
            let mut sweep = tokio::time::interval(SWEEP_INTERVAL);

            loop {
                tokio::select! {
                    biased;

                    Some((job_id, tx)) = register_rx.recv() => {
                        // Immediate DB check: no execution row means the job is terminal
                        match sqlx::query!(
                            "SELECT id FROM job_executions WHERE id = $1",
                            job_id as JobId,
                        )
                        .fetch_optional(&pool)
                        .await
                        {
                            Ok(None) => {
                                // Job is terminal — load entity for the state
                                match load_terminal_state(&repo, job_id).await {
                                    Some(state) => {
                                        let _ = tx.send(state);
                                        send_to_waiters(&mut waiters, job_id, state);
                                    }
                                    None => {
                                        // Could not determine state — register for later
                                        waiters.entry(job_id).or_default().push(tx);
                                    }
                                }
                            }
                            Ok(Some(_)) => {
                                waiters.entry(job_id).or_default().push(tx);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "register_waiter: failed to check execution row"
                                );
                                // Register anyway — sweep will catch it
                                waiters.entry(job_id).or_default().push(tx);
                            }
                        }
                    }

                    result = terminal_rx.recv() => {
                        match result {
                            Ok(job_id) => {
                                load_and_notify(&mut waiters, &repo, job_id).await;
                            }
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
                        for id in ids {
                            match sqlx::query!(
                                "SELECT id FROM job_executions WHERE id = $1",
                                id as JobId,
                            )
                            .fetch_optional(&pool)
                            .await
                            {
                                Ok(None) => {
                                    load_and_notify(&mut waiters, &repo, id).await;
                                }
                                Ok(Some(_)) => {}
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        "sweep: failed to check terminal job"
                                    );
                                }
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

/// Load a job entity and extract its terminal state.
/// Returns `None` and logs a warning on failure.
async fn load_terminal_state(repo: &JobRepo, job_id: JobId) -> Option<JobTerminalState> {
    match repo.find_by_id(job_id).await {
        Ok(job) => match job.terminal_state() {
            Some(state) => Some(state),
            None => {
                tracing::warn!(%job_id, "no execution row but job entity is not terminal");
                None
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, %job_id, "failed to load job entity for notification");
            None
        }
    }
}

/// Load the job entity and send the terminal state to all registered waiters.
/// On failure, waiters remain registered so the sweep can retry.
async fn load_and_notify(
    waiters: &mut HashMap<JobId, Vec<oneshot::Sender<JobTerminalState>>>,
    repo: &JobRepo,
    job_id: JobId,
) {
    if !waiters.contains_key(&job_id) {
        return;
    }

    if let Some(state) = load_terminal_state(repo, job_id).await {
        send_to_waiters(waiters, job_id, state);
    }
}

/// Deliver a known terminal state to all waiters for a job.
fn send_to_waiters(
    waiters: &mut HashMap<JobId, Vec<oneshot::Sender<JobTerminalState>>>,
    job_id: JobId,
    state: JobTerminalState,
) {
    if let Some(senders) = waiters.remove(&job_id) {
        for tx in senders {
            let _ = tx.send(state);
        }
    }
}
