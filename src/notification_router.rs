use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::JobId;
use crate::entity::JobType;
use crate::handle::OwnedTaskHandle;
use crate::outcome::JobTerminalState;
use crate::repo::JobRepo;
use crate::tracker::JobTracker;
use sqlx::postgres::{PgListener, PgPool};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::instrument;

/// Notification types from the unified `job_events` channel.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum JobNotification {
    ExecutionReady { job_type: String },
    JobTerminal { job_id: JobId },
}

type WaiterRegistration = (JobId, oneshot::Sender<JobTerminalState>);

pub(crate) struct JobNotificationRouter {
    pool: PgPool,
    repo: Arc<JobRepo>,
    terminal_tx: broadcast::Sender<JobId>,
    register_tx: OnceLock<mpsc::UnboundedSender<WaiterRegistration>>,
}

impl JobNotificationRouter {
    pub fn new(pool: &PgPool, repo: Arc<JobRepo>, buffer_size: usize) -> Self {
        let (terminal_tx, _) = broadcast::channel(buffer_size.max(1));
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
                                let _ = terminal_tx.send(job_id);
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
                        match has_execution_row(&pool, job_id).await {
                            Ok(false) => {
                                // No execution row → job is terminal, but the caller
                                // needs to know *how* it ended (Completed vs Errored),
                                // so we load the entity's event history.
                                // On transient DB failure the waiter is parked for the
                                // sweep to retry.
                                match load_terminal_state(&repo, job_id).await {
                                    Some(state) => {
                                        let _ = tx.send(state);
                                        send_to_waiters(&mut waiters, job_id, state);
                                    }
                                    None => {
                                        waiters.entry(job_id).or_default().push(tx);
                                    }
                                }
                            }
                            Ok(true) => {
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
                                let mut ids: Vec<JobId> = vec![job_id];
                                // Drain all buffered notifications before hitting the DB
                                loop {
                                    match terminal_rx.try_recv() {
                                        Ok(id) => ids.push(id),
                                        Err(broadcast::error::TryRecvError::Empty) => break,
                                        Err(broadcast::error::TryRecvError::Lagged(n)) => {
                                            tracing::warn!(
                                                missed = n,
                                                "terminal broadcast lagged during drain, \
                                                 running immediate sweep"
                                            );
                                            sweep_waiters(&mut waiters, &pool, &repo).await;
                                            ids.clear();
                                            break;
                                        }
                                        Err(broadcast::error::TryRecvError::Closed) => break,
                                    }
                                }
                                if !ids.is_empty() {
                                    batch_load_and_notify(&mut waiters, &repo, ids).await;
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!(
                                    missed = n,
                                    "terminal broadcast lagged, running immediate sweep"
                                );
                                sweep_waiters(&mut waiters, &pool, &repo).await;
                            }
                        }
                    }

                    _ = sweep.tick() => {
                        sweep_waiters(&mut waiters, &pool, &repo).await;
                    }
                }
            }
        });

        OwnedTaskHandle::new(handle)
    }
}

const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Check whether a `job_executions` row exists for the given ID.
async fn has_execution_row(pool: &PgPool, id: JobId) -> Result<bool, sqlx::Error> {
    let row = sqlx::query!("SELECT id FROM job_executions WHERE id = $1", id as JobId,)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// Sweep all waiters: prune closed receivers and check for newly-terminal jobs.
///
/// Uses a single batched query instead of N sequential queries to determine
/// which waited-on jobs have reached terminal state.
async fn sweep_waiters(
    waiters: &mut HashMap<JobId, Vec<oneshot::Sender<JobTerminalState>>>,
    pool: &PgPool,
    repo: &JobRepo,
) {
    // Prune dropped receivers
    waiters.retain(|_, senders| {
        senders.retain(|tx| !tx.is_closed());
        !senders.is_empty()
    });

    if waiters.is_empty() {
        return;
    }

    let ids: Vec<JobId> = waiters.keys().copied().collect();

    // Single batched query: returns the subset of IDs that still have execution rows
    // (i.e. are still running). Any ID NOT in the result set is terminal.
    let still_running: HashSet<JobId> = match sqlx::query_scalar!(
        "SELECT id FROM job_executions WHERE id = ANY($1)",
        &ids as &[JobId],
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows.into_iter().map(JobId::from).collect(),
        Err(e) => {
            tracing::warn!(error = %e, "sweep: failed to batch-check execution rows");
            return;
        }
    };

    // Notify waiters for jobs that no longer have an execution row
    for id in ids {
        if !still_running.contains(&id) {
            load_and_notify(waiters, repo, id).await;
        }
    }
}

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

/// Load multiple job entities in a single query and notify all registered waiters.
/// Jobs that fail to load remain registered for the sweep to retry.
#[instrument(name = "job.notification_router.batch_load_and_notify", skip_all)]
async fn batch_load_and_notify(
    waiters: &mut HashMap<JobId, Vec<oneshot::Sender<JobTerminalState>>>,
    repo: &JobRepo,
    ids: Vec<JobId>,
) {
    // Deduplicate and keep only IDs with active waiters
    let unique_ids: Vec<JobId> = ids
        .into_iter()
        .collect::<HashSet<_>>()
        .into_iter()
        .filter(|id| waiters.contains_key(id))
        .collect();

    if unique_ids.is_empty() {
        return;
    }

    match repo.find_all::<crate::Job>(&unique_ids).await {
        Ok(entities) => {
            for (job_id, job) in entities {
                if let Some(state) = job.terminal_state() {
                    send_to_waiters(waiters, job_id, state);
                } else {
                    tracing::warn!(
                        %job_id,
                        "no execution row but job entity is not terminal"
                    );
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                n_jobs = unique_ids.len(),
                "batch_load_and_notify: find_all failed, falling back to individual loads"
            );
            // Fall back to individual loads so one bad job doesn't block the rest
            for job_id in unique_ids {
                load_and_notify(waiters, repo, job_id).await;
            }
        }
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
