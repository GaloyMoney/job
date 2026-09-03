use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crate::JobId;
use crate::outcome::JobOutcome;
use crate::repo::JobRepo;
use crate::task::OwnedTaskHandle;
use crate::tracker::JobTracker;
use sqlx::postgres::{PgListener, PgPool};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{Span, instrument};

/// The single Postgres channel every job notification travels on.
pub(crate) const JOB_EVENTS_CHANNEL: &str = "job_events";

/// Notification types from the unified `job_events` channel.
///
/// `Serialize` is derived so the emitter builds payloads through this same
/// type and the wire format cannot drift.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum JobNotification {
    ExecutionReady { job_type: String },
    JobTerminal { job_id: JobId },
}

type WaiterRegistration = (JobId, oneshot::Sender<JobOutcome>);

/// The set of oneshot senders waiting on a single job, plus when the first of
/// them registered. `registered_at` powers the oldest-waiter-age gauge so a
/// stalled wait is observable instead of silent.
struct Waiters {
    senders: Vec<oneshot::Sender<JobOutcome>>,
    registered_at: Instant,
}

impl Waiters {
    fn new() -> Self {
        Self {
            senders: Vec::new(),
            registered_at: Instant::now(),
        }
    }
}

type WaiterMap = HashMap<JobId, Waiters>;

pub(crate) struct JobNotificationRouter {
    pool: PgPool,
    repo: Arc<JobRepo>,
    terminal_tx: broadcast::Sender<JobId>,
    register_tx: OnceLock<mpsc::UnboundedSender<WaiterRegistration>>,
    sweep_interval: Duration,
}

impl JobNotificationRouter {
    pub fn new(
        pool: &PgPool,
        repo: Arc<JobRepo>,
        buffer_size: usize,
        sweep_interval: Duration,
    ) -> Self {
        let (terminal_tx, _) = broadcast::channel(buffer_size.max(1));
        Self {
            pool: pool.clone(),
            repo,
            terminal_tx,
            register_tx: OnceLock::new(),
            sweep_interval,
        }
    }

    /// Handed to the notifier so a completion in this process resolves its
    /// waiters without a round trip through Postgres.
    pub fn terminal_sender(&self) -> broadcast::Sender<JobId> {
        self.terminal_tx.clone()
    }

    /// Register interest in a job reaching terminal state.
    /// Returns a oneshot receiver that delivers the terminal state, or
    /// `None` when the router has not been started yet (i.e. before
    /// `Jobs::start_poll`). Drop the receiver to unsubscribe.
    pub fn try_wait_for_terminal(&self, job_id: JobId) -> Option<oneshot::Receiver<JobOutcome>> {
        let register_tx = self.register_tx.get()?;
        let (tx, rx) = oneshot::channel();
        let _ = register_tx.send((job_id, tx));
        Some(rx)
    }

    /// Start the PG NOTIFY listener and waiter-manager tasks.
    /// Returns handles that abort both tasks when dropped.
    pub async fn start(
        &self,
        tracker: Arc<JobTracker>,
    ) -> Result<(OwnedTaskHandle, OwnedTaskHandle), sqlx::Error> {
        let (register_tx, register_rx) = mpsc::unbounded_channel();
        self.register_tx
            .set(register_tx)
            .expect("router started more than once");

        let listener_handle = self.start_listener(tracker).await?;
        let waiter_handle = Self::start_waiter_manager(
            register_rx,
            self.terminal_tx.subscribe(),
            self.pool.clone(),
            self.repo.clone(),
            self.sweep_interval,
        );
        Ok((listener_handle, waiter_handle))
    }

    async fn start_listener(
        &self,
        tracker: Arc<JobTracker>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(JOB_EVENTS_CHANNEL).await?;

        let terminal_tx = self.terminal_tx.clone();

        let handle = tokio::spawn(async move {
            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        let payload = notification.payload();
                        match serde_json::from_str::<JobNotification>(payload) {
                            Ok(JobNotification::ExecutionReady { job_type }) => {
                                tracker.job_execution_inserted(&job_type);
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
                        tracing::error!(
                            exception.message = %e,
                            exception.type = std::any::type_name_of_val(&e),
                            "job notification listener error"
                        );
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
        sweep_interval: Duration,
    ) -> OwnedTaskHandle {
        let handle = tokio::spawn(async move {
            let mut waiters: WaiterMap = HashMap::new();
            let mut sweep = tokio::time::interval(sweep_interval);
            // Missed ticks are redundant (the sweep is idempotent), so don't let
            // a brief stall queue a burst of catch-up sweeps.
            sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;

                    // The reconciliation sweep is polled FIRST so a sustained
                    // terminal-notification firehose can never starve it. It is
                    // the only backstop that resolves waiters whose single
                    // terminal notification was dropped (broadcast overflow) and
                    // is never redelivered; if it can be starved, those waiters
                    // wedge until the process restarts. `tick()` is ready only
                    // once per interval, so giving it priority only pre-empts the
                    // register/terminal branches for one iteration per period.
                    _ = sweep.tick() => {
                        sweep_waiters(&mut waiters, &pool, &repo).await;
                    }

                    // Registration is polled before the terminal branch so a
                    // waiter lands in the map before the terminal notification
                    // for its job is processed (avoids a lost wakeup). All
                    // immediately-available registrations are drained and checked
                    // with a single query rather than one round-trip per waiter.
                    Some(reg) = register_rx.recv() => {
                        let mut batch = vec![reg];
                        while let Ok(next) = register_rx.try_recv() {
                            batch.push(next);
                        }
                        register_waiters(&mut waiters, &pool, &repo, batch).await;
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
                }
            }
        });

        OwnedTaskHandle::new(handle)
    }
}

const WAITER_BATCH_CHUNK: usize = 1000;

async fn still_running(pool: &PgPool, ids: &[JobId]) -> HashSet<JobId> {
    let mut running = HashSet::with_capacity(ids.len());
    for chunk in ids.chunks(WAITER_BATCH_CHUNK) {
        match sqlx::query_scalar!(
            "SELECT id FROM job_executions WHERE id = ANY($1)",
            chunk as &[JobId],
        )
        .fetch_all(pool)
        .await
        {
            Ok(rows) => running.extend(rows.into_iter().map(JobId::from)),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    chunk_len = chunk.len(),
                    "failed to batch-check execution rows; treating this chunk as \
                     still running so its waiters stay parked for the next sweep"
                );
                running.extend(chunk.iter().copied());
            }
        }
    }
    running
}

/// Register a batch of completion-waiters.
///
/// Every registration is parked first (so a concurrent terminal notification can
/// find it), then a single query determines which jobs are still running; any
/// job with no execution row is already terminal and is resolved immediately in
/// one batched entity load. This replaces the previous one-DB-round-trip-per-
/// registration path, which both serialized large batches and monopolized the
/// waiter-manager while completion notifications piled up and overflowed.
async fn register_waiters(
    waiters: &mut WaiterMap,
    pool: &PgPool,
    repo: &JobRepo,
    batch: Vec<WaiterRegistration>,
) {
    let mut ids: Vec<JobId> = Vec::with_capacity(batch.len());
    for (job_id, tx) in batch {
        ids.push(job_id);
        waiters
            .entry(job_id)
            .or_insert_with(Waiters::new)
            .senders
            .push(tx);
    }

    let running = still_running(pool, &ids).await;
    let terminal_ids: Vec<JobId> = ids.into_iter().filter(|id| !running.contains(id)).collect();

    if !terminal_ids.is_empty() {
        batch_load_and_notify(waiters, repo, terminal_ids).await;
    }
}

/// Sweep all waiters: prune closed receivers and check for newly-terminal jobs.
///
/// Uses a single batched query instead of N sequential queries to determine
/// which waited-on jobs have reached terminal state.
///
/// A waiter resolved here is one whose terminal notification never arrived
/// (dropped on broadcast overflow, or a transient load failure at registration).
/// A nonzero `n_resolved` therefore means notification delivery is lagging and
/// the sweep is carrying completion delivery — the early warning that would have
/// surfaced the 35 h wedge in minutes — so it is logged at `warn`.
#[instrument(
    name = "job.notification_router.sweep_waiters",
    skip_all,
    fields(n_waiters, oldest_waiter_age_secs, n_resolved)
)]
async fn sweep_waiters(waiters: &mut WaiterMap, pool: &PgPool, repo: &JobRepo) {
    // Prune dropped receivers
    waiters.retain(|_, w| {
        w.senders.retain(|tx| !tx.is_closed());
        !w.senders.is_empty()
    });

    let span = Span::current();
    if waiters.is_empty() {
        span.record("n_waiters", 0);
        span.record("oldest_waiter_age_secs", 0u64);
        span.record("n_resolved", 0);
        return;
    }

    let n_waiters = waiters.len();
    let oldest_age = waiters
        .values()
        .map(|w| w.registered_at.elapsed())
        .max()
        .unwrap_or_default();
    span.record("n_waiters", n_waiters);
    span.record("oldest_waiter_age_secs", oldest_age.as_secs());

    let ids: Vec<JobId> = waiters.keys().copied().collect();
    let running = still_running(pool, &ids).await;
    let terminal_ids: Vec<JobId> = ids.into_iter().filter(|id| !running.contains(id)).collect();

    let before = waiters.len();
    batch_load_and_notify(waiters, repo, terminal_ids).await;
    let n_resolved = before.saturating_sub(waiters.len());
    span.record("n_resolved", n_resolved);

    if n_resolved > 0 {
        tracing::warn!(
            n_resolved,
            n_waiters,
            oldest_waiter_age_secs = oldest_age.as_secs(),
            "sweep resolved already-terminal waiters whose terminal notification \
             was never delivered — notification delivery is lagging"
        );
    }
}

/// Load multiple job entities in a single query and notify all registered waiters.
/// Jobs that fail to load remain registered for the sweep to retry.
#[instrument(name = "job.notification_router.batch_load_and_notify", skip_all)]
async fn batch_load_and_notify(waiters: &mut WaiterMap, repo: &JobRepo, ids: Vec<JobId>) {
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

    for chunk in unique_ids.chunks(WAITER_BATCH_CHUNK) {
        match repo.find_all::<crate::Job>(chunk).await {
            Ok(entities) => {
                for (job_id, job) in entities {
                    if let Some(state) = job.terminal_state() {
                        let outcome = JobOutcome::new(state, job.raw_return_value().cloned());
                        send_to_waiters(waiters, job_id, outcome);
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
                    n_jobs = chunk.len(),
                    "batch_load_and_notify: failed to load job entities"
                );
                // Waiters remain registered — sweep will retry
            }
        }
    }
}

/// Deliver a known terminal state to all waiters for a job.
fn send_to_waiters(waiters: &mut WaiterMap, job_id: JobId, outcome: JobOutcome) {
    if let Some(w) = waiters.remove(&job_id) {
        for tx in w.senders {
            let _ = tx.send(outcome.clone());
        }
    }
}
