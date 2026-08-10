//! Per-process debounced emitter for the `job_events` channel.
//!
//! Notify-bearing commits serialize on a cluster-wide `AccessExclusiveLock`
//! held across the WAL flush, so `job_executions` carries no notification
//! trigger. Instead the spawner, dispatcher and poller report here from an
//! es-entity *post-commit* hook, and one task emits at most one `pg_notify`
//! per debounce window on its own connection with `synchronous_commit = off`.
//!
//! This is safe because a notification is only ever an optimisation. Both
//! kinds have a table-derived fallback that does not depend on delivery:
//! `execution_ready` is backstopped by the poller's unconditional re-poll
//! (`MAX_WAIT`), and `job_terminal` by the waiter-manager's reconciliation
//! sweep (`sweep_interval`). Losing a notification -- or losing this process
//! between commit and emit -- costs latency only, never correctness.

use sqlx::postgres::PgPool;
use tokio::sync::{broadcast, mpsc};

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::JobId;
use crate::entity::JobType;
use crate::handle::OwnedTaskHandle;
use crate::notification_router::{JOB_EVENTS_CHANNEL, JobNotification};
use crate::tracker::JobTracker;

/// How long the emitter folds reports before emitting.
///
/// Deliberately not configurable: it bounds only added *hint* latency, and
/// every value in a sane range is negligible against the work it announces.
/// Matches the sibling emitter in `obix`.
const NOTIFY_DEBOUNCE: Duration = Duration::from_millis(25);

/// In-process delivery, registered when polling starts.
///
/// Most jobs are spawned and awaited in the same process that runs them, so
/// delivering locally means the round trip through Postgres only ever has to
/// reach *other* processes. `job_types` mirrors the membership test the
/// listener applies, so a local poller is never woken for a type it does not
/// serve.
struct LocalDelivery {
    tracker: Arc<JobTracker>,
    job_types: Vec<JobType>,
    terminal_tx: broadcast::Sender<JobId>,
}

pub(crate) struct JobEventNotifier {
    tx: mpsc::UnboundedSender<JobNotification>,
    local: OnceLock<LocalDelivery>,
    _handle: OwnedTaskHandle,
}

impl JobEventNotifier {
    pub fn spawn(pool: &PgPool) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(Self::run(pool.clone(), rx));
        Arc::new(Self {
            tx,
            local: OnceLock::new(),
            _handle: OwnedTaskHandle::new(handle),
        })
    }

    /// Register in-process delivery so same-process work skips the round trip.
    pub fn register_local_delivery(
        &self,
        tracker: Arc<JobTracker>,
        job_types: Vec<JobType>,
        terminal_tx: broadcast::Sender<JobId>,
    ) {
        let _ = self.local.set(LocalDelivery {
            tracker,
            job_types,
            terminal_tx,
        });
    }

    /// Report a notification. Infallible and non-blocking, so it is safe to
    /// call from a synchronous `post_commit` hook.
    ///
    /// Delivering locally *and* over `pg_notify` can hand the same event to
    /// this process twice; both receivers are idempotent (the tracker stores a
    /// single permit, and resolved waiters are removed from the map).
    pub fn notify(&self, notification: JobNotification) {
        if let Some(local) = self.local.get() {
            match &notification {
                JobNotification::ExecutionReady { job_type } => {
                    if local.job_types.iter().any(|jt| jt.as_str() == job_type) {
                        local.tracker.job_execution_inserted();
                    }
                }
                JobNotification::JobTerminal { job_id } => {
                    let _ = local.terminal_tx.send(*job_id);
                }
            }
        }
        // Unbounded: a synchronous post-commit hook must never block here.
        let _ = self.tx.send(notification);
    }

    /// Report that a job of `job_type` is (or became) ready to run, outside any
    /// operation. For callers whose write is already committed -- the poller's
    /// autocommit reclaim, which has no operation and so no commit hook.
    pub fn execution_ready(&self, job_type: &JobType) {
        self.notify(JobNotification::ExecutionReady {
            job_type: job_type.to_string(),
        });
    }

    /// Fold reports into one set per window and emit. A failed emit keeps the
    /// fold and retries on the next window; exits when all senders drop.
    async fn run(pool: PgPool, mut rx: mpsc::UnboundedReceiver<JobNotification>) {
        let mut pending: HashSet<JobNotification> = HashSet::new();
        loop {
            if pending.is_empty() {
                match rx.recv().await {
                    Some(notification) => {
                        pending.insert(notification);
                    }
                    None => return,
                }
            }
            drain(&mut rx, &mut pending);
            tokio::time::sleep(NOTIFY_DEBOUNCE).await;
            drain(&mut rx, &mut pending);

            match emit(&pool, &pending).await {
                Ok(()) => pending.clear(),
                Err(error) => record_notify_emit_failed(&error),
            }
        }
    }
}

fn drain(
    rx: &mut mpsc::UnboundedReceiver<JobNotification>,
    pending: &mut HashSet<JobNotification>,
) {
    while let Ok(notification) = rx.try_recv() {
        pending.insert(notification);
    }
}

/// Emit every pending notification in a *single* statement, so one debounce
/// window costs exactly one notify-bearing commit however many it carries.
///
/// `set_config(.., is_local => true)` scopes `synchronous_commit = off` to this
/// transaction. `synchronous_commit` is read at commit time, so the setting
/// applies regardless of target-list evaluation order -- which is the point: it
/// takes the WAL flush out of the notify lock's hold window.
async fn emit(pool: &PgPool, pending: &HashSet<JobNotification>) -> Result<(), sqlx::Error> {
    let payloads: Vec<String> = pending.iter().map(payload).collect();
    sqlx::query(
        "SELECT set_config('synchronous_commit', 'off', true), \
         pg_notify($1, payload) FROM unnest($2::text[]) AS t(payload)",
    )
    .bind(JOB_EVENTS_CHANNEL)
    .bind(&payloads)
    .execute(pool)
    .await?;
    Ok(())
}

/// Serialized through the same type the listener parses, so the wire format
/// cannot drift between emitter and receiver.
pub(crate) fn payload(notification: &JobNotification) -> String {
    serde_json::to_string(notification).expect("Could not serialize job notification payload")
}

#[tracing::instrument(
    name = "job.notifier.emit_failed",
    level = "warn",
    skip_all,
    fields(error = %error),
)]
fn record_notify_emit_failed(error: &sqlx::Error) {}

/// Post-commit hook that reports once the writer's transaction has actually
/// committed.
///
/// Multiple registrations on one operation merge into a single hook, so a bulk
/// spawn reports one entry per distinct notification rather than one per row.
pub(crate) struct JobEventHook {
    notifier: Arc<JobEventNotifier>,
    notifications: HashSet<JobNotification>,
}

impl es_entity::operation::hooks::CommitHook for JobEventHook {
    fn post_commit(self) {
        for notification in self.notifications {
            self.notifier.notify(notification);
        }
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.notifications.extend(other.notifications.drain());
        true
    }
}

/// Arrange for `notification` to be emitted once `op` commits.
///
/// Falls back to an in-transaction `pg_notify` when the operation has no
/// commit-hook buffer (a bare `sqlx::Transaction`), so hook-less callers keep
/// today's latency at today's cost rather than silently dropping to the
/// table-derived fallback. This mirrors obix's `persist_events_notifying`.
pub(crate) async fn notify_on_commit(
    notifier: &Arc<JobEventNotifier>,
    op: &mut impl es_entity::AtomicOperation,
    notification: JobNotification,
) -> Result<(), sqlx::Error> {
    let serialized = payload(&notification);
    let hook = JobEventHook {
        notifier: Arc::clone(notifier),
        notifications: HashSet::from([notification]),
    };

    if op.add_commit_hook(hook).is_err() {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(JOB_EVENTS_CHANNEL)
            .bind(serialized)
            .execute(op.as_executor())
            .await?;
    }

    Ok(())
}

/// Report that a job of `job_type` is (or became) ready to run.
pub(crate) async fn notify_execution_ready_on_commit(
    notifier: &Arc<JobEventNotifier>,
    op: &mut impl es_entity::AtomicOperation,
    job_type: &JobType,
) -> Result<(), sqlx::Error> {
    notify_on_commit(
        notifier,
        op,
        JobNotification::ExecutionReady {
            job_type: job_type.to_string(),
        },
    )
    .await
}

/// Report that a job reached terminal state and its execution row is gone.
pub(crate) async fn notify_job_terminal_on_commit(
    notifier: &Arc<JobEventNotifier>,
    op: &mut impl es_entity::AtomicOperation,
    job_id: JobId,
) -> Result<(), sqlx::Error> {
    notify_on_commit(notifier, op, JobNotification::JobTerminal { job_id }).await
}
