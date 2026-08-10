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
use std::sync::Arc;
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

/// The three places a report goes. The first reaches other processes; the
/// other two are this process's own receivers, called directly rather than
/// through a channel of their own.
pub(crate) struct JobEventNotifier {
    /// Hands the report to the debounced emitter task, which is what turns it
    /// into a `pg_notify`. The only path out of this process.
    publish_tx: mpsc::UnboundedSender<JobNotification>,
    /// In-process readiness: wakes this process's poll loop, if it polls that
    /// job type.
    tracker: Arc<JobTracker>,
    /// In-process completion: resolves this process's `await_completion`
    /// waiters.
    terminal_tx: broadcast::Sender<JobId>,
    _handle: OwnedTaskHandle,
}

impl JobEventNotifier {
    pub fn spawn(
        pool: &PgPool,
        tracker: Arc<JobTracker>,
        terminal_tx: broadcast::Sender<JobId>,
    ) -> Arc<Self> {
        let (publish_tx, publish_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(Self::run(pool.clone(), publish_rx));
        Arc::new(Self {
            publish_tx,
            tracker,
            terminal_tx,
            _handle: OwnedTaskHandle::new(handle),
        })
    }

    /// Report a notification. Infallible and non-blocking, so it is safe to
    /// call from a synchronous `post_commit` hook.
    ///
    /// **Both delivery paths always run.** In-process delivery is a latency
    /// optimisation for this process only; `pg_notify` is the sole path to
    /// every *other* process and is never skipped on account of it.
    ///
    /// The cost is that this process sees its own notifications twice -- once
    /// in process, then again ~a debounce window later through its own
    /// listener. Neither receiver needs deduplicating: `execution_ready`
    /// collapses into a single `tokio::sync::Notify` permit, and for
    /// `job_terminal` the first delivery removes the waiters from the map, so
    /// the second is filtered out before it reaches the database.
    ///
    /// Whether a report is worth acting on is decided by the receiver, not
    /// here: the tracker knows which job types this process polls, and both
    /// receivers are inert before polling starts.
    pub fn notify(&self, notification: JobNotification) {
        match &notification {
            JobNotification::ExecutionReady { job_type } => {
                self.tracker.job_execution_inserted(job_type);
            }
            JobNotification::JobTerminal { job_id } => {
                let _ = self.terminal_tx.send(*job_id);
            }
        }

        let _ = self.publish_tx.send(notification);
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
            tokio::time::sleep(NOTIFY_DEBOUNCE).await;
            drain(&mut rx, &mut pending);

            if let Err(error) = emit(&pool, &mut pending).await {
                record_notify_emit_failed(&error);
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
///
/// Clears `pending` once the emit has actually landed. Reading rather than
/// draining to build the payloads matters: `?` below leaves the fold intact on
/// failure, so a window that could not be emitted is retried on the next one
/// instead of being silently dropped.
async fn emit(pool: &PgPool, pending: &mut HashSet<JobNotification>) -> Result<(), sqlx::Error> {
    let payloads: Vec<String> = pending.iter().map(payload).collect();
    sqlx::query(
        "SELECT set_config('synchronous_commit', 'off', true), \
         pg_notify($1, payload) FROM unnest($2::text[]) AS t(payload)",
    )
    .bind(JOB_EVENTS_CHANNEL)
    .bind(&payloads)
    .execute(pool)
    .await?;
    pending.clear();
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

impl JobEventNotifier {
    /// Report that a job of `job_type` is ready to run, once `op` commits.
    pub(crate) async fn execution_ready_in_op(
        self: &Arc<Self>,
        op: &mut impl es_entity::AtomicOperation,
        job_type: &JobType,
    ) -> Result<(), sqlx::Error> {
        self.notify_in_op(
            op,
            JobNotification::ExecutionReady {
                job_type: job_type.to_string(),
            },
        )
        .await
    }

    /// Report that a job reached terminal state and its execution row is gone,
    /// once `op` commits.
    pub(crate) async fn job_terminal_in_op(
        self: &Arc<Self>,
        op: &mut impl es_entity::AtomicOperation,
        job_id: JobId,
    ) -> Result<(), sqlx::Error> {
        self.notify_in_op(op, JobNotification::JobTerminal { job_id })
            .await
    }

    /// Arrange for `notification` to be reported once `op` commits.
    ///
    /// Falls back to an in-transaction `pg_notify` when the operation has no
    /// commit-hook buffer (a bare `sqlx::Transaction`), so hook-less callers
    /// keep today's latency at today's cost rather than silently dropping to
    /// the table-derived fallback. This mirrors obix's
    /// `persist_events_notifying`.
    async fn notify_in_op(
        self: &Arc<Self>,
        op: &mut impl es_entity::AtomicOperation,
        notification: JobNotification,
    ) -> Result<(), sqlx::Error> {
        let serialized = payload(&notification);
        let hook = JobEventHook {
            notifier: Arc::clone(self),
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
}
