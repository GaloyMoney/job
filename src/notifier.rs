//! Per-process debounced emitter for the `job_events` channel.
//!
//! Notify-bearing commits serialize on a cluster-wide lock, so nothing writes
//! `pg_notify` on the write path; reports arrive from a post-commit hook and
//! one task emits at most one notification per debounce window. Safe because a
//! notification is only an optimisation: `execution_ready` is backstopped by
//! the poller's re-poll and `job_terminal` by the waiter-manager's sweep.

use sqlx::postgres::PgPool;
use tokio::sync::{broadcast, mpsc};

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::JobId;
use crate::entity::JobType;
use crate::notification_router::{JOB_EVENTS_CHANNEL, JobNotification};
use crate::task::OwnedTaskHandle;
use crate::tracker::JobTracker;

/// Deliberately not configurable: it bounds only added hint latency.
const NOTIFY_DEBOUNCE: Duration = Duration::from_millis(25);

pub(crate) struct JobEventNotifier {
    /// Feeds the emitter task; the only path to other processes.
    publish_tx: mpsc::UnboundedSender<JobNotification>,
    /// In-process readiness: wakes this process's poll loop.
    tracker: Arc<JobTracker>,
    /// In-process completion: resolves this process's waiters.
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
    /// Both paths always run: in-process delivery is an optimisation, and
    /// publishing is never skipped on account of it. This process therefore
    /// sees its own reports twice; neither receiver needs deduplicating.
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

    /// Report readiness outside any operation, for writes already committed.
    pub fn execution_ready(&self, job_type: &JobType) {
        self.notify(JobNotification::ExecutionReady {
            job_type: job_type.to_string(),
        });
    }

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

    /// Report that a job reached terminal state, once `op` commits.
    pub(crate) async fn job_terminal_in_op(
        self: &Arc<Self>,
        op: &mut impl es_entity::AtomicOperation,
        job_id: JobId,
    ) -> Result<(), sqlx::Error> {
        self.notify_in_op(op, JobNotification::JobTerminal { job_id })
            .await
    }

    /// Falls back to an in-transaction `pg_notify` when the operation has no
    /// commit-hook buffer, where `post_commit` would never run.
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

    /// Fold reports into one set per window and emit; exits when all senders
    /// drop.
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

/// Emits every pending notification in one statement, so a window costs one
/// notify-bearing commit. `synchronous_commit = off` keeps the WAL flush out of
/// the notify lock's hold window.
///
/// Clears `pending` only once the emit lands, so a failed window is retried
/// rather than dropped.
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

/// Built through the type the listener parses, so the wire format cannot drift.
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

/// Reports once the writer's transaction commits. Registrations on one
/// operation merge, so a bulk spawn reports one entry per distinct
/// notification.
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
