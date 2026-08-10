//! Per-process debounced `execution_ready` NOTIFY emitter.
//!
//! Notify-bearing commits serialize on a cluster-wide `AccessExclusiveLock`
//! held across the WAL flush, so the `job_executions` trigger no longer embeds
//! `pg_notify` on the write path. Instead the spawner, dispatcher and poller
//! report readiness here from an es-entity *post-commit* hook, and one task
//! emits at most one `pg_notify` per debounce window on its own connection
//! with `synchronous_commit = off`.
//!
//! This is safe because `execution_ready` is a hint, not a fact: the listener
//! collapses any burst into a single [`tokio::sync::Notify`] permit
//! (`tracker.rs`) and the poller re-polls on a `MAX_WAIT`-capped timer
//! regardless, so a lost hint costs pickup latency only -- never correctness.

use sqlx::postgres::PgPool;
use tokio::sync::mpsc;

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::entity::JobType;
use crate::handle::OwnedTaskHandle;
use crate::notification_router::{JOB_EVENTS_CHANNEL, JobNotification};
use crate::tracker::JobTracker;

/// How long the emitter folds reports before emitting.
///
/// Deliberately not configurable: this bounds added pickup latency, and every
/// value in a sane range is negligible against the poller's dequeue latency.
/// Matches the sibling emitter in `obix`.
const NOTIFY_DEBOUNCE: Duration = Duration::from_millis(25);

/// The in-process poller to wake directly, registered when polling starts.
///
/// Most spawns happen in the process that will also run the job, so poking the
/// local tracker means the NOTIFY only ever has to reach *other* processes.
/// `job_types` mirrors the membership test the listener applies, so a local
/// poller is not woken for a type it does not serve.
struct LocalPoller {
    tracker: Arc<JobTracker>,
    job_types: Vec<JobType>,
}

pub(crate) struct ExecutionReadyNotifier {
    tx: mpsc::UnboundedSender<JobType>,
    local: OnceLock<LocalPoller>,
    _handle: OwnedTaskHandle,
}

impl ExecutionReadyNotifier {
    pub fn spawn(pool: &PgPool) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(Self::run(pool.clone(), rx));
        Arc::new(Self {
            tx,
            local: OnceLock::new(),
            _handle: OwnedTaskHandle::new(handle),
        })
    }

    /// Register the in-process poller so same-process spawns skip the round trip.
    pub fn register_local_poller(&self, tracker: Arc<JobTracker>, job_types: Vec<JobType>) {
        let _ = self.local.set(LocalPoller { tracker, job_types });
    }

    /// Report that a job of `job_type` is (or became) ready to run.
    ///
    /// Infallible and non-blocking so it is safe to call from `post_commit`.
    pub fn execution_ready(&self, job_type: &JobType) {
        if let Some(local) = self.local.get()
            && local.job_types.contains(job_type)
        {
            local.tracker.job_execution_inserted();
        }
        // Unbounded: a sync post-commit hook must never block on the emitter.
        let _ = self.tx.send(job_type.clone());
    }

    /// Fold reports into one set per window and emit. A failed emit keeps the
    /// fold and retries on the next window; exits when all senders drop.
    async fn run(pool: PgPool, mut rx: mpsc::UnboundedReceiver<JobType>) {
        let mut pending: HashSet<JobType> = HashSet::new();
        loop {
            if pending.is_empty() {
                match rx.recv().await {
                    Some(job_type) => {
                        pending.insert(job_type);
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

fn drain(rx: &mut mpsc::UnboundedReceiver<JobType>, pending: &mut HashSet<JobType>) {
    while let Ok(job_type) = rx.try_recv() {
        pending.insert(job_type);
    }
}

/// Emit every pending job type in a *single* statement, so one debounce window
/// costs exactly one notify-bearing commit no matter how many types it covers.
///
/// `set_config(.., is_local => true)` scopes `synchronous_commit = off` to this
/// transaction. `synchronous_commit` is read at commit time, so the setting
/// applies regardless of target-list evaluation order -- which is the point: it
/// takes the WAL flush out of the notify lock's hold window.
async fn emit(pool: &PgPool, pending: &HashSet<JobType>) -> Result<(), sqlx::Error> {
    let payloads: Vec<String> = pending.iter().map(execution_ready_payload).collect();
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

/// Serialized through the same enum the listener parses, so the wire format
/// cannot drift between emitter and receiver.
pub(crate) fn execution_ready_payload(job_type: &JobType) -> String {
    serde_json::to_string(&JobNotification::ExecutionReady {
        job_type: job_type.to_string(),
    })
    .expect("Could not serialize job notification payload")
}

#[tracing::instrument(
    name = "job.notifier.emit_failed",
    level = "warn",
    skip_all,
    fields(error = %error),
)]
fn record_notify_emit_failed(error: &sqlx::Error) {}

/// Post-commit hook that reports `execution_ready` once the writer's
/// transaction has actually committed.
///
/// Multiple registrations on one operation merge into a single hook, so a bulk
/// spawn reports one entry per distinct job type rather than one per row.
pub(crate) struct ExecutionReadyHook {
    notifier: Arc<ExecutionReadyNotifier>,
    job_types: HashSet<JobType>,
}

impl es_entity::operation::hooks::CommitHook for ExecutionReadyHook {
    fn post_commit(self) {
        for job_type in &self.job_types {
            self.notifier.execution_ready(job_type);
        }
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.job_types.extend(other.job_types.drain());
        true
    }
}

/// Arrange for `job_type` to be reported ready once `op` commits.
///
/// Falls back to an in-transaction `pg_notify` when the operation has no
/// commit-hook buffer (a bare `sqlx::Transaction`), so hook-less callers keep
/// today's wake-up latency at today's cost rather than silently degrading to
/// the `MAX_WAIT` backstop.
pub(crate) async fn notify_ready_on_commit(
    notifier: &Arc<ExecutionReadyNotifier>,
    op: &mut impl es_entity::AtomicOperation,
    job_type: &JobType,
) -> Result<(), sqlx::Error> {
    let hook = ExecutionReadyHook {
        notifier: Arc::clone(notifier),
        job_types: HashSet::from([job_type.clone()]),
    };

    if op.add_commit_hook(hook).is_err() {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(JOB_EVENTS_CHANNEL)
            .bind(execution_ready_payload(job_type))
            .execute(op.as_executor())
            .await?;
    }

    Ok(())
}
