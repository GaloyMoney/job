//! Per-process debounced emitter for the `job_events` channel.
//!
//! Notify-bearing commits serialize on a cluster-wide lock, so nothing writes
//! `pg_notify` on the write path; reports arrive from a post-commit hook and
//! one task emits at most one notification per debounce window. Safe because a
//! notification is only an optimisation: `execution_ready` is backstopped by
//! the poller's re-poll and `job_terminal` by the waiter-manager's sweep.

use es_entity::AtomicOperation;
use sqlx::postgres::PgPool;
use tokio::sync::{broadcast, mpsc};

use std::collections::{HashMap, HashSet};
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
        op: &mut (impl AtomicOperation + ?Sized),
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
        op: &mut (impl AtomicOperation + ?Sized),
        job_id: JobId,
    ) -> Result<(), sqlx::Error> {
        self.notify_in_op(op, JobNotification::JobTerminal { job_id })
            .await
    }

    /// Falls back to an in-transaction `pg_notify` when the operation has no
    /// commit-hook buffer, where `post_commit` would never run.
    async fn notify_in_op(
        self: &Arc<Self>,
        mut op: &mut (impl AtomicOperation + ?Sized),
        notification: JobNotification,
    ) -> Result<(), sqlx::Error> {
        let serialized = payload(&notification);
        let hook = NotifierHook {
            notifier: Arc::clone(self),
            notifications: HashSet::from([notification]),
            added: HashMap::new(),
            claimed: HashMap::new(),
            forces: HashSet::new(),
        };

        if (&mut op).add_commit_hook(hook).is_err() {
            sqlx::query("SELECT pg_notify($1, $2)")
                .bind(JOB_EVENTS_CHANNEL)
                .bind(serialized)
                .execute(op.as_executor())
                .await?;
        }

        Ok(())
    }

    /// Registers one phase's contribution to the execution-ready netting for
    /// a spawn/claim commit pass (Fix 3, sb-max8): `added` names due-now
    /// landed-pending row ids this pass inserted, per type; `claimed` names
    /// row ids `ClaimHook` actually claimed, per type; `forces` names types
    /// that always notify regardless of coverage (a not-yet-due landed row,
    /// a promoted sibling, a pinned pending occupant a concurrent poll had
    /// to skip). See [`NotifierHook`]'s doc comment for the merge/decision
    /// semantics -- this is the same hook `execution_ready_in_op`/
    /// `job_terminal_in_op` register, carrying the netting inputs instead of
    /// (or alongside) a pre-decided notification.
    ///
    /// A no-op if all three are empty. `add_commit_hook` can only fail if
    /// `op` carries no commit-hook buffer at all, which cannot happen when
    /// called (as every call site is) from inside another hook's own
    /// `pre_commit` -- logged rather than force-executed if it ever does:
    /// forcing this one inline would fire (or skip) a notify with no
    /// suppression pass ever running to net it against, which is worse than
    /// simply not registering.
    pub(crate) fn register_execution_ready_in_op(
        self: &Arc<Self>,
        mut op: &mut (impl AtomicOperation + ?Sized),
        added: HashMap<JobType, HashSet<JobId>>,
        claimed: HashMap<JobType, HashSet<JobId>>,
        forces: HashSet<JobType>,
    ) {
        if added.is_empty() && claimed.is_empty() && forces.is_empty() {
            return;
        }
        let hook = NotifierHook {
            notifier: Arc::clone(self),
            notifications: HashSet::new(),
            added,
            claimed,
            forces,
        };
        if (&mut op).add_commit_hook(hook).is_err() {
            tracing::error!(
                "execution-ready netting could not register its commit hook; \
                 its contribution is dropped rather than fired unsuppressed or \
                 silently swallowed -- the ordinary poll still covers the type"
            );
        }
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
///
/// Two shapes of contribution share this one hook type, deliberately
/// collapsed rather than kept as separate hooks (a `pre_commit`-only
/// netting hook staging a `post_commit`-only delivery hook is one layer of
/// indirection this crate doesn't need):
/// - **Pre-decided** (`notifications`): `execution_ready_in_op`/
///   `job_terminal_in_op` already know exactly what to fire -- these go
///   straight into `notifications`.
/// - **Netted** (`added`/`claimed`/`forces`, via
///   [`JobEventNotifier::register_execution_ready_in_op`]): a spawn/claim
///   commit pass's `ExecutionInsertHook`/`ClaimHook` contribute per-type
///   due-now row ids re-entrantly; a type is notify-worthy iff it's in
///   `forces`, or some id in `added` for that type is NOT in `claimed` for
///   that type (exact per-row coverage, not a count -- `ClaimHook` always
///   claims a type's OLDEST due row, which can be pre-existing backlog
///   rather than one of `added`'s ids).
///
/// Both shapes resolve into the SAME delivery, and resolving them is pure,
/// infallible, DB-free computation -- so `pre_commit` is left at its
/// [`CommitHook`] default (a no-op that only exists to be gated by
/// [`Self::runs_after`], closing the merge window at the right point in the
/// pass) and everything, netting included, happens in [`Self::post_commit`].
///
/// [`CommitHook`]: es_entity::operation::hooks::CommitHook
pub(crate) struct NotifierHook {
    notifier: Arc<JobEventNotifier>,
    notifications: HashSet<JobNotification>,
    added: HashMap<JobType, HashSet<JobId>>,
    claimed: HashMap<JobType, HashSet<JobId>>,
    forces: HashSet<JobType>,
}

impl NotifierHook {
    /// [`CommitHook::runs_after`]'s dependency list. Declared unconditionally
    /// for every instance -- including a plain pre-decided registration with
    /// no netting data -- per the `es_entity` convention that all instances
    /// of one logical hook return the same list; over-declaring costs
    /// nothing when the named types never register or have already run.
    ///
    /// [`CommitHook::runs_after`]: es_entity::operation::hooks::CommitHook::runs_after
    const RUNS_AFTER: [std::any::TypeId; 3] = [
        std::any::TypeId::of::<crate::execution_hooks::ExecutionInsertHook>(),
        std::any::TypeId::of::<crate::execution_hooks::PromoteHeadsHook>(),
        std::any::TypeId::of::<crate::poller::ClaimHook>(),
    ];
}

impl es_entity::operation::hooks::CommitHook for NotifierHook {
    fn post_commit(self) {
        let mut notifications = self.notifications;
        for job_type in self.forces {
            notifications.insert(JobNotification::ExecutionReady {
                job_type: job_type.to_string(),
            });
        }
        for (job_type, added_ids) in self.added {
            let covered = self
                .claimed
                .get(&job_type)
                .is_some_and(|claimed_ids| added_ids.is_subset(claimed_ids));
            if !covered {
                notifications.insert(JobNotification::ExecutionReady {
                    job_type: job_type.to_string(),
                });
            }
        }
        for notification in notifications {
            self.notifier.notify(notification);
        }
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.notifications.extend(other.notifications.drain());
        for (job_type, ids) in other.added.drain() {
            self.added.entry(job_type).or_default().extend(ids);
        }
        for (job_type, ids) in other.claimed.drain() {
            self.claimed.entry(job_type).or_default().extend(ids);
        }
        self.forces.extend(other.forces.drain());
        true
    }

    fn runs_after(&self) -> &[std::any::TypeId] {
        &Self::RUNS_AFTER
    }
}
