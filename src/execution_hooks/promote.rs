use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use es_entity::operation::hooks::{CommitHook, HookOperation, PreCommitRet};

use crate::entity::JobType;
use crate::notifier::JobEventNotifier;

/// THE Invariant-B repair point (a queue's active row is its
/// min-`(execute_at, id)` live-or-parked row), covering the two ways a write
/// can leave that invariant broken:
///
/// - **`ids`** -- rows a caller just moved to `pending` (retry backoff,
///   voluntary reschedule, or a bulk reclaim sweep): promote an older parked
///   sibling ahead of them wherever one exists and is older -- swap them
///   (the newly-pending row → `parked`, the sibling → `pending`).
/// - **`freed_queues`** -- queues whose active row a completer just deleted
///   ([`crate::dispatcher::JobDispatcher::delete_execution_in_op`] and
///   `batch_dispatcher.rs`'s two completers): promote each one's oldest
///   parked sibling outright. This runs here, as a commit-hook statement
///   strictly AFTER the deleting statement, and never as a CTE of the
///   `DELETE` itself, for a snapshot-visibility reason: the `DELETE` blocks
///   on any in-flight spawn's `FOR KEY SHARE` occupant pin (`super::insert`'s
///   `lock_queue_occupants`), so by the time it returns, every spawn that
///   parked a row behind the occupant has committed -- but under READ
///   COMMITTED a statement that blocks on a row lock resumes with its
///   ORIGINAL snapshot, re-checking nothing beyond the conflicting row
///   itself (EvalPlanQual). A parked-sibling scan inside the SAME statement
///   therefore runs with a snapshot from before the pinning spawn committed,
///   sees no parked row, and orphans the queue until
///   `sweep_orphaned_parked_rows`, up to `job_lost_interval / 2` later. Only
///   a NEW statement -- this hook's -- gets a snapshot that sees the freshly
///   committed parked row. Pinned end-to-end by
///   `tests/parked_rows.rs::completion_blocked_on_a_spawn_pin_promotes_the_parked_row`.
///
/// Called either directly via [`Self::apply`] (when the caller has no
/// commit-hook buffer to register into, or needs the promoted rows back
/// synchronously within its own `pre_commit`) or via a *registered*
/// `PromoteHeadsHook` (when promotion is the entire unit of work for a
/// call).
///
/// Multiple registrations on one `op` merge (`own_types` unions, `ids` and
/// `freed_queues` concatenate) via [`Self::merge`], so e.g. a batch's retry
/// backoffs and its terminal deletes sharing one transaction promote in one
/// hook execution and notify their combined set of types once each. The
/// `ids` swap and the `freed_queues` promote stay two statements (each
/// skipped when its input is empty -- in practice an op carries one kind,
/// so it is one statement): fusing them would weave a second promotion
/// source into the swap CTE's delicate lock-order/demote-before-promote
/// reasoning to save a round trip only for ops that carry both.
///
/// **Notify policy**: always notifies every registered `own_types` entry,
/// plus every DISTINCT promoted type not already in it. This is
/// deliberately generous: after a merge there is no way to attribute "this
/// promoted type corresponds to THAT specific registration's own row", so
/// the only sound policy across merged registrations is "notify everything
/// that could plausibly have new work" -- a redundant notify is harmless
/// (the emitter coalesces, and an empty poll costs one query), a missed one
/// is not. Completers register with EMPTY `own_types`, preserving their
/// long-standing behavior of waking only the PROMOTED row's type, never
/// their own type on every completion.
pub(crate) struct PromoteHeadsHook {
    pub(crate) notifier: Arc<JobEventNotifier>,
    pub(crate) own_types: HashSet<JobType>,
    pub(crate) ids: Vec<uuid::Uuid>,
    pub(crate) freed_queues: Vec<String>,
}

/// One sibling promoted by [`PromoteHeadsHook::apply`]: its type (for
/// notify) and its own `execute_at`, unchanged by the promote (for callers
/// that need to know whether it is ACTUALLY due, not merely promoted --
/// see `super::insert::ExecutionInsertHook::due_now_by_type`).
pub(crate) struct PromotedRow {
    pub job_type: String,
    pub execute_at: DateTime<Utc>,
}

impl PromoteHeadsHook {
    /// The freed-queue promote statement: each freed queue's oldest parked
    /// sibling goes to `pending`, by the same (execute_at, id) tiebreak the
    /// claim query and [`Self::apply`] use -- ordering by execute_at alone
    /// would let this promote a different row, and so a different TYPE, than
    /// every peer would independently agree is the head. See the type-level
    /// docs for why this is its own statement, never a CTE of the completer's
    /// `DELETE`.
    ///
    /// The `NOT EXISTS` active-row guard makes the promote self-verifying
    /// rather than trusting the registrant's "I just deleted the queue's
    /// only active row" (Invariant A): if an active row exists after all,
    /// promoting a sibling would fail `idx_job_executions_queue_active`
    /// outright, so the guard skips a queue that needs no promotion instead
    /// of erroring.
    async fn apply_freed(
        op: &mut impl AtomicOperation,
        queue_ids: &[String],
    ) -> Result<Vec<PromotedRow>, sqlx::Error> {
        let deduped: Vec<String> = queue_ids
            .iter()
            .cloned()
            .collect::<HashSet<String>>()
            .into_iter()
            .collect();
        if deduped.is_empty() {
            return Ok(Vec::new());
        }
        sqlx::query_as!(
            PromotedRow,
            r#"
            WITH heads AS (
                -- At most one row per input queue (the LATERAL's LIMIT 1
                -- against deduped input), so no DISTINCT is needed.
                SELECT p.id
                FROM UNNEST($1::text[]) AS q(queue_id)
                CROSS JOIN LATERAL (
                    SELECT id FROM job_executions
                    WHERE state = 'parked' AND queue_id = q.queue_id
                    ORDER BY execute_at, id
                    LIMIT 1
                ) p
                WHERE NOT EXISTS (
                    SELECT 1 FROM job_executions a
                    WHERE a.queue_id = q.queue_id AND a.state IN ('pending', 'running')
                )
            )
            UPDATE job_executions je SET state = 'pending'
            FROM heads h WHERE je.id = h.id
            RETURNING je.job_type, je.execute_at AS "execute_at!"
            "#,
            &deduped,
        )
        .fetch_all(op.as_executor())
        .await
    }

    /// The swap statement itself. Set-based so one statement covers
    /// everything from a single-row retry to a bulk batch reschedule or
    /// reclaim sweep. Callers pass only the ids they just moved to
    /// `pending` -- a row this didn't touch is left alone even if it
    /// happens to belong to a queue with parked siblings (nothing changed
    /// for it, so there is nothing to fix).
    ///
    /// Every row this touches is locked up front by the `locked` CTE, in id
    /// order, matching every other multi-row locker of this table
    /// (`execution_hooks::insert`'s `lock_queue_occupants`,
    /// `batch_dispatcher.rs`'s two completers). Without it the `demote` and
    /// promote `UPDATE`s acquire in planner-determined scan order, so two
    /// concurrent multi-row callers -- two overlapping `reclaim_lost_jobs`
    /// sweeps, or a batch reschedule racing a reclaim -- could each hold a row
    /// the other wants. `FOR NO KEY UPDATE` rather than `FOR UPDATE` because
    /// it is what these `state`-only `UPDATE`s already take, and because it
    /// leaves a spawn's `FOR KEY SHARE` on the same row unblocked; `FOR
    /// UPDATE` would make this statement serialize concurrent spawns into the
    /// queue for no benefit.
    ///
    /// Returns the job type AND `execute_at` of every promoted sibling, so
    /// callers can wake the pollers that actually cover it -- a sibling can
    /// be a different type than the row it displaced (one `queue_id` can be
    /// shared across types), so notifying only the caller's own type would
    /// miss it. `execute_at` is unchanged by this statement (only `state`
    /// is set) -- it lets callers gate claim demand on whether a promoted
    /// row is ACTUALLY due, not merely promoted (a promoted row's own
    /// `execute_at` can be in the future).
    pub(crate) async fn apply(
        op: &mut impl AtomicOperation,
        ids: &[uuid::Uuid],
    ) -> Result<Vec<PromotedRow>, sqlx::Error> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        sqlx::query_as!(
            PromotedRow,
            r#"
            WITH candidates AS (
                SELECT je.id, je.queue_id, je.execute_at
                FROM job_executions je
                WHERE je.id = ANY($1) AND je.state = 'pending' AND je.queue_id IS NOT NULL
            ), swaps AS (
                SELECT c.id AS pending_id, sib.id AS parked_id
                FROM candidates c
                CROSS JOIN LATERAL (
                    SELECT id, execute_at FROM job_executions
                    WHERE state = 'parked' AND queue_id = c.queue_id
                    ORDER BY execute_at, id
                    LIMIT 1
                ) sib
                WHERE (sib.execute_at, sib.id) < (c.execute_at, c.id)
            ), locked AS MATERIALIZED (
                -- Take BOTH sides of every swap in id order, in one pass,
                -- before either UPDATE below runs. `demote` reads from this
                -- (not from `swaps`) purely to force that dependency. See the
                -- doc comment for why the order is load-bearing; the strength
                -- is exactly what the two writes below would take anyway --
                -- `state` is no index's key column -- so this changes lock
                -- ORDER only, never what conflicts with what.
                SELECT je.id FROM job_executions je
                WHERE je.id IN (
                    SELECT pending_id FROM swaps UNION SELECT parked_id FROM swaps
                )
                ORDER BY je.id
                FOR NO KEY UPDATE
            ), demote AS (
                UPDATE job_executions SET state = 'parked'
                WHERE id IN (
                    SELECT s.pending_id FROM swaps s JOIN locked l ON l.id = s.pending_id
                )
                RETURNING id
            )
            -- The promote UPDATE reads FROM `demote` (not `swaps`) so Postgres
            -- has a real data dependency forcing `demote` to run to completion
            -- first. Without it, this is two independent writes to the same
            -- table within one statement with no ordering guarantee between
            -- them, which can transiently make two rows active for one queue
            -- within the statement's own execution and violate
            -- `idx_job_executions_queue_active`.
            UPDATE job_executions je SET state = 'pending'
            FROM swaps s
            JOIN demote d ON d.id = s.pending_id
            WHERE je.id = s.parked_id
            RETURNING je.job_type, je.execute_at AS "execute_at!"
            "#,
            ids,
        )
        .fetch_all(op.as_executor())
        .await
    }

    /// Builds and registers a `PromoteHeadsHook` for `ids`, falling back to
    /// immediate execution if `op` carries no commit-hook buffer -- the
    /// promote (and its notify) must not be silently dropped either way, so
    /// callers never need their own fallback branch.
    pub(crate) async fn register(
        op: &mut impl AtomicOperation,
        notifier: &Arc<JobEventNotifier>,
        own_types: impl IntoIterator<Item = JobType>,
        ids: Vec<uuid::Uuid>,
    ) -> Result<(), sqlx::Error> {
        Self::register_hook(
            op,
            PromoteHeadsHook {
                notifier: Arc::clone(notifier),
                own_types: own_types.into_iter().collect(),
                ids,
                freed_queues: Vec::new(),
            },
        )
        .await
    }

    /// Builds and registers a `PromoteHeadsHook` for queues whose active row
    /// the caller just deleted, with the same no-hook-buffer fallback as
    /// [`Self::register`]. `own_types` is empty on purpose: a completer only
    /// ever wakes the PROMOTED row's type (see the type-level notify policy).
    pub(crate) async fn register_freed_queues(
        op: &mut impl AtomicOperation,
        notifier: &Arc<JobEventNotifier>,
        freed_queues: Vec<String>,
    ) -> Result<(), sqlx::Error> {
        if freed_queues.is_empty() {
            return Ok(());
        }
        Self::register_hook(
            op,
            PromoteHeadsHook {
                notifier: Arc::clone(notifier),
                own_types: HashSet::new(),
                ids: Vec::new(),
                freed_queues,
            },
        )
        .await
    }

    async fn register_hook(
        op: &mut impl AtomicOperation,
        hook: PromoteHeadsHook,
    ) -> Result<(), sqlx::Error> {
        if let Err(hook) = op.add_commit_hook(hook) {
            hook.force_execute_pre_commit(op).await?;
        }
        Ok(())
    }
}

impl CommitHook for PromoteHeadsHook {
    async fn pre_commit(
        self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        let mut promoted = Self::apply(&mut op, &self.ids).await?;
        promoted.extend(Self::apply_freed(&mut op, &self.freed_queues).await?);

        let mut notified: HashSet<String> = self.own_types.iter().map(|t| t.to_string()).collect();
        for job_type in &self.own_types {
            self.notifier
                .execution_ready_in_op(&mut op, job_type)
                .await?;
        }
        for row in promoted {
            if notified.insert(row.job_type.clone()) {
                self.notifier
                    .execution_ready_in_op(&mut op, &JobType::from_owned(row.job_type))
                    .await?;
            }
        }

        PreCommitRet::ok(self, op)
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.ids.append(&mut other.ids);
        self.freed_queues.append(&mut other.freed_queues);
        self.own_types.extend(other.own_types.drain());
        true
    }
}
