use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use es_entity::operation::hooks::{CommitHook, HookOperation, PreCommitRet};

use crate::entity::JobType;
use crate::notifier::JobEventNotifier;

/// Restores Invariant B (a queue's active row is its min-`(execute_at, id)`
/// live-or-parked row) for a set of rows a caller just moved to `pending`
/// (retry backoff, voluntary reschedule, or a bulk reclaim sweep), promoting
/// an older parked sibling ahead of them wherever one exists and is older:
/// swap them (the newly-pending row → `parked`, the sibling → `pending`).
///
/// Called either directly via [`Self::apply`] (when the caller has no
/// commit-hook buffer to register into, or needs the promoted rows back
/// synchronously within its own `pre_commit`) or via a *registered*
/// `PromoteHeadsHook` (when promotion is the entire unit of work for a
/// call).
///
/// Multiple registrations on one `op` merge (`own_types` unions, `ids`
/// concatenates) via [`Self::merge`], so e.g. a retry and a reschedule
/// sharing a hand-composed transaction promote in ONE statement and notify
/// their combined set of types once each.
///
/// **Notify policy**: always notifies every registered `own_types` entry,
/// plus every DISTINCT promoted type not already in it. This is
/// deliberately generous: after a merge there is no way to attribute "this
/// promoted type corresponds to THAT specific registration's own row", so
/// the only sound policy across merged registrations is "notify everything
/// that could plausibly have new work" -- a redundant notify is harmless
/// (the emitter coalesces, and an empty poll costs one query), a missed one
/// is not.
pub(crate) struct PromoteHeadsHook {
    pub(crate) notifier: Arc<JobEventNotifier>,
    pub(crate) own_types: HashSet<JobType>,
    pub(crate) ids: Vec<uuid::Uuid>,
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
    /// The swap statement itself. Set-based so one statement covers
    /// everything from a single-row retry to a bulk batch reschedule or
    /// reclaim sweep. Callers pass only the ids they just moved to
    /// `pending` -- a row this didn't touch is left alone even if it
    /// happens to belong to a queue with parked siblings (nothing changed
    /// for it, so there is nothing to fix).
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
            ), demote AS (
                UPDATE job_executions SET state = 'parked'
                WHERE id IN (SELECT pending_id FROM swaps)
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
        let hook = PromoteHeadsHook {
            notifier: Arc::clone(notifier),
            own_types: own_types.into_iter().collect(),
            ids,
        };
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
        let promoted = Self::apply(&mut op, &self.ids).await?;

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
        self.own_types.extend(other.own_types.drain());
        true
    }
}
