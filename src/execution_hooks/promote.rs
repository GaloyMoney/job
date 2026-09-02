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
///   (`finalizer.rs`'s terminal delete, serving completions and exhausted
///   retries for both dispatchers): promote each one's oldest
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

impl PromoteHeadsHook {
    /// [`CommitHook::runs_after`]'s dependency list -- see the hook-DAG note
    /// on [`crate::execution_hooks`] for the full picture. A hand-composed
    /// op that both spawns (registers `ExecutionInsertHook`) and promotes
    /// (registers this hook standalone -- e.g. a retry backoff sharing a
    /// transaction with an unrelated spawn) must see the spawn's rows before
    /// this hook looks for a parked sibling to swap; a freshly `parked` row
    /// from that spawn can be an older sibling this hook should have
    /// promoted instead.
    const RUNS_AFTER: [std::any::TypeId; 1] =
        [std::any::TypeId::of::<super::insert::ExecutionInsertHook>()];
}

pub(crate) struct PromotedRow {
    pub job_type: String,
    pub execute_at: Option<DateTime<Utc>>,
}

impl PromoteHeadsHook {
    /// The freed-queue promote statement: each freed queue's oldest parked
    /// sibling goes to `pending`, by the same (execute_at, id) tiebreak the
    /// claim query and [`Self::apply`] use
    ///
    /// The `NOT EXISTS` active-row guard makes the promote self-verifying
    /// rather than trusting the registrant's "I just deleted the queue's
    /// only active row" (Invariant A): if an active row exists after all,
    /// promoting a sibling would fail `idx_job_executions_queue_active`
    /// outright, so the guard skips a queue that needs no promotion instead
    /// of erroring.
    ///
    /// The final `UPDATE`'s `AND je.state = 'parked'` re-checks the SAME
    /// predicate `heads` established at snapshot time, on the UPDATE itself
    /// -- not only on `locked`'s lock-acquisition scan. Between `heads`'
    /// snapshot and `locked`'s `FOR NO KEY UPDATE` actually being granted, a
    /// concurrent claimer can promote this same row and claim it to
    /// `running` (nulling `execute_at`); under READ COMMITTED, once this
    /// statement's blocked lock acquisition unblocks, EvalPlanQual
    /// re-evaluates `locked`'s qual (an already-fixed `heads` id list, so
    /// the row still qualifies THERE) but the final `UPDATE`'s own qual is
    /// re-evaluated too -- and it is the only qual actually checked against
    /// the LATEST row version. Without `state = 'parked'` here, that
    /// re-evaluation trivially passes (`je.id = l.id` doesn't care what
    /// `state` is) and this statement blindly re-applies `state = 'pending'`
    /// over a row a concurrent claimer already promoted to `running`,
    /// returning its (`execute_at = NULL`) tuple -- which, absent a non-null
    /// decode assertion to abort the transaction, is a double-dispatch of an
    /// already-running row. Pinned by
    /// `tests::apply_freed_yields_to_a_concurrently_claimed_row`.
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
            ), locked AS MATERIALIZED (
                -- Lock every head in (queue_id, id) order before the UPDATE
                -- below touches any of them -- the same global order
                -- `lock_queue_occupants` and `Self::apply`'s own `locked` CTE
                -- use. A bare `UPDATE ... FROM heads` has no ordering
                -- guarantee of its own (`heads`'s row order is not a lock
                -- order), so a multi-queue batch completion freeing several
                -- queues here could otherwise acquire in planner/`UNNEST`
                -- order and deadlock against a concurrent spawn's pin or
                -- swap touching the same rows in the opposite order.
                SELECT je.id FROM job_executions je
                WHERE je.id IN (SELECT id FROM heads)
                ORDER BY je.queue_id, je.id
                FOR NO KEY UPDATE
            )
            UPDATE job_executions je SET state = 'pending'
            FROM locked l WHERE je.id = l.id AND je.state = 'parked'
            RETURNING je.job_type, je.execute_at AS "execute_at?"
            "#,
            &deduped,
        )
        .fetch_all(op.as_executor())
        .await
    }

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
                -- Take BOTH sides of every swap in (queue_id, id) order, in
                -- one pass, before either UPDATE below runs. `demote` reads
                -- from this (not from `swaps`) purely to force that
                -- dependency. See the doc comment for why the order is
                -- load-bearing; the strength is exactly what the two writes
                -- below would take anyway -- `state` is no index's key
                -- column -- so this changes lock ORDER only, never what
                -- conflicts with what. A swap's two rows always share one
                -- `queue_id` (the sibling lookup is `queue_id = c.queue_id`),
                -- so ordering by it groups each swap's pair together.
                SELECT je.id FROM job_executions je
                WHERE je.id IN (
                    SELECT pending_id FROM swaps UNION SELECT parked_id FROM swaps
                )
                ORDER BY je.queue_id, je.id
                FOR NO KEY UPDATE
            ), demote AS (
                UPDATE job_executions SET state = 'parked'
                WHERE id IN (
                    SELECT s.pending_id FROM swaps s JOIN locked l ON l.id = s.pending_id
                )
                AND state = 'pending'
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
            WHERE je.id = s.parked_id AND je.state = 'parked'
            RETURNING je.job_type, je.execute_at AS "execute_at?"
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

    fn runs_after(&self) -> &[std::any::TypeId] {
        &Self::RUNS_AFTER
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn init_pool() -> anyhow::Result<sqlx::PgPool> {
        let pg_con = std::env::var("PG_CON").unwrap();
        Ok(sqlx::PgPool::connect(&pg_con).await?)
    }

    async fn seed_job(
        pool: &sqlx::PgPool,
        job_type: &str,
        queue_id: &str,
        execute_at: DateTime<Utc>,
        state: &str,
    ) -> anyhow::Result<uuid::Uuid> {
        let id = uuid::Uuid::now_v7();
        sqlx::query(
            "INSERT INTO jobs (id, job_type, queue_id, created_at) VALUES ($1, $2, $3, NOW())",
        )
        .bind(id)
        .bind(job_type)
        .bind(queue_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, queue_id, state, attempt_index, execute_at, alive_at, created_at) \
             VALUES ($1, $2, $3, $4::JobExecutionState, 1, \
                     CASE WHEN $4 = 'running' THEN NULL ELSE $5 END, NOW(), NOW())",
        )
        .bind(id)
        .bind(job_type)
        .bind(queue_id)
        .bind(state)
        .bind(execute_at)
        .execute(pool)
        .await?;
        Ok(id)
    }

    /// `apply_freed`'s promote UPDATE must re-check `state` after its lock
    /// is granted. Forces the exact race: a holder transaction takes the
    /// row's lock first (mirroring a concurrent claimer that has already
    /// started), `apply_freed` blocks acquiring `locked`'s
    /// `FOR NO KEY UPDATE` on it, then the holder promotes-and-runs the row
    /// (nulling `execute_at`, exactly like a real claim) and commits --
    /// unblocking `apply_freed`. The re-checked predicate must make the
    /// statement affect zero rows for this id; without it the statement
    /// re-applies over the running row and panics the `execute_at!`
    /// non-null decode.
    #[tokio::test]
    async fn apply_freed_yields_to_a_concurrently_claimed_row() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("promote-race-{}", uuid::Uuid::now_v7());
        let queue = format!("promote-race-queue-{}", uuid::Uuid::now_v7());
        let claimer_instance = uuid::Uuid::now_v7();

        let head = seed_job(&pool, &job_type, &queue, chrono::Utc::now(), "parked").await?;

        let holder_pool = pool.clone();
        let holder = tokio::spawn(async move {
            let mut tx = holder_pool.begin().await?;
            // Takes the row lock immediately (mirrors a concurrent claimer
            // already mid-transaction when `apply_freed`'s snapshot runs).
            sqlx::query("UPDATE job_executions SET state = 'pending' WHERE id = $1")
                .bind(head)
                .execute(&mut *tx)
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            // Claims it to `running`, exactly like a real claim query does.
            sqlx::query(
                "UPDATE job_executions SET state = 'running', poller_instance_id = $2, \
                 execute_at = NULL WHERE id = $1",
            )
            .bind(head)
            .bind(claimer_instance)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok::<_, sqlx::Error>(())
        });

        // Gives the holder time to take the lock before `apply_freed` runs,
        // so its own `locked` CTE is the one left blocked.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut op = pool.begin().await?;
        let promoted = PromoteHeadsHook::apply_freed(&mut op, std::slice::from_ref(&queue)).await?;
        op.commit().await?;
        holder.await??;

        assert!(
            promoted.is_empty(),
            "the raced-away row must not be reported as promoted: {}",
            promoted.len()
        );

        let (state, poller_instance_id): (String, Option<uuid::Uuid>) = sqlx::query_as(
            "SELECT state::text, poller_instance_id FROM job_executions WHERE id = $1",
        )
        .bind(head)
        .fetch_one(&pool)
        .await?;
        assert_eq!(state, "running", "the concurrent claim must stand");
        assert_eq!(
            poller_instance_id,
            Some(claimer_instance),
            "the row must remain owned by the instance that actually claimed it"
        );

        Ok(())
    }

    /// `apply`'s `demote` CTE must re-check `state` when it re-locks a
    /// `pending` swap candidate. Without that, a concurrent claimer that
    /// promoted the same candidate to `running` between `candidates`'
    /// snapshot and `locked`'s lock gets silently demoted back to `parked`
    /// -- worse than `apply_freed`'s decode error, since nothing surfaces
    /// it: the candidate is not the RETURNING column that carries
    /// `execute_at`.
    #[tokio::test]
    async fn apply_demote_yields_to_a_concurrently_claimed_pending_row() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("promote-demote-race-{}", uuid::Uuid::now_v7());
        let queue = format!("promote-demote-race-queue-{}", uuid::Uuid::now_v7());
        let claimer_instance = uuid::Uuid::now_v7();
        let now = chrono::Utc::now();

        // The active (pending) occupant `apply` will try to demote in favor
        // of the older parked sibling below.
        let candidate = seed_job(&pool, &job_type, &queue, now, "pending").await?;
        // An older parked sibling -- older `execute_at` makes it the swap
        // target ahead of `candidate`.
        let _sibling = seed_job(
            &pool,
            &job_type,
            &queue,
            now - chrono::Duration::seconds(60),
            "parked",
        )
        .await?;

        let holder_pool = pool.clone();
        let holder = tokio::spawn(async move {
            let mut tx = holder_pool.begin().await?;
            // Re-takes the already-`pending` candidate's lock (mirrors a
            // concurrent claimer already mid-transaction).
            sqlx::query("UPDATE job_executions SET execute_at = execute_at WHERE id = $1")
                .bind(candidate)
                .execute(&mut *tx)
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            // Claims it to `running`, exactly like a real claim query does.
            sqlx::query(
                "UPDATE job_executions SET state = 'running', poller_instance_id = $2, \
                 execute_at = NULL WHERE id = $1",
            )
            .bind(candidate)
            .bind(claimer_instance)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok::<_, sqlx::Error>(())
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut op = pool.begin().await?;
        let promoted = PromoteHeadsHook::apply(&mut op, &[candidate]).await?;
        op.commit().await?;
        holder.await??;

        assert!(
            promoted.is_empty(),
            "no swap must be reported once the candidate raced away to running"
        );

        let candidate_state: String =
            sqlx::query_scalar("SELECT state::text FROM job_executions WHERE id = $1")
                .bind(candidate)
                .fetch_one(&pool)
                .await?;
        assert_eq!(
            candidate_state, "running",
            "the concurrently claimed row must NOT be demoted back to parked"
        );

        Ok(())
    }
}
