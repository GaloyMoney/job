//! Commit hooks that do real DB work in `pre_commit` (handoff addendum §8):
//! [`PromoteHeadsHook`] and [`ExecutionInsertHook`]. Both centralize SQL that
//! used to run inline at several call sites into one place per concern, and
//! both stage further hooks re-entrantly (`JobEventHook` for notify,
//! [`crate::poller::JobPoller::register_claim_demand`] for the head-swap
//! claim) rather than doing dispatch/claim work inline.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use es_entity::clock::ClockHandle;
use es_entity::operation::hooks::{CommitHook, HookOperation, PreCommitRet};

use crate::JobId;
use crate::entity::JobType;
use crate::notifier::JobEventNotifier;
use crate::poller::PollerHandle;

/// Restores Invariant B (a queue's active row is its min-`(execute_at, id)`
/// live-or-parked row) for a set of rows a caller just moved to `pending`
/// (retry backoff, voluntary reschedule, or a bulk reclaim sweep), promoting
/// an older parked sibling ahead of them wherever one exists and is older:
/// swap them (the newly-pending row → `parked`, the sibling → `pending`).
///
/// Replaces the free-standing `swap_older_parked_siblings_in_op` fn from an
/// earlier revision of this design -- identical SQL, now [`Self::apply`],
/// called either directly (by `reclaim_lost_jobs`'s raw pool transaction,
/// which has no commit-hook buffer to register into, and by
/// `ExecutionInsertHook::pre_commit` once it lands, as a same-pass
/// sequential step rather than a competing hook) or via a *registered*
/// `PromoteHeadsHook` (the retry/reschedule call sites, where promotion is
/// the entire unit of work for that `_in_op` call).
///
/// Multiple registrations on one `op` merge (`own_types` unions, `ids`
/// concatenates) via [`Self::merge`], so e.g. a retry and a reschedule
/// sharing a hand-composed transaction promote in ONE statement and notify
/// their combined set of types once each -- centralizing the notify-dedup
/// that used to be hand-rolled (inconsistently -- see below) at each call
/// site.
///
/// **Notify policy**: always notifies every registered `own_types` entry,
/// plus every DISTINCT promoted type not already in it. The two single-row
/// call sites in `dispatcher.rs` (`fail_job`'s retry branch, `reschedule_job`)
/// used to notify their own type ONLY when nothing promoted (reasoning: if
/// their one row got swapped back to `parked`, their own type has nothing
/// newly claimable). Centralizing drops that precision in favor of the
/// batch call sites' existing, safe over-notification: after a merge there
/// is no way to attribute "this promoted type corresponds to THAT specific
/// registration's own row", so the only sound policy across merged
/// registrations is "notify everything that could plausibly have new work" --
/// a redundant notify is harmless (the emitter coalesces, and an empty poll
/// costs one query), a missed one is not.
pub(crate) struct PromoteHeadsHook {
    pub(crate) notifier: Arc<JobEventNotifier>,
    pub(crate) own_types: HashSet<JobType>,
    pub(crate) ids: Vec<uuid::Uuid>,
}

impl PromoteHeadsHook {
    /// The swap statement itself. Set-based so one statement covers
    /// everything from a single-row retry to a bulk batch reschedule or
    /// reclaim sweep. Callers pass only the ids they just moved to
    /// `pending` -- a row this didn't touch is left alone even if it
    /// happens to belong to a queue with parked siblings (nothing changed
    /// for it, so there is nothing to fix).
    ///
    /// Returns the job type of every promoted sibling, so callers can wake
    /// the pollers that actually cover it -- a sibling can be a different
    /// type than the row it displaced (one `queue_id` can be shared across
    /// types), so notifying only the caller's own type would miss it,
    /// exactly the failure mode `delete_execution_in_op`'s `next_in_queue`
    /// resolution exists to prevent on the completion path.
    pub(crate) async fn apply(
        op: &mut impl AtomicOperation,
        ids: &[uuid::Uuid],
    ) -> Result<Vec<String>, sqlx::Error> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        sqlx::query_scalar!(
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
            -- them -- observed as a live, reproducible unique-violation on
            -- `idx_job_executions_queue_active` when the promote half committed
            -- before the demote half, transiently making two rows active for
            -- one queue within the statement's own execution.
            UPDATE job_executions je SET state = 'pending'
            FROM swaps s
            JOIN demote d ON d.id = s.pending_id
            WHERE je.id = s.parked_id
            RETURNING je.job_type
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
        for promoted_type in promoted {
            if notified.insert(promoted_type.clone()) {
                self.notifier
                    .execution_ready_in_op(&mut op, &JobType::from_owned(promoted_type))
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

/// One `job_executions` row to insert, as gathered by [`ExecutionInsertHook`].
/// `unique_key` is deliberately absent -- keyed and bulk/single spawning are
/// disjoint APIs (keyed spawn's own conflict resolution needs the row back
/// in the SAME round trip to resolve a live-key race, so it stays inline;
/// see `keyed.rs::KeyedJobSpawner::spawn`), so nothing ever registers this
/// hook with a `unique_key`'d row.
pub(crate) struct NewExecutionRow {
    pub id: JobId,
    pub job_type: JobType,
    pub schedule_at: DateTime<Utc>,
    pub queue_id: Option<String>,
}

/// One row returned by [`ExecutionInsertHook`]'s insert statement:
/// `landed_pending` says which outcome it got, `occupant_id` (only ever
/// `Some` for a parked row) names the queue's current occupant, if any, that
/// [`PromoteHeadsHook`] should re-check for a swap.
struct InsertedRow {
    id: JobId,
    landed_pending: bool,
    occupant_id: Option<uuid::Uuid>,
}

/// The insert half of the head-swap kernel (handoff addendum §8.2a): batches
/// every `spawn_in_op`/`spawn_all_in_op`/resident-spawn insert registered on
/// one `op` into ONE statement at commit time, instead of each call running
/// its own (for the pre-existing single-row path, up to four-round-trip)
/// insert inline. [`Self::merge`] is what makes this the "batch merge
/// semantics" win: N `spawn_in_op` calls sharing one transaction get one
/// multi-row insert, without the caller ever calling `spawn_all`.
///
/// `pre_commit` does three things, in order:
/// 1. One combined `INSERT ... ON CONFLICT DO NOTHING` / fallback-`INSERT
///    'parked'` statement for every row (queued and unqueued alike) --
///    [`Self::insert_many`].
/// 2. If anything parked behind an occupant, [`PromoteHeadsHook::apply`] on
///    those occupant ids, as a same-pass sequential step (not a competing
///    hook -- this ordering is already guaranteed by being sequential code
///    inside one `pre_commit`, independent of `PromoteHeadsHook`'s own
///    `runs_after`-based ordering guarantee for OTHER callers).
/// 3. Re-entrant registration (`op.add_commit_hook`, which this
///    `HookOperation` always supports -- see the [module docs on re-entrant
///    registration](es_entity::operation::hooks#re-entrant-registration)) of
///    a `JobEventHook` per type that landed pending or got promoted, and a
///    `ClaimHook` with `n_due` = this pass's own due-now landed-pending count
///    per type.
pub(crate) struct ExecutionInsertHook {
    pub(crate) notifier: Arc<JobEventNotifier>,
    pub(crate) poller: PollerHandle,
    pub(crate) clock: ClockHandle,
    pub(crate) rows: Vec<NewExecutionRow>,
}

impl ExecutionInsertHook {
    /// Builds and registers an `ExecutionInsertHook` for one row, falling
    /// back to immediate execution if `op` carries no commit-hook buffer.
    /// The single-row spawn call sites' entry point.
    pub(crate) async fn register_one(
        op: &mut impl AtomicOperation,
        notifier: &Arc<JobEventNotifier>,
        poller: &PollerHandle,
        clock: &ClockHandle,
        row: NewExecutionRow,
    ) -> Result<(), sqlx::Error> {
        Self::register(op, notifier, poller, clock, vec![row]).await
    }

    /// Builds and registers an `ExecutionInsertHook` for `rows`, falling
    /// back to immediate execution if `op` carries no commit-hook buffer --
    /// the insert must not be silently dropped either way. A no-op if `rows`
    /// is empty (mirrors `spawn_all_in_op`'s existing empty-specs check).
    pub(crate) async fn register(
        op: &mut impl AtomicOperation,
        notifier: &Arc<JobEventNotifier>,
        poller: &PollerHandle,
        clock: &ClockHandle,
        rows: Vec<NewExecutionRow>,
    ) -> Result<(), sqlx::Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let hook = ExecutionInsertHook {
            notifier: Arc::clone(notifier),
            poller: Arc::clone(poller),
            clock: clock.clone(),
            rows,
        };
        if let Err(hook) = op.add_commit_hook(hook) {
            hook.force_execute_pre_commit(op).await?;
        }
        Ok(())
    }

    /// Statement 1: try every row as `pending`, `ON CONFLICT DO NOTHING`
    /// against the queue's active slot (Postgres evaluates the arbiter per
    /// row within one statement, so at most one row of a batch sharing a
    /// `queue_id` lands `pending` and the rest see the first as already
    /// occupying the slot -- same as the pre-existing bulk path); whichever
    /// didn't land lands `parked`. An unqueued row (`queue_id.is_none()`)
    /// can never conflict -- it always lands `pending`.
    ///
    /// For every parked row, also resolves `occupant_id`: the id CURRENTLY
    /// holding its queue's active slot, checked in this priority order --
    /// (a) a sibling FROM THIS SAME `rows` batch that won the slot (read
    /// from `ins`'s own `RETURNING`, so it reflects this statement's own
    /// writes), or (b) a pre-existing occupant (read via a plain scan of
    /// `job_executions`, which -- since none of this statement's own insert
    /// commands touch an EXISTING row -- sees exactly the statement-start snapshot,
    /// i.e. whoever held the slot before this call ran). At most one of (a)/
    /// (b) can ever match per queue (Invariant A), so the `COALESCE` of two
    /// scalar subqueries is safe. This is what lets a batch's own losing
    /// rows -- not just pre-existing backlog -- get swap-checked by
    /// [`PromoteHeadsHook`] next, which the pre-refactor bulk path could
    /// never do (it had no swap logic for the bulk path at all).
    async fn insert_many(
        op: &mut impl AtomicOperation,
        rows: &[NewExecutionRow],
    ) -> Result<Vec<InsertedRow>, sqlx::Error> {
        let ids: Vec<JobId> = rows.iter().map(|r| r.id).collect();
        let job_types: Vec<JobType> = rows.iter().map(|r| r.job_type.clone()).collect();
        let queue_ids: Vec<Option<String>> = rows.iter().map(|r| r.queue_id.clone()).collect();
        let schedule_times: Vec<DateTime<Utc>> = rows.iter().map(|r| r.schedule_at).collect();

        sqlx::query_as!(
            InsertedRow,
            r#"
            WITH input AS (
                SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::timestamptz[])
                    AS t(id, job_type, queue_id, execute_at)
            ), ins AS (
                INSERT INTO job_executions
                    (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at)
                SELECT id, job_type, queue_id, NULL, 'pending', 1, execute_at,
                       COALESCE($5, NOW()), COALESCE($5, NOW())
                FROM input
                ON CONFLICT (queue_id) WHERE state IN ('pending','running') AND queue_id IS NOT NULL
                DO NOTHING
                RETURNING id, queue_id
            ), parked AS (
                INSERT INTO job_executions
                    (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at)
                SELECT i.id, i.job_type, i.queue_id, NULL, 'parked', 1, i.execute_at,
                       COALESCE($5, NOW()), COALESCE($5, NOW())
                FROM input i
                WHERE i.id NOT IN (SELECT id FROM ins)
                RETURNING id, queue_id
            )
            SELECT r.id AS "id!: JobId", TRUE AS "landed_pending!", NULL::uuid AS "occupant_id?"
            FROM ins r
            UNION ALL
            SELECT p.id AS "id!: JobId", FALSE AS "landed_pending!",
                COALESCE(
                    (SELECT w.id FROM ins w WHERE w.queue_id = p.queue_id),
                    (SELECT o.id FROM job_executions o
                     WHERE o.queue_id = p.queue_id AND o.state = 'pending')
                ) AS "occupant_id?"
            FROM parked p
            "#,
            &ids as _,
            &job_types as _,
            &queue_ids as _,
            &schedule_times,
            op.maybe_now(),
        )
        .fetch_all(op.as_executor())
        .await
    }
}

impl ExecutionInsertHook {
    /// The due-now-per-type subset of `inserted`'s landed-pending rows,
    /// cross-referenced against `rows` (the id -> job_type/schedule_at map
    /// this hook carries in). Pure -- no DB, no poller -- same reasoning as
    /// `spawner.rs`'s old `count_due_now` this folds in: a live background
    /// poller can independently claim a row the instant it lands pending, so
    /// asserting on post-insert row state in an integration test races it; a
    /// pure function taking the insert's own return value sidesteps that
    /// entirely.
    ///
    /// A row's `schedule_at` may be in the future (an explicit per-spec
    /// `JobSpec::schedule_at`, or a backdated/forward `spawn_at`); those must
    /// not count as due-now demand for
    /// [`crate::poller::JobPoller::register_claim_demand`].
    ///
    /// Also counts one unit of demand per `promoted` occurrence, REGARDLESS
    /// of whether the promoted row is due-now by its own `execute_at` --
    /// `promoted` names rows [`PromoteHeadsHook::apply`] just swapped into
    /// this call's queue's active `pending` slot (which may be an EXISTING
    /// parked sibling this call never touched, not one of `rows`, so there
    /// is nothing to cross-reference `schedule_at` against here even if we
    /// wanted to). Necessary, not just permissive: a promoted-but-not-yet-
    /// its-own-due-now row would otherwise get a `execution_ready` notify
    /// (see [`Self::notify_types`]) but no accompanying claim ATTEMPT within
    /// this SAME commit pass, silently downgrading it from "claimed
    /// immediately" to "wait for the next poll" -- the exact regression a
    /// live test caught (`retry_backoff_yields_to_an_older_parked_sibling`):
    /// an older backdated sibling that displaced a `pending` occupant via
    /// this promote must ALSO get its own immediate claim attempt, or it
    /// sits `pending` instead of dispatching within the spawning
    /// transaction. Safe either way: `claim_due_heads_in_op`'s own
    /// `execute_at <= now` filter is what actually gates claimability; an
    /// over-eager reservation here just finds nothing and releases.
    fn due_now_by_type(
        inserted: &[InsertedRow],
        rows: &[NewExecutionRow],
        promoted: &[String],
        now: DateTime<Utc>,
    ) -> HashMap<JobType, usize> {
        let by_id: HashMap<JobId, &NewExecutionRow> = rows.iter().map(|r| (r.id, r)).collect();
        let mut due: HashMap<JobType, usize> = HashMap::new();
        for row in inserted {
            if !row.landed_pending {
                continue;
            }
            let Some(new_row) = by_id.get(&row.id) else {
                continue;
            };
            if new_row.schedule_at <= now {
                *due.entry(new_row.job_type.clone()).or_insert(0) += 1;
            }
        }
        for promoted_type in promoted {
            *due.entry(JobType::from_owned(promoted_type.clone()))
                .or_insert(0) += 1;
        }
        due
    }

    /// Every type worth an `execution_ready` notify: one landed pending (so
    /// its own backlog gained a claimable row), or one got promoted by
    /// [`PromoteHeadsHook`] (so ITS backlog did). Pure, same reasoning as
    /// [`Self::due_now_by_type`].
    fn notify_types(
        inserted: &[InsertedRow],
        rows: &[NewExecutionRow],
        promoted: &[String],
    ) -> HashSet<JobType> {
        let by_id: HashMap<JobId, &NewExecutionRow> = rows.iter().map(|r| (r.id, r)).collect();
        let mut types: HashSet<JobType> = inserted
            .iter()
            .filter(|row| row.landed_pending)
            .filter_map(|row| by_id.get(&row.id).map(|new_row| new_row.job_type.clone()))
            .collect();
        types.extend(promoted.iter().cloned().map(JobType::from_owned));
        types
    }
}

impl CommitHook for ExecutionInsertHook {
    async fn pre_commit(
        self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        let inserted = Self::insert_many(&mut op, &self.rows).await?;

        let occupant_ids: Vec<uuid::Uuid> =
            inserted.iter().filter_map(|row| row.occupant_id).collect();
        let promoted = PromoteHeadsHook::apply(&mut op, &occupant_ids).await?;

        let now = op.maybe_now().unwrap_or_else(|| self.clock.now());
        for job_type in Self::notify_types(&inserted, &self.rows, &promoted) {
            self.notifier
                .execution_ready_in_op(&mut op, &job_type)
                .await?;
        }

        if let Some(poller) = self.poller.get().and_then(|w| w.upgrade()) {
            for (job_type, n_due) in Self::due_now_by_type(&inserted, &self.rows, &promoted, now) {
                poller.register_claim_demand(&mut op, &job_type, n_due);
            }
        }

        PreCommitRet::ok(self, op)
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.rows.append(&mut other.rows);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TYPE_A: JobType = JobType::new("execution-hooks-test-a");
    const TYPE_B: JobType = JobType::new("execution-hooks-test-b");

    fn row(id: JobId, job_type: JobType, schedule_at: DateTime<Utc>) -> NewExecutionRow {
        NewExecutionRow {
            id,
            job_type,
            schedule_at,
            queue_id: None,
        }
    }

    fn pending(id: JobId) -> InsertedRow {
        InsertedRow {
            id,
            landed_pending: true,
            occupant_id: None,
        }
    }

    fn parked(id: JobId, occupant_id: Option<uuid::Uuid>) -> InsertedRow {
        InsertedRow {
            id,
            landed_pending: false,
            occupant_id,
        }
    }

    /// The exact bug an earlier version of the bulk spawn path had (flagged
    /// by automated review on PR #173): counting every row that landed
    /// pending, including future-scheduled ones, as if it were due now.
    #[test]
    fn due_now_excludes_future_scheduled_rows() {
        let now = chrono::Utc::now();
        let due = JobId::new();
        let future = JobId::new();
        let rows = vec![
            row(due, TYPE_A.clone(), now - chrono::Duration::seconds(5)),
            row(future, TYPE_A.clone(), now + chrono::Duration::hours(1)),
        ];
        let inserted = vec![pending(due), pending(future)];

        let due_counts = ExecutionInsertHook::due_now_by_type(&inserted, &rows, &[], now);
        assert_eq!(
            due_counts.get(&TYPE_A).copied(),
            Some(1),
            "only the due row should count, not the future-scheduled one"
        );
    }

    /// A batch of entirely future work must count zero -- the case that let
    /// a bulk short-circuit reserve and drain unrelated due backlog before
    /// this was fixed.
    #[test]
    fn due_now_is_zero_for_all_future_work() {
        let now = chrono::Utc::now();
        let a = JobId::new();
        let b = JobId::new();
        let rows = vec![
            row(a, TYPE_A.clone(), now + chrono::Duration::hours(1)),
            row(b, TYPE_A.clone(), now + chrono::Duration::hours(1)),
        ];
        let inserted = vec![pending(a), pending(b)];

        assert!(ExecutionInsertHook::due_now_by_type(&inserted, &rows, &[], now).is_empty());
    }

    /// A row that landed `parked` (conflicted on its queue) must not count
    /// even if its own `schedule_at` was due, since it isn't claimable.
    #[test]
    fn due_now_ignores_rows_that_did_not_land_pending() {
        let now = chrono::Utc::now();
        let landed = JobId::new();
        let was_parked = JobId::new();
        let rows = vec![
            row(landed, TYPE_A.clone(), now - chrono::Duration::seconds(1)),
            row(
                was_parked,
                TYPE_A.clone(),
                now - chrono::Duration::seconds(1),
            ),
        ];
        let inserted = vec![pending(landed), parked(was_parked, None)];

        assert_eq!(
            ExecutionInsertHook::due_now_by_type(&inserted, &rows, &[], now)
                .get(&TYPE_A)
                .copied(),
            Some(1)
        );
    }

    #[test]
    fn due_now_treats_exactly_due_as_due() {
        let now = chrono::Utc::now();
        let id = JobId::new();
        let rows = vec![row(id, TYPE_A.clone(), now)];
        let inserted = vec![pending(id)];

        assert_eq!(
            ExecutionInsertHook::due_now_by_type(&inserted, &rows, &[], now)
                .get(&TYPE_A)
                .copied(),
            Some(1),
            "execute_at == now must count as due, matching the claim query's own `<=`"
        );
    }

    /// Due-now counts split correctly per type when a merged op's rows span
    /// several types -- the case `ExecutionInsertHook::merge` exists for.
    #[test]
    fn due_now_splits_by_type() {
        let now = chrono::Utc::now();
        let a = JobId::new();
        let b = JobId::new();
        let rows = vec![row(a, TYPE_A.clone(), now), row(b, TYPE_B.clone(), now)];
        let inserted = vec![pending(a), pending(b)];

        let due_counts = ExecutionInsertHook::due_now_by_type(&inserted, &rows, &[], now);
        assert_eq!(due_counts.get(&TYPE_A).copied(), Some(1));
        assert_eq!(due_counts.get(&TYPE_B).copied(), Some(1));
    }

    /// Caught live by `retry_backoff_yields_to_an_older_parked_sibling`: a
    /// row that lands `parked` behind a `pending` occupant, then displaces
    /// that occupant via `PromoteHeadsHook::apply`, must contribute claim
    /// demand for ITS type even though `inserted` never reports it
    /// `landed_pending` (statement 1 saw it park; the promotion is a LATER
    /// statement). Without this, the promoted row sits `pending` -- notified
    /// but never claimed within this commit pass -- instead of dispatching
    /// immediately.
    #[test]
    fn due_now_counts_promoted_rows_even_though_insert_saw_them_park() {
        let now = chrono::Utc::now();
        let backdated = JobId::new();
        let rows = vec![row(backdated, TYPE_A.clone(), now)];
        // Statement 1: our row conflicted and parked; statement 2 then
        // promoted it (it's now the queue's `pending` occupant), which
        // `PromoteHeadsHook::apply` reports back as `TYPE_A.to_string()`.
        let inserted = vec![parked(backdated, Some(uuid::Uuid::from(JobId::new())))];
        let promoted = vec![TYPE_A.to_string()];

        let due_counts = ExecutionInsertHook::due_now_by_type(&inserted, &rows, &promoted, now);
        assert_eq!(
            due_counts.get(&TYPE_A).copied(),
            Some(1),
            "a promoted row must contribute claim demand for its type"
        );
    }

    /// A landed-pending row's type is always notify-worthy; a parked row's
    /// is not, unless its queue's occupant got promoted.
    #[test]
    fn notify_types_covers_pending_and_promoted_not_bare_parked() {
        let now = chrono::Utc::now();
        let landed = JobId::new();
        let was_parked = JobId::new();
        let rows = vec![
            row(landed, TYPE_A.clone(), now),
            row(was_parked, TYPE_B.clone(), now),
        ];
        let inserted = vec![pending(landed), parked(was_parked, None)];

        let types = ExecutionInsertHook::notify_types(&inserted, &rows, &[]);
        assert!(types.contains(&TYPE_A));
        assert!(!types.contains(&TYPE_B));
    }

    #[test]
    fn notify_types_includes_promoted_types_even_with_nothing_pending() {
        let inserted: Vec<InsertedRow> = vec![];
        let rows: Vec<NewExecutionRow> = vec![];
        let promoted = vec![TYPE_B.to_string()];

        let types = ExecutionInsertHook::notify_types(&inserted, &rows, &promoted);
        assert_eq!(types, HashSet::from([TYPE_B]));
    }
}
