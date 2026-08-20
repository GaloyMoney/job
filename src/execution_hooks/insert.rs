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

use super::promote::{PromoteHeadsHook, PromotedRow};

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

/// Batches every `spawn_in_op`/`spawn_all_in_op`/resident-spawn insert
/// registered on one `op` into ONE statement at commit time. [`Self::merge`]
/// is what makes this a genuine batching win: N `spawn_in_op` calls sharing
/// one transaction get one multi-row insert, without the caller ever calling
/// `spawn_all`.
///
/// `pre_commit` does three things, in order:
/// 1. One combined `INSERT ... ON CONFLICT DO NOTHING` / fallback-`INSERT
///    'parked'` statement for every row (queued and unqueued alike) --
///    [`Self::insert_many`].
/// 2. If anything parked behind an occupant, [`PromoteHeadsHook::apply`] on
///    those occupant ids, as a sequential step within this same
///    `pre_commit`.
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
    /// occupying the slot); whichever didn't land lands `parked`. An
    /// unqueued row (`queue_id.is_none()`) can never conflict -- it always
    /// lands `pending`.
    ///
    /// For every parked row, also resolves `occupant_id`: the id CURRENTLY
    /// holding its queue's active slot, checked in this priority order --
    /// (a) a sibling FROM THIS SAME `rows` batch that won the slot (read
    /// from `ins`'s own `RETURNING`, so it reflects this statement's own
    /// writes), or (b) a pre-existing occupant (read via a plain scan of
    /// `job_executions`, which -- since none of this statement's own insert
    /// commands touch an EXISTING row -- sees exactly the statement-start
    /// snapshot, i.e. whoever held the slot before this call ran). At most
    /// one of (a)/(b) can ever match per queue (Invariant A), so the
    /// `COALESCE` of two scalar subqueries is safe. This is what lets a
    /// batch's own losing rows -- not just pre-existing backlog -- get
    /// swap-checked by [`PromoteHeadsHook`] next.
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
    /// this hook carries in). Pure -- no DB, no poller -- so this is
    /// testable without a live background poller independently claiming a
    /// row the instant it lands pending, which would otherwise race an
    /// integration test asserting on post-insert row state.
    ///
    /// A row's `schedule_at` may be in the future (an explicit per-spec
    /// `JobSpec::schedule_at`, or a backdated/forward `spawn_at`); those must
    /// not count as due-now demand for
    /// [`crate::poller::JobPoller::register_claim_demand`].
    ///
    /// Also counts one unit of demand per `promoted` row that is ITSELF
    /// due-now (`execute_at <= now`) -- `promoted` names rows
    /// [`PromoteHeadsHook::apply`] just swapped into this call's queue's
    /// active `pending` slot (which may be an EXISTING parked sibling this
    /// call never touched, not one of `rows`; its own `execute_at`, carried
    /// on [`PromotedRow`], is unchanged by the promote and is what this
    /// checks). A promoted-but-not-yet-due row still gets a plain
    /// `execution_ready` notify (see [`Self::notify_types`], which does NOT
    /// due-gate -- the ordinary poll will pick it up once it actually comes
    /// due) but must NOT count as claim demand here: `claim_due_heads_in_op`
    /// claims the type's OLDEST due row, not specifically the promoted one,
    /// so an ungated reservation for a future promotion could reserve and
    /// drain a completely UNRELATED due backlog row of the same type,
    /// bypassing `next_batch_size`'s `min_jobs` throttle for a promotion
    /// that has nothing to do with due-now admission.
    fn due_now_by_type(
        inserted: &[InsertedRow],
        rows: &[NewExecutionRow],
        promoted: &[PromotedRow],
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
        for row in promoted {
            if row.execute_at <= now {
                *due.entry(JobType::from_owned(row.job_type.clone()))
                    .or_insert(0) += 1;
            }
        }
        due
    }

    /// Every type worth an `execution_ready` notify: one landed pending (so
    /// its own backlog gained a claimable row), or one got promoted by
    /// [`PromoteHeadsHook`] (so ITS backlog did) -- regardless of the
    /// promoted row's own due-ness, unlike [`Self::due_now_by_type`]: a
    /// plain notify only ever wakes the ordinary poll (which re-checks
    /// `execute_at <= now` itself), so over-notifying a not-yet-due
    /// promotion is harmless, while over-COUNTING it as claim demand is not
    /// (see that method's doc comment).
    fn notify_types(
        inserted: &[InsertedRow],
        rows: &[NewExecutionRow],
        promoted: &[PromotedRow],
    ) -> HashSet<JobType> {
        let by_id: HashMap<JobId, &NewExecutionRow> = rows.iter().map(|r| (r.id, r)).collect();
        let mut types: HashSet<JobType> = inserted
            .iter()
            .filter(|row| row.landed_pending)
            .filter_map(|row| by_id.get(&row.id).map(|new_row| new_row.job_type.clone()))
            .collect();
        types.extend(
            promoted
                .iter()
                .map(|row| JobType::from_owned(row.job_type.clone())),
        );
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

    fn promoted_row(job_type: JobType, execute_at: DateTime<Utc>) -> PromotedRow {
        PromotedRow {
            job_type: job_type.to_string(),
            execute_at,
        }
    }

    /// A future-scheduled row that happens to land pending must not count
    /// as due-now demand -- only the genuinely due row should.
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

    /// A batch of entirely future work must count zero due-now demand.
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

    /// A row that lands `parked` behind a `pending` occupant, then displaces
    /// that occupant via `PromoteHeadsHook::apply`, must contribute claim
    /// demand for ITS type even though `inserted` never reports it
    /// `landed_pending` (statement 1 saw it park; the promotion is a later
    /// statement). Without this, the promoted row would be notified but
    /// never claimed within the same commit pass.
    #[test]
    fn due_now_counts_promoted_rows_even_though_insert_saw_them_park() {
        let now = chrono::Utc::now();
        let backdated = JobId::new();
        let rows = vec![row(backdated, TYPE_A.clone(), now)];
        let inserted = vec![parked(backdated, Some(uuid::Uuid::from(JobId::new())))];
        let promoted = vec![promoted_row(TYPE_A.clone(), now)];

        let due_counts = ExecutionInsertHook::due_now_by_type(&inserted, &rows, &promoted, now);
        assert_eq!(
            due_counts.get(&TYPE_A).copied(),
            Some(1),
            "a promoted row that is itself due-now must contribute claim demand"
        );
    }

    /// A promotion whose OWN `execute_at` is still in the future must NOT
    /// contribute claim demand, or an over-eager reservation would let
    /// `claim_due_heads_in_op` claim an UNRELATED due backlog row of that
    /// type instead, bypassing `next_batch_size`'s `min_jobs` throttle for a
    /// promotion that has nothing to do with due-now admission.
    #[test]
    fn due_now_excludes_a_promoted_row_that_is_not_itself_due() {
        let now = chrono::Utc::now();
        let promoted = vec![promoted_row(
            TYPE_A.clone(),
            now + chrono::Duration::hours(1),
        )];

        let due_counts = ExecutionInsertHook::due_now_by_type(&[], &[], &promoted, now);
        assert!(
            due_counts.is_empty(),
            "a not-yet-due promotion must not contribute claim demand"
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
        // notify never due-gates, unlike claim demand.
        let promoted = vec![promoted_row(
            TYPE_B.clone(),
            chrono::Utc::now() + chrono::Duration::hours(1),
        )];

        let types = ExecutionInsertHook::notify_types(&inserted, &rows, &promoted);
        assert_eq!(types, HashSet::from([TYPE_B]));
    }
}
