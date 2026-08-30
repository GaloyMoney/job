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
/// `unique_key` is `Some` only for a [`crate::JobSpec::dedup_key`]-bearing
/// bulk-spawn row; `spawn_in_op`/`spawn_at_in_op`/`spawn_with_queue_id_in_op`
/// (the single-item convenience methods) never set it. Keyed spawn's own
/// inserts stay entirely separate and inline
/// (`keyed.rs::KeyedJobSpawner::spawn_all_in_op`) rather than going through
/// this hook -- deferring them to commit time is exactly what would stop a
/// second keyed spawn on the SAME `op` from seeing the first's row in its
/// live-check, and that live-check is the single mechanism keyed spawn uses
/// to resolve same-op and cross-transaction collisions alike. See there.
/// `spawn_all_in_op` resolves a
/// dedup-key row's liveness BEFORE registering it here (see
/// `JobRepo::lock_and_check_live_keys_in_op`), so by the time a row reaches
/// this hook its key (if any) is either free or a cross-call collision
/// `Self::insert_many`'s `deduped` CTE still has to catch (see there).
///
/// `Clone` exists for [`ExecutionInsertHook::adopt_orphaned_queues`], which
/// re-submits a subset of these rows through [`ExecutionInsertHook::insert_many`]
/// a second time.
#[derive(Clone)]
pub(crate) struct NewExecutionRow {
    pub id: JobId,
    pub job_type: JobType,
    pub schedule_at: DateTime<Utc>,
    pub queue_id: Option<String>,
    pub unique_key: Option<String>,
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
///    a `NotifierHook` per type that landed pending or got promoted, and a
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
    ///
    /// (b) matches `state IN ('pending', 'running')`, not just `'pending'`,
    /// even though [`PromoteHeadsHook::apply`] only ever swaps a `'pending'`
    /// occupant: a `'running'` occupant can retry-reschedule to `'pending'`
    /// in a CONCURRENT transaction that commits after this statement's
    /// snapshot was taken but before `apply` runs (a few statements later, in
    /// this same transaction). Filtering here to `'pending'` only would miss
    /// that occupant entirely -- this row never becomes a `promote_ids`
    /// candidate, so nothing re-checks it, and the parked row we just
    /// inserted can be stranded behind a `'pending'` sibling that never gets
    /// swap-checked (until the next unrelated event on that queue, or
    /// `sweep_orphaned_parked_rows`). Passing the `'running'` occupant's id
    /// through is always safe: `apply`'s own statement re-reads state FRESH
    /// (a separate statement's snapshot) and simply finds nothing to swap if
    /// the occupant is still genuinely `'running'` by then. Pinned end-to-end
    /// by `tests/parked_rows.rs::retry_backoff_yields_to_an_older_parked_sibling`.
    ///
    /// `deduped` collapses same-`(job_type, unique_key)` rows to one BEFORE
    /// `input` applies the file's usual `(queue_id, id)` order: the merged
    /// batch can contain two rows sharing a dedup key when two
    /// `spawn_in_op`/`spawn_all_in_op` calls on the SAME `op` both target it
    /// (see `NewExecutionRow`'s doc) -- each call's own
    /// `lock_and_check_live_keys_in_op` pre-check only sees the durable
    /// table, not a sibling call's still-queued row, so both can pass their
    /// own check. Left uncaught, both rows would reach the `ins` INSERT
    /// together and unique-violate `idx_job_executions_job_type_unique_key`
    /// in one statement -- aborting this ENTIRE batch, including every
    /// unrelated keyless row sharing the transaction, which is exactly what
    /// AC5 (no statement-abort surfaced to the caller) rules out. There is
    /// no second `ON CONFLICT` arbiter available to catch it inline (see
    /// below), so it has to be filtered out before `ins` ever sees it.
    ///
    /// The `DISTINCT ON`/`ORDER BY` key is `(job_type,
    /// COALESCE(unique_key, id::text))`, matching
    /// `idx_job_executions_job_type_unique_key` exactly -- NOT `unique_key`
    /// alone (a real regression this crate shipped and caught in review:
    /// two DIFFERENT job types sharing one dedup_key STRING, e.g.
    /// facility-scoped cross-type work keyed by the facility id, collapsed
    /// to one execution row even though the index would happily hold both,
    /// silently -- both calls' own per-type live-checks had already passed,
    /// so both reported success while one ended up with an orphan `jobs`
    /// row and no execution row ever created). `COALESCE(unique_key,
    /// id::text)` is the fallback for keyless rows (`unique_key IS NULL`) so
    /// they never collapse against EACH OTHER -- Postgres treats all NULLs
    /// as one `DISTINCT ON` group per the leading key, and every row's own
    /// `id` is unique, so a keyless row's fallback key never collides with a
    /// sibling's; adding `job_type` to the key cannot introduce a NEW
    /// collapse here either, since `id` is already globally unique
    /// regardless of `job_type` -- prepending a column can only split an
    /// existing `DISTINCT ON` group further, never merge two apart.
    /// `ORDER BY ..., id` picks the earliest-created (`id` is a v7 uuid) row
    /// of a true collision deterministically. The loser's `jobs` row
    /// (already created by its own call's `create_all_in_op`, since dedup
    /// resolution for a same-op collision only surfaces here, at commit
    /// time) is left behind with an `Initialized` event and no execution
    /// row and no terminal event -- a state
    /// `load_snapshot_by_id`/`JobSnapshot::state()` treat as impossible
    /// (surfaces as an error/panic on lookup, never a false "Completed"),
    /// not silent corruption, but still worth avoiding: this is scoped to
    /// the narrow same-op, overlapping-`(job_type, dedup_key)` case, and is
    /// called out as a known edge in the PR rather than fixed further
    /// here.
    ///
    /// `input` is `MATERIALIZED` and re-`ORDER BY (queue_id, id)` over
    /// `deduped`'s already-collapsed rows, deliberately:
    /// the `ins` arbiter insert's `ON CONFLICT` WAITS on a concurrent
    /// uncommitted row holding the same queue slot, and per-row arbiter
    /// processing follows a materialized CTE's stored (i.e. sorted) row
    /// order -- the same mechanism `PromoteHeadsHook::apply`'s `locked` CTE
    /// uses to fix lock ACQUISITION order via `ORDER BY` + `FOR NO KEY
    /// UPDATE`. This puts the arbiter wait on the SAME global order as every
    /// other waiting locker of this table (`lock_queue_occupants` below,
    /// `PromoteHeadsHook::apply`/`apply_freed`), regardless of what order
    /// `rows` arrived in -- the row order is enforced in SQL, not by the
    /// caller/accumulator having sorted `rows` beforehand. Unqueued rows
    /// (`queue_id IS NULL`) sort last under plain `ORDER BY` and never
    /// conflict, so their position doesn't matter.
    async fn insert_many(
        op: &mut impl AtomicOperation,
        rows: &[NewExecutionRow],
    ) -> Result<Vec<InsertedRow>, sqlx::Error> {
        let ids: Vec<JobId> = rows.iter().map(|r| r.id).collect();
        let job_types: Vec<JobType> = rows.iter().map(|r| r.job_type.clone()).collect();
        let queue_ids: Vec<Option<String>> = rows.iter().map(|r| r.queue_id.clone()).collect();
        let schedule_times: Vec<DateTime<Utc>> = rows.iter().map(|r| r.schedule_at).collect();
        let unique_keys: Vec<Option<String>> = rows.iter().map(|r| r.unique_key.clone()).collect();

        sqlx::query_as!(
            InsertedRow,
            r#"
            WITH raw AS (
                SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::timestamptz[], $6::text[])
                    AS t(id, job_type, queue_id, execute_at, unique_key)
            ), deduped AS MATERIALIZED (
                SELECT DISTINCT ON (job_type, COALESCE(unique_key, id::text)) *
                FROM raw
                ORDER BY job_type, COALESCE(unique_key, id::text), id
            ), input AS MATERIALIZED (
                SELECT * FROM deduped ORDER BY queue_id, id
            ), ins AS (
                INSERT INTO job_executions
                    (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at)
                SELECT id, job_type, queue_id, unique_key, 'pending', 1, execute_at,
                       COALESCE($5, NOW()), COALESCE($5, NOW())
                FROM input
                ON CONFLICT (queue_id) WHERE state IN ('pending','running') AND queue_id IS NOT NULL
                DO NOTHING
                RETURNING id, queue_id
            ), parked AS (
                INSERT INTO job_executions
                    (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at)
                SELECT i.id, i.job_type, i.queue_id, i.unique_key, 'parked', 1, i.execute_at,
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
                     WHERE o.queue_id = p.queue_id AND o.state IN ('pending', 'running'))
                ) AS "occupant_id?"
            FROM parked p
            "#,
            &ids as _,
            &job_types as _,
            &queue_ids as _,
            &schedule_times,
            op.maybe_now(),
            &unique_keys as _,
        )
        .fetch_all(op.as_executor())
        .await
    }

    /// Statement 2: pin the active row of every queue that just parked a row,
    /// so that occupant cannot go terminal before this transaction's parked
    /// rows are visible to it. Returns the queues that still HAVE an active
    /// row, i.e. the ones now safely pinned.
    ///
    /// This closes the orphan race directly rather than leaving it to
    /// `sweep_orphaned_parked_rows`' slow backstop. Without it, a parked row
    /// is invisible (uncommitted) to the occupant's own promote-on-complete
    /// pass, so a completion landing anywhere between statement 1 and this
    /// transaction's `COMMIT` promotes nothing and leaves the queue with a
    /// parked backlog and no active row -- unclaimable until the sweep.
    ///
    /// `FOR KEY SHARE` is the weakest strength that still does the job, and
    /// the choice matters a great deal here (verified against the live
    /// schema by `key_share_blocks_only_the_delete`):
    /// - It **conflicts with `DELETE`**, which is the only operation that can
    ///   remove a queue's active row -- so the completer waits out this
    ///   transaction's commit tail, and its [`PromoteHeadsHook`] freed-queue
    ///   statement (whose FRESH statement snapshot is what actually makes
    ///   the freshly committed parked row visible -- the blocked `DELETE`
    ///   itself resumes with its original snapshot) then promotes it.
    /// - It does **not** conflict with a plain `UPDATE` of a non-key column,
    ///   so the keep-alive heartbeat (`poller.rs`'s `start_keep_alive`, one
    ///   bulk statement across every live job on the instance) is never
    ///   blocked by a spawn.
    /// - It does **not** conflict with `UPDATE ... SET state`, despite `state`
    ///   appearing in `idx_job_executions_queue_active`'s PREDICATE -- key
    ///   columns are the indexed columns themselves, not predicate
    ///   references -- so retry, reschedule, reclaim and promote all run
    ///   unimpeded. None of them can orphan a queue anyway: they leave an
    ///   active row behind (`pending`), or swap one for another atomically.
    /// - It does **not** conflict with ITSELF, so concurrent spawns into the
    ///   same queue never serialize against each other. Insert throughput is
    ///   unaffected by this statement.
    ///
    /// `ORDER BY queue_id, id` inside a `MATERIALIZED` CTE is what makes the
    /// lock acquisition order deterministic (the plan puts `LockRows` above
    /// `Sort`, so rows are locked in that order however the scan below
    /// reached them -- an unordered bitmap scan included). This is the same
    /// global order as [`PromoteHeadsHook::apply`]'s swap lock and
    /// [`Self::insert_many`]'s `input` CTE, which orders the arbiter insert
    /// the same way, in SQL, rather than relying on `rows` having arrived
    /// pre-sorted. Without agreement here, a multi-queue spawn and a
    /// multi-queue batch completion touching the same two rows in opposite
    /// orders would deadlock.
    ///
    /// `finalizer.rs`'s disposition writes (the reschedule updates and the
    /// terminal delete) are id-addressed (not queue-addressed,
    /// unlike the lockers above) but order their own pre-locks by
    /// `(queue_id, id)` too, for exactly this reason -- a batch spanning
    /// several queues is otherwise just as capable of disagreeing with this
    /// lock's order as a multi-queue spawn is.
    ///
    /// The one thing this DOES contend with is the claim: `poll_jobs` and
    /// `claim_due_heads_in_op` take `FOR UPDATE SKIP LOCKED`, which conflicts
    /// with `FOR KEY SHARE`, so a poll running inside this transaction's
    /// commit tail SKIPS a `pending` occupant it would otherwise have
    /// claimed. Skipping is not free: `poll_jobs`' `min_wait` only considers
    /// rows with `execute_at > now`, so a skipped DUE head contributes no
    /// `next_due_at`, and if `may_have_more` is false that type sleeps for up
    /// to `MAX_WAIT` with claimable work sitting there. A bare parked row
    /// notifies nothing by itself, so nothing would wake it.
    ///
    /// Hence the second return value: the types of the `pending` occupants
    /// pinned here, which the caller notifies. The notify is emitted
    /// post-commit, i.e. strictly after these locks release, so the woken
    /// poll finds the head claimable. `running` occupants need no such
    /// treatment -- they are not in the claim scan to begin with. Narrowing
    /// the lock to `running` rows would sidestep this too, but at the cost of
    /// reopening the race for a `pending` occupant that gets claimed, run and
    /// completed inside the commit tail -- exactly the short-job regime this
    /// bug was first observed in -- so the lock stays broad and the wake pays
    /// for it.
    async fn lock_queue_occupants(
        op: &mut impl AtomicOperation,
        queue_ids: &[String],
    ) -> Result<(HashSet<String>, HashSet<JobType>), sqlx::Error> {
        if queue_ids.is_empty() {
            return Ok((HashSet::new(), HashSet::new()));
        }
        let locked = sqlx::query!(
            r#"
            WITH locked AS MATERIALIZED (
                SELECT id, queue_id, job_type, state FROM job_executions
                WHERE queue_id = ANY($1) AND state IN ('pending', 'running')
                ORDER BY queue_id, id
                FOR KEY SHARE
            )
            SELECT queue_id AS "queue_id!", job_type AS "job_type!",
                   (state = 'pending') AS "claimable!"
            FROM locked
            "#,
            queue_ids,
        )
        .fetch_all(op.as_executor())
        .await?;

        let mut occupied = HashSet::new();
        let mut wake_types = HashSet::new();
        for row in locked {
            occupied.insert(row.queue_id);
            if row.claimable {
                wake_types.insert(JobType::from_owned(row.job_type));
            }
        }
        Ok((occupied, wake_types))
    }

    /// The rare other branch of [`Self::lock_queue_occupants`]: a queue this
    /// call parked into has NO active row left to pin, because its occupant
    /// completed between statement 1 and the lock. The parked row would be
    /// orphaned the moment this transaction commits, so re-arbitrate it here
    /// instead -- the whole point of locking rather than assuming.
    ///
    /// Deletes this call's own parked rows for those queues (they are this
    /// transaction's own uncommitted inserts, invisible to everyone else) and
    /// re-submits them through [`Self::insert_many`], whose `ON CONFLICT`
    /// arbiter is what makes this safe against a CONCURRENT adopter: two
    /// transactions that both lost their occupant in the same window would
    /// each promote their own row if this were a plain `UPDATE ... SET state
    /// = 'pending'`, and the second would fail the unique index outright. The
    /// arbiter instead lets exactly one land `pending` and parks the other,
    /// with no error on either side.
    ///
    /// The re-inserted row is not necessarily the queue's rightful head -- an
    /// OLDER parked sibling can have been sitting behind the vanished
    /// occupant all along -- so the caller feeds whatever lands `pending`
    /// here into [`PromoteHeadsHook::apply`], exactly like every other "rows
    /// just moved to pending" call site. That restores Invariant B in the
    /// same transaction.
    ///
    /// Returns the fresh [`InsertedRow`]s (which supersede statement 1's for
    /// these ids) and the ids that landed `pending`.
    async fn adopt_orphaned_queues(
        op: &mut impl AtomicOperation,
        readopt: &[NewExecutionRow],
    ) -> Result<(Vec<InsertedRow>, Vec<uuid::Uuid>), sqlx::Error> {
        let ids: Vec<JobId> = readopt.iter().map(|row| row.id).collect();
        sqlx::query!(
            r#"DELETE FROM job_executions WHERE id = ANY($1)"#,
            &ids as _
        )
        .execute(op.as_executor())
        .await?;

        let reinserted = Self::insert_many(op, readopt).await?;
        let landed = reinserted
            .iter()
            .filter(|row| row.landed_pending)
            .map(|row| uuid::Uuid::from(row.id))
            .collect();
        Ok((reinserted, landed))
    }
}

impl ExecutionInsertHook {
    /// The distinct `queue_id`s this call parked a row into -- the queues
    /// whose occupant [`Self::lock_queue_occupants`] must pin. A row that
    /// landed `pending` took its queue's active slot itself and has nothing
    /// to pin; an unqueued row can never park. Pure, for the same
    /// no-live-poller-needed testability reason as [`Self::due_now_by_type`].
    fn parked_queues(inserted: &[InsertedRow], rows: &[NewExecutionRow]) -> Vec<String> {
        let by_id: HashMap<JobId, &NewExecutionRow> = rows.iter().map(|r| (r.id, r)).collect();
        inserted
            .iter()
            .filter(|row| !row.landed_pending)
            .filter_map(|row| {
                by_id
                    .get(&row.id)
                    .and_then(|new_row| new_row.queue_id.clone())
            })
            .collect::<HashSet<String>>()
            .into_iter()
            .collect()
    }

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
    /// `execution_ready` notify (see [`Self::promoted_types`], which does NOT
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

    /// The specific ids behind [`Self::due_now_by_type`]'s LANDED-row half
    /// only (never promoted rows -- [`PromotedRow`] carries no id to
    /// attribute against, and promoted types are already unconditionally
    /// forced via [`Self::promoted_types`], so they never need this).
    ///
    /// [`NotifierHook`] needs the exact ids, not just a count:
    /// `ClaimHook` always claims a type's OLDEST due row via
    /// `claim_due_heads_in_op`, which can be a pre-existing backlog row
    /// rather than one of THESE newly-landed ones. A count comparison would
    /// wrongly call a type "covered" whenever a claim happened to match the
    /// count, even if it claimed different rows entirely and left these
    /// stuck. Comparing ids as sets closes that gap.
    ///
    /// [`NotifierHook`]: crate::notifier::NotifierHook
    fn due_now_landed_ids_by_type(
        inserted: &[InsertedRow],
        rows: &[NewExecutionRow],
        now: DateTime<Utc>,
    ) -> HashMap<JobType, HashSet<JobId>> {
        let by_id: HashMap<JobId, &NewExecutionRow> = rows.iter().map(|r| (r.id, r)).collect();
        let mut due: HashMap<JobType, HashSet<JobId>> = HashMap::new();
        for row in inserted {
            if !row.landed_pending {
                continue;
            }
            let Some(new_row) = by_id.get(&row.id) else {
                continue;
            };
            if new_row.schedule_at <= now {
                due.entry(new_row.job_type.clone())
                    .or_default()
                    .insert(row.id);
            }
        }
        due
    }

    /// Landed-pending rows whose `schedule_at` is still in the future -- the
    /// complement of [`Self::due_now_by_type`]'s landed-row half. These can
    /// never be reached by THIS SAME op's `ClaimHook` (which only claims
    /// rows already due), so [`NotifierHook`] must always
    /// notify their type rather than netting it against a claim count that
    /// was never going to cover it.
    ///
    /// [`NotifierHook`]: crate::notifier::NotifierHook
    fn not_yet_due_landed_types(
        inserted: &[InsertedRow],
        rows: &[NewExecutionRow],
        now: DateTime<Utc>,
    ) -> HashSet<JobType> {
        let by_id: HashMap<JobId, &NewExecutionRow> = rows.iter().map(|r| (r.id, r)).collect();
        inserted
            .iter()
            .filter(|row| row.landed_pending)
            .filter_map(|row| by_id.get(&row.id).copied())
            .filter(|new_row| new_row.schedule_at > now)
            .map(|new_row| new_row.job_type.clone())
            .collect()
    }

    /// Every type [`PromoteHeadsHook::apply`] promoted a sibling into,
    /// regardless of that sibling's own due-ness, unlike
    /// [`Self::due_now_by_type`]'s promoted-row half: a head-swap claim
    /// targets a type's OLDEST due row, not specifically the one promoted
    /// here, so a promotion may or may not be what gets claimed --
    /// [`NotifierHook`] always forces its type rather than
    /// netting it against this pass's claim count.
    ///
    /// [`NotifierHook`]: crate::notifier::NotifierHook
    fn promoted_types(promoted: &[PromotedRow]) -> HashSet<JobType> {
        promoted
            .iter()
            .map(|row| JobType::from_owned(row.job_type.clone()))
            .collect()
    }
}

impl CommitHook for ExecutionInsertHook {
    async fn pre_commit(
        self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        let mut inserted = Self::insert_many(&mut op, &self.rows).await?;

        // Statement 2 and, only if a queue lost its occupant in the meantime,
        // the adopt path -- see `lock_queue_occupants`/`adopt_orphaned_queues`.
        let parked_queues = Self::parked_queues(&inserted, &self.rows);
        let (occupied, wake_types) = Self::lock_queue_occupants(&mut op, &parked_queues).await?;
        let mut adopted_ids: Vec<uuid::Uuid> = Vec::new();
        if occupied.len() < parked_queues.len() {
            let parked: HashSet<JobId> = inserted
                .iter()
                .filter(|row| !row.landed_pending)
                .map(|row| row.id)
                .collect();
            let readopt: Vec<NewExecutionRow> = self
                .rows
                .iter()
                .filter(|row| parked.contains(&row.id))
                .filter(|row| {
                    row.queue_id
                        .as_ref()
                        .is_some_and(|queue_id| !occupied.contains(queue_id))
                })
                .cloned()
                .collect();
            let (reinserted, landed) = Self::adopt_orphaned_queues(&mut op, &readopt).await?;
            let superseded: HashSet<JobId> = reinserted.iter().map(|row| row.id).collect();
            inserted.retain(|row| !superseded.contains(&row.id));
            inserted.extend(reinserted);
            adopted_ids = landed;
        }

        let mut promote_ids: Vec<uuid::Uuid> =
            inserted.iter().filter_map(|row| row.occupant_id).collect();
        // An adopted row landed `pending` into a queue that had no active row
        // -- the one case where a row of THIS batch landing pending can still
        // have an older parked sibling to yield to.
        promote_ids.append(&mut adopted_ids);
        let promoted = PromoteHeadsHook::apply(&mut op, &promote_ids).await?;

        let now = op.maybe_now().unwrap_or_else(|| self.clock.now());
        let due_now = Self::due_now_by_type(&inserted, &self.rows, &promoted, now);

        // `forces`: notify-worthy regardless of what this pass's ClaimHook
        // claims below -- a not-yet-due landed row and a promoted sibling
        // can never be reached by a head-swap claim (which targets the
        // type's oldest DUE row), and a pinned pending occupant a concurrent
        // poll had to SKIP LOCKED past (see `lock_queue_occupants`) needs a
        // wake regardless of self-claim. Due-now LANDED rows are handled
        // separately below, by exact id, netted against `ClaimHook`'s
        // `claimed` ids by `NotifierHook` -- see `due_now_landed_ids_by_type`
        // for why ids and not counts.
        let mut forces = Self::not_yet_due_landed_types(&inserted, &self.rows, now);
        forces.extend(Self::promoted_types(&promoted));
        forces.extend(wake_types);
        let added = Self::due_now_landed_ids_by_type(&inserted, &self.rows, now);
        self.notifier
            .register_execution_ready_in_op(&mut op, added, HashMap::new(), forces);

        if let Some(poller) = self.poller.get().and_then(|w| w.upgrade()) {
            for (job_type, n_due) in due_now {
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
            unique_key: None,
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

    /// A due landed-pending row must NOT force a notify -- it belongs in
    /// `due_now_landed_ids_by_type`'s `added` instead, netted by
    /// `NotifierHook` against whatever this same pass's `ClaimHook` actually
    /// claims. Forcing it here would defeat the whole point of Fix 3: a
    /// self-claimed row would notify unconditionally again.
    #[test]
    fn not_yet_due_landed_types_excludes_rows_already_due() {
        let now = chrono::Utc::now();
        let due = JobId::new();
        let rows = vec![row(due, TYPE_A.clone(), now)];
        let inserted = vec![pending(due)];

        let types = ExecutionInsertHook::not_yet_due_landed_types(&inserted, &rows, now);
        assert!(
            !types.contains(&TYPE_A),
            "a due row must be handled via adds/suppress, not forced"
        );
    }

    /// A future-scheduled landed-pending row can never be reached by this
    /// same pass's head-swap claim (which only claims already-due rows), so
    /// it must always force a notify regardless of what got claimed.
    #[test]
    fn not_yet_due_landed_types_includes_future_scheduled_rows() {
        let now = chrono::Utc::now();
        let future = JobId::new();
        let rows = vec![row(
            future,
            TYPE_A.clone(),
            now + chrono::Duration::hours(1),
        )];
        let inserted = vec![pending(future)];

        let types = ExecutionInsertHook::not_yet_due_landed_types(&inserted, &rows, now);
        assert!(types.contains(&TYPE_A));
    }

    /// A row that landed `parked` (never claimable) contributes nothing,
    /// even with a future `schedule_at` -- nothing about it becoming
    /// claimable happens in this call.
    #[test]
    fn not_yet_due_landed_types_ignores_parked_rows() {
        let now = chrono::Utc::now();
        let was_parked = JobId::new();
        let rows = vec![row(
            was_parked,
            TYPE_B.clone(),
            now + chrono::Duration::hours(1),
        )];
        let inserted = vec![parked(was_parked, None)];

        let types = ExecutionInsertHook::not_yet_due_landed_types(&inserted, &rows, now);
        assert!(!types.contains(&TYPE_B));
    }

    /// Every promoted type forces a notify regardless of the promoted row's
    /// own due-ness -- a head-swap claim targets a type's oldest due row,
    /// not specifically the one promoted here, so it may or may not be
    /// what gets claimed.
    #[test]
    fn promoted_types_includes_every_promoted_type_regardless_of_due_ness() {
        let promoted = vec![
            promoted_row(TYPE_A.clone(), chrono::Utc::now()),
            promoted_row(
                TYPE_B.clone(),
                chrono::Utc::now() + chrono::Duration::hours(1),
            ),
        ];

        let types = ExecutionInsertHook::promoted_types(&promoted);
        assert_eq!(types, HashSet::from([TYPE_A, TYPE_B]));
    }
}
