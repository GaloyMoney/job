//! Short-circuit ("head-swap") claiming lets a spawn or completion claim due head rows of its
//! type inside its OWN committing transaction (an `es_entity` commit hook), skipping the poll
//! loop's latency; it always claims whichever OLDEST due rows exist rather than a specific row,
//! so admission stays oldest-first even on the fast path. `pre_commit` reserves tracker capacity
//! and claims under the same pool-unit budget as the ordinary poll (a saturated instance must
//! leave rows `pending` for healthy peers), and also subscribes the shutdown receivers there
//! since a broadcast never reaches late subscribers; `post_commit` then dispatches through the
//! already-held reservations. Demand merges per `op` (`fresh_demand` row counts from spawns,
//! `recycled` reservations from completions), and the hook runs after the insert/promote hooks
//! whose rows it would claim.
//!
//! `on_rollback` cannot tell a genuine rollback from a COMMIT that errored after landing, so
//! `ClaimReconciler` checks for rows genuinely stranded `running` under this instance and
//! un-claims them back to their ORIGINAL `execute_at` with no attempt bump (dispatch only
//! happens in `post_commit`, which was skipped), re-swapping their queues in the same
//! transaction; `reclaim_lost_jobs` remains the slower backstop.
//!
//! **The due-hint gate (P1, job-dev:handoff-write-path-efficiency-sb-max13.md).**
//! `pre_commit` snapshots `fresh_types` -- the job types THIS op's own spawn/promote
//! contributed `fresh_demand` for -- before draining it into reservations: a fresh-demand
//! type's claim is guaranteed to find the row it just inserted/promoted, so it always
//! probes and never touches the hint. A recycle-only reservation set (no fresh demand from
//! this op, i.e. a completion's `recycle_into_claim`) probes only if
//! `JobTracker::consume_due_hint` says the type's queue may hold something -- at steady state
//! a per-completion recycle probe finds an empty queue almost every time, yet paid the same
//! worst-case claim-probe cost as a hit before this gate. A probe that comes back full
//! re-arms the hint (`set_due_hint`), since more may remain behind it.
//!
//! A due-hint-skipped reservation set is DROPPED, never `.release()`d: it came from
//! `self.recycled`, whose originating dispatcher already detached from its own
//! `job_completed`/`batch_completed` release-and-wake, expecting this hook to hand the unit
//! back -- so `UnitReservation::drop`'s unresolved case is the only wake left for it.
//! `.release()` stays reserved for `truncate_to_unit_budget`, where a wake fired mid-loop
//! WOULD over-admit against the surviving (untruncated) reservations' still-uncommitted
//! headroom. Conflating the two here (job-dev bugbot finding #2 on PR #197) left a drained
//! type, or one that had just crossed below `min_jobs`, asleep until `MAX_WAIT` with no
//! other wake pending. See `UnitReservation::drop`'s doc for the general contract and
//! `reservation_release_is_quiet_but_unresolved_drop_wakes` (tracker.rs) for the two
//! release paths side by side.
//!
//! **Budget truncation still owes a DEFERRED wake when it reaches a recycled unit**
//! (job-dev bugbot finding #3 on PR #197). The two release rules above collide when the
//! pool budget truncates a reservation that came from `self.recycled`: it needs the wake
//! (nothing else will signal for it) but must not fire one mid-loop (the surviving
//! reservations are about to claim against headroom a woken poll cannot see yet). So
//! `truncate_to_unit_budget` keeps the quiet `.release()` and REPORTS that a recycled unit
//! was released; `pre_commit` then wakes once, after the claim loop, when this hook's own
//! rows are already `running` inside the uncommitted transaction and `SKIP LOCKED` keeps a
//! woken poll off them. Fresh-demand truncation stays silent -- the spawn/promote that
//! created that demand already armed `job_execution_inserted`'s notify, so its row still
//! has a signal pending and a second wake would only over-admit.
//!
//! Reachable whenever one op carries more reservations than `unit_budget` admits AND a
//! recycled one is among those truncated: a promote/insert hook merging fresh demand onto
//! a completion's op, under pool pressure, with the HashMap visiting the fresh type first.
//! Rare per-op today, since a completion contributes exactly one recycled reservation --
//! but the cost when it does hit is a full `MAX_WAIT` stall of a drainable backlog, and
//! anything that merges several recycles onto one op would make it ordinary.

use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use serde_json::Value as JsonValue;
use sqlx::postgres::PgPool;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use super::ShutdownSubs;
use crate::{
    JobId,
    entity::JobType,
    execution_hooks::{PromoteHeadsHook, PromotedRow},
    tracker::UnitReservation,
};

use super::JobPoller;

pub(crate) struct ClaimedRow {
    pub id: JobId,
    pub attempt: i32,
    pub queue_id: Option<String>,
    pub data_json: Option<JsonValue>,
    pub execute_at: DateTime<Utc>,
    pub job_type: JobType,
}

#[cfg(test)]
pub(crate) static PROBE_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub(super) async fn claim_due_heads_in_op(
    op: &mut impl es_entity::AtomicOperation,
    job_type: &JobType,
    instance_id: uuid::Uuid,
    now: DateTime<Utc>,
    limit: i64,
    fresh_only: bool,
) -> Result<Vec<ClaimedRow>, sqlx::Error> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    #[cfg(test)]
    PROBE_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let wall_now = chrono::Utc::now();
    sqlx::query_as!(
        ClaimedRow,
        r#"
        WITH heads AS (
            SELECT id, execute_at FROM job_executions
            WHERE job_type = $1 AND state = 'pending' AND execute_at <= $2
              AND (NOT $6 OR attempt_index = 1)
            ORDER BY execute_at, id
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        ),
        updated AS (
            UPDATE job_executions je
            SET state = 'running', poller_instance_id = $4, alive_at = $5, execute_at = NULL
            FROM heads WHERE je.id = heads.id
            RETURNING je.id, je.queue_id, je.attempt_index, heads.execute_at AS original_execute_at
        )
        SELECT u.id AS "id!: JobId", u.attempt_index AS "attempt!", u.queue_id AS "queue_id?",
               s.execution_state_json AS "data_json?", u.original_execute_at AS "execute_at!",
               $1 AS "job_type!: JobType"
        FROM updated u
        LEFT JOIN job_execution_states s ON s.id = u.id
        "#,
        job_type as &JobType,
        now,
        limit,
        instance_id,
        wall_now,
        fresh_only,
    )
    .fetch_all(op.as_executor())
    .await
}

pub(crate) enum DispatchTarget {
    Single(ClaimedRow),
    Batch(JobType, Vec<ClaimedRow>),
}

impl DispatchTarget {
    /// Rows actually claimed -- 1 for a single dispatch, the batch's length
    /// otherwise. Used by [`ClaimHook::pre_commit`]'s due-hint re-arm: a
    /// probe that came back full may have left more behind it.
    fn row_count(&self) -> i64 {
        match self {
            DispatchTarget::Single(_) => 1,
            DispatchTarget::Batch(_, rows) => rows.len() as i64,
        }
    }
}

/// Trims `units_by_type` in place so it holds at most `unit_budget`
/// reservations in total, quietly releasing the excess, and reports whether
/// any RECYCLED reservation was among the released -- the caller owes a
/// single deferred wake if so. `n_recycled` gives each type's recycled
/// count; since [`ClaimHook::pre_commit`] pushes `self.recycled` before any
/// `try_reserve`d fresh demand, a type's first `n_recycled` entries are its
/// recycled ones, so the truncation spends fresh demand first and a
/// recycled unit is only reached once `n_recycled` exceeds what fits.
fn truncate_to_unit_budget(
    units_by_type: &mut HashMap<JobType, Vec<UnitReservation>>,
    n_recycled: &HashMap<JobType, usize>,
    unit_budget: usize,
) -> bool {
    let mut remaining_units = unit_budget;
    let mut released_a_recycled_unit = false;
    for (job_type, reservations) in units_by_type.iter_mut() {
        if reservations.len() > remaining_units {
            if n_recycled.get(job_type).copied().unwrap_or(0) > remaining_units {
                released_a_recycled_unit = true;
            }
            for reservation in reservations.drain(remaining_units..) {
                reservation.release();
            }
        }
        remaining_units -= reservations.len();
    }
    released_a_recycled_unit
}

pub(crate) struct ClaimHook {
    poller: std::sync::Weak<JobPoller>,
    fresh_demand: HashMap<JobType, usize>,
    recycled: Vec<(JobType, UnitReservation)>,
    claimed: Vec<(UnitReservation, DispatchTarget, ShutdownSubs)>,
}

impl ClaimHook {
    pub(super) fn for_demand(
        poller: std::sync::Weak<JobPoller>,
        job_type: JobType,
        n_due: usize,
    ) -> Self {
        Self {
            poller,
            fresh_demand: HashMap::from([(job_type, n_due)]),
            recycled: Vec::new(),
            claimed: Vec::new(),
        }
    }

    pub(super) fn for_recycle(
        poller: std::sync::Weak<JobPoller>,
        job_type: JobType,
        reservation: UnitReservation,
    ) -> Self {
        Self {
            poller,
            fresh_demand: HashMap::new(),
            recycled: vec![(job_type, reservation)],
            claimed: Vec::new(),
        }
    }

    pub(crate) const RUNS_AFTER: [std::any::TypeId; 2] = [
        std::any::TypeId::of::<crate::execution_hooks::ExecutionInsertHook>(),
        std::any::TypeId::of::<PromoteHeadsHook>(),
    ];
}

impl es_entity::operation::hooks::CommitHook for ClaimHook {
    async fn pre_commit(
        mut self,
        mut op: es_entity::operation::hooks::HookOperation<'_>,
    ) -> Result<es_entity::operation::hooks::PreCommitRet<'_, Self>, sqlx::Error> {
        let Some(poller) = self.poller.upgrade() else {
            return es_entity::operation::hooks::PreCommitRet::ok(self, op);
        };

        if poller.is_shutting_down() {
            return es_entity::operation::hooks::PreCommitRet::ok(self, op);
        }

        let unit_budget = poller.budget.unit_budget();
        if unit_budget == 0 {
            return es_entity::operation::hooks::PreCommitRet::ok(self, op);
        }

        let fresh_types: HashSet<JobType> = self.fresh_demand.keys().cloned().collect();

        let mut units_by_type: HashMap<JobType, Vec<UnitReservation>> = HashMap::new();
        let mut n_recycled: HashMap<JobType, usize> = HashMap::new();
        for (job_type, reservation) in self.recycled.drain(..) {
            *n_recycled.entry(job_type.clone()).or_insert(0) += 1;
            units_by_type.entry(job_type).or_default().push(reservation);
        }
        for (job_type, n_due) in self.fresh_demand.drain() {
            if !poller.registry.short_circuit(&job_type) {
                continue;
            }
            let per_reservation = poller.claim_shape(&job_type).0.max(1) as usize;
            let n_reservations = n_due.div_ceil(per_reservation);
            let entry = units_by_type.entry(job_type.clone()).or_default();
            for _ in 0..n_reservations {
                match poller.try_reserve(&job_type) {
                    Some(reservation) => entry.push(reservation),
                    None => break,
                }
            }
        }

        let owes_a_truncation_wake =
            truncate_to_unit_budget(&mut units_by_type, &n_recycled, unit_budget);

        let now = op.maybe_now().unwrap_or_else(|| poller.clock.now());
        for (job_type, reservations) in units_by_type {
            if reservations.is_empty() {
                continue;
            }
            if !poller.registry.short_circuit(&job_type) {
                continue;
            }
            if !fresh_types.contains(&job_type) && !poller.tracker.consume_due_hint(&job_type) {
                drop(reservations);
                continue;
            }
            let (per_unit_limit, _) = poller.claim_shape(&job_type);
            let limit = per_unit_limit.max(1) * reservations.len() as i64;
            let targets = poller
                .claim_after_many(&mut op, &job_type, now, reservations.len())
                .await?;
            let n_claimed: i64 = targets.iter().map(DispatchTarget::row_count).sum();
            if n_claimed >= limit {
                poller.tracker.set_due_hint(&job_type);
            }
            for (reservation, target) in reservations.into_iter().zip(targets) {
                let subs = ShutdownSubs {
                    job: poller.shutdown_tx.subscribe(),
                    monitor: poller.shutdown_tx.subscribe(),
                };
                self.claimed.push((reservation, target, subs));
            }
        }

        if owes_a_truncation_wake {
            poller.tracker.wake();
        }

        let mut claimed_ids: HashMap<JobType, HashSet<JobId>> = HashMap::new();
        for (_, target, _) in &self.claimed {
            match target {
                DispatchTarget::Single(row) => {
                    claimed_ids
                        .entry(row.job_type.clone())
                        .or_default()
                        .insert(row.id);
                }
                DispatchTarget::Batch(job_type, rows) => {
                    let entry = claimed_ids.entry(job_type.clone()).or_default();
                    entry.extend(rows.iter().map(|row| row.id));
                }
            }
        }
        poller.notifier.register_execution_ready_in_op(
            &mut op,
            HashMap::new(),
            claimed_ids,
            HashSet::new(),
        );

        es_entity::operation::hooks::PreCommitRet::ok(self, op)
    }

    fn post_commit(self) {
        let Some(poller) = self.poller.upgrade() else {
            return;
        };
        for (reservation, target, subs) in self.claimed {
            let poller = Arc::clone(&poller);
            tokio::spawn(async move {
                match target {
                    DispatchTarget::Single(row) => {
                        let id = row.id;
                        if let Err(e) = poller
                            .dispatch_job_from_reservation(reservation, row, subs)
                            .await
                        {
                            tracing::error!(
                                job_id = %id,
                                exception.message = %e,
                                exception.type = std::any::type_name_of_val(&e),
                                "failed to dispatch a short-circuit-claimed job"
                            );
                        }
                    }
                    DispatchTarget::Batch(job_type, rows) => {
                        let n_items = rows.len();
                        if let Err(e) = poller
                            .dispatch_batch_from_reservation(
                                reservation,
                                job_type.clone(),
                                rows,
                                subs,
                            )
                            .await
                        {
                            tracing::error!(
                                job_type = %job_type,
                                n_items,
                                exception.message = %e,
                                exception.type = std::any::type_name_of_val(&e),
                                "failed to dispatch a short-circuit-claimed batch"
                            );
                        }
                    }
                }
            });
        }
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        for (job_type, demand) in other.fresh_demand.drain() {
            *self.fresh_demand.entry(job_type).or_insert(0) += demand;
        }
        self.recycled.append(&mut other.recycled);
        self.claimed.append(&mut other.claimed);
        true
    }

    fn runs_after(&self) -> &[std::any::TypeId] {
        &Self::RUNS_AFTER
    }

    fn on_rollback(self) {
        let Some(poller) = self.poller.upgrade() else {
            return;
        };
        let rows: Vec<(JobId, DateTime<Utc>, JobType)> = self
            .claimed
            .iter()
            .flat_map(|(_, target, _)| match target {
                DispatchTarget::Single(row) => {
                    vec![(row.id, row.execute_at, row.job_type.clone())]
                }
                DispatchTarget::Batch(_, rows) => rows
                    .iter()
                    .map(|row| (row.id, row.execute_at, row.job_type.clone()))
                    .collect(),
            })
            .collect();
        if rows.is_empty() {
            return;
        }
        tokio::spawn(ClaimReconciler::run(poller, rows));
    }
}

struct ClaimReconciler;

impl ClaimReconciler {
    const BACKOFF: [Duration; 3] = [
        Duration::from_millis(250),
        Duration::from_secs(1),
        Duration::from_secs(4),
    ];

    async fn run(poller: Arc<JobPoller>, rows: Vec<(JobId, DateTime<Utc>, JobType)>) {
        for (attempt, backoff) in Self::BACKOFF.into_iter().enumerate() {
            match Self::reconcile_unclaimed(poller.repo.pool(), poller.instance_id, &rows).await {
                Ok((reset_ids, promoted)) if reset_ids.is_empty() && promoted.is_empty() => {
                    return;
                }
                Ok((reset_ids, promoted)) => {
                    let reset_id_set: HashSet<JobId> = reset_ids.into_iter().collect();
                    let mut notify_types: HashSet<JobType> = rows
                        .iter()
                        .filter(|(id, _, _)| reset_id_set.contains(id))
                        .map(|(_, _, job_type)| job_type.clone())
                        .collect();
                    notify_types.extend(
                        promoted
                            .into_iter()
                            .map(|row| JobType::from_owned(row.job_type)),
                    );
                    for job_type in notify_types {
                        poller.notifier.execution_ready(&job_type);
                    }
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        attempt = attempt + 1,
                        exception.message = %error,
                        "claim reconciler retrying after a transient error"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
        tracing::error!(
            n_rows = rows.len(),
            "claim reconciler exhausted its retries; abandoning to reclaim_lost_jobs' slower backstop"
        );
    }

    /// The final `UPDATE`'s `AND je.state = 'running' AND je.poller_instance_id
    /// = $3` re-checks `locked`'s own snapshot-time predicate on the UPDATE
    /// itself, narrowing the same snapshot-vs-lock race
    /// `PromoteHeadsHook::apply_freed`'s doc explains in full: this instance
    /// scopes `locked` to rows it believes it still owns as `running`, but a
    /// legitimate finalize of the SAME row -- landing between `locked`'s
    /// snapshot and its lock being granted (this reconciler exists
    /// precisely for the ambiguous-outcome case where such a race is live)
    /// -- can complete it (delete, so no row to lock at all -- harmless) or
    /// reschedule it (`state = 'pending'`, `poller_instance_id = NULL`,
    /// STILL matching `je.id = l.id` with no re-check). Without the
    /// re-check, this statement would blindly stomp that legitimate
    /// disposition back to a stale `pending`/`execute_at`, discarding
    /// whatever the finalize just wrote. Audited per
    /// job-dev:handoff-promote-missing-state-recheck-race-sb-max13.md §4's
    /// "also audit" item; not the originally reported bug (no canary column
    /// forces a decode error here either way -- the finalize's own
    /// `poller_instance_id` filter already makes an ambiguous double-apply
    /// idempotent-safe on the ENTITY side), but the same defensive shape as
    /// every other multi-row locker of this table now agrees on.
    async fn reconcile_unclaimed(
        pool: &PgPool,
        instance_id: uuid::Uuid,
        rows: &[(JobId, DateTime<Utc>, JobType)],
    ) -> Result<(Vec<JobId>, Vec<PromotedRow>), sqlx::Error> {
        if rows.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let ids: Vec<uuid::Uuid> = rows
            .iter()
            .map(|(id, _, _)| uuid::Uuid::from(*id))
            .collect();
        let execute_ats: Vec<DateTime<Utc>> = rows.iter().map(|(_, at, _)| *at).collect();

        let mut tx = pool.begin().await?;
        let reset = sqlx::query_scalar!(
            r#"
            WITH locked AS MATERIALIZED (
                SELECT je.id, u.execute_at FROM job_executions je
                JOIN UNNEST($1::uuid[], $2::timestamptz[]) AS u(id, execute_at)
                  ON je.id = u.id
                WHERE je.state = 'running' AND je.poller_instance_id = $3
                ORDER BY je.queue_id, je.id
                FOR NO KEY UPDATE OF je
            )
            UPDATE job_executions je
            SET state = 'pending', poller_instance_id = NULL, execute_at = l.execute_at
            FROM locked l
            WHERE je.id = l.id AND je.state = 'running' AND je.poller_instance_id = $3
            RETURNING je.id AS "id!: JobId"
            "#,
            &ids,
            &execute_ats,
            instance_id,
        )
        .fetch_all(&mut *tx)
        .await?;

        let reset_uuids: Vec<uuid::Uuid> = reset.iter().map(|id| uuid::Uuid::from(*id)).collect();
        let promoted = PromoteHeadsHook::apply(&mut tx, &reset_uuids).await?;

        tx.commit().await?;
        Ok((reset, promoted))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{init_pool, row_state, seed_queued_job};
    use super::*;
    use crate::repo::JobRepo;

    async fn seed_landed_running_row(
        pool: &PgPool,
        repo: &JobRepo,
        job_type: &str,
        instance_id: uuid::Uuid,
    ) -> anyhow::Result<JobId> {
        let id = JobId::new();
        let new_job = crate::entity::NewJob::builder()
            .id(id)
            .job_type(JobType::from_owned(job_type.to_string()))
            .config(serde_json::json!({}))?
            .schedule_at(chrono::Utc::now())
            .build()
            .expect("build NewJob");
        repo.create(new_job).await?;

        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, state, attempt_index, execute_at, alive_at, poller_instance_id, created_at) \
             VALUES ($1, $2, 'running', 1, NULL, $3, $4, $3)",
        )
        .bind(uuid::Uuid::from(id))
        .bind(job_type)
        .bind(now)
        .bind(instance_id)
        .execute(pool)
        .await?;
        Ok(id)
    }

    #[tokio::test]
    async fn reconciler_resets_a_row_that_actually_landed_running() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = JobRepo::new(&pool);
        let job_type = format!("reconciler-landed-{}", uuid::Uuid::now_v7());
        let instance_id = uuid::Uuid::now_v7();
        let original_execute_at = chrono::Utc::now() - chrono::Duration::seconds(30);

        let id = seed_landed_running_row(&pool, &repo, &job_type, instance_id).await?;

        let (reset, promoted) = ClaimReconciler::reconcile_unclaimed(
            &pool,
            instance_id,
            &[(id, original_execute_at, JobType::from_owned(job_type))],
        )
        .await?;
        assert_eq!(reset, vec![id]);
        assert!(promoted.is_empty(), "no parked sibling exists in this test");

        let row = sqlx::query!(
            r#"SELECT state::text AS "state!", poller_instance_id, attempt_index, execute_at
               FROM job_executions WHERE id = $1"#,
            uuid::Uuid::from(id),
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.state, "pending");
        assert!(row.poller_instance_id.is_none());
        assert_eq!(
            row.attempt_index, 1,
            "no attempt bump -- this row never ran"
        );
        assert_eq!(
            row.execute_at.map(|at| at.timestamp_millis()),
            Some(original_execute_at.timestamp_millis()),
            "must restore the row's original execute_at, not re-timestamp it to now"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconciler_is_a_noop_for_a_row_that_never_landed() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("reconciler-never-landed-{}", uuid::Uuid::now_v7());
        let instance_id = uuid::Uuid::now_v7();
        let phantom_id = JobId::new();

        let (reset, promoted) = ClaimReconciler::reconcile_unclaimed(
            &pool,
            instance_id,
            &[(
                phantom_id,
                chrono::Utc::now(),
                JobType::from_owned(job_type),
            )],
        )
        .await?;
        assert!(reset.is_empty());
        assert!(promoted.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn reconciler_does_not_touch_a_different_instances_row() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = JobRepo::new(&pool);
        let job_type = format!("reconciler-guard-{}", uuid::Uuid::now_v7());
        let owner_instance = uuid::Uuid::now_v7();
        let our_instance = uuid::Uuid::now_v7();
        let original_execute_at = chrono::Utc::now() - chrono::Duration::seconds(10);

        let id = seed_landed_running_row(&pool, &repo, &job_type, owner_instance).await?;

        let (reset, promoted) = ClaimReconciler::reconcile_unclaimed(
            &pool,
            our_instance,
            &[(id, original_execute_at, JobType::from_owned(job_type))],
        )
        .await?;
        assert!(
            reset.is_empty(),
            "must not reset a row owned by a different instance"
        );
        assert!(promoted.is_empty());
        assert_eq!(row_state(&pool, id).await?, "running");

        Ok(())
    }

    #[tokio::test]
    async fn reconciler_swaps_an_older_parked_sibling_ahead_of_the_reset_row() -> anyhow::Result<()>
    {
        let pool = init_pool().await?;
        let repo = JobRepo::new(&pool);
        let job_type = format!("reconciler-swap-{}", uuid::Uuid::now_v7());
        let instance_id = uuid::Uuid::now_v7();
        let queue = format!("reconciler-swap-queue-{}", uuid::Uuid::now_v7());
        let younger_execute_at = chrono::Utc::now() - chrono::Duration::seconds(5);
        let older_execute_at = chrono::Utc::now() - chrono::Duration::seconds(60);

        let running_id = seed_landed_running_row(&pool, &repo, &job_type, instance_id).await?;
        sqlx::query("UPDATE job_executions SET queue_id = $2 WHERE id = $1")
            .bind(uuid::Uuid::from(running_id))
            .bind(&queue)
            .execute(&pool)
            .await?;

        let older_sibling =
            seed_queued_job(&pool, &job_type, &queue, older_execute_at, "parked").await?;

        let (reset, promoted) = ClaimReconciler::reconcile_unclaimed(
            &pool,
            instance_id,
            &[(
                running_id,
                younger_execute_at,
                JobType::from_owned(job_type),
            )],
        )
        .await?;
        assert_eq!(reset, vec![running_id]);
        assert_eq!(
            promoted.len(),
            1,
            "the older sibling must be promoted in the SAME call"
        );

        assert_eq!(
            row_state(&pool, older_sibling).await?,
            "pending",
            "the older sibling must now hold the queue's active slot"
        );
        assert_eq!(
            row_state(&pool, running_id).await?,
            "parked",
            "the reset row must yield to the genuinely older sibling"
        );

        Ok(())
    }

    /// P1's due-hint skip must WAKE the poll loop, not quietly `.release()`
    /// the reservation: it came from `self.recycled` (a completing
    /// dispatcher's `recycle_into_claim`), which already detached from its
    /// OWN `job_completed`/`batch_completed` release-and-wake specifically
    /// because it expected this hook to hand the unit back.
    /// `UnitReservation::drop`'s unresolved case is the only
    /// remaining signal for that unit -- `.release()` is quiet and belongs
    /// only to the budget-truncation case (where a wake would over-admit
    /// against surviving claims' still-uncommitted headroom), a distinction
    /// `reservation_release_is_quiet_but_unresolved_drop_wakes` (tracker.rs)
    /// pins directly. Fixes job-dev bugbot finding #2 on PR #197 -- before
    /// the fix, this due-hint-skip branch used `.release()` too, leaving a
    /// drained (or just-below-`min_jobs`) type asleep until `MAX_WAIT`
    /// (60s) with no other wake pending.
    ///
    /// No sleep-based sync: the probe-skip is asserted via `PROBE_CALL_
    /// COUNT` (so the test cannot pass vacuously if the gate stopped
    /// skipping), and the wake via a `tokio::time::timeout`-bounded await
    /// on the tracker's own `Notify`, the same pattern
    /// `reservation_release_is_quiet_but_unresolved_drop_wakes` already
    /// uses.
    #[tokio::test]
    async fn due_hint_skip_wakes_the_poll_loop_instead_of_a_quiet_release() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = JobType::from_owned(format!("due-hint-wake-{}", uuid::Uuid::now_v7()));

        let mut registry = crate::registry::JobRegistry::new();
        registry.add_initializer(super::super::test_support::ElasticInitializer {
            job_type: job_type.clone(),
        });

        let tracker = std::sync::Arc::new(crate::tracker::JobTracker::new(0, 10));
        tracker.set_job_types(vec![job_type.clone()]);
        let poller =
            super::super::test_support::build_poller(&pool, registry, Arc::clone(&tracker)).await?;

        tracker.dispatch_job(JobId::new(), &job_type);
        let reservation = tracker.recycle(&job_type);

        PROBE_CALL_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
        let notified = tracker.notified();

        let repo = JobRepo::new(&pool);
        let mut op = repo.begin_op_with_clock(&poller.clock).await?;
        poller.register_claim_recycle(&mut op, &job_type, reservation);
        op.commit().await?;

        assert_eq!(
            PROBE_CALL_COUNT.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the due-hint gate must actually skip the probe here, or this \
             test's wake assertion below would be vacuous"
        );
        tokio::time::timeout(std::time::Duration::from_millis(200), notified)
            .await
            .expect(
                "a due-hint-skipped recycle must wake the poll loop -- its \
                 dispatcher already suppressed its own wake, so this \
                 reservation's drop is the only one left",
            );
        Ok(())
    }

    /// Budget truncation that reaches a RECYCLED reservation must report a
    /// wake as owed. The release itself stays quiet (a wake fired mid-loop
    /// would let the woken poll over-admit against the surviving
    /// reservations' still-uncommitted claims), so the returned flag is the
    /// ONLY remaining signal for that freed unit -- its dispatcher already
    /// detached from `job_completed`'s release-and-wake. Fixes job-dev
    /// bugbot finding #3 on PR #197; before the fix this returned nothing
    /// and the quiet release was the last event, stalling a drainable
    /// backlog until `MAX_WAIT` (60s).
    #[tokio::test]
    async fn budget_truncation_of_a_recycled_reservation_reports_a_wake() {
        let tracker = Arc::new(crate::tracker::JobTracker::new(0, 10));
        let job_type = JobType::from_owned(format!("truncate-recycled-{}", uuid::Uuid::now_v7()));

        tracker.dispatch_job(JobId::new(), &job_type);
        tracker.dispatch_job(JobId::new(), &job_type);

        let mut units_by_type = HashMap::from([(
            job_type.clone(),
            vec![tracker.recycle(&job_type), tracker.recycle(&job_type)],
        )]);
        let n_recycled = HashMap::from([(job_type.clone(), 2)]);

        let notified = tracker.notified();
        let owed_a_wake = truncate_to_unit_budget(&mut units_by_type, &n_recycled, 1);

        assert_eq!(
            units_by_type[&job_type].len(),
            1,
            "a budget of 1 must keep exactly one reservation"
        );
        assert_eq!(
            tracker.units_in_flight(&job_type),
            1,
            "the truncated reservation's unit must still be freed"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), notified)
                .await
                .is_err(),
            "the truncating release must stay quiet -- a mid-loop wake would \
             over-admit against the surviving reservation's claims"
        );
        assert!(
            owed_a_wake,
            "truncating a recycled reservation must report a wake as owed: \
             its dispatcher detached from job_completed, so nothing else \
             will signal that this type's backlog became claimable again"
        );
    }

    /// The mirror case: truncation that only reaches FRESH demand owes no
    /// wake. The spawn/promote that created that demand already ran
    /// `job_execution_inserted` (notify + due-hint), so the row it would
    /// have claimed still has a signal pending -- waking again here would
    /// only over-admit. Also pins the ordering the fix relies on: recycled
    /// reservations are pushed first, so a mixed type spends its fresh
    /// demand before any recycled unit is reached.
    #[tokio::test]
    async fn budget_truncation_of_fresh_demand_only_stays_quiet() {
        let tracker = Arc::new(crate::tracker::JobTracker::new(0, 10));
        let job_type = JobType::from_owned(format!("truncate-fresh-{}", uuid::Uuid::now_v7()));

        let mut units_by_type = HashMap::from([(
            job_type.clone(),
            vec![
                tracker
                    .try_reserve(&job_type, None)
                    .expect("under max_jobs"),
                tracker
                    .try_reserve(&job_type, None)
                    .expect("under max_jobs"),
            ],
        )]);
        assert!(
            !truncate_to_unit_budget(&mut units_by_type, &HashMap::new(), 1),
            "a truncated fresh-demand reservation owes no wake -- its \
             spawn/promote already armed job_execution_inserted's notify"
        );
        assert_eq!(tracker.units_in_flight(&job_type), 1);

        tracker.dispatch_job(JobId::new(), &job_type);
        let mut mixed = HashMap::from([(
            job_type.clone(),
            vec![
                tracker.recycle(&job_type),
                tracker
                    .try_reserve(&job_type, None)
                    .expect("under max_jobs"),
            ],
        )]);
        assert!(
            !truncate_to_unit_budget(&mut mixed, &HashMap::from([(job_type.clone(), 1)]), 1),
            "a mixed type must spend its fresh demand first, leaving the \
             recycled unit intact and no wake owed"
        );
    }
}
