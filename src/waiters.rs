//! The waiters table (`job_waiters`): a job parked until another is terminal.
//!
//! `job_waiters` carries no entity and no events -- it is a plain relation
//! between two [`JobId`]s -- so it lives here rather than on the
//! event-sourced [`crate::repo::JobRepo`]. Every write is on the caller's
//! `op`; [`JobWaiters::wake_in_op`] is the consumer, driven by
//! `finalizer.rs` when a job reaches a terminal state.

use sqlx::PgPool;

use crate::{JobId, entity::JobType, error::JobError};

/// One wait to register: `waiter` is to be woken when `callee` reaches a
/// terminal state. Every batch registration in this module -- and every
/// caller assembling one -- pairs the two [`JobId`]s at the call site
/// instead of building two parallel slices, so a callee list can never end
/// up out of sync with its waiter list.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Wait {
    pub(crate) callee: JobId,
    pub(crate) waiter: JobId,
}

/// Writes and reads of `job_waiters`, plus the by-id pull-forward that a
/// wake is built from.
#[derive(Clone)]
pub(crate) struct JobWaiters {
    pool: PgPool,
}

impl JobWaiters {
    pub(crate) fn new(pool: &PgPool) -> Self {
        Self { pool: pool.clone() }
    }

    /// The subset of `ids` whose execution rows still exist: the racy,
    /// lock-free form of the same liveness question
    /// [`Self::register_waiters_on_live_in_op`] answers exactly. Both forms
    /// live here so it is visible that they ARE one question asked two ways.
    ///
    /// Racy is the right trade for the in-memory await path, which re-asks
    /// on a timer and so self-heals; a caller that must not lose the answer
    /// -- one about to commit a durable wait against it -- needs the locking
    /// form instead.
    pub(crate) async fn live_ids(&self, ids: &[JobId]) -> Result<Vec<JobId>, sqlx::Error> {
        let rows = sqlx::query_scalar!(
            "SELECT id FROM job_executions WHERE id = ANY($1)",
            ids as &[JobId],
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(JobId::from).collect())
    }

    /// Register every `wait.waiter` on `wait.callee` for a callee created in
    /// this same `op` (its execution row may not even exist yet -- keyless
    /// rows insert at commit), so there is nothing to lock against: a callee
    /// created here cannot be terminal before this op commits. A repeated
    /// pair re-arms the wait (clears `woken_at`).
    ///
    /// `DISTINCT` is load-bearing, not tidiness: `ON CONFLICT DO UPDATE`
    /// raises "cannot affect row a second time" if one statement presents
    /// the same `(job_id, waiter_job_id)` twice, and a spawn batch reaches
    /// here with duplicates whenever two specs share a `dedup_key` AND a
    /// waiter (the second coalesces onto the first's id, re-presenting the
    /// identical pair).
    pub(crate) async fn insert_waiters_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        waits: &[Wait],
    ) -> Result<(), JobError> {
        if waits.is_empty() {
            return Ok(());
        }
        let callees: Vec<JobId> = waits.iter().map(|w| w.callee).collect();
        let waiters: Vec<JobId> = waits.iter().map(|w| w.waiter).collect();
        sqlx::query!(
            r#"
            INSERT INTO job_waiters (job_id, waiter_job_id, created_at)
            SELECT DISTINCT t.job_id, t.waiter_job_id, COALESCE($3, NOW())
            FROM UNNEST($1::uuid[], $2::uuid[]) AS t(job_id, waiter_job_id)
            ON CONFLICT (job_id, waiter_job_id) DO NOTHING
            "#,
            &callees as &[JobId],
            &waiters as &[JobId],
            op.maybe_now(),
        )
        .execute(op.as_executor())
        .await?;
        Ok(())
    }

    /// Register every `wait.waiter` on `wait.callee` for callees that
    /// already exist, returning the callees that were live and so actually
    /// attached. A callee absent from the result is already terminal:
    /// nothing was written for it and the caller should read its outcome
    /// rather than park.
    ///
    /// `FOR SHARE` is what makes the answer honest: the finalizer that
    /// deletes an execution row takes `FOR UPDATE` on it first, so this
    /// either sees the row and blocks that finalizer until this op commits
    /// (and so is woken), or sees it already gone. Without the lock a
    /// registration could commit just after the wake ran and never be
    /// consumed.
    ///
    /// The locks are taken in one statement in `(queue_id, id)` order -- the
    /// crate-wide order every multi-row locker of `job_executions` uses (see
    /// `execution_hooks::insert`) -- because taking them one callee at a
    /// time in spec order deadlocks against a batch finalizer locking the
    /// same rows in the crate order, and an application-owned `op` gets no
    /// retry.
    ///
    /// For a callee this op is itself creating (a keyless spawn whose
    /// execution row has not been inserted yet), go through
    /// [`Self::register_waiters_in_op`] instead -- this form's `FOR SHARE`
    /// finds nothing for a row that does not exist yet and reports it
    /// terminal.
    pub(crate) async fn register_waiters_on_live_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        waits: &[Wait],
    ) -> Result<Vec<JobId>, JobError> {
        if waits.is_empty() {
            return Ok(Vec::new());
        }
        let callees: Vec<JobId> = waits.iter().map(|w| w.callee).collect();
        let waiters: Vec<JobId> = waits.iter().map(|w| w.waiter).collect();
        let rows = sqlx::query!(
            r#"
            WITH locked AS MATERIALIZED (
                SELECT je.id FROM job_executions je
                WHERE je.id = ANY($1)
                ORDER BY je.queue_id, je.id
                FOR SHARE
            )
            INSERT INTO job_waiters (job_id, waiter_job_id, created_at)
            SELECT DISTINCT t.job_id, t.waiter_job_id, COALESCE($3, NOW())
            FROM UNNEST($1::uuid[], $2::uuid[]) AS t(job_id, waiter_job_id)
            JOIN locked l ON l.id = t.job_id
            ON CONFLICT (job_id, waiter_job_id) DO UPDATE SET job_id = EXCLUDED.job_id
            RETURNING job_id AS "job_id!: JobId"
            "#,
            &callees as &[JobId],
            &waiters as &[JobId],
            op.maybe_now(),
        )
        .fetch_all(op.as_executor())
        .await?;
        let mut attached: Vec<JobId> = rows.into_iter().map(|r| r.job_id).collect();
        attached.sort_unstable();
        attached.dedup();
        Ok(attached)
    }

    /// Register each `waits[n].waiter` on `waits[n].callee`, whether the
    /// callee already exists or is being created on this same `op`.
    /// Returns the callees attached; a callee absent from the result is
    /// terminal and nothing was written for it.
    ///
    /// A callee whose execution row this op will insert at commit
    /// ([`crate::execution_hooks::ExecutionInsertHook::pending_ids`])
    /// cannot be terminal before this op commits, so it takes the lock-free
    /// insert form; every other callee takes the share-locked form that
    /// proves liveness. Both forms exist already; this is only the routing
    /// between them.
    pub(crate) async fn register_waiters_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        waits: &[Wait],
    ) -> Result<Vec<JobId>, JobError> {
        let pending = crate::execution_hooks::ExecutionInsertHook::pending_ids(op);
        let (mut same_op, mut live): (Vec<Wait>, Vec<Wait>) = (Vec::new(), Vec::new());
        for wait in waits {
            if pending.contains(&wait.callee) {
                same_op.push(*wait);
            } else {
                live.push(*wait);
            }
        }
        self.insert_waiters_in_op(op, &same_op).await?;
        let mut attached = self.register_waiters_on_live_in_op(op, &live).await?;
        attached.extend(same_op.into_iter().map(|w| w.callee));
        attached.sort_unstable();
        attached.dedup();
        Ok(attached)
    }

    /// Wake every job waiting on one of `terminal`, and return the rows that
    /// actually moved for the caller to announce.
    ///
    /// ONE write, against `job_executions` only: each waiter is either pulled
    /// forward (pending, first attempt, scheduled later than `now`) or marked
    /// `woken_at` because it could not be. A waiter that could not be moved
    /// is `running` (a claim nulls `execute_at`, so there is no field to
    /// write a time into), `parked` behind a queue sibling (lowering a parked
    /// row's `execute_at` breaks Invariant B), or in retry backoff. The mark
    /// is honoured by whichever write next makes the row runnable -- the
    /// `Disposition::Fresh` park write, or a parked-to-pending promote
    /// (`execution_hooks::promote`) -- each of which clears it in the same
    /// statement.
    ///
    /// The mark lives on the waiter's execution row rather than on the wait,
    /// because it is a property of the WAITER: a wake dedups to distinct
    /// waiters and every consumer read it that way. Keeping it here is what
    /// lets both consults be local column tests on a row they are already
    /// updating, instead of a probe into a table that is O(live waits).
    ///
    /// Every `job_waiters` edge to one of `terminal` is consumed in the same
    /// statement, moved or marked alike -- a callee goes terminal once, so
    /// after this the edge carries nothing. Edges to callees that have NOT
    /// finished have a different `job_id` and are untouched, so a waiter that
    /// re-parks stays attached to them.
    pub(crate) async fn wake_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        terminal: &[JobId],
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<(JobId, JobType)>, JobError> {
        if terminal.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query!(
            r#"
            -- Eligibility is decided INSIDE the locking CTE, not re-checked on
            -- the UPDATE: a `FOR NO KEY UPDATE` scan that blocks resumes on
            -- the LATEST committed row version (EvalPlanQual), so `movable`
            -- is computed against the same version the UPDATE will write, and
            -- the lock is held for the rest of the transaction so nothing can
            -- change underneath it in between. That is also what lets the
            -- flag be returned honestly -- reading it back off `je` would see
            -- the row this statement just wrote.
            WITH locked AS MATERIALIZED (
                SELECT je.id,
                       COALESCE(je.state = 'pending'
                                AND je.attempt_index <= 1
                                AND je.execute_at > $2, false) AS movable
                FROM job_executions je
                WHERE je.id IN (
                        SELECT waiter_job_id FROM job_waiters WHERE job_id = ANY($1)
                    )
                ORDER BY je.queue_id, je.id
                FOR NO KEY UPDATE
            ), woken AS (
                UPDATE job_executions je
                   SET execute_at = CASE WHEN l.movable THEN LEAST(je.execute_at, $2)
                                         ELSE je.execute_at END,
                       woken_at = CASE WHEN l.movable THEN je.woken_at ELSE $2 END
                  FROM locked l
                 WHERE je.id = l.id
                RETURNING je.id, je.job_type, l.movable AS movable
            ), consumed AS (
                DELETE FROM job_waiters WHERE job_id = ANY($1)
            )
            SELECT id AS "id!: JobId", job_type AS "job_type!: JobType"
            FROM woken WHERE movable
            "#,
            terminal as &[JobId],
            now,
        )
        .fetch_all(op.as_executor())
        .await?;
        Ok(rows.into_iter().map(|r| (r.id, r.job_type)).collect())
    }

    /// Every wait registered BY `ids`: a terminal waiter leaves no rows.
    pub(crate) async fn delete_waits_of_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        ids: &[JobId],
    ) -> Result<(), JobError> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query!(
            "DELETE FROM job_waiters WHERE waiter_job_id = ANY($1)",
            ids as &[JobId],
        )
        .execute(op.as_executor())
        .await?;
        Ok(())
    }

    /// `execute_at = LEAST(execute_at, target)` for every one of `ids` that
    /// is parked and eligible: pending, first attempt, scheduled later than
    /// `target`. Returns the rows moved with their type. The by-id twin of
    /// `KeyedJobSpawner::pull_forward_in_op`, with the same guards for the
    /// same reasons (see there); the two differ only in how the row is
    /// addressed.
    ///
    /// Locks in `(queue_id, id)` order for the reason
    /// [`Self::register_waiters_on_live_in_op`] gives, and re-checks the
    /// guards on the `UPDATE` itself because only the `UPDATE`'s own qual is
    /// re-evaluated against the latest row version after a blocked lock
    /// acquisition unblocks (see `execution_hooks::promote`).
    pub(crate) async fn pull_forward_ids_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        ids: &[JobId],
        target: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<(JobId, JobType)>, JobError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query!(
            r#"
            WITH locked AS MATERIALIZED (
                SELECT je.id FROM job_executions je
                WHERE je.id = ANY($1)
                  AND je.state = 'pending'
                  AND je.attempt_index <= 1
                  AND je.execute_at > $2
                ORDER BY je.queue_id, je.id
                FOR NO KEY UPDATE
            )
            UPDATE job_executions je
               SET execute_at = LEAST(je.execute_at, $2)
              FROM locked l
             WHERE je.id = l.id
               AND je.state = 'pending'
               AND je.attempt_index <= 1
               AND je.execute_at > $2
            RETURNING je.id AS "id!: JobId", je.job_type AS "job_type!: JobType"
            "#,
            ids as &[JobId],
            target,
        )
        .fetch_all(op.as_executor())
        .await?;
        Ok(rows.into_iter().map(|r| (r.id, r.job_type)).collect())
    }
}
