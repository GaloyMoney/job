//! The waiters table (`job_waiters`): a job parked until another is terminal.
//!
//! `job_waiters` carries no entity and no events -- it is a plain relation
//! between two [`JobId`]s -- so it lives here rather than on the
//! event-sourced [`crate::repo::JobRepo`]. Every write is on the caller's
//! `op`; [`JobWaiters::wake_in_op`] is the consumer, driven by
//! `finalizer.rs` when a job reaches a terminal state.

use sqlx::PgPool;

use crate::{JobId, entity::JobType, error::JobError};

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

    /// Register `waiters[n]` on `callees[n]` for callees created in this
    /// same `op` (their execution rows may not even exist yet -- keyless
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
        callees: &[JobId],
        waiters: &[JobId],
    ) -> Result<(), JobError> {
        if callees.is_empty() {
            return Ok(());
        }
        sqlx::query!(
            r#"
            INSERT INTO job_waiters (job_id, waiter_job_id, created_at)
            SELECT DISTINCT t.job_id, t.waiter_job_id, COALESCE($3, NOW())
            FROM UNNEST($1::uuid[], $2::uuid[]) AS t(job_id, waiter_job_id)
            ON CONFLICT (job_id, waiter_job_id) DO UPDATE SET woken_at = NULL
            "#,
            callees as &[JobId],
            waiters as &[JobId],
            op.maybe_now(),
        )
        .execute(op.as_executor())
        .await?;
        Ok(())
    }

    /// Register `waiters[n]` on `callees[n]` for callees that already exist,
    /// returning the callees that were live and so actually attached. A
    /// callee absent from the result is already terminal: nothing was
    /// written for it and the caller should read its outcome rather than
    /// park.
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
    pub(crate) async fn register_waiters_on_live_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        callees: &[JobId],
        waiters: &[JobId],
    ) -> Result<Vec<JobId>, JobError> {
        if callees.is_empty() {
            return Ok(Vec::new());
        }
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
            ON CONFLICT (job_id, waiter_job_id) DO UPDATE SET woken_at = NULL
            RETURNING job_id AS "job_id!: JobId"
            "#,
            callees as &[JobId],
            waiters as &[JobId],
            op.maybe_now(),
        )
        .fetch_all(op.as_executor())
        .await?;
        let mut attached: Vec<JobId> = rows.into_iter().map(|r| r.job_id).collect();
        attached.sort_unstable();
        attached.dedup();
        Ok(attached)
    }

    /// Wake every job parked on one of `terminal`: pull each waiter's row
    /// forward to `now` under the by-id guards, delete the waits of those
    /// actually moved, and mark (`woken_at`) the waits of those not moved.
    /// Returns the rows moved, for the caller to announce.
    ///
    /// A waiter that could not be moved is `running` (its park is being
    /// written by another finalizer, or not yet at all) or `parked` behind a
    /// queue sibling. Its mark is the record that the wake happened, for
    /// whichever write next makes the row runnable to consult -- the
    /// waiter's own `Fresh` disposition write
    /// ([`Self::take_woken_marks_in_op`]) or a parked-to-pending promote
    /// (`execution_hooks::promote`).
    ///
    /// One statement, because the delete and the mark touch provably
    /// disjoint row sets (moved vs. not-moved): fusing writes that could
    /// hit the SAME `job_waiters` row would be the unsupported
    /// "update the same row twice in one statement" case. Data-modifying
    /// CTEs always run to completion whether or not the primary query reads
    /// them, so `consumed` and `marked` apply even though it selects only
    /// from `moved`.
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
            WITH locked AS MATERIALIZED (
                SELECT je.id FROM job_executions je
                WHERE je.id IN (
                        SELECT waiter_job_id FROM job_waiters WHERE job_id = ANY($1)
                    )
                  AND je.state = 'pending'
                  AND je.attempt_index <= 1
                  AND je.execute_at > $2
                ORDER BY je.queue_id, je.id
                FOR NO KEY UPDATE
            ), moved AS (
                UPDATE job_executions je
                   SET execute_at = LEAST(je.execute_at, $2)
                  FROM locked l
                 WHERE je.id = l.id
                   AND je.state = 'pending'
                   AND je.attempt_index <= 1
                   AND je.execute_at > $2
                RETURNING je.id, je.job_type
            ), consumed AS (
                DELETE FROM job_waiters
                 WHERE job_id = ANY($1)
                   AND waiter_job_id IN (SELECT id FROM moved)
            ), marked AS (
                UPDATE job_waiters SET woken_at = $2
                 WHERE job_id = ANY($1)
                   AND woken_at IS NULL
                   AND waiter_job_id NOT IN (SELECT id FROM moved)
            )
            SELECT id AS "id!: JobId", job_type AS "job_type!: JobType" FROM moved
            "#,
            terminal as &[JobId],
            now,
        )
        .fetch_all(op.as_executor())
        .await?;
        Ok(rows.into_iter().map(|r| (r.id, r.job_type)).collect())
    }

    /// The waiters among `ids` that a wake marked while they could not be
    /// moved: deletes those marks and returns the distinct waiters, for the
    /// park write to land due instead of at the deadline they asked for.
    pub(crate) async fn take_woken_marks_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        ids: &[JobId],
    ) -> Result<Vec<JobId>, JobError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query!(
            r#"
            DELETE FROM job_waiters
            WHERE waiter_job_id = ANY($1) AND woken_at IS NOT NULL
            RETURNING waiter_job_id AS "waiter_job_id!: JobId"
            "#,
            ids as &[JobId],
        )
        .fetch_all(op.as_executor())
        .await?;
        let mut waiters: Vec<JobId> = rows.into_iter().map(|r| r.waiter_job_id).collect();
        waiters.sort_unstable();
        waiters.dedup();
        Ok(waiters)
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
