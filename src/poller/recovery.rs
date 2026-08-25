//! Crash repair, run on real wall-clock time so liveness keeps moving even
//! under a frozen manual clock. The keep-alive loop heartbeats `alive_at`
//! on this instance's running rows, locked in `(queue_id, id)` order with
//! `FOR NO KEY UPDATE SKIP LOCKED` -- an unordered multi-row writer would
//! re-poison every ordered one, and a skipped beat still bounds the worst
//! gap to half the liveness threshold. The lost handler reclaims rows
//! whose heartbeat went stale, bumping the attempt count (the job MAY
//! have run) and re-swapping the queue.
//!
//! Piggybacked on the lost handler's cadence, the orphan sweep promotes
//! parked rows whose queue lost its active occupant -- a backstop for
//! peers on pre-lock builds, since `ExecutionInsertHook` closes that race
//! at the source. The stale-jobs loop only reports. Every multi-row
//! locker in this module takes `(queue_id, id)` order, the table's one
//! global lock order.

use chrono::{DateTime, Utc};
use sqlx::postgres::PgPool;
use tracing::{Instrument, Span};

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use es_entity::clock::ClockHandle;

use crate::{
    JobId, entity::JobType, execution_hooks::PromoteHeadsHook, notifier::JobEventNotifier,
    task::OwnedTaskHandle, tracker::JobTracker,
};

pub(super) struct Recovery {
    pub(super) pool: PgPool,
    pub(super) clock: ClockHandle,
    pub(super) supported_job_types: Vec<JobType>,
    pub(super) instance_id: uuid::Uuid,
    pub(super) tracker: Arc<JobTracker>,
    pub(super) notifier: Arc<JobEventNotifier>,
    pub(super) job_lost_interval: Duration,
    pub(super) pending_jobs_check_interval: Duration,
}

impl Recovery {
    pub(super) fn spawn_lost_handler(&self) -> OwnedTaskHandle {
        let pool = self.pool.clone();
        let clock = self.clock.clone();
        let supported_job_types = self.supported_job_types.clone();
        let instance_id = self.instance_id;
        let tracker = Arc::clone(&self.tracker);
        let notifier = Arc::clone(&self.notifier);
        let job_lost_interval = self.job_lost_interval;
        OwnedTaskHandle::new(spawn_named_task!("job-poller-lost-handler", async move {
            loop {
                tokio::time::sleep(job_lost_interval / 2).await;
                let alive_threshold = chrono::Utc::now() - job_lost_interval;
                let reschedule_at = clock.now();

                let self_live_ids = tracker.live_job_ids();

                let span = tracing::debug_span!(
                    parent: None,
                    "job.detect_lost_jobs",
                    alive_threshold = %alive_threshold,
                    reschedule_at = %reschedule_at,
                    instance_id = %instance_id,
                    n_live_jobs = self_live_ids.len(),
                    n_lost_jobs = tracing::field::Empty,
                    n_orphaned_parked = tracing::field::Empty,
                );

                async {
                    match reclaim_lost_jobs(
                        &pool,
                        instance_id,
                        &supported_job_types,
                        alive_threshold,
                        reschedule_at,
                        &self_live_ids,
                    )
                    .await
                    {
                        Ok((reclaimed, promoted)) => {
                            Span::current().record("n_lost_jobs", reclaimed.len());
                            let mut reported: HashSet<String> = HashSet::new();
                            let reclaimed_at = chrono::Utc::now();
                            for job in &reclaimed {
                                tracing::error!(
                                    job_id = %job.id,
                                    job_type = %job.job_type,
                                    stall_secs = (reclaimed_at - job.alive_at).num_seconds(),
                                    "lost job"
                                );
                                if reported.insert(job.job_type.to_string()) {
                                    notifier.execution_ready(&job.job_type);
                                }
                            }
                            for promoted_type in promoted {
                                if reported.insert(promoted_type.clone()) {
                                    notifier.execution_ready(&JobType::from_owned(promoted_type));
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                exception.message = %e,
                                exception.type = std::any::type_name_of_val(&e),
                                "lost-handler failed to reclaim lost jobs"
                            );
                            Span::current().record("n_lost_jobs", 0);
                        }
                    }

                    match sweep_orphaned_parked_rows(&pool).await {
                        Ok(promoted) => {
                            Span::current().record("n_orphaned_parked", promoted.len());
                            if !promoted.is_empty() {
                                tracing::warn!(
                                    n_orphaned_parked = promoted.len(),
                                    "recovered orphaned parked rows"
                                );
                            }
                            let mut reported: HashSet<String> = HashSet::new();
                            for job_type in promoted {
                                if reported.insert(job_type.clone()) {
                                    notifier.execution_ready(&JobType::from_owned(job_type));
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                exception.message = %e,
                                exception.type = std::any::type_name_of_val(&e),
                                "lost-handler failed to sweep orphaned parked rows"
                            );
                            Span::current().record("n_orphaned_parked", 0);
                        }
                    }
                }
                .instrument(span)
                .await;
            }
        }))
    }

    pub(super) fn spawn_keep_alive_handler(&self) -> OwnedTaskHandle {
        let pool = self.pool.clone();
        let instance_id = self.instance_id;
        let tracker = Arc::clone(&self.tracker);
        let job_lost_interval = self.job_lost_interval;
        OwnedTaskHandle::new(spawn_named_task!(
            "job-poller-keep-alive-handler",
            async move {
                let mut failures = 0;
                loop {
                    let live_ids = tracker.live_job_ids();

                    let now = chrono::Utc::now();
                    let span = tracing::debug_span!(
                        parent: None,
                        "job.keep_alive",
                        instance_id = %instance_id,
                        now = %now,
                        n_live_jobs = live_ids.len(),
                        failures
                    );

                    let timeout = async {
                        if live_ids.is_empty() {
                            failures = 0;
                            return job_lost_interval / 4;
                        }
                        match sqlx::query!(
                            r#"
                        WITH to_touch AS MATERIALIZED (
                            SELECT id FROM job_executions
                            WHERE poller_instance_id = $2
                              AND state = 'running'
                              AND id = ANY($3)
                            ORDER BY queue_id, id
                            FOR NO KEY UPDATE SKIP LOCKED
                        )
                        UPDATE job_executions je
                        SET alive_at = $1
                        FROM to_touch t
                        WHERE je.id = t.id
                        "#,
                            now,
                            instance_id,
                            &live_ids,
                        )
                        .execute(&pool)
                        .await
                        {
                            Ok(_) => {
                                failures = 0;
                                job_lost_interval / 4
                            }
                            Err(e) => {
                                failures += 1;
                                tracing::error!(
                                    instance_id = %instance_id,
                                    exception.message = %e,
                                    exception.type = std::any::type_name_of_val(&e),
                                    "keep alive error"
                                );
                                Duration::from_millis(50 << failures.min(12))
                            }
                        }
                    }
                    .instrument(span)
                    .await;

                    tokio::time::sleep(timeout).await;
                }
            }
        ))
    }

    pub(super) fn spawn_stale_jobs_handler(&self) -> OwnedTaskHandle {
        let pool = self.pool.clone();
        let clock = self.clock.clone();
        let supported_job_types = self.supported_job_types.clone();
        let pending_jobs_check_interval = self.pending_jobs_check_interval;
        OwnedTaskHandle::new(spawn_named_task!(
            "job-poller-stale-jobs-handler",
            async move {
                loop {
                    tokio::time::sleep(pending_jobs_check_interval).await;
                    let now = clock.now();

                    let span = tracing::info_span!(
                        parent: None,
                        "job.check_stale_pending_jobs",
                        n_stale_pending = tracing::field::Empty,
                        max_pending_duration_secs = tracing::field::Empty,
                    );

                    async {
                        match sqlx::query!(
                            r#"
                        SELECT
                            job_type,
                            COUNT(*)::INT4 AS "count!: i32",
                            EXTRACT(EPOCH FROM ($1::timestamptz - MIN(execute_at)))::FLOAT8
                                AS "max_pending_duration_secs!: f64"
                        FROM job_executions
                        WHERE state = 'pending'
                        AND execute_at <= $1::timestamptz
                        AND job_type = ANY($2)
                        GROUP BY job_type
                        "#,
                            now,
                            &supported_job_types as _,
                        )
                        .fetch_all(&pool)
                        .await
                        {
                            Ok(rows) => {
                                let mut total_stale: i64 = 0;
                                let mut max_pending_secs: f64 = 0.0;

                                for row in &rows {
                                    total_stale += row.count as i64;
                                    if row.max_pending_duration_secs > max_pending_secs {
                                        max_pending_secs = row.max_pending_duration_secs;
                                    }
                                    tracing::warn!(
                                        job_type = %row.job_type,
                                        count = row.count,
                                        max_pending_duration_secs = row.max_pending_duration_secs,
                                        "stale pending jobs detected"
                                    );
                                }

                                Span::current().record("n_stale_pending", total_stale);
                                Span::current()
                                    .record("max_pending_duration_secs", max_pending_secs);
                            }
                            Err(e) => {
                                tracing::error!(
                                    exception.message = %e,
                                    exception.type = std::any::type_name_of_val(&e),
                                    "failed to check stale pending jobs"
                                );
                            }
                        }
                    }
                    .instrument(span)
                    .await;
                }
            }
        ))
    }
}

struct ReclaimedJob {
    id: JobId,
    job_type: JobType,
    alive_at: DateTime<Utc>,
}

async fn reclaim_lost_jobs(
    pool: &PgPool,
    instance_id: uuid::Uuid,
    supported_job_types: &[JobType],
    alive_threshold: DateTime<Utc>,
    reschedule_at: DateTime<Utc>,
    self_live_ids: &[uuid::Uuid],
) -> Result<(Vec<ReclaimedJob>, Vec<String>), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query!(
        r#"
        WITH locked AS MATERIALIZED (
            SELECT je.id FROM job_executions je
            WHERE je.state = 'running'
              AND je.alive_at < $1::timestamptz
              AND je.job_type = ANY($2)
              AND (je.poller_instance_id IS DISTINCT FROM $4 OR je.id <> ALL($5))
            ORDER BY je.queue_id, je.id
            FOR NO KEY UPDATE
        )
        UPDATE job_executions je
        SET state = 'pending', execute_at = $3, attempt_index = attempt_index + 1, poller_instance_id = NULL
        FROM locked l WHERE je.id = l.id
        RETURNING je.id AS "id!: JobId", je.job_type AS "job_type!: JobType",
                  je.alive_at AS "alive_at!"
        "#,
        alive_threshold,
        supported_job_types as _,
        reschedule_at,
        instance_id,
        self_live_ids,
    )
    .fetch_all(&mut *tx)
    .await?;

    let reclaimed_uuids: Vec<uuid::Uuid> = rows.iter().map(|r| uuid::Uuid::from(r.id)).collect();
    let promoted = PromoteHeadsHook::apply(&mut tx, &reclaimed_uuids).await?;

    tx.commit().await?;
    Ok((
        rows.into_iter()
            .map(|r| ReclaimedJob {
                id: r.id,
                job_type: r.job_type,
                alive_at: r.alive_at,
            })
            .collect(),
        promoted.into_iter().map(|row| row.job_type).collect(),
    ))
}

async fn sweep_orphaned_parked_rows(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar!(
        r#"
        WITH orphan_queues AS (
            SELECT DISTINCT p.queue_id
            FROM job_executions p
            WHERE p.state = 'parked'
              AND NOT EXISTS (
                  SELECT 1 FROM job_executions a
                  WHERE a.queue_id = p.queue_id AND a.state IN ('pending', 'running')
              )
        ), heads AS (
            SELECT h.id FROM orphan_queues oq
            CROSS JOIN LATERAL (
                SELECT id FROM job_executions
                WHERE state = 'parked' AND queue_id = oq.queue_id
                ORDER BY execute_at, id
                LIMIT 1
            ) h
        ), locked AS MATERIALIZED (
            SELECT je.id FROM job_executions je
            WHERE je.id IN (SELECT id FROM heads)
            ORDER BY je.queue_id, je.id
            FOR NO KEY UPDATE
        )
        UPDATE job_executions je SET state = 'pending'
        FROM locked l WHERE je.id = l.id
        RETURNING je.job_type
        "#,
    )
    .fetch_all(pool)
    .await
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{init_pool, row_state, seed_queued_job, seed_running_job};
    use super::*;

    #[tokio::test]
    async fn self_reclaim_skips_live_jobs() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let self_id = uuid::Uuid::now_v7();
        let other_id = uuid::Uuid::now_v7();
        let job_type = format!("reclaim-gate-{}", uuid::Uuid::now_v7());
        let stale = chrono::Utc::now() - chrono::Duration::seconds(600);

        let live_self = seed_running_job(&pool, &job_type, self_id, stale).await?;
        let orphan_self = seed_running_job(&pool, &job_type, self_id, stale).await?;
        let other_instance = seed_running_job(&pool, &job_type, other_id, stale).await?;

        let threshold = chrono::Utc::now() - chrono::Duration::seconds(60);
        let reschedule_at = chrono::Utc::now();
        let self_live_ids = vec![uuid::Uuid::from(live_self)];
        let types = vec![JobType::from_owned(job_type.clone())];

        let reclaimed: std::collections::HashSet<JobId> = reclaim_lost_jobs(
            &pool,
            self_id,
            &types,
            threshold,
            reschedule_at,
            &self_live_ids,
        )
        .await?
        .0
        .into_iter()
        .map(|job| job.id)
        .collect();

        assert!(
            reclaimed.contains(&orphan_self),
            "self-owned orphan (no live future) must be reclaimed"
        );
        assert!(
            reclaimed.contains(&other_instance),
            "another instance's stale row must be reclaimed"
        );
        assert!(
            !reclaimed.contains(&live_self),
            "self-owned row with a live runner must NOT be reclaimed"
        );

        let row: (String, Option<uuid::Uuid>, i32) = sqlx::query_as(
            "SELECT state::text, poller_instance_id, attempt_index \
             FROM job_executions WHERE id = $1",
        )
        .bind(uuid::Uuid::from(live_self))
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.0, "running");
        assert_eq!(row.1, Some(self_id));
        assert_eq!(row.2, 1);

        Ok(())
    }

    async fn seed_orphan_with_id(
        pool: &PgPool,
        job_type: &str,
        queue_id: &str,
        id: uuid::Uuid,
    ) -> anyhow::Result<()> {
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
             VALUES ($1, $2, $3, 'parked', 1, NOW() - INTERVAL '600 seconds', NOW(), NOW())",
        )
        .bind(id)
        .bind(job_type)
        .bind(queue_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn orphan_sweep_locks_heads_in_queue_id_order() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("orphan-lockorder-{}", uuid::Uuid::now_v7());

        let qa = format!("orphan-lockorder-qa-{}", uuid::Uuid::now_v7());
        let qb = format!("orphan-lockorder-qb-{}", uuid::Uuid::now_v7());

        let id_b = uuid::Uuid::now_v7();
        let id_a = uuid::Uuid::now_v7();
        assert!(qa < qb && id_b < id_a, "the two orderings must disagree");

        seed_orphan_with_id(&pool, &job_type, &qb, id_b).await?;
        seed_orphan_with_id(&pool, &job_type, &qa, id_a).await?;

        let holder_pool = pool.clone();
        let holder = tokio::spawn(async move {
            let mut tx = holder_pool.begin().await?;
            sqlx::query("SELECT id FROM job_executions WHERE id = $1 FOR NO KEY UPDATE")
                .bind(id_a)
                .fetch_one(&mut *tx)
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            sqlx::query("SELECT id FROM job_executions WHERE id = $1 FOR NO KEY UPDATE")
                .bind(id_b)
                .fetch_one(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok::<_, sqlx::Error>(())
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let sweep = sweep_orphaned_parked_rows(&pool).await;

        sweep.expect("orphan sweep must not deadlock against an ordered holder");
        holder
            .await?
            .expect("holder must not deadlock against the sweep");

        Ok(())
    }

    #[tokio::test]
    async fn orphan_sweeper_recovers_orphaned_parked_row() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("orphan-{}", uuid::Uuid::now_v7());
        let queue = format!("orphan-queue-{}", uuid::Uuid::now_v7());
        let base = chrono::Utc::now() - chrono::Duration::seconds(600);

        let orphan = seed_queued_job(&pool, &job_type, &queue, base, "parked").await?;

        sweep_orphaned_parked_rows(&pool).await?;
        assert_eq!(row_state(&pool, orphan).await?, "pending");

        Ok(())
    }

    #[tokio::test]
    async fn orphan_sweeper_promotes_the_oldest_parked_sibling() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("orphan-multi-{}", uuid::Uuid::now_v7());
        let queue = format!("orphan-multi-queue-{}", uuid::Uuid::now_v7());
        let base = chrono::Utc::now() - chrono::Duration::seconds(600);

        let oldest = seed_queued_job(&pool, &job_type, &queue, base, "parked").await?;
        let _middle = seed_queued_job(
            &pool,
            &job_type,
            &queue,
            base + chrono::Duration::seconds(10),
            "parked",
        )
        .await?;
        let _youngest = seed_queued_job(
            &pool,
            &job_type,
            &queue,
            base + chrono::Duration::seconds(20),
            "parked",
        )
        .await?;

        sweep_orphaned_parked_rows(&pool).await?;

        assert_eq!(row_state(&pool, oldest).await?, "pending");
        let still_parked: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_executions WHERE queue_id = $1 AND state = 'parked'",
        )
        .bind(&queue)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            still_parked, 2,
            "only the oldest sibling is promoted; the rest stay parked"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reclaim_lets_an_older_parked_sibling_run_first() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let instance_id = uuid::Uuid::now_v7();
        let job_type = format!("reclaim-swap-{}", uuid::Uuid::now_v7());
        let queue = format!("reclaim-swap-queue-{}", uuid::Uuid::now_v7());
        let stale_alive_at = chrono::Utc::now() - chrono::Duration::seconds(600);
        let older = stale_alive_at - chrono::Duration::seconds(60);

        let lost = seed_queued_job(&pool, &job_type, &queue, stale_alive_at, "running").await?;
        sqlx::query(
            "UPDATE job_executions SET poller_instance_id = $2, alive_at = $3 WHERE id = $1",
        )
        .bind(uuid::Uuid::from(lost))
        .bind(instance_id)
        .bind(stale_alive_at)
        .execute(&pool)
        .await?;
        let sibling = seed_queued_job(&pool, &job_type, &queue, older, "parked").await?;

        let threshold = chrono::Utc::now() - chrono::Duration::seconds(300);
        let reschedule_at = chrono::Utc::now();
        let (reclaimed, promoted) = reclaim_lost_jobs(
            &pool,
            instance_id,
            &[JobType::from_owned(job_type.clone())],
            threshold,
            reschedule_at,
            &[],
        )
        .await?;
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].id, lost);
        assert_eq!(
            promoted,
            vec![job_type],
            "the reclaim must report the promoted sibling's type so its poller \
             can be woken -- even here, where it happens to match the reclaimed \
             row's own type"
        );

        assert_eq!(
            row_state(&pool, sibling).await?,
            "pending",
            "the older parked sibling must be promoted"
        );
        assert_eq!(
            row_state(&pool, lost).await?,
            "parked",
            "the reclaimed row must yield its slot to the older sibling"
        );

        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_executions WHERE queue_id = $1 AND state IN ('pending','running')",
        )
        .bind(&queue)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active, 1, "Invariant A must still hold after the swap");

        Ok(())
    }

    #[tokio::test]
    async fn reclaim_reports_a_promoted_sibling_of_a_different_type() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let instance_id = uuid::Uuid::now_v7();
        let lost_type = format!("reclaim-cross-lost-{}", uuid::Uuid::now_v7());
        let sibling_type = format!("reclaim-cross-sibling-{}", uuid::Uuid::now_v7());
        let queue = format!("reclaim-cross-queue-{}", uuid::Uuid::now_v7());
        let stale_alive_at = chrono::Utc::now() - chrono::Duration::seconds(600);
        let older = stale_alive_at - chrono::Duration::seconds(60);

        let lost = seed_queued_job(&pool, &lost_type, &queue, stale_alive_at, "running").await?;
        sqlx::query(
            "UPDATE job_executions SET poller_instance_id = $2, alive_at = $3 WHERE id = $1",
        )
        .bind(uuid::Uuid::from(lost))
        .bind(instance_id)
        .bind(stale_alive_at)
        .execute(&pool)
        .await?;
        let sibling = seed_queued_job(&pool, &sibling_type, &queue, older, "parked").await?;

        let threshold = chrono::Utc::now() - chrono::Duration::seconds(300);
        let reschedule_at = chrono::Utc::now();
        let (reclaimed, promoted) = reclaim_lost_jobs(
            &pool,
            instance_id,
            &[JobType::from_owned(lost_type)],
            threshold,
            reschedule_at,
            &[],
        )
        .await?;
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].id, lost);
        assert_eq!(
            promoted,
            vec![sibling_type],
            "the reclaim must report the promoted sibling's OWN type, distinct \
             from every reclaimed row's type, so its poller can be woken"
        );
        assert_eq!(row_state(&pool, sibling).await?, "pending");
        assert_eq!(row_state(&pool, lost).await?, "parked");

        Ok(())
    }
}
