//! The claim statement is one autocommit round trip on the dedicated pool
//! (session GUCs force ordered index access, see `build_internal_pool`): a
//! per-type LATERAL window over `idx_job_executions_pending_execute_at`
//! bounded by each type's own budget (O(budget), flat in backlog;
//! `state = 'pending'` holds only claimable rows, blocked queue backlogs
//! are `parked`), interleaved round-robin so one backlogged type cannot
//! consume the global LIMIT, locked `FOR UPDATE SKIP LOCKED` over a small
//! contention overscan, then budget-enforced and flipped to `running`.
//! Ordering is always `(execute_at, id)`, the total order every
//! head-resolving mechanism in the crate agrees on (see PERFORMANCE.md,
//! "Claim admission").
//!
//! The same statement also computes the sleep window the poll loop parks
//! on: `min_wait` (next FUTURE deadline across planned AND
//! rotation-excluded types), `excluded_due` (a due-NOW row among the
//! excluded types, consumed by `recheck` and kept separate from
//! `may_have_more` so the spin bound can tell the causes apart), and
//! `may_have_more` (claimable work provably left behind, so re-poll
//! immediately). Both sleep probes are per-type first-row index descents,
//! not `ANY(..)` aggregate/EXISTS forms, because the naive shapes let the
//! planner pick seq scans or full-range reads that cost orders of
//! magnitude more (PR #188).

use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use serde_json::Value as JsonValue;
use sqlx::postgres::PgPool;
use tracing::{Span, instrument};

use std::time::Duration;

use super::MAX_WAIT;
use crate::JobId;
use crate::dispatcher::PolledJob;

pub(super) const CONTENTION_HEADROOM: i32 = 4;

#[instrument(
    name = "job.poll_jobs",
    level = "debug",
    skip(pool, pollable_types, rotation_excluded, row_limits, clock),
    fields(n_jobs_to_poll, instance_id = %instance_id, n_jobs_found = tracing::field::Empty)
)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn poll_jobs(
    pool: &PgPool,
    n_jobs_to_poll: usize,
    instance_id: uuid::Uuid,
    pollable_types: &[crate::entity::JobType],
    rotation_excluded: &[crate::entity::JobType],
    row_limits: &[i32],
    headroom: i32,
    clock: &ClockHandle,
) -> Result<JobPollResult, sqlx::Error> {
    let sim_now = clock.now();
    let wall_now = chrono::Utc::now();
    Span::current().record("now", tracing::field::display(sim_now));

    let rows = sqlx::query_as!(
        JobPollRow,
        r#"
        WITH limits AS (
            SELECT l.job_type, l.row_limit,
                   LEAST(l.row_limit, $1::int4) * $7::int4 AS type_window_limit
            FROM UNNEST($4::text[], $6::int4[]) AS l(job_type, row_limit)
            WHERE l.row_limit > 0
        ),
        window_rows AS (
            SELECT d.id, d.execute_at, d.job_type
            FROM limits t
            CROSS JOIN LATERAL (
                SELECT je.id, je.execute_at, je.job_type
                FROM job_executions je
                WHERE je.state = 'pending'
                  AND je.job_type = t.job_type
                  AND je.execute_at <= $2::timestamptz
                ORDER BY je.execute_at, je.id
                LIMIT t.type_window_limit
            ) d
        ),
        ordered_candidates AS (
            SELECT id, execute_at, job_type,
                   ROW_NUMBER() OVER (
                       PARTITION BY job_type ORDER BY execute_at
                   ) AS type_rn
            FROM window_rows
        ),
        locked AS (
            -- FOR UPDATE OF je: bare FOR UPDATE errors on a nullable join side.
            SELECT je.id, je.attempt_index, c.job_type, c.execute_at
            FROM ordered_candidates c
            JOIN job_executions je ON je.id = c.id
            ORDER BY c.type_rn ASC, c.execute_at ASC
            LIMIT $1
            FOR UPDATE OF je SKIP LOCKED
        ),
        selected_jobs AS (
            SELECT t.id, cp.execution_state_json AS data_json, t.attempt_index
            FROM (
                SELECT l.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY l.job_type ORDER BY l.execute_at
                       ) AS type_rn
                FROM locked l
            ) t
            JOIN limits lim ON lim.job_type = t.job_type
            LEFT JOIN job_execution_states cp ON cp.id = t.id
            WHERE t.type_rn <= lim.row_limit
        ),
        updated AS (
            UPDATE job_executions AS je
            SET state = 'running', alive_at = $5, execute_at = NULL, poller_instance_id = $3
            FROM selected_jobs
            WHERE je.id = selected_jobs.id
              AND je.state = 'pending'
            RETURNING je.id, selected_jobs.data_json, je.attempt_index, je.queue_id
        ),
        min_wait AS (
            SELECT MIN(next_due) AS next_due_at
            FROM UNNEST($4::text[] || $8::text[]) AS mt(job_type)
            CROSS JOIN LATERAL (
                SELECT je.execute_at AS next_due
                FROM job_executions je
                WHERE je.state = 'pending'
                  AND je.job_type = mt.job_type
                  AND je.execute_at > $2::timestamptz
                ORDER BY je.execute_at
                LIMIT 1
            ) probe
        ),
        excluded_due AS (
            SELECT EXISTS (
                SELECT 1
                FROM UNNEST($8::text[]) AS et(job_type)
                CROSS JOIN LATERAL (
                    SELECT 1 AS hit
                    FROM job_executions je
                    WHERE je.state = 'pending'
                      AND je.job_type = et.job_type
                      AND je.execute_at <= $2::timestamptz
                    LIMIT 1
                ) probe
            ) AS excluded_due
        ),
        window_counts AS (
            SELECT job_type, COUNT(*) AS cnt FROM window_rows GROUP BY job_type
        ),
        poll_status AS (
            SELECT ((SELECT COUNT(*) FROM locked) >= $1
                 OR (EXISTS (
                        SELECT 1 FROM window_counts wc
                        JOIN limits t ON t.job_type = wc.job_type
                        WHERE wc.cnt >= t.type_window_limit
                     )
                     AND (SELECT COUNT(*) FROM ordered_candidates) > 0)) AS may_have_more
        )
        SELECT * FROM (
            SELECT
                u.id AS "id?: JobId",
                u.data_json AS "data_json?: JsonValue",
                u.attempt_index AS "attempt_index?",
                u.queue_id AS "queue_id?",
                NULL::TIMESTAMPTZ AS "next_due_at?",
                ps.may_have_more AS "may_have_more!",
                ed.excluded_due AS "excluded_due!"
            FROM updated u, poll_status ps, excluded_due ed
            UNION ALL
            SELECT
                NULL::UUID AS "id?: JobId",
                NULL::JSONB AS "data_json?: JsonValue",
                NULL::INT AS "attempt_index?",
                NULL::VARCHAR AS "queue_id?",
                mw.next_due_at AS "next_due_at?",
                ps.may_have_more AS "may_have_more!",
                ed.excluded_due AS "excluded_due!"
            FROM min_wait mw, poll_status ps, excluded_due ed
        ) AS result
        "#,
        n_jobs_to_poll as i32,
        sim_now,
        instance_id,
        pollable_types as _,
        wall_now,
        row_limits,
        headroom,
        rotation_excluded as _,
    )
    .fetch_all(pool)
    .await?;

    Span::current().record("n_jobs_found", rows.len());
    Ok(JobPollResult::from_rows(rows))
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PollWindow {
    next_due_at: Option<DateTime<Utc>>,
    may_have_more: bool,
    pub(super) excluded_due: bool,
}

impl PollWindow {
    pub(super) fn sleep_for(&self, now: DateTime<Utc>) -> Duration {
        if self.may_have_more {
            Duration::ZERO
        } else {
            duration_until(self.next_due_at, now)
        }
    }
}

#[derive(Debug)]
pub(super) enum JobPollResult {
    Jobs {
        jobs: Vec<PolledJob>,
        window: PollWindow,
    },
    WaitTillNextJob(PollWindow),
}

#[derive(Debug)]
struct JobPollRow {
    id: Option<JobId>,
    data_json: Option<JsonValue>,
    attempt_index: Option<i32>,
    queue_id: Option<String>,
    next_due_at: Option<DateTime<Utc>>,
    may_have_more: bool,
    excluded_due: bool,
}

impl JobPollResult {
    fn from_rows(rows: Vec<JobPollRow>) -> Self {
        let mut jobs = Vec::with_capacity(rows.len());
        let mut window = PollWindow {
            next_due_at: None,
            may_have_more: false,
            excluded_due: false,
        };
        for row in rows {
            window.may_have_more = row.may_have_more;
            window.excluded_due = row.excluded_due;
            match (row.id, row.attempt_index) {
                (Some(id), Some(attempt_index)) => jobs.push(PolledJob {
                    id,
                    data_json: row.data_json,
                    attempt: attempt_index as u32,
                    queue_id: row.queue_id,
                }),
                _ => window.next_due_at = row.next_due_at,
            }
        }
        if jobs.is_empty() {
            JobPollResult::WaitTillNextJob(window)
        } else {
            JobPollResult::Jobs { jobs, window }
        }
    }
}

fn duration_until(deadline: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Duration {
    match deadline {
        Some(at) => (at - now).to_std().unwrap_or(Duration::ZERO),
        None => MAX_WAIT,
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{init_pool, seed_pending_job, seed_queued_job};
    use super::*;
    use crate::JobType;

    #[tokio::test]
    async fn capped_type_backlog_does_not_starve_another_type() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let instance_id = uuid::Uuid::now_v7();
        let type_a = format!("budget-dominant-{}", uuid::Uuid::now_v7());
        let type_b = format!("budget-starved-{}", uuid::Uuid::now_v7());

        let n_jobs_to_poll = 2usize;

        let base = chrono::Utc::now() - chrono::Duration::seconds(3600);
        let mut a_ids = Vec::new();
        for i in 0..10i64 {
            a_ids
                .push(seed_pending_job(&pool, &type_a, base + chrono::Duration::seconds(i)).await?);
        }
        let b_id = seed_pending_job(
            &pool,
            &type_b,
            chrono::Utc::now() - chrono::Duration::seconds(1),
        )
        .await?;

        let pollable_types = vec![
            JobType::from_owned(type_a.clone()),
            JobType::from_owned(type_b.clone()),
        ];
        let row_limits = vec![1, n_jobs_to_poll as i32];
        let clock = ClockHandle::realtime();

        let result = poll_jobs(
            &pool,
            n_jobs_to_poll,
            instance_id,
            &pollable_types,
            &[],
            &row_limits,
            CONTENTION_HEADROOM,
            &clock,
        )
        .await?;

        match result {
            JobPollResult::Jobs { jobs, .. } => {
                let claimed: std::collections::HashSet<JobId> = jobs.iter().map(|j| j.id).collect();
                assert!(
                    claimed.contains(&b_id),
                    "B's due row must be claimed: A's older backlog can no \
                     longer consume the window B's budget entitles it to"
                );
                assert_eq!(
                    claimed.iter().filter(|id| a_ids.contains(id)).count(),
                    1,
                    "A is capped at 1 and must claim exactly one row"
                );
                assert_eq!(claimed.len(), 2, "one row per type, both claimed");
            }
            other => panic!("expected a Jobs claim, got {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn blocked_queue_backlog_does_not_consume_the_budget() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let instance_id = uuid::Uuid::now_v7();
        let job_type = format!("cliff-{}", uuid::Uuid::now_v7());
        let hot_queue = format!("hot-{}", uuid::Uuid::now_v7());

        let n_jobs_to_poll = 2usize;
        let base = chrono::Utc::now() - chrono::Duration::seconds(3600);

        seed_queued_job(&pool, &job_type, &hot_queue, base, "running").await?;
        for i in 0..(n_jobs_to_poll as i64 * CONTENTION_HEADROOM as i64 * 3) {
            seed_queued_job(
                &pool,
                &job_type,
                &hot_queue,
                base + chrono::Duration::seconds(i),
                "parked",
            )
            .await?;
        }

        let recent = chrono::Utc::now() - chrono::Duration::seconds(1);
        let mut claimable = Vec::new();
        for _ in 0..2 {
            let q = format!("cold-{}", uuid::Uuid::now_v7());
            claimable.push(seed_queued_job(&pool, &job_type, &q, recent, "pending").await?);
        }

        let pollable_types = vec![JobType::from_owned(job_type.clone())];
        let row_limits = vec![n_jobs_to_poll as i32];
        let clock = ClockHandle::realtime();

        let result = poll_jobs(
            &pool,
            n_jobs_to_poll,
            instance_id,
            &pollable_types,
            &[],
            &row_limits,
            CONTENTION_HEADROOM,
            &clock,
        )
        .await?;

        match result {
            JobPollResult::Jobs { jobs, .. } => {
                let claimed: std::collections::HashSet<JobId> = jobs.iter().map(|j| j.id).collect();
                assert_eq!(
                    claimed.len(),
                    2,
                    "the blocked queue's backlog must not crowd out claimable queues"
                );
                for id in &claimable {
                    assert!(
                        claimed.contains(id),
                        "every unblocked queue head is claimed"
                    );
                }
            }
            other => panic!("expected a Jobs claim, got {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn queue_active_unique_index_enforces_exclusion() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("excl-{}", uuid::Uuid::now_v7());
        let queue = format!("excl-queue-{}", uuid::Uuid::now_v7());
        let now = chrono::Utc::now();

        let insert = |id: uuid::Uuid| {
            let pool = pool.clone();
            let job_type = job_type.clone();
            let queue = queue.clone();
            async move {
                sqlx::query(
                    "INSERT INTO jobs (id, job_type, queue_id, created_at) \
                     VALUES ($1, $2, $3, $4)",
                )
                .bind(id)
                .bind(&job_type)
                .bind(&queue)
                .bind(now)
                .execute(&pool)
                .await?;
                sqlx::query(
                    "INSERT INTO job_executions \
                     (id, job_type, queue_id, state, attempt_index, execute_at, alive_at, created_at) \
                     VALUES ($1, $2, $3, 'pending', 1, $4, $4, $4)",
                )
                .bind(id)
                .bind(&job_type)
                .bind(&queue)
                .bind(now)
                .execute(&pool)
                .await
            }
        };

        let (a, b) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
        let (ra, rb) = tokio::join!(insert(a), insert(b));

        let results = [ra, rb];
        let n_ok = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            n_ok, 1,
            "exactly one concurrent insert must win the queue's active slot"
        );
        let err = results
            .into_iter()
            .find_map(|r| r.err())
            .expect("exactly one insert must fail");
        assert_eq!(
            err.as_database_error().and_then(|d| d.constraint()),
            Some("idx_job_executions_queue_active"),
            "the loser must fail specifically on the exclusion index"
        );

        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_executions WHERE queue_id = $1 AND state IN ('pending','running')",
        )
        .bind(&queue)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active, 1, "at most one active row per queue, ever");

        Ok(())
    }
}
