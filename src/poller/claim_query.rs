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
//! immediately).
//!
//! `min_wait` is a single `MIN(..) WHERE job_type = ANY(..)` aggregate
//! over the same partial index the claim itself uses
//! (`idx_job_executions_pending_execute_at`), not the per-type
//! `CROSS JOIN LATERAL` probe PR #188 introduced when it widened the
//! scope to `rotation_excluded` types. **This is a real trade-off, not a
//! strict win -- read before changing either form back:**
//!
//! - The `ANY(..)` aggregate has no per-type early-out: Postgres visits
//!   every *future* `pending` row matching the scope, then aggregates.
//!   Cost scales with **future rows in scope**, ~flat in type count.
//! - The LATERAL form's per-type `ORDER BY execute_at LIMIT 1` stops
//!   after one row per type. Cost scales with **types in scope**
//!   (~2-4 bufs/type measured), ~flat in backlog depth.
//!
//! Measured (this repo, `min_wait_plan_is_a_single_index_scan_not_a_per_type_fanout`
//! and `min_wait_any_form_cost_is_bounded_under_a_large_future_backlog`):
//! at 40 types / ~80 future rows, `ANY(..)` wins 5-44 bufs vs LATERAL's
//! 81-120 (vacuumed vs churned/unvacuumed); at 13 types / ~7.6k future
//! rows -- PR #188's own bench shape -- LATERAL wins 27-52 bufs vs
//! `ANY(..)`'s 70-800 (same two conditions). That puts the crossover at
//! roughly **40 future rows per scope-type under churn, ~230 freshly
//! vacuumed** -- use the churned figure as the actionable floor, since
//! the scenario below that pushes the ratio up also tends to starve
//! autovacuum of the CPU/IO it needs. Neither a `GROUP BY job_type`
//! rewrite nor any other single-query form tried closes the gap
//! (Postgres has no loose/skip index scan for a cross-group `MIN` here,
//! even on PG18), so this is a chosen regime, not an oversight.
//!
//! `ANY(..)` is primary because lana's real registry currently sits
//! nowhere near that crossover (production AlloyDB QI: 190 -> 318
//! blks/call comparing 0.13.5's `ANY(..)` against 0.13.9's LATERAL, live
//! traffic; point-sampled backlog ~2 future rows / 57 types). **Named
//! risk, not a hypothetical one:** `entity.rs::RetryPolicy::next_attempt_at`
//! reschedules a failed job to `state = 'pending'`, `execute_at = now +
//! backoff` -- squarely inside `min_wait`'s scope -- so a failure storm
//! (a downstream outage, a bad deploy, a saturated pool) that keeps many
//! instances of a few types retrying concurrently is exactly the "few
//! types, deep future backlog" shape this file's `ANY(..)` choice is
//! worse at, and it lands while the system is already degraded. The
//! ceiling this trades against is bounded --
//! `min_wait_any_form_cost_is_bounded_under_a_large_future_backlog` pins
//! it -- so this does not stall the poller, it just costs more DB work
//! exactly when DB work is scarcest.
//!
//! **Revisit trigger (checkable on a live database, not a vague
//! "if this changes"):** run
//! `SELECT job_type, count(*) FROM job_executions WHERE state = 'pending'
//! AND execute_at > now() GROUP BY job_type ORDER BY 2 DESC;` -- if any
//! pollable type's count regularly exceeds ~30 (safety margin below the
//! ~40 churned-crossover above), re-run this file's two `min_wait`
//! benchmarks against that shape and reconsider the form. See
//! `job-dev:handoff-claim-deadline-lazy-eval.md` for the full analysis
//! and PR #193's description for the reproduction.
//!
//! `excluded_due` stays a per-type LATERAL EXISTS probe: it only ever
//! scans `rotation_excluded`, a small set, so the naive shape there still
//! risks a seq scan / full-range read if flattened to a bare `ANY(..)`
//! EXISTS (PR #188).

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
            SELECT MIN(execute_at) AS next_due_at
            FROM job_executions
            WHERE state = 'pending'
              AND job_type = ANY($4::text[] || $8::text[])
              AND execute_at > $2::timestamptz
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

    /// Regression for the 0.13.6-0.13.9 `min_wait` cost blowup
    /// (job-dev:handoff-claim-deadline-lazy-eval.md): PR #188 widened
    /// `min_wait`'s type scope to cover `rotation_excluded` (correct, and
    /// unrelated to this test) but incidentally rewrote it from a single
    /// `ANY(..)` aggregate into a `CROSS JOIN LATERAL` probe *per type* --
    /// one index descent per pollable type instead of one array-keyed scan.
    /// At lana's real registry size (dozens of pollable types, not the
    /// handful other tests here use) that measured +67% blocks/call in
    /// production. A three-type EXPLAIN can't distinguish an O(1) scan from
    /// an O(types) fan-out, so this seeds a realistic count.
    ///
    /// Asserts on plan *shape*, not absolute cost, so it's stable across
    /// environments: the LATERAL form's per-type fan-out is a `Nested Loop`
    /// over the type array; the single-aggregate form is one `Index Scan`
    /// (or `Index Only Scan`) directly on the shared claim index. Reverting
    /// `min_wait` to the LATERAL form fails this deterministically -- the
    /// plan always contains `Nested Loop`, never intermittently.
    #[tokio::test]
    async fn min_wait_plan_is_a_single_index_scan_not_a_per_type_fanout() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let mut conn = pool.acquire().await?;
        // Mirrors the session GUCs `build_internal_pool` sets on the real
        // poll pool (see `src/poller/mod.rs`) -- without them a freshly
        // analyzed, near-empty test table can plan either form as a scan
        // and hide the shape difference this test exists to catch.
        sqlx::query("SET enable_seqscan = off")
            .execute(&mut *conn)
            .await?;
        sqlx::query("SET enable_bitmapscan = off")
            .execute(&mut *conn)
            .await?;

        let pollable: Vec<String> = (0..40)
            .map(|i| format!("min-wait-plan-pollable-{i}-{}", uuid::Uuid::now_v7()))
            .collect();
        let excluded: Vec<String> = (0..5)
            .map(|i| format!("min-wait-plan-excluded-{i}-{}", uuid::Uuid::now_v7()))
            .collect();

        // Verbatim mirror of the `min_wait` CTE in `poll_jobs` above --
        // kept as one SELECT so it can be EXPLAINed on its own; update both
        // together.
        let plan: JsonValue = sqlx::query_scalar(
            r#"
            EXPLAIN (FORMAT JSON)
            SELECT MIN(execute_at) AS next_due_at
            FROM job_executions
            WHERE state = 'pending'
              AND job_type = ANY($1::text[] || $2::text[])
              AND execute_at > $3::timestamptz
            "#,
        )
        .bind(&pollable)
        .bind(&excluded)
        .bind(chrono::Utc::now())
        .fetch_one(&mut *conn)
        .await?;

        let plan_text = plan.to_string();
        assert!(
            !plan_text.contains("Nested Loop"),
            "min_wait regressed to a per-type fan-out (a Nested Loop over \
             the type array) -- this is the 0.13.6-0.13.9 cost regression \
             job-dev:handoff-claim-deadline-lazy-eval.md fixes:\n{plan_text}"
        );
        assert!(
            plan_text.contains("idx_job_executions_pending_execute_at"),
            "min_wait must still use the shared pending-claim index:\n{plan_text}"
        );

        Ok(())
    }

    /// The converse of `min_wait_plan_is_a_single_index_scan_not_a_per_type_fanout`:
    /// pins the accepted COST of the regime the `ANY(..)` form is worse
    /// at, rather than pretending it doesn't exist. #188 measured the
    /// same shape (13 types, ~7.6k future rows -- what lana's registry
    /// would look like if it started scheduling substantial work ahead of
    /// time) at 7,737 bufs for a naive `MIN(..) WHERE job_type = ANY(..)`
    /// aggregate, because that form has no per-type early-out: it visits
    /// every future row in scope, not one row per type. `ANY(..)` is
    /// still chosen (see this file's header) because lana's real
    /// registry sits nowhere near that shape today -- but that choice
    /// should stay a *chosen, bounded* cost, not an unbounded one. This
    /// seeds the same shape and asserts the buffer cost stays
    /// proportional to the future-row count actually in scope (allowing
    /// generous headroom for vacuum/bloat variance -- this repo's own
    /// churned-table repro measured up to ~0.1 bufs/row, #188's
    /// production-churned table ~1.0) and that the plan still uses the
    /// claim index rather than degrading to a sequential scan. A
    /// regression that made this unbounded (e.g. losing the
    /// `execute_at > ..` predicate, or matching outside the intended
    /// scope) fails this even though it would not touch the other test.
    #[tokio::test]
    async fn min_wait_any_form_cost_is_bounded_under_a_large_future_backlog() -> anyhow::Result<()>
    {
        let pool = init_pool().await?;
        let mut conn = pool.acquire().await?;
        sqlx::query("SET enable_seqscan = off")
            .execute(&mut *conn)
            .await?;
        sqlx::query("SET enable_bitmapscan = off")
            .execute(&mut *conn)
            .await?;

        // #188's own bench shape: a handful of types, each carrying a
        // deep future-dated backlog -- not a shape lana's registry is in
        // today (see this file's header), but the one the ANY(..) form
        // is worse at, so it's the one this test must exercise.
        const N_TYPES: i64 = 13;
        const ROWS_PER_TYPE: i64 = 600;
        let run = format!("min-wait-backlog-{}", uuid::Uuid::now_v7());
        for t in 0..N_TYPES {
            let job_type = format!("{run}-{t}");
            for r in 0..ROWS_PER_TYPE {
                seed_pending_job(
                    &pool,
                    &job_type,
                    chrono::Utc::now() + chrono::Duration::minutes(r + 1),
                )
                .await?;
            }
        }
        sqlx::query("ANALYZE job_executions")
            .execute(&mut *conn)
            .await?;

        let scope: Vec<String> = (0..N_TYPES).map(|t| format!("{run}-{t}")).collect();
        let empty: Vec<String> = Vec::new();

        let plan: JsonValue = sqlx::query_scalar(
            r#"
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT MIN(execute_at) AS next_due_at
            FROM job_executions
            WHERE state = 'pending'
              AND job_type = ANY($1::text[] || $2::text[])
              AND execute_at > $3::timestamptz
            "#,
        )
        .bind(&scope)
        .bind(&empty)
        .bind(chrono::Utc::now())
        .fetch_one(&mut *conn)
        .await?;

        let plan_text = plan.to_string();
        assert!(
            !plan_text.contains("Seq Scan"),
            "min_wait must never degrade to a sequential scan, whatever \
             the backlog shape:\n{plan_text}"
        );
        assert!(
            plan_text.contains("idx_job_executions_pending_execute_at"),
            "min_wait must still use the shared pending-claim index:\n{plan_text}"
        );

        let total_future_rows = N_TYPES * ROWS_PER_TYPE;
        // "Shared Hit Blocks" on the top-level Plan node is cumulative
        // (self + every child), so this is the whole query's buffer cost.
        // Generous ceiling: this repo's own churned-table repro measured
        // ~0.1 bufs/row here, #188's production-churned table ~1.0. 2.0
        // leaves headroom for CI variance while still catching an
        // unbounded regression (a lost predicate scanning the whole
        // table would blow past this by orders of magnitude).
        let shared_hit = plan[0]["Plan"]["Shared Hit Blocks"]
            .as_i64()
            .expect("EXPLAIN (BUFFERS) always reports Shared Hit Blocks on the top plan node");
        assert!(
            shared_hit <= total_future_rows * 2,
            "min_wait's ANY(..) form cost {shared_hit} shared buffers for \
             {total_future_rows} future rows in scope -- more than the \
             accepted 2 bufs/row ceiling (see this file's header for the \
             crossover this trades against):\n{plan_text}"
        );

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
