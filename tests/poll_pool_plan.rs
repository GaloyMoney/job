//! Regression for `build_poll_pool`'s `enable_seqscan = off` pin
//! (src/poller.rs).
//!
//! Under `plan_cache_mode = force_generic_plan`, a per-type claim probe can
//! fall back to a full heap seq scan when the planner's cost estimate for
//! the pending index is inflated (bloat, or stats reading near-empty) —
//! measured on sb-max9 as the dominant cost of the poll's backlog-independent
//! floor (idle poll: 3,192 -> 59 shared blocks/call with the pin). This test
//! only asserts the *plan shape* the pin buys: no seq scan of
//! `job_executions` under the exact GUCs `build_poll_pool` sets. It does not
//! reproduce the bloat/near-empty-stats trigger condition itself (that needs
//! a churned substrate; see the sb-max9 evidence report for the full bench).
//!
//! Deliberately narrow: it probes a minimal query shaped like one iteration
//! of the poll's `window_rows` LATERAL (not the full multi-CTE poll
//! statement, which is private to `poller.rs`), so it isn't coupled to the
//! poll query's exact text and won't break on unrelated poll-query edits.

mod helpers;

#[tokio::test]
async fn poll_pool_guards_against_seq_scan() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let mut conn = pool.acquire().await?;

    // Mirror `build_poll_pool`'s `after_connect` GUCs exactly.
    sqlx::query("SET plan_cache_mode = force_generic_plan")
        .execute(&mut *conn)
        .await?;
    sqlx::query("SET enable_bitmapscan = off")
        .execute(&mut *conn)
        .await?;
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *conn)
        .await?;

    // Shape of one `window_rows` LATERAL iteration: a per-type prefix probe
    // against the pending-claim index (`idx_job_executions_pending_execute_at`,
    // leading column `job_type`).
    let rows: Vec<(String,)> = sqlx::query_as(
        r#"
        EXPLAIN (FORMAT TEXT)
        SELECT je.id::text
        FROM job_executions je
        WHERE je.state = 'pending'
          AND je.job_type = $1
          AND je.execute_at <= now()
        ORDER BY je.execute_at, je.id
        LIMIT 60
        "#,
    )
    .bind("nonexistent.bench.probe-type")
    .fetch_all(&mut *conn)
    .await?;

    let plan: String = rows
        .into_iter()
        .map(|(line,)| line)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !plan.contains("Seq Scan on job_executions"),
        "poll-pool GUCs (force_generic_plan + enable_bitmapscan=off + enable_seqscan=off) \
         should force ordered index access on job_executions, but the plan fell back to a \
         heap seq scan:\n{plan}"
    );

    Ok(())
}
