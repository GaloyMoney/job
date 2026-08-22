//! Shared test scaffolding. Compiled separately into every integration test
//! binary (`mod helpers;`), so items unused by a given binary need
//! `#[allow(dead_code)]`.

#[allow(dead_code)]
pub async fn init_pool() -> anyhow::Result<sqlx::PgPool> {
    let pg_con = std::env::var("PG_CON").unwrap();
    // Bounded explicitly rather than taking sqlx's default of 10. Under
    // nextest every test is its own process with its own pool, so the default
    // multiplies by the runner's width and exhausts the server's connection
    // slots; the failure then surfaces as unrelated tests timing out.
    //
    // Worth knowing when sizing this: `JobNotificationRouter`'s `LISTEN`
    // connection permanently checks one connection out of whichever pool
    // `Jobs` is given, for the life of `Jobs` (`PgListener::connect_with`
    // in sqlx-core acquires and never releases it). So even fully idle,
    // live headroom on this pool tops out at `max_connections - 1`, not
    // `max_connections`. `JobPoller::pool_unit_budget` (pool-aware
    // claiming) reads exactly that live headroom and divides it by
    // `PER_DISPATCH_UNIT_CONNECTION_COST` (2) to get a poll's dispatch-unit
    // budget -- so at `max_connections(5)` (this pool's size before
    // pool-aware claiming existed), budget bottoms out at
    // `(5 - 1) / 2 = 2` units per poll REGARDLESS of how fairly it's
    // spent across types (see `JobRegistry::plan_claim`'s smallest-first
    // ordering). 2 is workably tight for a single capped-to-1 type, but
    // several existing tests spawn backlogs sized assuming a much more
    // generous per-poll claim (e.g. 20 items at `max_batch_size: 3`,
    // wanting several batch-slots' worth of units at once) -- at budget 2
    // those need several EXTRA poll round-trips to drain, which is not
    // wrong, just slower, and occasionally slow enough to brush a test's
    // own timeout under full-suite load. Measured: `max_connections(5)`
    // reproduces real, if intermittent, timeouts across repeated full-suite
    // runs even with pool-aware claiming's dispatch-unit accounting (not
    // row-count accounting) in place; `max_connections(8)` (budget
    // `(8 - 1) / 2 = 3`) did not, across 5 repeated full-suite runs.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&pg_con)
        .await?;
    Ok(pool)
}

/// A process-unique string for `prefix`.
///
/// The dev DB is persistent, and `job_executions` rows only disappear when a
/// job reaches a terminal state — so a run that is interrupted (a timeout, a
/// panic, a killed `nextest`) leaves `pending`/`running` rows behind. A
/// re-run that reused the same `job_type` would claim those orphans into its
/// own batches, corrupting exact item- and probe-count assertions. Suffixing
/// makes every run's types disjoint; the orphans are inert, because every
/// claim path filters on `registry.registered_job_types()`.
#[allow(dead_code)]
pub fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7())
}

/// [`unique`] as a [`JobType`](job::JobType).
///
/// Leaks the string because `JobType::new` takes `&'static str` and
/// `from_owned` is crate-private. Bounded by the number of types a single
/// test process constructs.
#[allow(dead_code)]
pub fn job_type(prefix: &str) -> job::JobType {
    job::JobType::new(Box::leak(unique(prefix).into_boxed_str()))
}
