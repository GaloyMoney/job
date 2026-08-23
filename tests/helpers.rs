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
    // claiming) reads exactly that live headroom as a poll's dispatch-unit
    // budget, one connection per unit -- so at `max_connections(5)`, budget
    // bottoms out at `5 - 1 = 4` units per poll REGARDLESS of how fairly
    // it's spent across types (see `JobRegistry::plan_claim`'s
    // smallest-first ordering).
    //
    // An earlier revision of this pool-aware claiming feature priced a unit
    // at 2 connections (to cover a runner using non-`_in_op` convenience
    // methods alongside its own open op) rather than 1, which more than
    // halved the above budget and needed `max_connections(8)` to pass
    // reliably. That per-unit price was dropped in favor of the simpler,
    // uniform 1: the crate cannot know how many connections an arbitrary
    // runner's own code opens (zero, one, or many), so pricing for a
    // specific worst case taxed every OTHER case for nothing, and the
    // asymmetry that justified taxing high in the first place (an
    // under-priced unit hitting a real `PoolTimedOut`) got much cheaper in
    // this same feature: the congestion classification (`congestion.rs`,
    // `CongestionHandler::maybe_reclassify`) now reschedules that
    // job a few seconds out instead of burning a `RetrySettings` attempt.
    // Measured: `max_connections(5)` with the 1-connection-per-unit budget
    // passed the full suite cleanly across 8 repeated runs -- back to this
    // pool's size before pool-aware claiming existed at all.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
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
