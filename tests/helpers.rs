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
