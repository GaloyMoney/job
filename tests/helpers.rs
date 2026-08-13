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
