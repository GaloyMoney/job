pub async fn init_pool() -> anyhow::Result<es_entity::db::Pool> {
    let db_name = uuid::Uuid::now_v7();
    let url = format!("sqlite:file:{db_name}?mode=memory&cache=shared");
    let pool = es_entity::db::Pool::connect(&url).await?;
    sqlx::migrate!().run(&pool).await?;
    Ok(pool)
}
