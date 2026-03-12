use es_entity::db;

use es_entity::*;

use super::entity::*;
use crate::JobId;

#[derive(EsRepo, Clone)]
#[es_repo(
    entity = "Job",
    columns(
        job_type(
            ty = "JobType",
            update(persist = false),
            constraint = "idx_unique_job_type"
        ),
        unique_per_type(ty = "bool", update(persist = false)),
    ),
    persist_event_context = false
)]
pub struct JobRepo {
    pool: db::Pool,
}

impl JobRepo {
    pub(super) fn new(pool: &db::Pool) -> Self {
        Self { pool: pool.clone() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::JobError;

    pub async fn init_pool() -> anyhow::Result<db::Pool> {
        let db_name = uuid::Uuid::now_v7();
        let url = format!("sqlite:file:{db_name}?mode=memory&cache=shared");
        let pool = db::Pool::connect(&url).await?;
        sqlx::migrate!().run(&pool).await?;
        Ok(pool)
    }

    #[tokio::test]
    async fn unique_per_job_type() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = JobRepo::new(&pool);
        let type_a = JobType::from_owned(uuid::Uuid::now_v7().to_string());
        let type_b = JobType::from_owned(uuid::Uuid::now_v7().to_string());
        let type_c = JobType::from_owned(uuid::Uuid::now_v7().to_string());

        let a_id = JobId::new();
        let new_job = NewJob::builder()
            .id(a_id)
            .unique_per_type(true)
            .job_type(type_a.clone())
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        repo.create(new_job).await?;

        // Different id same type
        let new_job = NewJob::builder()
            .id(JobId::new())
            .unique_per_type(true)
            .job_type(type_a.clone())
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        let err: JobError = repo
            .create(new_job)
            .await
            .err()
            .expect("expected error")
            .into();
        assert!(matches!(err, JobError::DuplicateUniqueJobType(_)));

        // Same type same id — SQLite may report either the PK or unique
        // index violation first when both are violated simultaneously.
        let new_job = NewJob::builder()
            .id(a_id)
            .unique_per_type(true)
            .job_type(type_a.clone())
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        let err: JobError = repo
            .create(new_job)
            .await
            .err()
            .expect("expected error")
            .into();
        assert!(
            matches!(
                err,
                JobError::DuplicateId(_) | JobError::DuplicateUniqueJobType(_)
            ),
            "Expected DuplicateId or DuplicateUniqueJobType, got: {err:?}"
        );

        let new_job = NewJob::builder()
            .id(JobId::new())
            .unique_per_type(true)
            .job_type(type_b)
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        repo.create(new_job).await?;

        let new_job = NewJob::builder()
            .id(JobId::new())
            .job_type(type_c.clone())
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        repo.create(new_job).await?;
        let new_job = NewJob::builder()
            .id(JobId::new())
            .job_type(type_c.clone())
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        repo.create(new_job).await?;
        let new_job = NewJob::builder()
            .id(a_id)
            .job_type(type_c)
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        let err: JobError = repo
            .create(new_job)
            .await
            .err()
            .expect("expected error")
            .into();
        assert!(matches!(err, JobError::DuplicateId(_)));

        Ok(())
    }
}
