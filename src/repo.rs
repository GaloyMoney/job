use es_entity::clock::ClockHandle;
use sqlx::PgPool;

use es_entity::*;

use super::entity::*;
use super::error::JobError;
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
    pool: PgPool,
}

impl JobRepo {
    pub(super) fn new(pool: &PgPool) -> Self {
        Self { pool: pool.clone() }
    }
}

/// Atomically delete the execution row and record cancellation events.
///
/// Used by both the dispatcher (cooperative cancel) and the monitor task
/// (force-abort) to finalize a cancelled job. Runs the DELETE and event
/// recording in a single transaction to avoid partial state.
pub(crate) async fn finalize_cancelled_job(
    repo: &JobRepo,
    clock: &ClockHandle,
    job_id: JobId,
    instance_id: uuid::Uuid,
) -> Result<(), JobError> {
    let mut op = repo.begin_op_with_clock(clock).await?;
    let mut job = repo.find_by_id(job_id).await?;
    sqlx::query!(
        r#"
        DELETE FROM job_executions
        WHERE id = $1 AND poller_instance_id = $2
        "#,
        job_id as JobId,
        instance_id,
    )
    .execute(op.as_executor())
    .await?;
    job.cancel_job();
    repo.update_in_op(&mut op, &mut job).await?;
    op.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::JobError;

    pub async fn init_pool() -> anyhow::Result<sqlx::PgPool> {
        let pg_con = std::env::var("PG_CON").unwrap();
        let pool = sqlx::PgPool::connect(&pg_con).await?;
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

        // Same type same id
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
        assert!(matches!(err, JobError::DuplicateId(_)));

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
