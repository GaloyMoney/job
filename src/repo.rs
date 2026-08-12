use sqlx::PgPool;

use es_entity::*;

use super::entity::*;
use crate::{
    JobId,
    error::JobError,
    job_execution::{JobExecutionRow, JobExecutionState},
    snapshot::JobSnapshot,
};

#[derive(EsRepo, Clone)]
#[es_repo(
    entity = "Job",
    columns(
        job_type(ty = "JobType", update(persist = false)),
        unique_per_type(ty = "bool", update(persist = false)),
        queue_id(ty = "Option<String>", update(persist = false)),
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

    /// Resolve the single unique job of `job_type`, if one exists.
    ///
    /// Direct index-table query (via `es_query!`): the partial unique index
    /// `idx_unique_job_type ON jobs (job_type) WHERE unique_per_type = TRUE`
    /// (migrations/20250904065521_job_setup.sql) guarantees at most one match,
    /// and `jobs` rows are never deleted — so this is a race-free single-row
    /// lookup used to resolve `spawn_unique`'s duplicate path.
    pub(super) async fn find_unique_by_job_type(
        &self,
        job_type: &JobType,
    ) -> Result<Option<Job>, JobError> {
        Ok(es_query!(
            "SELECT id FROM jobs WHERE job_type = $1 AND unique_per_type = TRUE",
            job_type as &JobType,
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Load a point-in-time [`JobSnapshot`] for `id`: the execution row (if the
    /// job is still schedulable/running) paired with the durable entity.
    ///
    /// Both reads run inside a single internally-opened op, and the **entity is
    /// authoritative for terminal state**: once `JobCompleted` is appended the
    /// job is done and its execution row is logically gone, so if the entity is
    /// terminal the row is discarded here. This keeps the snapshot internally
    /// consistent even if a concurrent completion commits between the two
    /// statements under `READ COMMITTED` (which could otherwise return a live
    /// row alongside a terminal entity) — the snapshot never reports
    /// `Pending`/`Running` for a finished job.
    pub(super) async fn load_snapshot_by_id(&self, id: JobId) -> Result<JobSnapshot, JobError> {
        // Read-only op, created internally; dropped (rolled back) without a commit.
        let mut op = self.begin_op().await?;

        let row = sqlx::query_as!(
            JobExecutionRow,
            r#"
            SELECT state AS "state: JobExecutionState", execute_at, attempt_index, alive_at, execution_state_json
            FROM job_executions WHERE id = $1
            "#,
            id as JobId,
        )
        .fetch_optional(op.as_executor())
        .await?;

        let job = self.find_by_id_in_op(&mut op, id).await?;

        let row = if job.terminal_state().is_some() {
            // Terminal entity ⇒ the execution row is logically gone. Discard any
            // row a concurrent read caught mid-completion so the snapshot is
            // consistent (terminal status, no live-row-derived fields).
            None
        } else {
            // Non-terminal entity ⇒ a live execution row must exist. Its absence
            // would be a torn write — impossible, since the terminal DELETE and
            // the terminal events commit atomically (`src/dispatcher.rs`), so a
            // visible DELETE implies visible terminal events — surfaced rather
            // than silently returning a bogus snapshot.
            if row.is_none() {
                return Err(JobError::JobExecutionError(format!(
                    "job {id} has no execution row but its entity is not terminal"
                )));
            }
            row
        };

        Ok(JobSnapshot::from_parts(job, row))
    }
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
