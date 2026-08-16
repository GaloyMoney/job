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
        unique_key(ty = "Option<String>", update(persist = false)),
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

    /// Resolve the single keyed singleton of `(job_type, key)`, if one exists.
    ///
    /// Direct index-table query (via `es_query!`): the partial unique index
    /// `idx_jobs_job_type_unique_key ON jobs (job_type, unique_key) WHERE
    /// unique_key IS NOT NULL` (migrations/20250904065521_job_setup.sql)
    /// guarantees at most one match, and `jobs` rows are never deleted — so
    /// this is a race-free single-row lookup used to resolve
    /// `spawn_keyed`/`spawn_unique`'s duplicate path.
    pub(super) async fn find_keyed(
        &self,
        job_type: &JobType,
        key: &str,
    ) -> Result<Option<Job>, JobError> {
        Ok(es_query!(
            "SELECT id FROM jobs WHERE job_type = $1 AND unique_key = $2",
            job_type as &JobType,
            key,
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    /// `(unique_key, id)` of every keyed singleton of `job_type`, ordered by
    /// key. The entry point for [`Jobs::keyed_handles`](crate::Jobs::keyed_handles).
    pub(super) async fn list_keyed_ids_by_job_type(
        &self,
        job_type: &JobType,
    ) -> Result<Vec<(String, JobId)>, JobError> {
        let rows = sqlx::query!(
            r#"
            SELECT unique_key AS "unique_key!", id AS "id: JobId"
            FROM jobs
            WHERE job_type = $1 AND unique_key IS NOT NULL
            ORDER BY unique_key
            "#,
            job_type as &JobType,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| (r.unique_key, r.id)).collect())
    }

    /// Read only the committed `execution_state_json` for `id` — a single-row
    /// SELECT on `job_executions`, no entity hydration, no snapshot
    /// reconciliation. `Ok(None)` on a missing row or unset state.
    ///
    /// The cheap point-read behind [`JobHandle::execution_state`]; unlike
    /// [`load_snapshot_by_id`](Self::load_snapshot_by_id) it does not scan
    /// `job_events`, so it stays flat as retries grow the event log.
    pub(super) async fn execution_state_json_by_id(
        &self,
        id: JobId,
    ) -> Result<Option<serde_json::Value>, JobError> {
        let row = sqlx::query!(
            r#"SELECT execution_state_json FROM job_executions WHERE id = $1"#,
            id as JobId,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| r.execution_state_json))
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

    /// D13: a duplicate `(job_type, unique_key)` insert must map to
    /// `JobError::DuplicateUniqueKey`. es_entity's composite-index column
    /// attribution resolves it to the last key column (`unique_key`) as of
    /// the pinned version — this pins that assumption; if a future es_entity
    /// bump ever attributes it to `job_type` (or leaves it unattributed) the
    /// `From<JobCreateError>` arm in `error.rs` (and, on total
    /// unattribution, this assertion) needs adjusting alongside it.
    #[tokio::test]
    async fn unique_per_job_type_and_key() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = JobRepo::new(&pool);
        let type_a = JobType::from_owned(uuid::Uuid::now_v7().to_string());
        let type_b = JobType::from_owned(uuid::Uuid::now_v7().to_string());
        let type_c = JobType::from_owned(uuid::Uuid::now_v7().to_string());

        let a_id = JobId::new();
        let new_job = NewJob::builder()
            .id(a_id)
            .unique_key("k1")
            .job_type(type_a.clone())
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        repo.create(new_job).await?;

        // Same type, same key.
        let new_job = NewJob::builder()
            .id(JobId::new())
            .unique_key("k1")
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
        assert!(matches!(err, JobError::DuplicateUniqueKey(_)));

        // Same type, different key: ok, not a collision.
        let new_job = NewJob::builder()
            .id(JobId::new())
            .unique_key("k2")
            .job_type(type_a.clone())
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        repo.create(new_job).await?;

        // Same id (regardless of key/type): a primary-key collision, not a
        // unique-key collision.
        let new_job = NewJob::builder()
            .id(a_id)
            .unique_key("k3")
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

        // Different type, same key: ok — the key namespace is per-type.
        let new_job = NewJob::builder()
            .id(JobId::new())
            .unique_key("k1")
            .job_type(type_b)
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        repo.create(new_job).await?;

        // Keyless jobs of one type: never collide with each other or with a
        // keyed job of the same type (`unique_key IS NULL` is outside the
        // partial index).
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
