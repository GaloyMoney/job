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
        resident(ty = "bool", update(persist = false)),
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

    /// Resolve the LIVE job of `(job_type, key)`, if one exists.
    ///
    /// Direct point-read on `job_executions`: the partial unique index
    /// `idx_job_executions_job_type_unique_key` (migrations/20250904065521_job_setup.sql)
    /// guarantees at most one match, and execution rows exist iff the job is
    /// pending/running — so this is a race-free single-row lookup used to
    /// resolve `KeyedJobSpawner::spawn`'s duplicate-while-live path.
    pub(super) async fn find_live_keyed(
        &self,
        job_type: &JobType,
        key: &str,
    ) -> Result<Option<JobId>, JobError> {
        let row = sqlx::query!(
            r#"SELECT id AS "id: JobId" FROM job_executions WHERE job_type = $1 AND unique_key = $2"#,
            job_type as &JobType,
            key,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.id))
    }

    /// Resolve the keyed singleton of `(job_type, key)`: the LIVE job if one
    /// exists, else the latest generation (for terminal read-back). `None`
    /// only when the key has never been spawned.
    ///
    /// Live-first via `job_executions` (index-exact, see
    /// [`find_live_keyed`](Self::find_live_keyed)), falling back to `jobs`
    /// ordered by `created_at DESC` when no execution row is live — `jobs`
    /// accumulates one row per generation and is never deleted, so the
    /// latest generation is the answer whether it's live or long-terminal.
    pub(super) async fn find_keyed(
        &self,
        job_type: &JobType,
        key: &str,
    ) -> Result<Option<Job>, JobError> {
        if let Some(id) = self.find_live_keyed(job_type, key).await? {
            // `jobs` rows are never deleted, so the entity `find_live_keyed`
            // just resolved an id for is guaranteed to exist.
            return Ok(Some(self.find_by_id(id).await?));
        }
        Ok(es_query!(
            "SELECT id, created_at FROM jobs WHERE job_type = $1 AND unique_key = $2 ORDER BY created_at DESC, id DESC LIMIT 1",
            job_type as &JobType,
            key,
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Resolve the id of the resident job of `job_type`, if one exists.
    ///
    /// There is at most one, ever — enforced by `idx_jobs_job_type_resident`
    /// (migrations/20250904065521_job_setup.sql). `jobs` rows are never
    /// deleted, so unlike [`find_live_keyed`](Self::find_live_keyed) this is
    /// a point-read on `jobs` itself, not `job_executions`.
    pub(super) async fn find_resident_id(
        &self,
        job_type: &JobType,
    ) -> Result<Option<JobId>, JobError> {
        let id = sqlx::query_scalar!(
            r#"SELECT id AS "id: JobId" FROM jobs WHERE job_type = $1 AND resident"#,
            job_type as &JobType,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(id)
    }

    /// `(unique_key, id)` of every keyed singleton of `job_type`, ordered by
    /// key — one entry per key, the live-or-latest generation. The entry
    /// point for [`Jobs::keyed_handles`](crate::Jobs::keyed_handles).
    ///
    /// `jobs` rows alone suffice here (no join against `job_executions`
    /// needed): a new generation can only be created after the prior one
    /// went terminal, so the latest generation by `created_at` is always the
    /// live one when a live one exists.
    pub(super) async fn list_keyed_ids_by_job_type(
        &self,
        job_type: &JobType,
    ) -> Result<Vec<(String, JobId)>, JobError> {
        let rows = sqlx::query!(
            r#"
            SELECT DISTINCT ON (unique_key)
                unique_key AS "unique_key!", id AS "id: JobId"
            FROM jobs
            WHERE job_type = $1 AND unique_key IS NOT NULL
            ORDER BY unique_key, created_at DESC, id DESC
            "#,
            job_type as &JobType,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| (r.unique_key, r.id)).collect())
    }

    /// Read only the committed `execution_state_json` for `id`. `Ok(None)` on
    /// a missing row or unset state.
    ///
    /// The cheap point-read behind [`JobHandle::execution_state`]; unlike
    /// [`load_snapshot_by_id`](Self::load_snapshot_by_id) it does not scan
    /// `job_events`, so it stays flat as retries grow the event log.
    pub(super) async fn execution_state_json_by_id(
        &self,
        id: JobId,
    ) -> Result<Option<serde_json::Value>, JobError> {
        let row = sqlx::query!(
            r#"SELECT execution_state_json FROM job_execution_states WHERE id = $1"#,
            id as JobId,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.execution_state_json))
    }

    /// Load a point-in-time [`JobSnapshot`] for `id`: the execution row (if the
    /// job is still schedulable/running) paired with the durable entity, plus
    /// the execution state.
    ///
    /// The row and entity reads run inside a single internally-opened op, and
    /// the **entity is authoritative for terminal state**: once `JobCompleted`
    /// is appended the job is done and its execution row is logically gone, so
    /// if the entity is terminal the row is discarded here. This keeps the
    /// snapshot internally consistent even if a concurrent completion commits
    /// between the two statements under `READ COMMITTED` (which could
    /// otherwise return a live row alongside a terminal entity) — the
    /// snapshot never reports `Pending`/`Running` for a finished job.
    ///
    /// The execution state, however, is read independently of the row/status
    /// pairing above: a **keyed** job's state row is retained on terminal
    /// (`dispatcher.rs`'s `delete_execution_in_op`), so once the entity is
    /// terminal this re-reads `job_execution_states` directly by id rather
    /// than reusing the (now stale/absent) value the row's join produced —
    /// otherwise a terminal keyed job's retained state would be
    /// unreachable through this path, even though it is still in the DB.
    pub(super) async fn load_snapshot_by_id(&self, id: JobId) -> Result<JobSnapshot, JobError> {
        // Read-only op, created internally; dropped (rolled back) without a commit.
        let mut op = self.begin_op().await?;

        let row = sqlx::query_as!(
            JobExecutionRow,
            r#"
            SELECT je.state AS "state: JobExecutionState", je.execute_at, je.attempt_index,
                   je.alive_at, cp.execution_state_json AS "execution_state_json?"
            FROM job_executions je
            LEFT JOIN job_execution_states cp ON cp.id = je.id
            WHERE je.id = $1
            "#,
            id as JobId,
        )
        .fetch_optional(op.as_executor())
        .await?;

        let job = self.find_by_id_in_op(&mut op, id).await?;

        let (row, execution_state_json) = if job.terminal_state().is_some() {
            // Terminal entity ⇒ the execution row is logically gone. Discard any
            // row a concurrent read caught mid-completion so the snapshot is
            // consistent (terminal status, no live-row-derived fields) — but
            // re-read the state directly, since a keyed job's row outlives
            // the execution row it was joined from above.
            let execution_state_json = sqlx::query_scalar!(
                r#"SELECT execution_state_json FROM job_execution_states WHERE id = $1"#,
                id as JobId,
            )
            .fetch_optional(op.as_executor())
            .await?;
            (None, execution_state_json)
        } else {
            // Non-terminal entity ⇒ a live execution row must exist. Its absence
            // would be a torn write — impossible, since the terminal DELETE and
            // the terminal events commit atomically (`src/dispatcher.rs`), so a
            // visible DELETE implies visible terminal events — surfaced rather
            // than silently returning a bogus snapshot.
            let Some(row) = row else {
                return Err(JobError::JobExecutionError(format!(
                    "job {id} has no execution row but its entity is not terminal"
                )));
            };
            let execution_state_json = row.execution_state_json.clone();
            (Some(row), execution_state_json)
        };

        Ok(JobSnapshot::from_parts(job, row, execution_state_json))
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

    /// Pins `ResidentJobSpawner::spawn`'s absolutely-unique enforcement:
    /// `idx_jobs_job_type_resident` (a dedicated boolean column — not the
    /// `unique_key` string column, so DB-level enforcement doesn't depend on
    /// a sentinel value) rejects a second `resident = true` row of one
    /// `job_type`, forever — unlike keyed rows (see
    /// `unique_per_job_type_and_key` below), there is no terminal/respawn
    /// escape hatch here.
    #[tokio::test]
    async fn resident_is_absolutely_unique() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = JobRepo::new(&pool);
        let type_a = JobType::from_owned(uuid::Uuid::now_v7().to_string());
        let type_b = JobType::from_owned(uuid::Uuid::now_v7().to_string());

        let a_id = JobId::new();
        let new_job = NewJob::builder()
            .id(a_id)
            .resident(true)
            .job_type(type_a.clone())
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        repo.create(new_job).await?;

        // Same type, resident again: conflicts, even with a different id.
        let new_job = NewJob::builder()
            .id(JobId::new())
            .resident(true)
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
        assert!(matches!(err, JobError::DuplicateResident(_)));

        // Different type: ok, not a collision — the flag is per-type.
        let new_job = NewJob::builder()
            .id(JobId::new())
            .resident(true)
            .job_type(type_b)
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        repo.create(new_job).await?;

        // A non-resident job of the same type never collides with the
        // resident row (the index is partial: `WHERE resident`).
        let new_job = NewJob::builder()
            .id(JobId::new())
            .job_type(type_a)
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        repo.create(new_job).await?;

        Ok(())
    }

    /// D13 (rewritten for live-keyed semantics): `jobs` no longer enforces
    /// per-arbitrary-key uniqueness — it accumulates one row per generation
    /// of a keyed singleton, and liveness enforcement moved to
    /// `job_executions` (`idx_job_executions_job_type_unique_key`, see
    /// `migrations/20250904065521_job_setup.sql`). The absolutely-unique
    /// `resident` slot (`ResidentJobSpawner::spawn`) is unaffected by this —
    /// see `resident_is_absolutely_unique` above. This test pins both
    /// halves of the ordinary-key story: (a) two `jobs` rows for one
    /// arbitrary key coexist fine (the generation model), and (b) a second
    /// LIVE `job_executions` row for one `(job_type, unique_key)` conflicts
    /// on that index by name, while a second row inserted after the first
    /// goes terminal (its row deleted, mirroring `dispatcher.rs`'s
    /// completion path) succeeds.
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

        // Same type, same key: `jobs` itself no longer conflicts — a second
        // generation is an ordinary row.
        let new_job = NewJob::builder()
            .id(JobId::new())
            .unique_key("k1")
            .job_type(type_a.clone())
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        repo.create(new_job).await?;

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

        // -- `job_executions`-level liveness enforcement (the new invariant) --
        let type_d = JobType::from_owned(uuid::Uuid::now_v7().to_string());

        let live_id = uuid::Uuid::now_v7();
        sqlx::query!(
            "INSERT INTO jobs (id, unique_key, job_type) VALUES ($1, $2, $3)",
            live_id,
            "k-live",
            &type_d as &JobType,
        )
        .execute(&pool)
        .await?;
        sqlx::query!(
            r#"INSERT INTO job_executions (id, job_type, unique_key, alive_at, created_at)
               VALUES ($1, $2, $3, NOW(), NOW())"#,
            live_id,
            &type_d as &JobType,
            "k-live",
        )
        .execute(&pool)
        .await?;

        // A second LIVE execution of the same `(job_type, unique_key)` conflicts.
        let other_id = uuid::Uuid::now_v7();
        sqlx::query!(
            "INSERT INTO jobs (id, unique_key, job_type) VALUES ($1, $2, $3)",
            other_id,
            "k-live",
            &type_d as &JobType,
        )
        .execute(&pool)
        .await?;
        let conflict = sqlx::query!(
            r#"INSERT INTO job_executions (id, job_type, unique_key, alive_at, created_at)
               VALUES ($1, $2, $3, NOW(), NOW())"#,
            other_id,
            &type_d as &JobType,
            "k-live",
        )
        .execute(&pool)
        .await
        .expect_err("second live execution of the same key must conflict");
        assert_eq!(
            conflict.as_database_error().and_then(|d| d.constraint()),
            Some("idx_job_executions_job_type_unique_key")
        );

        // Once the first generation's execution goes terminal (row deleted,
        // mirroring `dispatcher.rs`'s completion path), the key is
        // respawnable — a second row for the same key now succeeds.
        sqlx::query!("DELETE FROM job_executions WHERE id = $1", live_id)
            .execute(&pool)
            .await?;
        sqlx::query!(
            r#"INSERT INTO job_executions (id, job_type, unique_key, alive_at, created_at)
               VALUES ($1, $2, $3, NOW(), NOW())"#,
            other_id,
            &type_d as &JobType,
            "k-live",
        )
        .execute(&pool)
        .await?;

        Ok(())
    }
}
