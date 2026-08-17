use sqlx::PgPool;

use es_entity::*;

use super::entity::*;
use crate::{
    JobId,
    error::JobError,
    job_execution::{JobExecutionRow, JobExecutionState},
    snapshot::JobSnapshot,
};

/// Outcome of a conflict-tolerant event append (see
/// [`JobRepo::append_events_in_op_with_retry`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendOutcome {
    /// The events were appended and will persist with the op's commit.
    Applied,
    /// The job already reached a terminal state — a concurrent writer
    /// finalized it, so there is nothing left to append for this job.
    AlreadyTerminal,
}

/// Result of a conflict-tolerant append: the outcome plus the job's
/// (immutable) type, so callers can drive type-scoped cleanup without a
/// second load.
#[derive(Debug, Clone)]
pub(crate) struct AppendResult {
    pub(crate) id: JobId,
    pub(crate) outcome: AppendOutcome,
    pub(crate) job_type: JobType,
}

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

    /// Append events to a job's event log, tolerating — and merging with —
    /// concurrent appends.
    ///
    /// Every executor-side writer of the `jobs` entity appends commutative
    /// events (completion, abort+reschedule, retry, return-value updates), and
    /// two of them run concurrently BY DESIGN: the shutdown/lost-job killer
    /// can claim a still-running execution while the job's own dispatcher is
    /// finalizing it, and a handler's `set_result` can commit while the killer
    /// is between its entity load and its append. es-entity detects a lost
    /// append race as a unique violation on `job_events (id, sequence)` —
    /// surfaced as `JobModifyError::ConcurrentModification` — which historically
    /// propagated as a hard error, failing `Jobs::shutdown` and wedging job
    /// finalization.
    ///
    /// Because appends commute, the right response to a lost race is to merge:
    /// roll back to a savepoint (a failed INSERT would otherwise abort the
    /// whole op), reload the entity, and append after the winner's events. The
    /// savepoint brackets ONLY the find/apply/update statements; `JobRepo`
    /// registers no commit hooks, so there is no staged-hook state to unwind
    /// (if a hook-bearing write ever moves inside this loop, it must stage its
    /// hooks only after the append succeeds).
    ///
    /// A job that is already terminal is never appended to — the concurrent
    /// writer finalized it — and callers get [`AppendOutcome::AlreadyTerminal`]
    /// to run their cleanup (a terminal job must not keep a schedulable
    /// execution row).
    ///
    /// `apply` may be invoked more than once (once per attempt); it must be
    /// idempotent with respect to its own captured state — i.e. overwrite, not
    /// accumulate, any side-channel data it records for the caller.
    pub(crate) async fn append_events_in_op_with_retry<F>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: &JobId,
        mut apply: F,
    ) -> Result<AppendResult, JobError>
    where
        F: FnMut(&mut Job),
    {
        const MAX_ATTEMPTS: usize = 3;
        const SAVEPOINT: &str = "job_event_append_retry";

        let mut job_type: Option<JobType> = None;
        for attempt in 1..=MAX_ATTEMPTS {
            sqlx::query(&format!("SAVEPOINT {SAVEPOINT}"))
                .execute(op.as_executor())
                .await?;

            let mut job = match self.find_by_id_in_op(&mut *op, id).await {
                Ok(job) => job,
                Err(e) => return Err(JobError::from(e)),
            };
            job_type.get_or_insert_with(|| job.job_type.clone());

            if job.terminal_state().is_some() {
                // Someone else finalized this job while we raced. Release the
                // savepoint (nothing to unwind) and let the caller clean up.
                sqlx::query(&format!("RELEASE SAVEPOINT {SAVEPOINT}"))
                    .execute(op.as_executor())
                    .await?;
                return Ok(AppendResult {
                    id: *id,
                    outcome: AppendOutcome::AlreadyTerminal,
                    job_type: job_type.expect("job type captured on first load"),
                });
            }

            apply(&mut job);

            match self.update_in_op(&mut *op, &mut job).await {
                Ok(_) => {
                    sqlx::query(&format!("RELEASE SAVEPOINT {SAVEPOINT}"))
                        .execute(op.as_executor())
                        .await?;
                    return Ok(AppendResult {
                        id: *id,
                        outcome: AppendOutcome::Applied,
                        job_type: job_type.expect("job type captured on first load"),
                    });
                }
                Err(e) if matches!(e, JobModifyError::ConcurrentModification) => {
                    if attempt == MAX_ATTEMPTS {
                        tracing::error!(
                            job_id = %id,
                            attempt,
                            "job event append lost {MAX_ATTEMPTS} concurrent-modification races"
                        );
                        return Err(JobError::Modify(e));
                    }
                    tracing::warn!(
                        job_id = %id,
                        attempt,
                        "concurrent job-entity modification while appending events, retrying"
                    );
                    // The failed event INSERT left the op's transaction
                    // aborted; ROLLBACK TO SAVEPOINT is the one statement that
                    // works in that state and restores it.
                    sqlx::query(&format!("ROLLBACK TO SAVEPOINT {SAVEPOINT}"))
                        .execute(op.as_executor())
                        .await?;
                }
                Err(e) => return Err(JobError::Modify(e)),
            }
        }
        unreachable!("retry loop returns from every path")
    }

    /// Batch variant of [`append_events_in_op_with_retry`](Self::append_events_in_op_with_retry):
    /// loads `ids`, appends via `apply` to every non-terminal job, and
    /// persists them in one `update_all` — retrying the whole load/apply/persist
    /// cycle (inside a savepoint, so the op stays usable) when a concurrent
    /// writer's append collides on any entity.
    ///
    /// Terminal jobs are skipped (returned as [`AppendOutcome::AlreadyTerminal`])
    /// — their rows are the caller's to clean up. `apply` may run more than
    /// once per entity across retries and must be idempotent in the same
    /// sense as the single-entity variant.
    pub(crate) async fn append_all_events_in_op_with_retry<F>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        ids: &[JobId],
        mut apply: F,
    ) -> Result<Vec<AppendResult>, JobError>
    where
        F: FnMut(&mut Job),
    {
        const MAX_ATTEMPTS: usize = 3;
        const SAVEPOINT: &str = "job_event_append_all_retry";

        for attempt in 1..=MAX_ATTEMPTS {
            sqlx::query(&format!("SAVEPOINT {SAVEPOINT}"))
                .execute(op.as_executor())
                .await?;

            let mut entities = self.find_all_in_op::<Job>(&mut *op, ids).await?;
            let mut results = Vec::with_capacity(ids.len());
            let mut to_persist = Vec::with_capacity(ids.len());
            for id in ids {
                let Some(mut job) = entities.remove(id) else {
                    continue;
                };
                if job.terminal_state().is_some() {
                    results.push(AppendResult {
                        id: *id,
                        outcome: AppendOutcome::AlreadyTerminal,
                        job_type: job.job_type.clone(),
                    });
                    continue;
                }
                apply(&mut job);
                results.push(AppendResult {
                    id: *id,
                    outcome: AppendOutcome::Applied,
                    job_type: job.job_type.clone(),
                });
                to_persist.push(job);
            }

            match self.update_all_in_op(&mut *op, &mut to_persist).await {
                Ok(_) => {
                    sqlx::query(&format!("RELEASE SAVEPOINT {SAVEPOINT}"))
                        .execute(op.as_executor())
                        .await?;
                    return Ok(results);
                }
                Err(e) if matches!(e, JobModifyError::ConcurrentModification) => {
                    if attempt == MAX_ATTEMPTS {
                        tracing::error!(
                            n = ids.len(),
                            attempt,
                            "batch job event append lost {MAX_ATTEMPTS} concurrent-modification races"
                        );
                        return Err(JobError::Modify(e));
                    }
                    tracing::warn!(
                        n = ids.len(),
                        attempt,
                        "concurrent job-entity modification while appending batch events, retrying"
                    );
                    sqlx::query(&format!("ROLLBACK TO SAVEPOINT {SAVEPOINT}"))
                        .execute(op.as_executor())
                        .await?;
                }
                Err(e) => return Err(JobError::Modify(e)),
            }
        }
        unreachable!("retry loop returns from every path")
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

    // ── append_events_in_op_with_retry ───────────────────────────────────
    //
    // These tests reproduce the historical failure deterministically using
    // lock waits rather than sleeps-against-probability: an INSERT that would
    // duplicate an UNCOMMITTED row from another transaction blocks on the
    // unique index until that transaction resolves, so the interleaving
    // (writer A loads -> writer B commits -> A appends) is forced, not raced.

    use crate::entity::NewJob;

    async fn repo_test_pool() -> anyhow::Result<sqlx::PgPool> {
        let pg_con = std::env::var("PG_CON").unwrap();
        Ok(sqlx::PgPool::connect(&pg_con).await?)
    }

    async fn seed_repo_test_job(
        repo: &JobRepo,
        job_type: &str,
        with_execution_row: bool,
    ) -> anyhow::Result<JobId> {
        let id = JobId::new();
        let new_job = NewJob::builder()
            .id(id)
            .job_type(crate::JobType::from_owned(job_type.to_string()))
            .config(serde_json::json!({}))?
            .build()
            .expect("Could not build new job");
        let mut job = repo.create(new_job).await?;
        job.schedule_execution(chrono::Utc::now());
        repo.update(&mut job).await?;
        if with_execution_row {
            sqlx::query(
                "INSERT INTO job_executions \
                 (id, job_type, state, poller_instance_id, attempt_index, alive_at, created_at) \
                 VALUES ($1, $2, 'running', $3, 1, $4, $5)",
            )
            .bind(uuid::Uuid::from(id))
            .bind(job_type)
            .bind(uuid::Uuid::now_v7())
            .bind(chrono::Utc::now())
            .bind(chrono::Utc::now())
            .execute(repo.pool())
            .await?;
        }
        Ok(id)
    }

    async fn max_event_sequence(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<i32> {
        let seq: Option<i32> =
            sqlx::query_scalar("SELECT MAX(sequence) FROM job_events WHERE id = $1")
                .bind(uuid::Uuid::from(id))
                .fetch_one(pool)
                .await?;
        Ok(seq.unwrap_or(0))
    }

    /// A competing writer commits an append between this writer's entity load
    /// and its append — the exact interleaving that used to surface
    /// `ConcurrentModification` from `Jobs::shutdown` (kill vs `set_result`)
    /// and from dispatcher finalization vs the kill. The append must retry on
    /// the fresh entity and merge with the competitor's events.
    #[tokio::test]
    async fn append_merges_with_concurrent_committed_append() -> anyhow::Result<()> {
        let pool = repo_test_pool().await?;
        let repo = JobRepo::new(&pool);
        let id = seed_repo_test_job(&repo, "append-merge-race", false).await?;

        let next_seq = max_event_sequence(&pool, id).await? + 1;

        // Competitor tx: insert an event row at the next sequence but hold it
        // UNCOMMITTED. The helper's own INSERT (same sequence) will block on
        // the unique index until we commit — the forced interleaving.
        let mut competitor = pool.begin().await?;
        sqlx::query(
            "INSERT INTO job_events (id, recorded_at, sequence, event_type, event) \
             VALUES ($1, NOW(), $2, 'execution_completed', '{\"type\":\"execution_completed\"}')",
        )
        .bind(uuid::Uuid::from(id))
        .bind(next_seq)
        .execute(&mut *competitor)
        .await?;

        let append = {
            let pool = pool.clone();
            let repo = JobRepo::new(&pool);
            tokio::spawn(async move {
                let mut op = repo.begin_op().await?;
                let result = repo
                    .append_events_in_op_with_retry(&mut op, &id, |job| {
                        job.abort_execution("killed job".to_string(), chrono::Utc::now(), 7);
                    })
                    .await?;
                op.commit().await?;
                anyhow::Ok(result)
            })
        };

        // Give the append time to reach (and block on) its INSERT.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        competitor.commit().await?;
        let result = append.await??;

        assert_eq!(result.outcome, super::AppendOutcome::Applied);
        assert_eq!(result.id, id);

        // The entity carries BOTH writers' events: the competitor's
        // ExecutionCompleted (sequence next_seq) and our abort events after it.
        let job = repo.find_by_id(id).await?;
        let event_types: Vec<String> = job
            .events()
            .iter_all()
            .map(|e| {
                serde_json::to_value(e)
                    .ok()
                    .and_then(|v| v.get("type").cloned())
                    .and_then(|v| v.as_str().map(String::from))
                    .unwrap_or_default()
            })
            .collect();
        let competitor_idx = event_types
            .iter()
            .position(|t| t == "execution_completed")
            .expect("competitor event persisted");
        let abort_idx = event_types
            .iter()
            .position(|t| t == "execution_aborted")
            .expect("abort event appended after the retry");
        assert!(abort_idx > competitor_idx, "{event_types:?}");
        Ok(())
    }

    /// A terminal job is never appended to: the concurrent writer that
    /// finalized it wins, and the caller learns to clean up instead.
    #[tokio::test]
    async fn append_skips_terminal_job() -> anyhow::Result<()> {
        let pool = repo_test_pool().await?;
        let repo = JobRepo::new(&pool);
        let id = seed_repo_test_job(&repo, "append-terminal-skip", true).await?;

        let mut op = repo.begin_op().await?;
        repo.append_events_in_op_with_retry(&mut op, &id, |job| job.complete_job())
            .await?;
        op.commit().await?;
        let terminal_seq = max_event_sequence(&pool, id).await?;

        let mut op = repo.begin_op().await?;
        let result = repo
            .append_events_in_op_with_retry(&mut op, &id, |job| {
                job.abort_execution("killed job".to_string(), chrono::Utc::now(), 2);
            })
            .await?;
        op.commit().await?;

        assert_eq!(result.outcome, super::AppendOutcome::AlreadyTerminal);
        assert_eq!(max_event_sequence(&pool, id).await?, terminal_seq);
        Ok(())
    }
}
