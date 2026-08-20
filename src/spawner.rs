//! Job spawner for creating jobs of a specific type.

use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::{marker::PhantomData, sync::Arc};
use tracing::instrument;

use super::{
    Job, JobId,
    entity::{JobType, NewJob},
    error::JobError,
    notifier::JobEventNotifier,
    poller::PollerHandle,
    repo::JobRepo,
};

/// Describes a job to be created as part of a bulk [`JobSpawner::spawn_all`] call.
///
/// Use [`JobSpec::new`] to create a spec with just an id and config, then
/// chain [`JobSpec::schedule_at`] or [`JobSpec::queue_id`] for optional
/// overrides. Bulk spawning is deliberately regular-only — there is no
/// keyed equivalent; use [`KeyedJobSpawner::spawn`](crate::KeyedJobSpawner::spawn)
/// one key at a time.
///
/// # Examples
///
/// ```ignore
/// let specs = vec![
///     JobSpec::new(JobId::new(), MyConfig { value: 1 }),
///     JobSpec::new(JobId::new(), MyConfig { value: 2 })
///         .schedule_at(future_time)
///         .queue_id("my-queue"),
/// ];
/// spawner.spawn_all(specs).await?;
/// ```
pub struct JobSpec<Config> {
    pub id: JobId,
    pub config: Config,
    pub schedule_at: Option<DateTime<Utc>>,
    pub queue_id: Option<String>,
}

impl<Config> JobSpec<Config> {
    pub fn new(id: impl Into<JobId>, config: Config) -> Self {
        Self {
            id: id.into(),
            config,
            schedule_at: None,
            queue_id: None,
        }
    }

    pub fn schedule_at(mut self, schedule_at: DateTime<Utc>) -> Self {
        self.schedule_at = Some(schedule_at);
        self
    }

    pub fn queue_id(mut self, queue_id: impl Into<String>) -> Self {
        self.queue_id = Some(queue_id.into());
        self
    }
}

/// A handle for spawning jobs of a specific type.
///
/// Returned by [`crate::Jobs::add_initializer`]. The spawner encapsulates the job type
/// and provides type-safe job creation methods.
///
/// # Examples
///
/// ```ignore
/// // Registration returns a spawner
/// let spawner = jobs.add_initializer(MyInitializer);
///
/// // Use the spawner to create jobs
/// spawner.spawn(JobId::new(), MyConfig { value: 42 }).await?;
/// ```
#[derive(Clone)]
pub struct JobSpawner<Config> {
    repo: Arc<JobRepo>,
    job_type: JobType,
    clock: ClockHandle,
    notifier: Arc<JobEventNotifier>,
    /// Reaches this process's poller for the short-circuit spawn fast path
    /// (§3.2 of the handoff this implements) once it exists. See
    /// [`PollerHandle`].
    poller_ref: PollerHandle,
    _phantom: PhantomData<Config>,
}

impl<Config> JobSpawner<Config>
where
    Config: Serialize + Send + Sync,
{
    pub(crate) fn new(
        repo: Arc<JobRepo>,
        job_type: JobType,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
        poller_ref: PollerHandle,
    ) -> Self {
        Self {
            repo,
            job_type,
            clock,
            notifier,
            poller_ref,
            _phantom: PhantomData,
        }
    }

    /// Returns the job type this spawner creates.
    pub fn job_type(&self) -> &JobType {
        &self.job_type
    }

    /// Create and spawn a job for immediate execution.
    #[instrument(
        name = "job_spawner.spawn",
        skip(self, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn(
        &self,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
    ) -> Result<Job, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let job = self.spawn_in_op(&mut op, id, config).await?;
        op.commit().await?;
        Ok(job)
    }

    /// Create and spawn a job as part of an existing atomic operation.
    #[instrument(
        name = "job_spawner.spawn_in_op",
        skip(self, op, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
    ) -> Result<Job, JobError> {
        let schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());
        self.spawn_at_in_op(op, id, config, schedule_at).await
    }

    /// Create and spawn a job for execution at a specific time.
    #[instrument(
        name = "job_spawner.spawn_at",
        skip(self, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_at(
        &self,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
    ) -> Result<Job, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let job = self
            .spawn_at_in_op(&mut op, id, config, schedule_at)
            .await?;
        op.commit().await?;
        Ok(job)
    }

    /// Create and spawn a job for execution at a specific time as part of an existing atomic operation.
    #[instrument(
        name = "job_spawner.spawn_at_in_op",
        skip(self, op, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_at_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
    ) -> Result<Job, JobError> {
        self.create_job_internal(op, id.into(), config, schedule_at, None)
            .await
    }

    /// Create and spawn a job for immediate execution within a queue.
    ///
    /// At most one job per `queue_id` will run globally at any time.
    #[instrument(
        name = "job_spawner.spawn_with_queue_id",
        skip(self, config, queue_id),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_with_queue_id(
        &self,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        queue_id: impl Into<String> + Send,
    ) -> Result<Job, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let job = self
            .spawn_with_queue_id_in_op(&mut op, id, config, queue_id)
            .await?;
        op.commit().await?;
        Ok(job)
    }

    /// Create and spawn a job within a queue as part of an existing atomic operation.
    ///
    /// At most one job per `queue_id` will run globally at any time.
    #[instrument(
        name = "job_spawner.spawn_with_queue_id_in_op",
        skip(self, op, config, queue_id),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_with_queue_id_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        queue_id: impl Into<String> + Send,
    ) -> Result<Job, JobError> {
        let schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());
        self.spawn_at_with_queue_id_in_op(op, id, config, schedule_at, queue_id)
            .await
    }

    /// Create and spawn a job for execution at a specific time within a queue.
    ///
    /// At most one job per `queue_id` will run globally at any time.
    #[instrument(
        name = "job_spawner.spawn_at_with_queue_id",
        skip(self, config, queue_id),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_at_with_queue_id(
        &self,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
        queue_id: impl Into<String> + Send,
    ) -> Result<Job, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let job = self
            .spawn_at_with_queue_id_in_op(&mut op, id, config, schedule_at, queue_id)
            .await?;
        op.commit().await?;
        Ok(job)
    }

    /// Create and spawn a job for execution at a specific time within a queue,
    /// as part of an existing atomic operation.
    ///
    /// At most one job per `queue_id` will run globally at any time.
    #[instrument(
        name = "job_spawner.spawn_at_with_queue_id_in_op",
        skip(self, op, config, queue_id),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_at_with_queue_id_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
        queue_id: impl Into<String> + Send,
    ) -> Result<Job, JobError> {
        self.create_job_internal(op, id.into(), config, schedule_at, Some(queue_id.into()))
            .await
    }

    /// Create and spawn multiple jobs in a single atomic operation.
    ///
    /// All jobs are created within a single transaction — either all succeed or all roll back.
    /// Each [`JobSpec`] can independently specify `schedule_at` and `queue_id`.
    #[instrument(
        name = "job_spawner.spawn_all",
        skip(self, specs),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_all(&self, specs: Vec<JobSpec<Config>>) -> Result<Vec<Job>, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let jobs = self.spawn_all_in_op(&mut op, specs).await?;
        op.commit().await?;
        Ok(jobs)
    }

    /// Create and spawn multiple jobs as part of an existing atomic operation.
    ///
    /// Each [`JobSpec`] can independently specify `schedule_at` and `queue_id`.
    /// Internally uses batch inserts for both the job entities and `job_executions` rows.
    #[instrument(
        name = "job_spawner.spawn_all_in_op",
        skip(self, op, specs),
        fields(job_type = %self.job_type, count)
    )]
    pub async fn spawn_all_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        specs: Vec<JobSpec<Config>>,
    ) -> Result<Vec<Job>, JobError> {
        tracing::Span::current().record("count", specs.len());
        if specs.is_empty() {
            return Ok(Vec::new());
        }

        let default_schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());

        let mut new_jobs = Vec::with_capacity(specs.len());
        let mut schedule_times = Vec::with_capacity(specs.len());
        let mut queue_ids: Vec<Option<String>> = Vec::with_capacity(specs.len());

        for spec in specs {
            schedule_times.push(spec.schedule_at.unwrap_or(default_schedule_at));

            let mut builder = NewJob::builder();
            builder
                .id(spec.id)
                .job_type(self.job_type.clone())
                .config(spec.config)?
                .tracing_context(es_entity::context::TracingContext::current())
                .queue_id(spec.queue_id.clone());
            let new_job = builder.build().expect("Could not build new job");
            new_jobs.push(new_job);
            queue_ids.push(spec.queue_id);
        }

        let mut jobs = self.repo.create_all_in_op(op, new_jobs).await?;

        let ids: Vec<JobId> = jobs.iter().map(|j| j.id).collect();
        // `unique_key` is always NULL here: keyed and bulk spawning are
        // disjoint APIs (`JobSpec` deliberately carries no unique_key — see
        // `KeyedJobSpawner::spawn` for the keyed path).
        //
        // Same park-or-take semantics as `insert_execution` (no swap logic
        // for the bulk path — out of scope, see the handoff this
        // implements): try every row as `pending`, `ON CONFLICT DO NOTHING`
        // against the queue's active slot (this also resolves conflicts
        // BETWEEN rows of this same bulk call that share a `queue_id`:
        // Postgres evaluates the arbiter per row within one statement, so at
        // most one of them lands `pending` and the rest see the first as
        // already occupying the slot); whichever didn't land lands `parked`.
        let landed_pending: Vec<JobId> = sqlx::query_scalar(
            r#"
            WITH ins AS (
                INSERT INTO job_executions
                    (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at)
                SELECT unnested.id, $2, unnested.queue_id, NULL, 'pending', 1, unnested.execute_at,
                       COALESCE($5, NOW()), COALESCE($5, NOW())
                FROM UNNEST($1::uuid[], $3::text[], $4::timestamptz[])
                    AS unnested(id, queue_id, execute_at)
                ON CONFLICT (queue_id) WHERE state IN ('pending','running') AND queue_id IS NOT NULL
                DO NOTHING
                RETURNING id
            ), parked AS (
                INSERT INTO job_executions
                    (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at)
                SELECT unnested.id, $2, unnested.queue_id, NULL, 'parked', 1, unnested.execute_at,
                       COALESCE($5, NOW()), COALESCE($5, NOW())
                FROM UNNEST($1::uuid[], $3::text[], $4::timestamptz[])
                    AS unnested(id, queue_id, execute_at)
                WHERE unnested.id NOT IN (SELECT id FROM ins)
                RETURNING id
            )
            SELECT id FROM ins
            "#,
        )
        .bind(&ids)
        .bind(&self.job_type)
        .bind(&queue_ids)
        .bind(&schedule_times)
        .bind(op.maybe_now())
        .fetch_all(op.as_executor())
        .await?;

        // Nothing to wake a poller for if every row of this call landed
        // parked (rare: only when every distinct queue_id in the batch was
        // already occupied). Otherwise at least one row is claimable.
        if !landed_pending.is_empty() {
            self.notifier
                .execution_ready_in_op(op, &self.job_type)
                .await?;

            // Head-swap short-circuit for the bulk path (handoff addendum
            // §7.3 site 4): greedily reserve as much capacity as this call's
            // own DUE-NOW count could use, claiming this type's oldest due
            // backlog per reservation. Under-reservation is not a failure --
            // every row already landed pending/parked above regardless, so
            // whatever isn't claimed here is picked up by the ordinary poll
            // exactly as before this addendum.
            //
            // `n_due` must be the due-now subset of `landed_pending`, not
            // its raw length: `landed_pending` includes rows whose
            // `schedule_at` is in the future (an explicit per-spec
            // `JobSpec::schedule_at`), which are not claimable and must not
            // count toward how many reservations to attempt -- mirrors the
            // single-spawn path's own `schedule_at <= self.clock.now()`
            // gate. Without it, a `spawn_all` of entirely future-scheduled
            // work could still reserve and drain this type's ENTIRE
            // unrelated due backlog, bypassing `next_batch_size`'s
            // `min_jobs` throttle for a call that has nothing to do with
            // due-now admission.
            if let Some(poller) = self.poller_ref.get().and_then(|w| w.upgrade()) {
                let now = op.maybe_now().unwrap_or(default_schedule_at);
                let n_due = count_due_now(&landed_pending, &ids, &schedule_times, now);
                poller.register_claim_demand(op, &self.job_type, n_due);
            }
        }

        for (job, schedule_at) in jobs.iter_mut().zip(&schedule_times) {
            job.schedule_execution(*schedule_at);
        }
        self.repo.update_all_in_op(op, &mut jobs).await?;

        Ok(jobs)
    }

    #[instrument(name = "job.create_internal", skip(self, op, config), fields(job_type = %self.job_type))]
    async fn create_job_internal<C: Serialize + Send>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
        config: C,
        schedule_at: DateTime<Utc>,
        queue_id: Option<String>,
    ) -> Result<Job, JobError> {
        let new_job = NewJob::builder()
            .id(id)
            .job_type(self.job_type.clone())
            .config(config)?
            .tracing_context(es_entity::context::TracingContext::current())
            .queue_id(queue_id.clone())
            .build()
            .expect("Could not build new job");

        let mut job = self.repo.create_in_op(op, new_job).await?;
        // @@ I was thinking more something like:
        // op.add_hook(InsertExecutionHook::new(...))

        insert_execution(
            &self.repo,
            &self.notifier,
            op,
            &mut job,
            schedule_at,
            queue_id.as_deref(),
            None,
        )
        .await?;

        // Head-swap short-circuit (handoff addendum §7.1/§7.3 site 1): a
        // due-now spawn tries to claim capacity for this type's oldest due
        // backlog -- which may or may not be the row inserted just above.
        // Runs as a LATER statement in the same `op`, so it sees that insert
        // with guaranteed ordering. `schedule_at` in the future (an explicit
        // `spawn_at`) never qualifies -- nothing of this type is claimable
        // sooner just because this call happened.
        // // @@ This should be part of the same insert hook
        // or perhaps its own hook that insert registeres
        if schedule_at <= self.clock.now()
            && let Some(poller) = self.poller_ref.get().and_then(|w| w.upgrade())
        {
            poller.register_claim_demand(op, &self.job_type, 1);
        }

        Ok(job)
    }
}

/// The due-now subset count of `landed_pending`, cross-referenced against
/// `ids`/`schedule_times` (the parallel arrays `spawn_all_in_op` builds
/// before its bulk insert). Extracted as a pure function -- no DB, no
/// poller -- specifically so this counting logic is unit-testable without
/// the live-background-poller race that made an earlier integration-test
/// version of this check (asserting on row state after a real `spawn_all`
/// call, with a real poll loop running) flaky: `spawn_all_in_op` always
/// fires `execution_ready_in_op` when anything landed pending, which wakes
/// that live poller regardless of whether THIS count is right, so it could
/// independently claim an unrelated due backlog row before any such
/// assertion ran -- a false failure unrelated to what this function computes.
///
/// `landed_pending` may include rows whose `schedule_at` is in the future
/// (an explicit per-spec `JobSpec::schedule_at`); those must not count
/// toward how many reservations `try_claim_after_bulk_spawn` should
/// attempt, mirroring the single-spawn path's own
/// `schedule_at <= self.clock.now()` gate.
fn count_due_now(
    landed_pending: &[JobId],
    ids: &[JobId],
    schedule_times: &[DateTime<Utc>],
    now: DateTime<Utc>,
) -> usize {
    let due_by_id: std::collections::HashMap<JobId, DateTime<Utc>> = ids
        .iter()
        .copied()
        .zip(schedule_times.iter().copied())
        .collect();
    landed_pending
        .iter()
        .filter(|id| due_by_id.get(id).is_some_and(|at| *at <= now))
        .count()
}

/// Insert the `job_executions` row for a freshly created job and mark it
/// scheduled. Shared by all three spawner flavors — [`JobSpawner`] passes
/// `unique_key: None`, [`crate::KeyedJobSpawner`] passes the key,
/// [`crate::ResidentJobSpawner`] passes `None`.
///
/// A `queue_id`'d row lands `pending` if the queue's active slot is free, or
/// `parked` otherwise (see [`insert_or_park_in_op`]). An unqueued row always
/// lands `pending` — nothing can ever block it. `execution_ready_in_op` is
/// skipped for a row that lands `parked`: nothing can claim it yet, so there
/// is nothing to wake a poller for.
#[instrument(name = "job.insert_execution", skip_all)]
pub(crate) async fn insert_execution(
    repo: &JobRepo,
    notifier: &Arc<JobEventNotifier>,
    op: &mut impl es_entity::AtomicOperation,
    job: &mut Job,
    schedule_at: DateTime<Utc>,
    queue_id: Option<&str>,
    unique_key: Option<&str>,
) -> Result<(), JobError> {
    let landed_pending =
        insert_or_park_in_op(op, job.id, &job.job_type, schedule_at, queue_id, unique_key).await?;
    if landed_pending {
        notifier.execution_ready_in_op(op, &job.job_type).await?;
    }
    job.schedule_execution(schedule_at);
    repo.update_in_op(op, job).await?;
    Ok(())
}

/// Insert one `job_executions` row, parking it instead of landing it
/// `pending` if its queue's active slot (Invariant A: at most one
/// `pending`/`running` row per `queue_id`) is already taken. Returns
/// `true` iff the row landed `pending`.
///
/// Unqueued rows (`queue_id.is_none()`) can never conflict — always `pending`.
///
/// Queued rows try the active slot first (`ON CONFLICT (queue_id) WHERE
/// state IN ('pending','running') AND queue_id IS NOT NULL DO NOTHING`,
/// inferring `idx_job_executions_queue_active`; the arbiter clause must
/// repeat the index predicate exactly or inference fails). On conflict:
///
/// - **Invariant B ordering edge**: if the occupying row is `pending` (not
///   `running`) and this new row sorts strictly before it by
///   `(execute_at, id)` — a backdated `spawn_at` in the past — swap: demote
///   the occupying row to `parked`, take the slot ourselves. The demote is
///   guarded (`WHERE id = $1 AND state = 'pending'`) so a concurrent
///   claim/completion of that row is detected rather than clobbered; losing
///   the guard just means landing `parked` below, which is always safe.
/// - Otherwise (occupied by `running`, or a `pending` row that doesn't sort
///   after us, or the swap guard lost a race): land `parked`.
///
/// **Orphan race**: between the failed first insert and this function's
/// final `parked` insert, the occupying row can complete-and-promote-nothing
/// (no parked sibling was visible to it yet, since ours hadn't landed) —
/// this row is then orphaned (its queue has no active row) until the next
/// orphan-sweep cycle (`poller.rs`, piggybacked on `reclaim_lost_jobs`).
/// Real, expected, and bounded — not handled inline here.
async fn insert_or_park_in_op(
    op: &mut impl es_entity::AtomicOperation,
    id: JobId,
    job_type: &JobType,
    schedule_at: DateTime<Utc>,
    queue_id: Option<&str>,
    unique_key: Option<&str>,
) -> Result<bool, JobError> {
    // @@ So many round trips - should be condensed to 1 or 2 max - move logic into the query
    let alive_at = op.maybe_now();

    let Some(queue_id) = queue_id else {
        sqlx::query!(
            r#"
            INSERT INTO job_executions
                (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at)
            VALUES ($1, $2, NULL, $3, 'pending', 1, $4, COALESCE($5, NOW()), COALESCE($5, NOW()))
            "#,
            id as JobId,
            job_type as &JobType,
            unique_key,
            schedule_at,
            alive_at,
        )
        .execute(op.as_executor())
        .await?;
        return Ok(true);
    };

    let inserted = sqlx::query_scalar!(
        r#"
        INSERT INTO job_executions
            (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at)
        VALUES ($1, $2, $3, $4, 'pending', 1, $5, COALESCE($6, NOW()), COALESCE($6, NOW()))
        ON CONFLICT (queue_id) WHERE state IN ('pending','running') AND queue_id IS NOT NULL
        DO NOTHING
        RETURNING id AS "id!: JobId"
        "#,
        id as JobId,
        job_type as &JobType,
        queue_id,
        unique_key,
        schedule_at,
        alive_at,
    )
    .fetch_optional(op.as_executor())
    .await?;
    if inserted.is_some() {
        return Ok(true);
    }

    // Conflicted: the queue's active slot is taken. Read who holds it to
    // decide whether the ordering edge (Invariant B) applies.
    let active = sqlx::query!(
        r#"
        SELECT id AS "id: JobId", execute_at, (state = 'pending') AS "is_pending!"
        FROM job_executions
        WHERE queue_id = $1 AND state IN ('pending', 'running')
        "#,
        queue_id,
    )
    .fetch_optional(op.as_executor())
    .await?;

    let should_swap = active.as_ref().is_some_and(|active| {
        active.is_pending
            && (schedule_at, id)
                < (
                    active
                        .execute_at
                        .expect("pending row always has execute_at"),
                    active.id,
                )
    });

    if should_swap {
        let active_id = active.expect("checked by should_swap above").id;
        let demoted = sqlx::query!(
            "UPDATE job_executions SET state = 'parked' WHERE id = $1 AND state = 'pending'",
            uuid::Uuid::from(active_id),
        )
        .execute(op.as_executor())
        .await?;
        if demoted.rows_affected() == 1 {
            sqlx::query!(
                r#"
                INSERT INTO job_executions
                    (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at)
                VALUES ($1, $2, $3, $4, 'pending', 1, $5, COALESCE($6, NOW()), COALESCE($6, NOW()))
                "#,
                id as JobId,
                job_type as &JobType,
                queue_id,
                unique_key,
                schedule_at,
                alive_at,
            )
            .execute(op.as_executor())
            .await?;
            return Ok(true);
        }
        // Lost the guard (the occupying row was concurrently claimed,
        // completed, or already parked by a peer) — fall through to parking
        // below, exactly as the non-swap case does.
    }

    sqlx::query!(
        r#"
        INSERT INTO job_executions
            (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at)
        VALUES ($1, $2, $3, $4, 'parked', 1, $5, COALESCE($6, NOW()), COALESCE($6, NOW()))
        "#,
        id as JobId,
        job_type as &JobType,
        queue_id,
        unique_key,
        schedule_at,
        alive_at,
    )
    .execute(op.as_executor())
    .await?;
    Ok(false)
}

/// Restore Invariant B (a queue's active row is its min-`(execute_at, id)`
/// live-or-parked row) for every id in `ids` that a caller just reset to
/// `pending` on a queued row (retry backoff, voluntary reschedule, or a
/// bulk reclaim sweep).
///
/// A row moving `running`/`parked` → `pending` keeps its queue's active slot
/// (Invariant A never needs re-checking here — the row was already the
/// active occupant, or reclaim just made it so), but an *older* parked
/// sibling should run first: swap them (this row → `parked`, the sibling →
/// `pending`) wherever one exists and is older.
///
/// Set-based so one statement covers everything from a single-row retry to
/// a bulk batch reschedule or reclaim sweep. Callers pass only the ids they
/// just moved to `pending` — a row this didn't touch is left alone even if
/// it happens to belong to a queue with parked siblings (nothing changed for
/// it, so there is nothing to fix).
///
/// Returns the job type of every promoted sibling, so callers can wake the
/// pollers that actually cover it — a sibling can be a different type than
/// the row it displaced (one `queue_id` can be shared across types), so
/// notifying only the caller's own type would miss it, exactly the failure
/// mode `delete_execution_in_op`'s `next_in_queue` resolution exists to
/// prevent on the completion path.
pub(crate) async fn swap_older_parked_siblings_in_op(
    op: &mut impl es_entity::AtomicOperation,
    ids: &[uuid::Uuid],
) -> Result<Vec<String>, sqlx::Error> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let promoted = sqlx::query_scalar!(
        r#"
        WITH candidates AS (
            SELECT je.id, je.queue_id, je.execute_at
            FROM job_executions je
            WHERE je.id = ANY($1) AND je.state = 'pending' AND je.queue_id IS NOT NULL
        ), swaps AS (
            SELECT c.id AS pending_id, sib.id AS parked_id
            FROM candidates c
            CROSS JOIN LATERAL (
                SELECT id, execute_at FROM job_executions
                WHERE state = 'parked' AND queue_id = c.queue_id
                ORDER BY execute_at, id
                LIMIT 1
            ) sib
            WHERE (sib.execute_at, sib.id) < (c.execute_at, c.id)
        ), demote AS (
            UPDATE job_executions SET state = 'parked'
            WHERE id IN (SELECT pending_id FROM swaps)
            RETURNING id
        )
        -- The promote UPDATE reads FROM `demote` (not `swaps`) so Postgres
        -- has a real data dependency forcing `demote` to run to completion
        -- first. Without it, this is two independent writes to the same
        -- table within one statement with no ordering guarantee between
        -- them -- observed as a live, reproducible unique-violation on
        -- `idx_job_executions_queue_active` when the promote half committed
        -- before the demote half, transiently making two rows active for
        -- one queue within the statement's own execution.
        UPDATE job_executions je SET state = 'pending'
        FROM swaps s
        JOIN demote d ON d.id = s.pending_id
        WHERE je.id = s.parked_id
        RETURNING je.job_type
        "#,
        ids,
    )
    .fetch_all(op.as_executor())
    .await?;
    Ok(promoted)
}

/// One row claimed by [`claim_due_heads_in_op`]: everything a dispatcher
/// needs besides the `Job` entity itself (which the caller still re-fetches
/// by id -- see `JobPoller::dispatch_job_from_reservation`'s doc comment for
/// why).
pub(crate) struct ClaimedRow {
    pub id: JobId,
    pub attempt: i32,
    pub queue_id: Option<String>,
    pub data_json: Option<JsonValue>,
}

/// Claim up to `limit` of the oldest due `pending` rows of `job_type`,
/// landing them `running`-by-`instance_id` -- byte-identical to what a poll
/// claim would produce (same `state`/`poller_instance_id`/`alive_at`/
/// `execute_at` columns, same tiebreak). The head-swap kernel: a
/// short-circuit event (spawn or completion) never dispatches a specific
/// row, it claims whichever oldest due row(s) of `job_type` exist right now
/// -- see the handoff addendum this implements (§7.1) for why that is the
/// fairness fix over the born-claimed design it replaces.
///
/// Always a LATER statement in the SAME transaction as the caller's write
/// (insert, promote, reclaim) -- sees that write's uncommitted rows with
/// guaranteed statement ordering, which is what keeps this immune to the
/// independent-CTE ordering hazard documented on
/// [`swap_older_parked_siblings_in_op`].
///
/// `FOR UPDATE SKIP LOCKED` races the ordinary poll claim (and any other
/// concurrent caller of this function) harmlessly: each skips whatever the
/// other already holds. Returns fewer than `limit` rows -- down to zero --
/// whenever fewer than `limit` due rows exist; callers must treat a short
/// claim as fully expected, not an error.
///
/// `fresh_only` excludes `attempt_index > 1` rows: required for a batched
/// claim (retries always run alone -- see `dispatch_batches`' identical
/// split of an ordinary poll claim) and irrelevant for a plain-job claim
/// (`limit = 1`, dispatched alone regardless of attempt either way).
pub(crate) async fn claim_due_heads_in_op(
    op: &mut impl es_entity::AtomicOperation,
    job_type: &JobType,
    instance_id: uuid::Uuid,
    now: DateTime<Utc>,
    limit: i64,
    fresh_only: bool,
) -> Result<Vec<ClaimedRow>, sqlx::Error> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    // wall_now drives alive_at, exactly like poll_jobs -- liveness is always
    // measured in real time, independent of manual-clock advances.
    let wall_now = chrono::Utc::now();
    sqlx::query_as!(
        ClaimedRow,
        r#"
        WITH heads AS (
            SELECT id FROM job_executions
            WHERE job_type = $1 AND state = 'pending' AND execute_at <= $2
              AND (NOT $6 OR attempt_index = 1)
            ORDER BY execute_at, id
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        ),
        updated AS (
            UPDATE job_executions je
            SET state = 'running', poller_instance_id = $4, alive_at = $5, execute_at = NULL
            FROM heads WHERE je.id = heads.id
            RETURNING je.id, je.queue_id, je.attempt_index
        )
        SELECT u.id AS "id!: JobId", u.attempt_index AS "attempt!", u.queue_id AS "queue_id?",
               s.execution_state_json AS "data_json?"
        FROM updated u
        LEFT JOIN job_execution_states s ON s.id = u.id
        "#,
        job_type as &JobType,
        now,
        limit,
        instance_id,
        wall_now,
        fresh_only,
    )
    .fetch_all(op.as_executor())
    .await
}

/// Outcome of a keyed execution insert.
pub(crate) enum KeyedInsert {
    /// The key was free; this job now holds it.
    Inserted,
    /// The key is already held by this LIVE job.
    Live(JobId),
    /// The key was taken, but its holder is no longer visible — it went
    /// terminal between the conflict and the read. The caller should retry.
    Contended,
}

/// Insert a keyed execution row, resolving a live-key conflict in the SAME
/// round trip rather than following up with a separate lookup.
///
/// `ON CONFLICT DO NOTHING` (inferring the partial live-key index) turns the
/// conflict into data instead of an error, so the holder's id comes back
/// alongside it and there is no constraint-name string matching to keep in
/// sync with the schema. The holder is read at this statement's snapshot, so a
/// key claimed by a transaction that committed after it reports
/// [`KeyedInsert::Contended`] rather than a stale id.
#[instrument(name = "job.insert_keyed_execution", skip_all)]
pub(crate) async fn insert_keyed_execution(
    repo: &JobRepo,
    notifier: &Arc<JobEventNotifier>,
    op: &mut impl es_entity::AtomicOperation,
    job: &mut Job,
    schedule_at: DateTime<Utc>,
    unique_key: &str,
) -> Result<KeyedInsert, JobError> {
    let row = sqlx::query!(
        r#"
        WITH ins AS (
            INSERT INTO job_executions
                (id, job_type, queue_id, unique_key, execute_at, alive_at, created_at)
            VALUES ($1, $2, NULL, $3, $4, COALESCE($5, NOW()), COALESCE($5, NOW()))
            ON CONFLICT (job_type, unique_key) WHERE unique_key IS NOT NULL
            DO NOTHING
            RETURNING id
        )
        SELECT (SELECT id FROM ins) AS "inserted?: JobId",
               (SELECT id FROM job_executions
                WHERE job_type = $2 AND unique_key = $3) AS "live?: JobId"
        "#,
        job.id as JobId,
        &job.job_type as &JobType,
        unique_key,
        schedule_at,
        op.maybe_now(),
    )
    .fetch_one(op.as_executor())
    .await?;

    if row.inserted.is_some() {
        notifier.execution_ready_in_op(op, &job.job_type).await?;
        job.schedule_execution(schedule_at);
        repo.update_in_op(op, job).await?;
        return Ok(KeyedInsert::Inserted);
    }
    Ok(match row.live {
        Some(id) => KeyedInsert::Live(id),
        None => KeyedInsert::Contended,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact bug an earlier version of `spawn_all_in_op` had (flagged
    /// by automated review on PR #173): counting every row that landed
    /// pending, including future-scheduled ones, as if it were due now.
    #[test]
    fn count_due_now_excludes_future_scheduled_rows() {
        let now = chrono::Utc::now();
        let due = JobId::new();
        let future = JobId::new();
        let ids = vec![due, future];
        let schedule_times = vec![
            now - chrono::Duration::seconds(5),
            now + chrono::Duration::hours(1),
        ];
        let landed_pending = vec![due, future];

        assert_eq!(
            count_due_now(&landed_pending, &ids, &schedule_times, now),
            1,
            "only the due row should count, not the future-scheduled one"
        );
    }

    /// A `spawn_all` of entirely future work must count zero -- the case
    /// that let a bulk short-circuit reserve and drain unrelated due
    /// backlog before this was fixed.
    #[test]
    fn count_due_now_is_zero_for_all_future_work() {
        let now = chrono::Utc::now();
        let a = JobId::new();
        let b = JobId::new();
        let ids = vec![a, b];
        let schedule_times = vec![now + chrono::Duration::hours(1); 2];
        let landed_pending = vec![a, b];

        assert_eq!(
            count_due_now(&landed_pending, &ids, &schedule_times, now),
            0
        );
    }

    /// A row that landed `parked` (conflicted on its queue) is absent from
    /// `landed_pending` -- it must not count even if its own `schedule_at`
    /// was due, since it isn't claimable.
    #[test]
    fn count_due_now_ignores_rows_that_did_not_land_pending() {
        let now = chrono::Utc::now();
        let landed = JobId::new();
        let parked = JobId::new();
        let ids = vec![landed, parked];
        let schedule_times = vec![now - chrono::Duration::seconds(1); 2];
        let landed_pending = vec![landed]; // `parked` conflicted, absent here

        assert_eq!(
            count_due_now(&landed_pending, &ids, &schedule_times, now),
            1
        );
    }

    #[test]
    fn count_due_now_treats_exactly_due_as_due() {
        let now = chrono::Utc::now();
        let id = JobId::new();
        let ids = vec![id];
        let schedule_times = vec![now];
        let landed_pending = vec![id];

        assert_eq!(
            count_due_now(&landed_pending, &ids, &schedule_times, now),
            1,
            "execute_at == now must count as due, matching the claim query's own `<=`"
        );
    }
}
