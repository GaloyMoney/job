//! Job spawner for creating jobs of a specific type.

use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use serde::Serialize;
use std::{marker::PhantomData, sync::Arc};
use tracing::instrument;

use super::{
    Job, JobId,
    entity::{JobType, NewJob},
    error::JobError,
    notifier::JobEventNotifier,
    poller::{PollerHandle, ShortCircuitOutcome},
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

        // Short-circuit spawn fast path: a due-now spawn whose type allows
        // it and whose process has spare capacity is inserted born-claimed
        // and handed straight to the dispatcher on commit — no NOTIFY, no
        // poll. `schedule_at` in the future (an explicit `spawn_at`) never
        // qualifies: it isn't claimable yet regardless. See
        // `JobPoller::try_short_circuit_spawn` for what "allows it" checks.
        let short_circuited = if schedule_at <= self.clock.now()
            && let Some(poller) = self.poller_ref.get().and_then(|w| w.upgrade())
        {
            match poller
                .try_short_circuit_spawn(
                    op,
                    id,
                    &self.job_type,
                    queue_id.as_deref(),
                    None,
                    schedule_at,
                )
                .await?
            {
                ShortCircuitOutcome::NotAttempted => false,
                ShortCircuitOutcome::Dispatching | ShortCircuitOutcome::Parked => true,
            }
        } else {
            false
        };

        if !short_circuited {
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
        } else {
            // The row already exists (running or parked) — `insert_execution`
            // would only be needed for the schedule-event bookkeeping below,
            // which still applies either way.
            job.schedule_execution(schedule_at);
            self.repo.update_in_op(op, &mut job).await?;
        }
        Ok(job)
    }
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

/// Attempt the short-circuit spawn fast path's born-claimed insert: land the
/// row `running`-by-`instance_id` directly instead of `pending`. Returns
/// `true` iff it landed running; `false` means the queue's active slot was
/// taken and the row was parked instead (mirrors [`insert_or_park_in_op`]'s
/// step 2, minus the Invariant B swap check -- a due-now fast-path spawn is
/// never backdated, so there is nothing to swap against).
///
/// Unqueued rows always land running -- nothing can ever block them.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn try_insert_born_claimed_in_op(
    op: &mut impl es_entity::AtomicOperation,
    id: JobId,
    job_type: &JobType,
    queue_id: Option<&str>,
    unique_key: Option<&str>,
    instance_id: uuid::Uuid,
    schedule_at: DateTime<Utc>,
    alive_at: DateTime<Utc>,
) -> Result<bool, JobError> {
    let Some(queue_id) = queue_id else {
        sqlx::query!(
            r#"
            INSERT INTO job_executions
                (id, job_type, queue_id, unique_key, poller_instance_id, state,
                 attempt_index, execute_at, alive_at, created_at)
            VALUES ($1, $2, NULL, $3, $4, 'running', 1, NULL, $5, $5)
            "#,
            id as JobId,
            job_type as &JobType,
            unique_key,
            instance_id,
            alive_at,
        )
        .execute(op.as_executor())
        .await?;
        return Ok(true);
    };

    let claimed = sqlx::query_scalar!(
        r#"
        INSERT INTO job_executions
            (id, job_type, queue_id, unique_key, poller_instance_id, state,
             attempt_index, execute_at, alive_at, created_at)
        VALUES ($1, $2, $3, $4, $5, 'running', 1, NULL, $6, $6)
        ON CONFLICT (queue_id) WHERE state IN ('pending','running') AND queue_id IS NOT NULL
        DO NOTHING
        RETURNING id AS "id!: JobId"
        "#,
        id as JobId,
        job_type as &JobType,
        queue_id,
        unique_key,
        instance_id,
        alive_at,
    )
    .fetch_optional(op.as_executor())
    .await?;
    if claimed.is_some() {
        return Ok(true);
    }

    // Conflicted. The ordinary Invariant B ordering edge applies here too:
    // "due-now/backdated spawn never needs to swap" assumed the occupant's
    // `execute_at` reflects when it was FIRST scheduled, but an occupant that
    // already rescheduled itself (retry backoff, voluntary reschedule) can
    // carry a future `execute_at` while still being `pending` -- a fast-path
    // spawn older than THAT is exactly the ordering edge the swap exists
    // for. Unlike the ordinary path, a successful swap here lands the new
    // row `running` (not `pending`): it keeps going through the fast path
    // rather than falling back to an ordinary claim.
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
                    (id, job_type, queue_id, unique_key, poller_instance_id, state,
                     attempt_index, execute_at, alive_at, created_at)
                VALUES ($1, $2, $3, $4, $5, 'running', 1, NULL, $6, $6)
                "#,
                id as JobId,
                job_type as &JobType,
                queue_id,
                unique_key,
                instance_id,
                alive_at,
            )
            .execute(op.as_executor())
            .await?;
            return Ok(true);
        }
        // Lost the guard -- fall through to parking below, same as the
        // non-swap case.
    }

    sqlx::query!(
        r#"
        INSERT INTO job_executions
            (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at)
        VALUES ($1, $2, $3, $4, 'parked', 1, $5, $6, $6)
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
