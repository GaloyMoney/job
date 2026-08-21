//! Batched job execution — one runner invocation handles many claimed jobs.
//!
//! A job type registered via [`Jobs::add_batched_initializer`](crate::Jobs::add_batched_initializer)
//! is dispatched in *batches*: the poller hands a [`BatchedJobRunner`] every job
//! of that type it claimed in one poll, so the use case can be executed once for
//! the whole set — typically inside a single transaction.
//!
//! Spawning and awaiting are **unchanged**: a batched job is spawned with the
//! same [`JobSpawner`] methods and awaited with the same
//! [`Jobs::await_completion`](crate::Jobs::await_completion), because batching
//! only changes *how* the framework executes claimed rows, never what it writes
//! per job.
//!
//! # The batch invariant
//!
//! The poll query already selects at most one job per `queue_id` per poll (and
//! never one whose queue has a job running), so **a batch never contains two
//! jobs sharing a `queue_id`**. Items are handed over sorted by
//! `queue_id` (falling back to job id) so that concurrent batches acquire
//! domain locks in the same order.
//!
//! # Guidance for implementors
//!
//! - A batch of one is the ordinary case under light load — write `run_batch`
//!   so it is correct for any length, including 1 and 0.
//! - Treat a batch as *equivalent to running its items one at a time, in order,
//!   inside one transaction* — later items may observe earlier items' writes.
//! - `queue_id` uniqueness is not the same as entity disjointness: two items
//!   can touch the same aggregate if the queue key isn't that aggregate's id.
//!   Pick `queue_id` to be whatever you need serialized.
//! - **Do not perform external calls** (HTTP, mail, RPC) inside a shared batch
//!   transaction — keep those job types on the per-job [`JobRunner`](crate::JobRunner).
//! - A per-item **DB** error (a constraint violation, say) run straight
//!   against the batch's `op` aborts the whole transaction. To isolate it —
//!   so batch-mates still commit — run that item's work through
//!   [`CurrentBatchedJob::run_isolated`], which wraps each item in its own
//!   `SAVEPOINT` via [`es_entity::DbOp::with_savepoint`]. Savepoints isolate
//!   *failures*, not *contention*: they do not release Postgres locks, so
//!   they are not a lever for raising batch size under lock pressure.
//! - A **true batch** (batch-load, mutate, batch-persist as one statement)
//!   has no per-item loop for `run_isolated` to wrap. For that shape, use
//!   [`CurrentBatchedJob::run_bisected`] instead: it probes the whole batch
//!   in one savepoint and, only on failure, auto-bisects into smaller
//!   sub-ranges until each culprit is isolated — clean items still commit
//!   in this dispatch instead of every item failing through to solo retry.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use sqlx::PgPool;

use std::collections::BinaryHeap;
use std::sync::Arc;

use super::{
    JobId,
    entity::{Job, JobType},
    error::JobError,
    outcome::JobReturnValue,
    repo::JobRepo,
    runner::RetrySettings,
    spawner::JobSpawner,
};

/// Default upper bound on the number of jobs handed to one `run_batch` call.
///
/// Kept well below `max_jobs_per_process` so several batches of a type can be
/// in flight at once rather than one giant transaction serializing the type.
pub const DEFAULT_MAX_BATCH_SIZE: usize = 25;

/// Default number of batches of one type that may run concurrently per process.
///
/// Two gives pipelining — one batch commits while the next executes — without
/// claiming rows that no batch is ready to work on. The poller claims at most
/// `max_batch_size × free batch slots` rows of a batched type per poll, so the
/// rest of a backlog stays *unclaimed*: visible to other poller instances and
/// accumulating into fuller future batches instead of sitting locked here.
pub const DEFAULT_MAX_CONCURRENT_BATCHES: usize = 2;

pub(crate) type ShutdownRx =
    tokio::sync::broadcast::Receiver<tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>>;

/// Describes how to construct a [`BatchedJobRunner`] for a given job type.
///
/// The batched counterpart of [`JobInitializer`](crate::JobInitializer). Note
/// that `init` receives no `&Job`: a runner serves a whole batch, so per-job
/// configuration is read from [`BatchedJobItem::config`] instead.
pub trait BatchedJobInitializer: Send + Sync + 'static {
    /// The configuration type for jobs of this type.
    type Config: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Returns the job type identifier.
    fn job_type(&self) -> JobType;

    /// Retry settings applied to **each** job in a batch that fails.
    fn retry_on_error_settings(&self) -> RetrySettings {
        Default::default()
    }

    /// Upper bound on how many jobs are handed to a single `run_batch` call.
    ///
    /// Claims beyond this are split into further batches, each dispatched
    /// concurrently. Owned by the implementor because the right bound is a
    /// property of the use case (transaction duration, lock footprint), not of
    /// the process.
    fn max_batch_size(&self) -> usize {
        DEFAULT_MAX_BATCH_SIZE
    }

    /// How many batches of this type may run concurrently in one process.
    ///
    /// This is the claim throttle: each poll may claim at most
    /// `max_batch_size × free slots` rows of this type, so rows are only taken
    /// out of the shared table when a batch can actually start. While every
    /// slot is busy the backlog stays claimable by other poller instances and
    /// grows into fuller batches. The default of
    /// [`DEFAULT_MAX_CONCURRENT_BATCHES`] keeps one batch committing while the
    /// next executes.
    ///
    /// The unit here is a *batch* (one execution unit), unlike the plain
    /// [`JobInitializer::max_concurrent_per_process`](crate::JobInitializer::max_concurrent_per_process),
    /// whose unit is a job — both bound "concurrent execution units per
    /// process" for their respective job shapes.
    fn max_concurrent_per_process(&self) -> usize {
        DEFAULT_MAX_CONCURRENT_BATCHES
    }

    /// Whether a due-now spawn or completion of this type may take the
    /// head-swap short-circuit path (a batch slot claimed and dispatched
    /// with no poll in between). See
    /// [`crate::JobInitializer::short_circuit`] for the full trade-off --
    /// identical here, just counted in batches (one unit) rather than rows.
    /// Defaults to `true`.
    fn short_circuit(&self) -> bool {
        true
    }

    /// Produce a runner instance for a batch.
    ///
    /// The spawner allows the runner to spawn additional jobs of the same type.
    fn init(
        &self,
        spawner: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>>;
}

/// Implemented by executors that process many jobs of one type together.
#[async_trait]
pub trait BatchedJobRunner: Send + Sync + 'static {
    /// The configuration type carried by each job in the batch.
    type Config: DeserializeOwned + Send + Sync + 'static;

    /// Execute every job in the batch and report how each should be progressed.
    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<Self::Config>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>>;
}

/// How a single job within a batch should be progressed.
#[derive(Debug, Clone)]
pub enum BatchItemOutcome {
    /// Mark this job completed.
    Complete,
    /// Schedule this job's next run after a delay. Resets the attempt counter.
    RescheduleIn(std::time::Duration),
    /// Schedule this job's next run at an exact time. Resets the attempt counter.
    RescheduleAt(DateTime<Utc>),
    /// Treat this job as failed: it is retried per the type's
    /// [`RetrySettings`], or marked errored once attempts are exhausted.
    ///
    /// For failures detected without poisoning the shared transaction
    /// (validation, missing precondition, a domain rule saying "no"), just
    /// return this outcome directly. A failed SQL statement run straight
    /// against the batch's `op` still aborts the whole transaction — return
    /// `Err` from `run_batch` in that case so the entire batch is retried.
    /// To let a per-item **DB** error resolve to this outcome too, without
    /// taking batch-mates down with it, run that item's work through
    /// [`CurrentBatchedJob::run_isolated`] (or
    /// [`es_entity::DbOp::with_savepoint`] directly): the savepoint rolls
    /// back only that item's writes, and the shared transaction stays usable
    /// for the rest of the batch.
    Fail(String),
}

/// Per-job dispositions returned by a batched runner.
pub type BatchOutcomes = Vec<(JobId, BatchItemOutcome)>;

/// How many probes [`CurrentBatchedJob::run_bisected_with`] may spend on one
/// batch. A probe is one invocation of the caller's closure — successful or
/// not — including the initial whole-batch attempt.
///
/// This is the one knob the helper exposes, and it is per-call, not a
/// property of the job type: the right budget depends on how expensive the
/// caller's probe closure is, which only the call site knows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BisectBudget {
    /// `2 * ceil(log2(N)) + 1` probes, computed per call from the batch
    /// size — exactly enough to fully isolate **one** culprit and complete
    /// everything else, at any batch size (9 probes at N=10, 11 at N=25, 15
    /// at N=100). A flat constant can't do this: at N=25 a cap of, say, 4
    /// would exhaust after salvaging only ~12 of 25 items on a single
    /// culprit, sending the other 12 *innocent* items through solo retry
    /// for no reason. More than one culprit degrades gracefully:
    /// largest-first probing still salvages the big clean ranges, and the
    /// (smaller) remainder budget-fails into the ordinary solo-retry path.
    #[default]
    Auto,
    /// An explicit hard cap, clamped to at least 1 (the first probe is
    /// mandatory — nothing can be dispositioned without it).
    /// `MaxProbes(1)` is the degenerate case: it probes once, and on
    /// failure every item budget-fails — equivalent to returning `Err`
    /// from `run_batch` directly, just routed through this helper.
    MaxProbes(usize),
    /// Run the search to completion instead of capping it: at most
    /// `2N - 1` probes, typically `~N + O(log N)` once escalation kicks in
    /// on a mostly-bad batch. This is bounded by the batch size, not
    /// actually unbounded — but it is the variant most likely to spend a
    /// probe budget you didn't intend, so pick it explicitly rather than by
    /// default.
    FullResolution,
}

impl BisectBudget {
    /// Resolve to a concrete probe cap for a batch of `n` items.
    fn effective_cap(self, n: usize) -> usize {
        match self {
            BisectBudget::Auto => 2 * ceil_log2(n) + 1,
            BisectBudget::MaxProbes(max) => max.max(1),
            BisectBudget::FullResolution => usize::MAX,
        }
    }
}

/// `ceil(log2(n))`, defined as `0` for `n <= 1`.
fn ceil_log2(n: usize) -> usize {
    if n <= 1 {
        return 0;
    }
    (usize::BITS - (n - 1).leading_zeros()) as usize
}

/// A contiguous, half-open range of item indices still awaiting a probe in
/// [`CurrentBatchedJob::run_bisected_with`]'s search.
///
/// `Ord` makes this a max-heap key of `(length DESC, start ASC)`: the
/// longest pending range is probed next, and among equal lengths the
/// earliest one wins — both purely for determinism, so probe order (and
/// therefore probe *count*) is reproducible given the same input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingRange {
    start: usize,
    end: usize,
}

impl PendingRange {
    fn len(&self) -> usize {
        self.end - self.start
    }
}

impl Ord for PendingRange {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.len()
            .cmp(&other.len())
            .then_with(|| other.start.cmp(&self.start))
    }
}

impl PartialOrd for PendingRange {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Result returned by [`BatchedJobRunner::run_batch`].
///
/// Every variant commits **all** of the batch's bookkeeping in a single
/// transaction — one commit for K jobs instead of K commits.
///
/// Returning `Err` instead fails *every* job in the batch through the type's
/// retry policy. Jobs on their second or later attempt are never batched, so a
/// job that keeps failing is retried on its own rather than repeatedly taking
/// its batch-mates down with it.
pub enum JobBatchCompletion {
    /// Every job in the batch completed; the service opens and commits its own
    /// operation.
    CompleteAll,
    /// Per-job dispositions; the service opens and commits its own operation.
    WithOutcomes(BatchOutcomes),
    #[cfg(feature = "es-entity")]
    /// Every job completed, committed together with the runner's `DbOp`.
    CompleteAllWithOp(es_entity::DbOp<'static>),
    #[cfg(feature = "es-entity")]
    /// Per-job dispositions, committed together with the runner's `DbOp`.
    WithOutcomesWithOp(es_entity::DbOp<'static>, BatchOutcomes),
    /// Every job completed, committed together with the runner's transaction.
    CompleteAllWithTx(sqlx::Transaction<'static, sqlx::Postgres>),
    /// Per-job dispositions, committed together with the runner's transaction.
    WithOutcomesWithTx(sqlx::Transaction<'static, sqlx::Postgres>, BatchOutcomes),
}

/// One job inside a batch: its identity, attempt, typed config and per-job
/// execution state.
pub struct BatchedJobItem<C> {
    id: JobId,
    attempt: u32,
    queue_id: Option<String>,
    config: C,
    execution_state_json: Option<JsonValue>,
    pool: PgPool,
    clock: ClockHandle,
    repo: Arc<JobRepo>,
}

impl<C> BatchedJobItem<C> {
    /// This job's id — the same id the spawner returned and awaiters wait on.
    pub fn id(&self) -> JobId {
        self.id
    }

    /// The attempt number for this job. Always `1` inside a batch of more than
    /// one: retries are dispatched on their own.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The queue this job was spawned into, if any.
    pub fn queue_id(&self) -> Option<&str> {
        self.queue_id.as_deref()
    }

    /// This job's typed configuration.
    pub fn config(&self) -> &C {
        &self.config
    }

    /// Deserialize this job's persisted execution state.
    pub fn execution_state<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        match self.execution_state_json.as_ref() {
            Some(value) => serde_json::from_value(value.clone()).map(Some),
            None => Ok(None),
        }
    }

    /// Update this job's execution state as part of an existing operation.
    pub async fn update_execution_state_in_op<T: Serialize>(
        &mut self,
        op: &mut impl es_entity::AtomicOperation,
        execution_state: &T,
    ) -> Result<(), JobError> {
        let execution_state_json = serde_json::to_value(execution_state)
            .map_err(JobError::CouldNotSerializeExecutionState)?;
        sqlx::query!(
            r#"
          INSERT INTO job_execution_states (id, execution_state_json)
          VALUES ($1, $2)
          ON CONFLICT (id) DO UPDATE SET execution_state_json = EXCLUDED.execution_state_json
        "#,
            &self.id as &JobId,
            execution_state_json,
        )
        .execute(op.as_executor())
        .await?;
        self.execution_state_json = Some(execution_state_json);
        Ok(())
    }

    /// Update this job's execution state in its own transaction.
    pub async fn update_execution_state<T: Serialize>(
        &mut self,
        execution_state: &T,
    ) -> Result<(), JobError> {
        let execution_state_json = serde_json::to_value(execution_state)
            .map_err(JobError::CouldNotSerializeExecutionState)?;
        sqlx::query!(
            r#"
          INSERT INTO job_execution_states (id, execution_state_json)
          VALUES ($1, $2)
          ON CONFLICT (id) DO UPDATE SET execution_state_json = EXCLUDED.execution_state_json
        "#,
            &self.id as &JobId,
            execution_state_json,
        )
        .execute(&self.pool)
        .await?;
        self.execution_state_json = Some(execution_state_json);
        Ok(())
    }

    /// Attach or update this job's result value as part of an existing
    /// operation, so awaiters see it via
    /// [`JobOutcome::result`](crate::JobOutcome::result).
    pub async fn set_result_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        result: &impl Serialize,
    ) -> Result<(), JobError> {
        let job_result =
            JobReturnValue::try_from(result).map_err(JobError::CouldNotSerializeResult)?;
        let mut job = self.repo.find_by_id_in_op(&mut *op, self.id).await?;
        if job.update_return_value(job_result).did_execute() {
            self.repo.update_in_op(op, &mut job).await?;
        }
        Ok(())
    }

    /// Attach or update this job's result value in its own transaction.
    pub async fn set_result(&self, result: &impl Serialize) -> Result<(), JobError> {
        let job_result =
            JobReturnValue::try_from(result).map_err(JobError::CouldNotSerializeResult)?;
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let mut job = self.repo.find_by_id_in_op(&mut op, self.id).await?;
        if job.update_return_value(job_result).did_execute() {
            self.repo.update_in_op(&mut op, &mut job).await?;
            op.commit().await?;
        }
        Ok(())
    }
}

/// Context handed to a [`BatchedJobRunner`] for one batch.
pub struct CurrentBatchedJob<C> {
    items: Vec<BatchedJobItem<C>>,
    pool: PgPool,
    clock: ClockHandle,
    shutdown_rx: ShutdownRx,
}

impl<C> CurrentBatchedJob<C> {
    /// The jobs in this batch, ordered by `queue_id` (job id when unqueued) so
    /// concurrent batches take domain locks in a consistent order.
    ///
    /// Never contains two jobs with the same `queue_id`.
    pub fn items(&self) -> &[BatchedJobItem<C>] {
        &self.items
    }

    /// Mutable access, for [`BatchedJobItem::update_execution_state_in_op`].
    pub fn items_mut(&mut self) -> &mut [BatchedJobItem<C>] {
        &mut self.items
    }

    /// Consume the context and take ownership of its items.
    pub fn into_items(self) -> Vec<BatchedJobItem<C>> {
        self.items
    }

    /// Number of jobs in this batch.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the batch is empty. Never true as dispatched.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Every job id in this batch, in item order.
    pub fn ids(&self) -> Vec<JobId> {
        self.items.iter().map(|item| item.id).collect()
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The clock configured on the job service.
    pub fn clock(&self) -> &ClockHandle {
        &self.clock
    }

    /// Begin a new database operation using the job service's clock.
    #[cfg(feature = "es-entity")]
    pub async fn begin_op(&self) -> Result<es_entity::DbOp<'static>, JobError> {
        Ok(es_entity::DbOp::init_with_clock(&self.pool, &self.clock).await?)
    }

    /// Run `f` once per item, each inside its own `SAVEPOINT` on `op`, and
    /// collect a [`BatchOutcomes`] — the batch-loop counterpart of
    /// [`es_entity::DbOp::with_savepoint`].
    ///
    /// A failing item is rolled back to its own savepoint without poisoning
    /// `op`: earlier and later items still commit in the same transaction.
    /// `f`'s `Ok(outcome)` is recorded as-is — usually
    /// [`BatchItemOutcome::Complete`], though a successfully-released
    /// savepoint can still resolve to `RescheduleIn`/`RescheduleAt` if the
    /// item wants to run again later. `f`'s `Err` is translated to
    /// [`BatchItemOutcome::Fail`] (via `E`'s `Display`), so the item is
    /// retried per the type's [`RetrySettings`] like any other failure.
    ///
    /// Typical use, feeding the result into
    /// [`JobBatchCompletion::WithOutcomesWithOp`]:
    ///
    /// ```rust,ignore
    /// let mut op = current_batch.begin_op().await?;
    /// let outcomes = current_batch
    ///     .run_isolated(&mut op, |op, item| async move {
    ///         self.execute_in_op(op, item.config()).await?;
    ///         Ok(BatchItemOutcome::Complete)
    ///     })
    ///     .await?;
    /// Ok(JobBatchCompletion::WithOutcomesWithOp(op, outcomes))
    /// ```
    ///
    /// # Errors
    ///
    /// The returned `Err` is the *outer* layer of
    /// [`with_savepoint`](es_entity::DbOp::with_savepoint): the savepoint
    /// machinery itself failed (e.g. a dead connection), not any one item's
    /// domain logic. That failure isn't attributable to a single item —
    /// propagate it with `?` and let the whole batch retry, exactly like a
    /// whole-batch `Err` returned from `run_batch` today.
    ///
    /// Use only `*_in_op` methods against the savepoint inside `f`. The
    /// pool-backed [`BatchedJobItem::update_execution_state`] /
    /// [`BatchedJobItem::set_result`] commit in their own transaction
    /// outside `op` entirely, so they do **not** unwind if this item's
    /// savepoint rolls back.
    #[cfg(feature = "es-entity")]
    pub async fn run_isolated<E>(
        &self,
        op: &mut es_entity::DbOp<'static>,
        f: impl AsyncFn(
            &mut es_entity::SavepointOp<'_>,
            &BatchedJobItem<C>,
        ) -> Result<BatchItemOutcome, E>,
    ) -> Result<BatchOutcomes, sqlx::Error>
    where
        E: std::fmt::Display,
    {
        let mut outcomes = Vec::with_capacity(self.items.len());
        for item in &self.items {
            let outcome = match op.with_savepoint(async |sp| f(sp, item).await).await? {
                Ok(outcome) => outcome,
                Err(e) => BatchItemOutcome::Fail(e.to_string()),
            };
            outcomes.push((item.id, outcome));
        }
        Ok(outcomes)
    }

    /// Run the whole batch as one unit inside a single `SAVEPOINT`; on
    /// failure, auto-bisect: roll back and probe smaller contiguous
    /// sub-ranges — largest pending range first — until each culprit is
    /// isolated (or [`BisectBudget`] runs out). Clean items resolve to
    /// [`BatchItemOutcome::Complete`] in this very dispatch; each culprit to
    /// [`BatchItemOutcome::Fail`] (via `E`'s `Display`, from its own
    /// singleton probe — so the message is attributable).
    ///
    /// The counterpart of [`run_isolated`](Self::run_isolated) for **true
    /// batch** implementations: a batch-load/mutate/batch-persist runner has
    /// no per-item loop for `run_isolated` to wrap, so its only failure
    /// shape was previously "every item fails, all N retry solo". `f` here
    /// takes a *slice* of items — probe them with one statement, the same
    /// shape a true batch already uses.
    ///
    /// Equivalent to [`run_bisected_with`](Self::run_bisected_with) with
    /// [`BisectBudget::default()`] (`Auto`).
    ///
    /// # Cost
    ///
    /// Happy path: one extra `SAVEPOINT`/`RELEASE` pair for the whole batch.
    /// With `k` culprits in `N` items: roughly `2k·log₂(N/k)` probes. If a
    /// type's dominant failure mode is "everything fails together" (a
    /// shared upstream precondition), returning `Err` from `run_batch`
    /// directly is still cheaper — this helper targets `k ≪ N`.
    ///
    /// # Contract on `f`
    ///
    /// - **Must tolerate re-execution over arbitrary contiguous sub-slices,
    ///   in non-positional order.** Each probe's DB writes unwind with its
    ///   savepoint on failure, but *host-side* state (counters, caches) does
    ///   not — derive everything `f` needs from `(sp, slice)`. In the worst
    ///   case `f` runs `2N-1` times; any single item is re-probed at most
    ///   `⌈log₂ N⌉+1` times.
    /// - **No external calls** (HTTP, mail, RPC) — already the rule for
    ///   shared batch transactions; bisection would repeat them per probe.
    /// - **Only `*_in_op` methods against `sp`.** The pool-backed
    ///   [`BatchedJobItem::update_execution_state`] /
    ///   [`BatchedJobItem::set_result`] commit outside this transaction and
    ///   will NOT unwind with a rolled-back probe.
    /// - **Set semantics, not sequence semantics.** A bisected batch gives
    ///   up the positional "equivalent to running in order" guarantee the
    ///   module documents for `run_isolated`/plain loops: probes run
    ///   largest-range-first, so two items with a real dependency between
    ///   them (a shared unique key, a read-your-write coupling) resolve
    ///   deterministically by probe order, not item order. Only use this
    ///   for batches whose items are safe to probe in any grouping —
    ///   i.e. the module's existing entity-disjointness guidance
    ///   (`queue_id` is the serialization unit). A deadlock from probing out
    ///   of the usual `(queue_id, id)` lock order is detected by Postgres
    ///   and surfaces as an ordinary failing probe.
    /// - Locks acquired inside a probe are **not released** by its
    ///   rollback (`ROLLBACK TO` keeps them) — same caveat as
    ///   `run_isolated`: this is not a lever for raising batch size under
    ///   lock pressure.
    ///
    /// # Errors
    ///
    /// The returned `Err` is the *outer* layer of
    /// [`with_savepoint`](es_entity::DbOp::with_savepoint): the savepoint
    /// machinery itself failed (e.g. a dead connection), not any probe's
    /// domain logic. Propagate it with `?` and let the whole batch retry,
    /// exactly like [`run_isolated`](Self::run_isolated).
    ///
    /// ```rust,ignore
    /// let mut op = current_batch.begin_op().await?;
    /// let outcomes = current_batch
    ///     .run_bisected(&mut op, |sp, slice| async move {
    ///         self.execute_many_in_op(sp, slice).await
    ///     })
    ///     .await?;
    /// Ok(JobBatchCompletion::WithOutcomesWithOp(op, outcomes))
    /// ```
    #[cfg(feature = "es-entity")]
    pub async fn run_bisected<E>(
        &self,
        op: &mut es_entity::DbOp<'static>,
        f: impl AsyncFn(&mut es_entity::SavepointOp<'_>, &[BatchedJobItem<C>]) -> Result<(), E>,
    ) -> Result<BatchOutcomes, sqlx::Error>
    where
        E: std::fmt::Display,
    {
        self.run_bisected_with(op, BisectBudget::default(), f).await
    }

    /// [`run_bisected`](Self::run_bisected) with an explicit [`BisectBudget`]
    /// instead of the default `Auto`. See `run_bisected`'s doc comment for
    /// the full contract; this is the same helper, just with the probe
    /// budget spelled out at the call site.
    #[cfg(feature = "es-entity")]
    pub async fn run_bisected_with<E>(
        &self,
        op: &mut es_entity::DbOp<'static>,
        budget: BisectBudget,
        f: impl AsyncFn(&mut es_entity::SavepointOp<'_>, &[BatchedJobItem<C>]) -> Result<(), E>,
    ) -> Result<BatchOutcomes, sqlx::Error>
    where
        E: std::fmt::Display,
    {
        let n = self.items.len();
        let mut outcomes = Vec::with_capacity(n);
        if n == 0 {
            return Ok(outcomes);
        }

        let cap = budget.effective_cap(n);
        // Escalation threshold: once a run of this many consecutive probe
        // failures is observed, singleton-scan the rest rather than keep
        // halving. See `BisectBudget`'s module docs for why this is
        // streak-based (not resolved-item density) under largest-first
        // ordering.
        let streak_threshold = usize::max(4, ceil_log2(n));

        let mut heap = BinaryHeap::new();
        heap.push(PendingRange { start: 0, end: n });

        let mut probes_used = 0usize;
        let mut streak = 0usize;
        let mut escalated = false;
        let mut last_error: Option<String> = None;
        let mut logged_bisecting = false;

        while let Some(range) = heap.pop() {
            if probes_used >= cap {
                let message = match &last_error {
                    Some(e) => format!(
                        "bisect probe budget exhausted after {probes_used} probes; \
                         last error: {e}"
                    ),
                    None => {
                        format!("bisect probe budget exhausted after {probes_used} probes")
                    }
                };
                outcomes.extend(
                    self.items[range.start..range.end]
                        .iter()
                        .map(|item| (item.id, BatchItemOutcome::Fail(message.clone()))),
                );
                continue;
            }

            let slice = &self.items[range.start..range.end];
            probes_used += 1;
            match op.with_savepoint(async |sp| f(sp, slice).await).await? {
                Ok(()) => {
                    streak = 0;
                    outcomes.extend(
                        slice
                            .iter()
                            .map(|item| (item.id, BatchItemOutcome::Complete)),
                    );
                }
                Err(e) => {
                    if !logged_bisecting {
                        logged_bisecting = true;
                        tracing::warn!(
                            target: "job.batch_bisect",
                            n_items = n,
                            "batch failed; bisecting"
                        );
                    }
                    let message = e.to_string();
                    streak += 1;

                    if range.len() == 1 {
                        outcomes.push((
                            self.items[range.start].id,
                            BatchItemOutcome::Fail(message.clone()),
                        ));
                        last_error = Some(message);
                    } else if !escalated && streak >= streak_threshold {
                        escalated = true;
                        last_error = Some(message);
                        tracing::warn!(
                            target: "job.batch_bisect",
                            streak,
                            probes_used,
                            "bisect escalating to singleton scan"
                        );
                        // Drain this range plus everything still pending
                        // into singletons — see the module docs: under
                        // largest-first ordering a streak of big-range
                        // failures is strong all-bad evidence.
                        let mut remaining: Vec<usize> = (range.start..range.end).collect();
                        while let Some(pending) = heap.pop() {
                            remaining.extend(pending.start..pending.end);
                        }
                        for idx in remaining {
                            heap.push(PendingRange {
                                start: idx,
                                end: idx + 1,
                            });
                        }
                    } else {
                        last_error = Some(message);
                        let mid = range.start + range.len() / 2;
                        heap.push(PendingRange {
                            start: mid,
                            end: range.end,
                        });
                        heap.push(PendingRange {
                            start: range.start,
                            end: mid,
                        });
                    }
                }
            }
        }

        Ok(outcomes)
    }

    /// Build a [`BatchOutcomes`] by deciding an outcome for every item.
    ///
    /// Applying the closure to each item makes it impossible to leave a job
    /// undispositioned, which the dispatcher would otherwise reject.
    pub fn outcomes_for_each(
        &self,
        mut f: impl FnMut(&BatchedJobItem<C>) -> BatchItemOutcome,
    ) -> BatchOutcomes {
        self.items.iter().map(|item| (item.id, f(item))).collect()
    }

    /// Wait for a shutdown signal. Returns `true` if shutdown was requested.
    pub async fn shutdown_requested(&mut self) -> bool {
        self.shutdown_rx.recv().await.is_ok()
    }

    /// Non-blocking check for a shutdown request.
    pub fn is_shutdown_requested(&mut self) -> bool {
        self.shutdown_rx.try_recv().is_ok()
    }
}

/// A claimed row plus its hydrated entity, before the config is typed.
pub(crate) struct RawBatchItem {
    pub job: Job,
    pub attempt: u32,
    pub queue_id: Option<String>,
    pub execution_state_json: Option<JsonValue>,
}

/// Everything a batch needs from the service, independent of `Config`.
pub(crate) struct BatchRunCtx {
    pub pool: PgPool,
    pub clock: ClockHandle,
    pub repo: Arc<JobRepo>,
    pub shutdown_rx: ShutdownRx,
}

/// Object-safe view of a [`BatchedJobRunner`], with `Config` erased.
#[async_trait]
pub(crate) trait AnyBatchedJobRunner: Send + Sync + 'static {
    async fn run_batch_erased(
        &self,
        items: Vec<RawBatchItem>,
        ctx: BatchRunCtx,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>>;
}

pub(crate) struct ErasedBatchedRunner<C> {
    inner: Box<dyn BatchedJobRunner<Config = C>>,
}

impl<C> ErasedBatchedRunner<C> {
    pub(crate) fn new(inner: Box<dyn BatchedJobRunner<Config = C>>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<C> AnyBatchedJobRunner for ErasedBatchedRunner<C>
where
    C: DeserializeOwned + Send + Sync + 'static,
{
    async fn run_batch_erased(
        &self,
        items: Vec<RawBatchItem>,
        ctx: BatchRunCtx,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        let BatchRunCtx {
            pool,
            clock,
            repo,
            shutdown_rx,
        } = ctx;

        let mut typed = Vec::with_capacity(items.len());
        for raw in items {
            typed.push(BatchedJobItem {
                id: raw.job.id,
                attempt: raw.attempt,
                queue_id: raw.queue_id,
                config: raw.job.config::<C>()?,
                execution_state_json: raw.execution_state_json,
                pool: pool.clone(),
                clock: clock.clone(),
                repo: Arc::clone(&repo),
            });
        }

        self.inner
            .run_batch(CurrentBatchedJob {
                items: typed,
                pool,
                clock,
                shutdown_rx,
            })
            .await
    }
}

/// Object-safe view of a [`BatchedJobInitializer`].
pub(crate) trait AnyBatchedJobInitializer: Send + Sync + 'static {
    fn init_erased(
        &self,
        repo: Arc<JobRepo>,
        router: Arc<crate::notification_router::JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<crate::notifier::JobEventNotifier>,
    ) -> Result<Box<dyn AnyBatchedJobRunner>, Box<dyn std::error::Error>>;
}

impl<T: BatchedJobInitializer> AnyBatchedJobInitializer for T {
    fn init_erased(
        &self,
        repo: Arc<JobRepo>,
        _router: Arc<crate::notification_router::JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<crate::notifier::JobEventNotifier>,
    ) -> Result<Box<dyn AnyBatchedJobRunner>, Box<dyn std::error::Error>> {
        // Always-empty handle: fan-out spawns from within a batch runner
        // take the ordinary insert path.
        let spawner = JobSpawner::<T::Config>::new(
            repo,
            self.job_type(),
            clock,
            notifier,
            Arc::new(std::sync::OnceLock::new()),
        );
        let runner = BatchedJobInitializer::init(self, spawner)?;
        Ok(Box::new(ErasedBatchedRunner::new(runner)))
    }
}
