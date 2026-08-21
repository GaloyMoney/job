//! Traits and types used when defining job logic.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};

use super::{
    current::CurrentJob,
    entity::{Job, JobType, RetryPolicy},
    spawner::JobSpawner,
};

/// Describes how to construct a [`JobRunner`] for a given job type.
pub trait JobInitializer: Send + Sync + 'static {
    /// The configuration type for jobs of this type.
    type Config: Serialize + DeserializeOwned + Send + Sync;

    /// Returns the job type identifier.
    ///
    /// For simple cases, return a constant:
    /// ```ignore
    /// fn job_type(&self) -> JobType {
    ///     JobType::new("my-job")
    /// }
    /// ```
    ///
    /// For configured/parameterized initializers, return from instance:
    /// ```ignore
    /// fn job_type(&self) -> JobType {
    ///     self.job_type.clone()
    /// }
    /// ```
    fn job_type(&self) -> JobType;

    /// Retry settings to use when the runner returns an error.
    fn retry_on_error_settings(&self) -> RetrySettings {
        Default::default()
    }

    /// Max concurrent jobs of this type executing in THIS process. `None` =
    /// unlimited. When every slot is busy the poller stops claiming rows of
    /// this type: the backlog stays `pending` — cheap database rows, visible
    /// to other poller instances — instead of pinning executor slots,
    /// connections, and memory. The direct defense against a slow external
    /// dependency confiscating `max_jobs_per_process` from other types.
    fn max_concurrent_per_process(&self) -> Option<usize> {
        None
    }

    /// Whether a due-now spawn or completion of this type may take the
    /// head-swap short-circuit path: capacity for this type is reserved and
    /// its oldest due `pending` row is claimed and handed to the dispatcher
    /// the moment the transaction commits, with no NOTIFY and no poll in
    /// between. The row claimed is always the type's oldest due row —
    /// possibly a different one than whatever triggered the short-circuit —
    /// so admission stays `(execute_at, id)`-ordered exactly like the
    /// ordinary poll path; there is no fairness trade-off to opt out of.
    ///
    /// Defaults to `true`. Override to `false` for a type that would rather
    /// never pay the extra claim statement and always go through the
    /// ordinary poll instead.
    ///
    /// # What this costs a batched type
    ///
    /// With `short_circuit` on and spare capacity, **each spawned item
    /// dispatches as its own single-item batch**: the spawn reserves a slot
    /// and claims the moment it commits, so the next spawn finds nothing left
    /// to batch with. At 2 items with 2 free slots,
    /// [`run_batch`](crate::BatchedJobRunner::run_batch) never sees `N > 1`.
    /// (Confirmed with instrumentation on lana PR #8414, and pinned here by
    /// `separately_committed_spawns_each_dispatch_as_their_own_batch`.)
    ///
    /// This is the intended trade: latency now, at the cost of the batching
    /// that only pays off when work is actually queueing. Under sustained
    /// load the slots are busy, spawns stop short-circuiting, and the backlog
    /// batches normally.
    ///
    /// # Getting `N > 1` in a test
    ///
    /// Batch formation is governed by the **transaction boundary**, not by
    /// this flag: items that become due in one transaction are claimed by one
    /// claim statement and handed to one `run_batch` call. So spawn them
    /// together — [`JobSpawner::spawn_all`](crate::JobSpawner::spawn_all), or
    /// several [`spawn_in_op`](crate::JobSpawner::spawn_in_op) calls sharing
    /// one `op` — and the batch forms on the short-circuit path itself, with
    /// the production configuration untouched and the poller running with
    /// spare capacity. It is deterministic by construction: the claim is a
    /// later statement in the same transaction as the inserts, so no other
    /// claimant can observe the rows unclaimed. Pinned by
    /// `one_transaction_forms_one_batch_despite_spare_capacity`.
    ///
    /// Setting `short_circuit()` to `false` in a test-only initializer does
    /// **not** help, and is not the lever it looks like: separate spawns each
    /// wake the poller through the ordinary notify path and still arrive one
    /// per batch. Worse, the flag is read once at registration
    /// (`JobRegistry::add_*_initializer`), so a test that flips it exercises a
    /// different configuration than production for no gain.
    fn short_circuit(&self) -> bool {
        true
    }

    /// Produce a runner instance for the provided job.
    ///
    /// The spawner parameter allows the runner to spawn additional jobs of the
    /// same type (e.g., for fan-out patterns).
    fn init(
        &self,
        job: &Job,
        spawner: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>>;
}

/// Result returned by [`JobRunner::run`] describing how to progress the job.
pub enum JobCompletion {
    /// Job finished successfully; mark the record as completed.
    Complete,
    #[cfg(feature = "es-entity")]
    /// Job finished and returns an `EsEntity` operation that the job service will commit.
    CompleteWithOp(es_entity::DbOp<'static>),
    /// Job finished and returns a transaction that the job service will commit.
    CompleteWithTx(sqlx::Transaction<'static, sqlx::Postgres>),
    /// Schedule a new run immediately.
    RescheduleNow,
    #[cfg(feature = "es-entity")]
    /// Schedule a new run immediately and return an `EsEntity` operation that the job service will commit.
    RescheduleNowWithOp(es_entity::DbOp<'static>),
    /// Schedule a new run immediately and return a transaction that the job service will commit.
    RescheduleNowWithTx(sqlx::Transaction<'static, sqlx::Postgres>),
    /// Schedule the next run after a delay.
    RescheduleIn(std::time::Duration),
    #[cfg(feature = "es-entity")]
    /// Schedule the next run after a delay and return an `EsEntity` operation that the job service will commit.
    RescheduleInWithOp(es_entity::DbOp<'static>, std::time::Duration),
    /// Schedule the next run after a delay and return a transaction that the job service will commit.
    RescheduleInWithTx(
        sqlx::Transaction<'static, sqlx::Postgres>,
        std::time::Duration,
    ),
    /// Schedule the next run at an exact timestamp.
    RescheduleAt(DateTime<Utc>),
    #[cfg(feature = "es-entity")]
    /// Schedule the next run at an exact timestamp and return an `EsEntity` operation that the job service will commit.
    RescheduleAtWithOp(es_entity::DbOp<'static>, DateTime<Utc>),
    /// Schedule the next run at an exact timestamp and return a transaction that the job service will commit.
    RescheduleAtWithTx(sqlx::Transaction<'static, sqlx::Postgres>, DateTime<Utc>),
}

#[async_trait]
/// Implemented by job executors that perform the actual work.
pub trait JobRunner: Send + Sync + 'static {
    /// Execute the job and return how it should be completed or retried.
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>>;
}

#[derive(Debug, Clone)]
/// Controls retry attempt limits, telemetry escalation thresholds, and exponential backoff behaviour.
/// Use [`RetrySettings::n_warn_attempts`] to decide how many failures remain `WARN` events before
/// escalation. Set it to `None` to keep every retry at `WARN`.
pub struct RetrySettings {
    /// Maximum number of consecutive attempts before the job is failed for good. `None` retries
    /// indefinitely.
    pub n_attempts: Option<u32>,
    /// Number of consecutive failures that can be emitted as `WARN` telemetry before the crate
    /// promotes subsequent failures to `ERROR`. `None` disables escalation and keeps every retry
    /// at `WARN`.
    pub n_warn_attempts: Option<u32>,
    /// Smallest backoff duration when rescheduling failures. Acts as the base for exponential
    /// backoff growth.
    pub min_backoff: std::time::Duration,
    /// Maximum backoff duration. Once the exponentially increasing delay reaches this value it will
    /// stop growing.
    pub max_backoff: std::time::Duration,
    /// Percentage (0-100) jitter applied to the computed backoff window to avoid thundering herds.
    pub backoff_jitter_pct: u8,
    /// Multiplier applied to the previous backoff window. Once the elapsed time since the last
    /// scheduled run exceeds `previous_backoff * attempt_reset_after_backoff_multiples`, the job is
    /// treated as healthy again and the attempt counter resets to `1`.
    pub attempt_reset_after_backoff_multiples: u32,
}

impl RetrySettings {
    pub fn repeat_indefinitely() -> Self {
        Self {
            n_attempts: None,
            ..Default::default()
        }
    }
}

impl Default for RetrySettings {
    fn default() -> Self {
        const SECS_IN_ONE_HOUR: u64 = 60 * 60;
        Self {
            n_attempts: Some(30),
            n_warn_attempts: Some(3),
            min_backoff: std::time::Duration::from_secs(1),
            max_backoff: std::time::Duration::from_secs(SECS_IN_ONE_HOUR),
            backoff_jitter_pct: 20,
            attempt_reset_after_backoff_multiples: 3,
        }
    }
}

impl From<&RetrySettings> for RetryPolicy {
    fn from(settings: &RetrySettings) -> Self {
        Self {
            max_attempts: settings.n_attempts,
            min_backoff: settings.min_backoff,
            max_backoff: settings.max_backoff,
            backoff_jitter_pct: settings.backoff_jitter_pct,
            attempt_reset_after_backoff_multiples: settings.attempt_reset_after_backoff_multiples,
        }
    }
}
