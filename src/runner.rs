//! Traits and types used when defining job logic.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};

use super::{
    current::CurrentJob,
    entity::{Job, JobType, RetryPolicy},
    error::InvalidBackoff,
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
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>>;
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
    /// The job has concluded it cannot succeed. Terminal: no retry, no revive.
    /// The error is classified for the span (lane, code, source chain) and its
    /// message is recorded on the job; whatever the domain should know goes in
    /// the op/tx handed back by the `With*` variants. `Box<dyn Error + Send +
    /// Sync>` is `From<String>` and `From<&str>`, so a plain reason is
    /// `Fail("…".into())`.
    Fail(Box<dyn std::error::Error + Send + Sync>),
    #[cfg(feature = "es-entity")]
    /// Like [`JobCompletion::Fail`], returning an `EsEntity` operation that the job service will commit.
    FailWithOp(
        es_entity::DbOp<'static>,
        Box<dyn std::error::Error + Send + Sync>,
    ),
    /// Like [`JobCompletion::Fail`], returning a transaction that the job service will commit.
    FailWithTx(
        sqlx::Transaction<'static, sqlx::Postgres>,
        Box<dyn std::error::Error + Send + Sync>,
    ),
}

#[async_trait]
/// Implemented by job executors that perform the actual work.
pub trait JobRunner: Send + Sync + 'static {
    /// Execute the job and return how it should be completed or retried.
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Debug, Clone)]
/// Controls how a failed attempt is retried. A runner error never ends a job:
/// it is retried on [`Backoff`]'s curve, and only the job itself can end in
/// failure, by returning [`JobCompletion::Fail`].
pub struct RetrySettings {
    /// The retry curve, divided into a transient and a fatal band.
    pub backoff: Backoff,
    /// How long a single execution must run before its failure is treated as evidence the job had
    /// recovered, forgiving the accumulated attempt count back to `1`.
    ///
    /// Measured on a monotonic clock over the execution itself, so neither an application-clock
    /// advance nor scheduler latency can satisfy it. `None` disables attempt-count forgiveness
    /// entirely.
    ///
    /// Pick a value comfortably longer than a *failing* run of this job type: a deterministic
    /// failure must not clear it, or the job never climbs to the fatal band.
    pub attempt_reset_after_healthy_run: Option<std::time::Duration>,
}

/// The retry curve, divided into two bands by the errlanes lane of the
/// failure. Delays double per attempt from the band's entry point, are
/// capped at `max`, and carry ±`jitter_pct` jitter.
#[derive(Debug, Clone)]
pub struct Backoff {
    transient_min: std::time::Duration,
    fatal_min: std::time::Duration,
    max: std::time::Duration,
    jitter_pct: u8,
}

impl Backoff {
    /// Panics unless `transient_min < fatal_min <= max` and `jitter_pct <= 100`.
    /// For literal settings at registration; config-driven values go through
    /// [`Backoff::try_new`].
    pub const fn new(
        transient_min: std::time::Duration,
        fatal_min: std::time::Duration,
        max: std::time::Duration,
        jitter_pct: u8,
    ) -> Self {
        if let Err(reason) = Self::check(transient_min, fatal_min, max, jitter_pct) {
            panic!("{}", reason);
        }
        Self {
            transient_min,
            fatal_min,
            max,
            jitter_pct,
        }
    }

    /// Fallible [`Backoff::new`] for values that come from configuration.
    pub fn try_new(
        transient_min: std::time::Duration,
        fatal_min: std::time::Duration,
        max: std::time::Duration,
        jitter_pct: u8,
    ) -> Result<Self, InvalidBackoff> {
        Self::check(transient_min, fatal_min, max, jitter_pct)
            .map_err(|reason| InvalidBackoff { reason })?;
        Ok(Self {
            transient_min,
            fatal_min,
            max,
            jitter_pct,
        })
    }

    const fn check(
        transient_min: std::time::Duration,
        fatal_min: std::time::Duration,
        max: std::time::Duration,
        jitter_pct: u8,
    ) -> Result<(), &'static str> {
        if transient_min.as_nanos() >= fatal_min.as_nanos() {
            return Err("transient_min must be less than fatal_min");
        }
        if fatal_min.as_nanos() > max.as_nanos() {
            return Err("fatal_min must not exceed max");
        }
        if jitter_pct > 100 {
            return Err("jitter_pct must be at most 100");
        }
        Ok(())
    }

    /// Smallest delay a `Transient` failure is retried at. The curve starts here.
    pub fn transient_min(&self) -> std::time::Duration {
        self.transient_min
    }

    /// Smallest delay a `Fatal` failure is retried at. A `Transient` streak
    /// whose delay would reach this is narrowed to `Fatal(Exhausted)` and
    /// continues from here, at `ERROR`.
    pub fn fatal_min(&self) -> std::time::Duration {
        self.fatal_min
    }

    /// Largest delay either band is retried at.
    pub fn max(&self) -> std::time::Duration {
        self.max
    }

    /// Percentage (0-100) jitter applied to each delay to avoid thundering herds.
    pub fn jitter_pct(&self) -> u8 {
        self.jitter_pct
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(512),
            std::time::Duration::from_secs(6 * 60 * 60),
            20,
        )
    }
}

impl Default for RetrySettings {
    fn default() -> Self {
        Self {
            backoff: Backoff::default(),
            attempt_reset_after_healthy_run: Some(std::time::Duration::from_secs(60 * 60)),
        }
    }
}

impl From<&RetrySettings> for RetryPolicy {
    fn from(settings: &RetrySettings) -> Self {
        Self {
            backoff: settings.backoff.clone(),
            attempt_reset_after_healthy_run: settings.attempt_reset_after_healthy_run,
        }
    }
}
