//! Resident jobs — at most one job of a `job_type` EVER exists, and it
//! cannot complete (only reschedule). Use this flavor for a process-wide
//! singleton that should just keep running: a poller, a periodic sweep, a
//! background sync loop.
//!
//! Contrast with [`crate::KeyedJobInitializer`] (keyed jobs): a resident job
//! never terminates and never respawns — there is exactly one job, forever,
//! for the type's whole lifetime. A keyed job terminates and respawns under
//! its key, and many keys of one type coexist.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use serde::{Serialize, de::DeserializeOwned};
use tracing::instrument;

use std::{marker::PhantomData, sync::Arc};

use super::{
    Job, JobId,
    current::CurrentJob,
    entity::{JobType, NewJob},
    error::JobError,
    handle::JobHandle,
    notification_router::JobNotificationRouter,
    notifier::JobEventNotifier,
    repo::{JobCreateError, JobRepo},
    runner::{JobCompletion, JobRunner, RetrySettings},
    spawner::insert_execution,
};

/// Result returned by [`ResidentJobRunner::run`] describing how to progress
/// a resident job. A reschedule-only mirror of [`JobCompletion`] — there is
/// deliberately no `Complete` variant, so a resident job accidentally
/// finishing is a compile error rather than a job that silently stops
/// existing.
pub enum ResidentJobCompletion {
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

impl From<ResidentJobCompletion> for JobCompletion {
    fn from(value: ResidentJobCompletion) -> Self {
        match value {
            ResidentJobCompletion::RescheduleNow => JobCompletion::RescheduleNow,
            #[cfg(feature = "es-entity")]
            ResidentJobCompletion::RescheduleNowWithOp(op) => {
                JobCompletion::RescheduleNowWithOp(op)
            }
            ResidentJobCompletion::RescheduleNowWithTx(tx) => {
                JobCompletion::RescheduleNowWithTx(tx)
            }
            ResidentJobCompletion::RescheduleIn(d) => JobCompletion::RescheduleIn(d),
            #[cfg(feature = "es-entity")]
            ResidentJobCompletion::RescheduleInWithOp(op, d) => {
                JobCompletion::RescheduleInWithOp(op, d)
            }
            ResidentJobCompletion::RescheduleInWithTx(tx, d) => {
                JobCompletion::RescheduleInWithTx(tx, d)
            }
            ResidentJobCompletion::RescheduleAt(t) => JobCompletion::RescheduleAt(t),
            #[cfg(feature = "es-entity")]
            ResidentJobCompletion::RescheduleAtWithOp(op, t) => {
                JobCompletion::RescheduleAtWithOp(op, t)
            }
            ResidentJobCompletion::RescheduleAtWithTx(tx, t) => {
                JobCompletion::RescheduleAtWithTx(tx, t)
            }
        }
    }
}

#[async_trait]
/// Implemented by resident job executors. Mirrors [`JobRunner`] except its
/// result type cannot express completion.
pub trait ResidentJobRunner: Send + Sync + 'static {
    /// Execute one run and return how it should be rescheduled.
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error>>;
}

/// Describes how to construct a [`ResidentJobRunner`] for a resident job
/// type. The resident counterpart of [`crate::JobInitializer`].
///
/// No spawner parameter (unlike [`crate::JobInitializer::init`]): a resident
/// job type has at most one job, ever, so no same-type fan-out can exist. No
/// concurrency-cap methods either, for the same reason.
pub trait ResidentJobInitializer: Send + Sync + 'static {
    /// The configuration type for this resident job.
    type Config: Serialize + DeserializeOwned + Send + Sync;

    /// Returns the job type identifier.
    fn job_type(&self) -> JobType;

    /// Backoff SHAPE applied between failed attempts. `n_attempts` is
    /// ignored: registration always forces eternal retry (a resident job can
    /// never be exhausted into a terminal error) regardless of what is
    /// returned here — only `min_backoff`/`max_backoff`/`backoff_jitter_pct`/
    /// `attempt_reset_after_backoff_multiples` are honored.
    fn retry_on_error_settings(&self) -> RetrySettings {
        Default::default()
    }

    /// Produce a runner instance for the resident job.
    fn init(&self, job: &Job) -> Result<Box<dyn ResidentJobRunner>, Box<dyn std::error::Error>>;
}

/// Adapts a [`ResidentJobRunner`] to the ordinary [`JobRunner`] the
/// dispatcher already knows how to drive, converting
/// [`ResidentJobCompletion`] into [`JobCompletion`] — the dispatcher itself
/// needs no resident-specific code path.
pub(crate) struct ResidentRunnerAdapter(pub(crate) Box<dyn ResidentJobRunner>);

#[async_trait]
impl JobRunner for ResidentRunnerAdapter {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        self.0.run(current_job).await.map(Into::into)
    }
}

/// A handle for spawning the single resident job of a type.
///
/// Returned by [`crate::Jobs::add_resident_initializer`]. `Clone` so a
/// caller can hold on to one copy while consuming another via
/// [`Self::spawn`] — spawning still only ever creates the one resident job
/// the type allows.
#[derive(Clone)]
pub struct ResidentJobSpawner<Config> {
    repo: Arc<JobRepo>,
    job_type: JobType,
    router: Arc<JobNotificationRouter>,
    clock: ClockHandle,
    notifier: Arc<JobEventNotifier>,
    _phantom: PhantomData<Config>,
}

impl<Config> ResidentJobSpawner<Config>
where
    Config: Serialize + Send + Sync,
{
    pub(crate) fn new(
        repo: Arc<JobRepo>,
        job_type: JobType,
        router: Arc<JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
    ) -> Self {
        Self {
            repo,
            job_type,
            router,
            clock,
            notifier,
            _phantom: PhantomData,
        }
    }

    /// Returns the job type this spawner creates.
    pub fn job_type(&self) -> &JobType {
        &self.job_type
    }

    /// Create the resident job, or resolve to the persisted one if it
    /// already exists.
    ///
    /// The job's id is generated internally — a resident job is identified
    /// by its type, not a caller-chosen id — so read the id back from the
    /// returned [`JobHandle`]. Consumes the spawner: since at most one
    /// resident job of this type can ever exist, nothing further can be
    /// created from it.
    ///
    /// Note: a resident job never completes (see [`ResidentJobCompletion`]),
    /// so [`JobHandle::await_completion`] on the returned handle only ever
    /// times out — use [`JobHandle::load`]/[`JobHandle::execution_state`] to
    /// observe it instead.
    #[instrument(
        name = "resident_job_spawner.spawn",
        skip(self, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn(self, config: Config) -> Result<JobHandle, JobError> {
        let new_job = NewJob::builder()
            .id(JobId::new())
            .resident(true)
            .job_type(self.job_type.clone())
            .config(config)?
            .tracing_context(es_entity::context::TracingContext::current())
            .build()
            .expect("Could not build new job");
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        match self.repo.create_in_op(&mut op, new_job).await {
            Ok(mut job) => {
                let schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());
                insert_execution(
                    &self.repo,
                    &self.notifier,
                    &mut op,
                    &mut job,
                    schedule_at,
                    None,
                    None,
                )
                .await?;
                op.commit().await?;
                Ok(self.handle(job.id))
            }
            // The id is an internally generated v7 uuid, so the only
            // constraint that can fire on this insert is
            // `idx_jobs_job_type_resident`, a single-column index on
            // `job_type` — es_entity attributes its violation
            // deterministically to that column (see `error.rs`). Resolve
            // the persisted job by lookup; the violation poisons the
            // transaction, so drop (roll back) the op before reading.
            Err(JobCreateError::ConstraintViolation { .. }) => {
                drop(op);
                let existing = self
                    .repo
                    .find_resident_id(&self.job_type)
                    .await?
                    // `jobs` rows are never deleted and the resident
                    // collision just fired, so exactly one row exists.
                    .expect("resident collision guarantees the row exists");
                Ok(self.handle(existing))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn handle(&self, id: JobId) -> JobHandle {
        JobHandle::new(
            id,
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            self.clock.clone(),
        )
    }
}
