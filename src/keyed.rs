//! Keyed jobs — at most one LIVE (pending/running) job per `(job_type,
//! key)`. Once that job reaches a terminal state the key becomes
//! respawnable: the next [`KeyedJobSpawner::spawn`] call creates a NEW job
//! (new internally-generated id, new config) under the same key — `jobs`
//! accumulates one row per generation, while `job_executions` holds at most
//! one live row per key (liveness is structural: the row is deleted on
//! terminal).
//!
//! Contrast with [`crate::ResidentJobInitializer`] (resident jobs): a
//! resident job never terminates and there is exactly one, forever, for the
//! type's whole lifetime. A keyed job terminates and respawns under its key,
//! and many keys of one type coexist — the shape for a sharded consumer (one
//! singleton per shard) or any other per-entity singleton that should be
//! recreated after it finishes.

use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use es_entity::clock::ClockHandle;
use tracing::instrument;

use super::{
    Job, JobId,
    entity::{JobType, NewJob},
    error::JobError,
    handle::JobHandle,
    notification_router::JobNotificationRouter,
    notifier::JobEventNotifier,
    repo::JobRepo,
    runner::{JobRunner, RetrySettings},
    spawner::insert_execution,
};

/// Describes how to construct a [`crate::JobRunner`] for a keyed job type.
/// The keyed counterpart of [`crate::JobInitializer`] — keyed jobs use the
/// ordinary [`crate::JobRunner`]/[`crate::JobCompletion`] (they legitimately
/// complete; that's what makes them respawnable), so only registration and
/// spawning are distinct.
pub trait KeyedJobInitializer: Send + Sync + 'static {
    /// The configuration type for jobs of this type.
    type Config: Serialize + DeserializeOwned + Send + Sync;

    /// Returns the job type identifier.
    fn job_type(&self) -> JobType;

    /// Retry settings to use when the runner returns an error.
    fn retry_on_error_settings(&self) -> RetrySettings {
        Default::default()
    }

    /// Max concurrent jobs of this type executing in THIS process. `None` =
    /// unlimited. See [`crate::JobInitializer::max_concurrent_per_process`].
    fn max_concurrent_per_process(&self) -> Option<usize> {
        None
    }

    /// Max concurrent jobs of this type across ALL poller instances (soft).
    /// See [`crate::JobInitializer::max_concurrent_global`].
    fn max_concurrent_global(&self) -> Option<usize> {
        None
    }

    /// Whether a new generation's execution state should be seeded from the
    /// previous generation's final state before it starts. Defaults to
    /// `false` — most keyed jobs start each generation fresh; opt in when a
    /// respawn should resume where the last generation left off (e.g. a
    /// sharded listener's checkpoint carrying across a respawn).
    ///
    /// When `false` (the default), a terminal generation's execution state
    /// is still retained and readable (see
    /// [`crate::JobSnapshot::execution_state`]) until the key's next spawn
    /// compacts it away — this flag controls only whether the NEXT
    /// generation inherits it as a starting point, not whether the state is
    /// kept at all.
    fn inherits_state(&self) -> bool {
        false
    }

    /// Produce a runner instance for the provided job.
    ///
    /// The spawner parameter allows the runner to spawn further generations
    /// of the same key (or other keys) of this type.
    fn init(
        &self,
        job: &Job,
        spawner: KeyedJobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>>;
}

/// A handle for spawning keyed jobs of a specific type.
///
/// Returned by [`crate::Jobs::add_keyed_initializer`].
#[derive(Clone)]
pub struct KeyedJobSpawner<Config> {
    repo: Arc<JobRepo>,
    job_type: JobType,
    router: Arc<JobNotificationRouter>,
    clock: ClockHandle,
    notifier: Arc<JobEventNotifier>,
    inherits_state: bool,
    _phantom: PhantomData<Config>,
}

impl<Config> KeyedJobSpawner<Config>
where
    Config: Serialize + Send + Sync,
{
    pub(crate) fn new(
        repo: Arc<JobRepo>,
        job_type: JobType,
        router: Arc<JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
        inherits_state: bool,
    ) -> Self {
        Self {
            repo,
            job_type,
            router,
            clock,
            notifier,
            inherits_state,
            _phantom: PhantomData,
        }
    }

    /// Returns the job type this spawner creates.
    pub fn job_type(&self) -> &JobType {
        &self.job_type
    }

    /// Create a keyed job, or resolve to the LIVE one if `key` already holds
    /// one.
    ///
    /// While a job is LIVE (pending/running) under `key`, spawning again is
    /// a no-op: returns a [`JobHandle`] for the persisted job, so a
    /// double-spawning caller always observes the job that actually runs.
    /// Once that job reaches a terminal state the key becomes respawnable —
    /// the next call creates a NEW job (new internally-generated id, new
    /// config). The job's id is generated internally — a keyed job is
    /// identified by its `(job_type, key)`, not a caller-chosen id — so read
    /// the id back from the returned handle every time.
    ///
    /// Keys are opaque to the crate — a sharded consumer can spawn one
    /// singleton per shard and later enumerate them all via
    /// [`crate::Jobs::keyed_handles`], pairing each handle with
    /// [`JobHandle::load`]'s execution state to answer "caught up?" per
    /// shard. Does not consume the spawner: many keys of one type are
    /// expected.
    #[instrument(
        name = "keyed_job_spawner.spawn",
        skip(self, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn(
        &self,
        key: impl Into<String> + Send + Debug,
        config: Config,
    ) -> Result<JobHandle, JobError> {
        let key = key.into();
        // Serialized once so a retry (see below) doesn't need `Config: Clone`.
        let config = serde_json::to_value(config).map_err(JobError::CouldNotSerializeConfig)?;

        for _attempt in 0..3 {
            let new_job = NewJob::builder()
                .id(JobId::new())
                .unique_key(key.clone())
                .job_type(self.job_type.clone())
                .config(config.clone())?
                .tracing_context(es_entity::context::TracingContext::current())
                .build()
                .expect("Could not build new job");
            let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
            // The id is an internally generated v7 uuid, `resident` defaults
            // to false here (this path never sets it), and `jobs` carries no
            // unique-key-string constraint (liveness enforcement is on
            // `job_executions`, see below) — this can no longer conflict.
            let mut job = self.repo.create_in_op(&mut op, new_job).await?;
            let schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());
            match insert_execution(
                &self.repo,
                &self.notifier,
                &mut op,
                &mut job,
                schedule_at,
                None,
                Some(&key),
            )
            .await
            {
                Ok(()) => {
                    if self.inherits_state {
                        self.seed_state_in_op(&mut op, job.id, &key).await?;
                    }
                    self.compact_state_in_op(&mut op, job.id, &key).await?;
                    op.commit().await?;
                    return Ok(self.handle(job.id));
                }
                Err(e) if is_keyed_conflict(&e) => {
                    // Rolls back the job row too — nothing leaks.
                    drop(op);
                    if let Some(live) = self.repo.find_live_keyed(&self.job_type, &key).await? {
                        return Ok(self.handle(live));
                    }
                    // The holder went terminal between the conflict and this
                    // lookup — retry the whole spawn.
                }
                Err(e) => return Err(e),
            }
        }
        Err(JobError::KeyedSpawnRace(self.job_type.clone(), key))
    }

    /// Seed the new generation's (empty) state row from the previous
    /// generation's final state, if any. Race-free without extra locking:
    /// the new generation's execution insert (in this same `op`, just
    /// committed logically but not yet to disk) can only have succeeded if
    /// the previous LIVE holder's key was already released, which happens
    /// atomically with that holder's final state write
    /// (`dispatcher.rs::delete_execution_in_op` retains keyed state rows) —
    /// so by the time we're here, the predecessor's last state write, if
    /// any, is already visible. The new row has no state yet, so this
    /// INSERT cannot conflict.
    async fn seed_state_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
        key: &str,
    ) -> Result<(), JobError> {
        sqlx::query!(
            r#"
            INSERT INTO job_execution_states (id, execution_state_json)
            SELECT $1, s.execution_state_json
            FROM job_execution_states s
            JOIN jobs j ON j.id = s.id
            WHERE j.job_type = $2 AND j.unique_key = $3 AND j.id != $1
            ORDER BY j.created_at DESC, j.id DESC
            LIMIT 1
            "#,
            id as JobId,
            &self.job_type as &JobType,
            key,
        )
        .execute(op.as_executor())
        .await?;
        Ok(())
    }

    /// Delete every OTHER generation's state row for this key — run on every
    /// spawn regardless of `inherits_state`, so a key's retained state never
    /// grows past one row: the previous generation's (just possibly seeded
    /// from, above) plus the new one this job will write as it runs.
    async fn compact_state_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
        key: &str,
    ) -> Result<(), JobError> {
        sqlx::query!(
            r#"
            DELETE FROM job_execution_states s
            USING jobs j
            WHERE s.id = j.id AND j.job_type = $1 AND j.unique_key = $2 AND j.id != $3
            "#,
            &self.job_type as &JobType,
            key,
            id as JobId,
        )
        .execute(op.as_executor())
        .await?;
        Ok(())
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

/// Detects the `job_executions`-level live-key conflict raised by
/// [`KeyedJobSpawner::spawn`]'s execution insert. This is a raw `sqlx`
/// insert (not the entity repo), so the constraint name is reliable — unlike
/// es_entity's version-dependent column attribution for composite indexes on
/// `jobs` (see `error.rs`), so match it narrowly by name rather than broadly.
pub(crate) fn is_keyed_conflict(err: &JobError) -> bool {
    matches!(
        err,
        JobError::Sqlx(e)
            if e.as_database_error().and_then(|d| d.constraint())
                == Some("idx_job_executions_job_type_unique_key")
    )
}
