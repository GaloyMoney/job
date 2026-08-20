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
    poller::PollerHandle,
    repo::JobRepo,
    runner::{JobRunner, RetrySettings},
    spawner::{KeyedInsert, insert_keyed_execution},
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

    /// Whether this key's execution state outlives the generation that wrote
    /// it. Defaults to `false` — each generation starts fresh and its state is
    /// deleted when it terminates, exactly like a regular job.
    ///
    /// Set it when a respawn should resume where the last generation left off
    /// (e.g. a sharded listener's checkpoint carrying across a respawn). Then
    /// a terminal generation's state is kept rather than deleted, seeded into
    /// the next generation at spawn, and older generations' rows are compacted
    /// away — so a key holds at most one retained row. It also stays readable
    /// after terminal via [`crate::JobSnapshot::execution_state`].
    fn inherits_state(&self) -> bool {
        false
    }

    /// Whether a due-now spawn or completion of this type may take the
    /// head-swap short-circuit path. See
    /// [`crate::JobInitializer::short_circuit`] for the full trade-off.
    /// Defaults to `true`.
    fn short_circuit(&self) -> bool {
        true
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
    /// Reaches this process's poller for the head-swap short-circuit fast
    /// path (handoff addendum §7.3 site 5). See [`PollerHandle`].
    poller_ref: PollerHandle,
    _phantom: PhantomData<Config>,
}

impl<Config> KeyedJobSpawner<Config>
where
    Config: Serialize + Send + Sync,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        repo: Arc<JobRepo>,
        job_type: JobType,
        router: Arc<JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
        inherits_state: bool,
        poller_ref: PollerHandle,
    ) -> Self {
        Self {
            repo,
            job_type,
            router,
            clock,
            notifier,
            inherits_state,
            poller_ref,
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
    ///
    /// # Errors
    ///
    /// Spawning against a held key is not an error — it resolves to the
    /// holder. The one failure mode of its own is
    /// [`JobError::KeyedSpawnRace`], and it is practically unreachable: each
    /// attempt that loses the key re-reads the holder, and only a holder that
    /// went terminal in the instant between those two steps forces a retry.
    /// The error means that happened on three consecutive attempts.
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
            match insert_keyed_execution(
                &self.repo,
                &self.notifier,
                &mut op,
                &mut job,
                schedule_at,
                &key,
            )
            .await?
            {
                KeyedInsert::Inserted => {
                    self.carry_state_in_op(&mut op, job.id, &key).await?;

                    // Head-swap short-circuit (handoff addendum §7.3 site 5):
                    // a due-now keyed spawn tries to claim capacity for this
                    // type's oldest due backlog, same as the plain spawn
                    // path. A later statement in this same `op`, so it sees
                    // the insert above with guaranteed ordering.
                    if schedule_at <= self.clock.now()
                        && let Some(poller) = self.poller_ref.get().and_then(|w| w.upgrade())
                    {
                        poller.register_claim_demand(&mut op, &self.job_type, 1);
                    }

                    op.commit().await?;
                    return Ok(self.handle(job.id));
                }
                KeyedInsert::Live(live) => {
                    // Rolls back the job row too — nothing leaks.
                    drop(op);
                    return Ok(self.handle(live));
                }
                KeyedInsert::Contended => {
                    // The holder went terminal between the conflict and the
                    // read — retry the whole spawn.
                    drop(op);
                }
            }
        }
        Err(JobError::KeyedSpawnRace(self.job_type.clone(), key))
    }

    /// Carry the previous generation's final state into the new one and drop
    /// every older generation's row, in a single statement.
    ///
    /// Both halves read the same snapshot, so the seeding SELECT still sees
    /// the predecessor's row that the DELETE removes, and the DELETE cannot
    /// see the row the INSERT writes (they are disjoint anyway: `id = $1` vs
    /// `id != $1`). The result is that a key's state never grows past one
    /// row.
    ///
    /// Race-free without extra locking: the new generation's execution insert
    /// (earlier in this same `op`) can only have succeeded once the previous
    /// LIVE holder released the key, which happens atomically with that
    /// holder's final state write — so the predecessor's last write, if any,
    /// is already visible here.
    ///
    /// `$4` is [`KeyedJobInitializer::inherits_state`]: when false the seeding
    /// half is a no-op and only compaction runs.
    async fn carry_state_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
        key: &str,
    ) -> Result<(), JobError> {
        sqlx::query!(
            r#"
            WITH seeded AS (
                INSERT INTO job_execution_states (id, execution_state_json)
                SELECT $1, s.execution_state_json
                FROM job_execution_states s
                JOIN jobs j ON j.id = s.id
                WHERE j.job_type = $2 AND j.unique_key = $3 AND j.id != $1
                  AND $4::boolean
                ORDER BY j.created_at DESC, j.id DESC
                LIMIT 1
                RETURNING id
            )
            DELETE FROM job_execution_states s
            USING jobs j
            WHERE s.id = j.id AND j.job_type = $2 AND j.unique_key = $3 AND j.id != $1
            "#,
            id as JobId,
            &self.job_type as &JobType,
            key,
            self.inherits_state,
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
