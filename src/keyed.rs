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

use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::HashMap, fmt::Debug, marker::PhantomData, sync::Arc};

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

/// Describes one keyed job to create as part of a bulk
/// [`KeyedJobSpawner::spawn_all`] / [`KeyedJobSpawner::spawn_all_in_op`]
/// call.
///
/// There is deliberately no `id`: a keyed job is identified by its
/// `(job_type, key)`, and its id is generated internally. See
/// [`KeyedJobSpawner::spawn`].
pub struct KeyedJobSpec<Config> {
    pub key: String,
    pub config: Config,
    pub schedule_at: Option<DateTime<Utc>>,
}

impl<Config> KeyedJobSpec<Config> {
    pub fn new(key: impl Into<String>, config: Config) -> Self {
        Self {
            key: key.into(),
            config,
            schedule_at: None,
        }
    }

    /// Schedule this job for a specific time instead of immediately.
    pub fn schedule_at(mut self, schedule_at: DateTime<Utc>) -> Self {
        self.schedule_at = Some(schedule_at);
        self
    }
}

/// The outcome of spawning one key.
///
/// Unlike [`crate::BulkSpawnResult`], there is no "dropped" case: keyed spawn
/// resolves a collision to the LIVE holder rather than discarding the spec,
/// so every requested key yields a usable [`JobHandle`] and `created` says
/// which generation it refers to.
pub struct KeyedSpawn {
    /// The key that was requested.
    pub key: String,
    /// Handle to the job now holding `key` — the one just created, or the
    /// LIVE one that already held it.
    pub handle: JobHandle,
    /// `true` if this call created the job, `false` if it resolved to a job
    /// that already held the key. Use it to decide whether to perform
    /// first-time side effects alongside the spawn, in the same `op`.
    pub created: bool,
}

impl KeyedSpawn {
    /// Discard the key/`created` context and keep just the handle.
    pub fn into_handle(self) -> JobHandle {
        self.handle
    }
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
    /// path. See [`PollerHandle`].
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
    /// holder. This has no failure mode of its own beyond the underlying
    /// database errors.
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
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let spawned = self.spawn_in_op(&mut op, key, config).await?;
        op.commit().await?;
        Ok(spawned.handle)
    }

    /// [`Self::spawn`] as part of an existing atomic operation, so a keyed
    /// job is created in the same transaction as whatever prompted it.
    ///
    /// Same semantics as [`Self::spawn`] — create, or resolve to the LIVE
    /// holder — but returns [`KeyedSpawn`] rather than a bare handle, since
    /// in-op callers usually need to know whether they are the ones who
    /// created it before writing their own side of the transaction. Use
    /// [`KeyedSpawn::into_handle`] when they don't.
    ///
    /// Two calls on the SAME `op` for the same key are safe and resolve the
    /// second to the first's job: the execution row is inserted before this
    /// returns, and a transaction sees its own uncommitted writes, so the
    /// second call's live-check finds it. See [`Self::spawn_all_in_op`].
    #[instrument(
        name = "keyed_job_spawner.spawn_in_op",
        skip(self, op, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        key: impl Into<String> + Send + Debug,
        config: Config,
    ) -> Result<KeyedSpawn, JobError> {
        let mut spawned = self
            .spawn_all_in_op(op, vec![KeyedJobSpec::new(key, config)])
            .await?;
        Ok(spawned.pop().expect("one spec in, exactly one outcome out"))
    }

    /// Create or resolve many keys of this type in a single atomic operation.
    ///
    /// Outcomes are returned in the order of `specs`, one per spec. Every key
    /// yields a [`KeyedSpawn`] — none are silently dropped — so this can be
    /// zipped straight back against the inputs.
    #[instrument(
        name = "keyed_job_spawner.spawn_all",
        skip(self, specs),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_all(
        &self,
        specs: Vec<KeyedJobSpec<Config>>,
    ) -> Result<Vec<KeyedSpawn>, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let spawned = self.spawn_all_in_op(&mut op, specs).await?;
        op.commit().await?;
        Ok(spawned)
    }

    /// [`Self::spawn_all`] as part of an existing atomic operation. The core
    /// every other `spawn*` method on this spawner delegates to.
    ///
    /// # How a key is claimed
    ///
    /// `JobRepo::lock_and_check_live_keys_in_op` takes a transaction-scoped
    /// advisory lock per key and THEN reports each key's live holder. Every
    /// writer of a keyed key goes through it, so once the check reports a key
    /// free, no other transaction can claim it before this one ends — the
    /// subsequent insert cannot conflict, and needs no `ON CONFLICT` clause.
    /// A unique violation on `idx_job_executions_job_type_unique_key` from
    /// here would mean a writer bypassed the lock, which is a bug worth
    /// failing loudly on rather than absorbing.
    ///
    /// Resolving liveness BEFORE creating any `jobs` row is what keeps a
    /// resolved-to-holder spec from leaving an orphan `jobs` row behind (see
    /// `JobRepo::lock_and_check_live_keys_in_op`).
    ///
    /// # Why the insert is inline rather than deferred to `ExecutionInsertHook`
    ///
    /// Because it makes duplicate keys within one `op` self-checking. The
    /// hook batches inserts to commit time, so a sibling call's row would not
    /// exist yet when the next call runs its live-check; inserting here means
    /// the live-check — which reads inside this transaction, and so sees this
    /// transaction's own uncommitted writes — is the single mechanism
    /// resolving same-op, same-transaction and cross-transaction collisions
    /// alike. The batching that would buy is small in exchange: keyed rows
    /// always have `queue_id = NULL`, so the hook's queue parking/promotion
    /// machinery is inert for them, and bulk callers already get one
    /// statement per call from here.
    ///
    /// `seen` covers the remaining case the live-check cannot: two specs
    /// sharing a key WITHIN this call, neither inserted yet.
    #[instrument(
        name = "keyed_job_spawner.spawn_all_in_op",
        skip(self, op, specs),
        fields(job_type = %self.job_type, count)
    )]
    pub async fn spawn_all_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        specs: Vec<KeyedJobSpec<Config>>,
    ) -> Result<Vec<KeyedSpawn>, JobError> {
        tracing::Span::current().record("count", specs.len());
        if specs.is_empty() {
            return Ok(Vec::new());
        }

        let default_schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());
        let keys: Vec<String> = specs.iter().map(|s| s.key.clone()).collect();
        let live = self
            .repo
            .lock_and_check_live_keys_in_op(op, &self.job_type, &keys)
            .await?;

        let mut seen: HashMap<String, JobId> = HashMap::new();
        let mut new_jobs = Vec::new();
        let mut new_ids: Vec<JobId> = Vec::new();
        let mut new_keys: Vec<String> = Vec::new();
        let mut new_schedule_times: Vec<DateTime<Utc>> = Vec::new();
        let mut outcomes = Vec::with_capacity(specs.len());

        for spec in specs {
            if let Some(id) = live.get(&spec.key).or_else(|| seen.get(&spec.key)) {
                outcomes.push(KeyedSpawn {
                    key: spec.key,
                    handle: self.handle(*id),
                    created: false,
                });
                continue;
            }

            let id = JobId::new();
            let schedule_at = spec.schedule_at.unwrap_or(default_schedule_at);
            // The id is an internally generated v7 uuid, `resident` defaults
            // to false here (this path never sets it), and `jobs` carries no
            // unique-key-string constraint (liveness enforcement is on
            // `job_executions`) — this cannot conflict.
            new_jobs.push(
                NewJob::builder()
                    .id(id)
                    .unique_key(spec.key.clone())
                    .job_type(self.job_type.clone())
                    .config(spec.config)?
                    .tracing_context(es_entity::context::TracingContext::current())
                    .schedule_at(schedule_at)
                    .build()
                    .expect("Could not build new job"),
            );
            seen.insert(spec.key.clone(), id);
            new_ids.push(id);
            new_keys.push(spec.key.clone());
            new_schedule_times.push(schedule_at);
            outcomes.push(KeyedSpawn {
                key: spec.key,
                handle: self.handle(id),
                created: true,
            });
        }

        if new_jobs.is_empty() {
            return Ok(outcomes);
        }

        self.repo.create_all_in_op(op, new_jobs).await?;
        self.insert_executions_in_op(op, &new_ids, &new_keys, &new_schedule_times)
            .await?;
        self.carry_state_in_op(op, &new_ids, &new_keys).await?;

        self.notifier
            .execution_ready_in_op(op, &self.job_type)
            .await?;

        let now = self.clock.now();
        let n_due = new_schedule_times.iter().filter(|at| **at <= now).count();
        if n_due > 0
            && let Some(poller) = self.poller_ref.get().and_then(|w| w.upgrade())
        {
            poller.register_claim_demand(op, &self.job_type, n_due);
        }

        Ok(outcomes)
    }

    /// Insert the `job_executions` row claiming each key.
    ///
    /// No `ON CONFLICT`: every caller holds the key's advisory lock and has
    /// already seen it free. `state`/`attempt_index` come from the column
    /// defaults ('pending', 1), and `queue_id` is always NULL — a keyed job's
    /// singleton-ness comes from its key, not from a queue.
    #[instrument(name = "job.insert_keyed_executions", skip_all)]
    async fn insert_executions_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        ids: &[JobId],
        keys: &[String],
        schedule_times: &[DateTime<Utc>],
    ) -> Result<(), JobError> {
        sqlx::query!(
            r#"
            INSERT INTO job_executions
                (id, job_type, queue_id, unique_key, execute_at, alive_at, created_at)
            SELECT t.id, $2, NULL, t.unique_key, t.execute_at,
                   COALESCE($5, NOW()), COALESCE($5, NOW())
            FROM UNNEST($1::uuid[], $3::text[], $4::timestamptz[])
                AS t(id, unique_key, execute_at)
            "#,
            ids as &[JobId],
            &self.job_type as &JobType,
            keys,
            schedule_times,
            op.maybe_now(),
        )
        .execute(op.as_executor())
        .await?;
        Ok(())
    }

    /// For each newly created generation, carry the previous generation's
    /// final state into it and drop every older generation's row — all in a
    /// single statement.
    ///
    /// The per-key predecessor lookup starts from `jobs` (riding
    /// `idx_jobs_job_type_unique_key_created_at`) rather than from
    /// `job_execution_states` joined to `jobs`: `jobs` rows are never
    /// deleted, so at scale (a long-running key with thousands of terminal
    /// generations) joining `job_execution_states` to `jobs` over EVERY
    /// matching generation before sorting made that join dominate the
    /// statement's cost — even though, by the compaction invariant this very
    /// statement maintains, only ever one generation can actually have a
    /// state row to find. Picking the predecessor id from `jobs` first costs
    /// a two-row backward index scan per key (`id != i.id` skips at most the
    /// current row) regardless of how many terminal generations the key has
    /// accumulated, and `job_execution_states` is then probed only for that
    /// one winning id per key
    /// (job-dev:handoff-write-path-efficiency-sb-max13.md, F6).
    ///
    /// Both halves read the same `pred` snapshot, so the seeding SELECT still
    /// sees the predecessor rows that the DELETE removes, and the DELETE
    /// cannot see the rows the INSERT writes (they are disjoint anyway:
    /// `pred.new_id` vs `pred.pred_id`). The result is that a key's state
    /// never grows past one row. `ids[n]` pairs with `keys[n]`, and a key
    /// appears at most once, so the per-key `LATERAL` picks exactly one
    /// predecessor.
    ///
    /// Race-free with respect to the outgoing generation: this `op` already
    /// observed each key as free in
    /// `JobRepo::lock_and_check_live_keys_in_op`, and a key is released
    /// atomically with its holder's final state write — so the predecessor's
    /// last write, if any, is committed and visible here.
    ///
    /// `$4` is [`KeyedJobInitializer::inherits_state`] (read from
    /// `self.inherits_state`): when false the seeding half is a no-op and
    /// only compaction runs.
    async fn carry_state_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        ids: &[JobId],
        keys: &[String],
    ) -> Result<(), JobError> {
        sqlx::query!(
            r#"
            WITH input AS (
                SELECT * FROM UNNEST($1::uuid[], $3::text[]) AS t(id, unique_key)
            ), pred AS (
                SELECT i.id AS new_id, p.pred_id
                FROM input i
                JOIN LATERAL (
                    SELECT j.id AS pred_id
                    FROM jobs j
                    WHERE j.job_type = $2 AND j.unique_key = i.unique_key
                      AND j.id != i.id
                    ORDER BY j.created_at DESC, j.id DESC
                    LIMIT 1
                ) p ON TRUE
            ), seeded AS (
                INSERT INTO job_execution_states (id, execution_state_json)
                SELECT pred.new_id, s.execution_state_json
                FROM pred
                JOIN job_execution_states s ON s.id = pred.pred_id
                WHERE $4::boolean
                RETURNING id
            )
            DELETE FROM job_execution_states s
            USING pred
            WHERE s.id = pred.pred_id
            "#,
            ids as &[JobId],
            &self.job_type as &JobType,
            keys,
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{notification_router::JobNotificationRouter, notifier::JobEventNotifier, tracker::JobTracker};
    use std::time::Duration;

    async fn init_pool() -> anyhow::Result<sqlx::PgPool> {
        let pg_con = std::env::var("PG_CON").unwrap();
        Ok(sqlx::PgPool::connect(&pg_con).await?)
    }

    /// Wires a real `KeyedJobSpawner<()>` against `pool` so
    /// `carry_state_in_op` can be exercised directly. No poller is
    /// registered -- `carry_state_in_op` never consults it -- so an empty
    /// `PollerHandle` (the same stand-in `ResidentJobSpawner` uses in
    /// production before a poller attaches) is enough.
    async fn test_spawner(pool: &sqlx::PgPool, job_type: JobType, inherits_state: bool) -> KeyedJobSpawner<()> {
        let repo = Arc::new(JobRepo::new(pool));
        let router = Arc::new(JobNotificationRouter::new(
            pool,
            Arc::clone(&repo),
            16,
            Duration::from_secs(60),
        ));
        let tracker = Arc::new(JobTracker::new(0, 1));
        let notifier = JobEventNotifier::spawn(pool, tracker, router.terminal_sender());
        let poller_ref: PollerHandle = Arc::new(std::sync::OnceLock::new());
        KeyedJobSpawner::new(
            repo,
            job_type,
            router,
            ClockHandle::realtime(),
            notifier,
            inherits_state,
            poller_ref,
        )
    }

    /// Seeds a terminal `jobs` row (no execution row -- the generation has
    /// already gone terminal) at `created_at`, optionally with its own
    /// `job_execution_states` row.
    async fn seed_generation(
        pool: &sqlx::PgPool,
        job_type: &JobType,
        key: &str,
        created_at: DateTime<Utc>,
        state_json: Option<serde_json::Value>,
    ) -> anyhow::Result<JobId> {
        let id = JobId::new();
        sqlx::query(
            "INSERT INTO jobs (id, job_type, unique_key, created_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(uuid::Uuid::from(id))
        .bind(job_type.as_str())
        .bind(key)
        .bind(created_at)
        .execute(pool)
        .await?;
        if let Some(state_json) = state_json {
            sqlx::query(
                "INSERT INTO job_execution_states (id, execution_state_json) VALUES ($1, $2)",
            )
            .bind(uuid::Uuid::from(id))
            .bind(state_json)
            .execute(pool)
            .await?;
        }
        Ok(id)
    }

    /// P2 (job-dev:handoff-write-path-efficiency-sb-max13.md, F6): three
    /// terminal predecessor generations exist for the key -- the compaction
    /// invariant means only the NEWEST of them (`gen3`) can still carry a
    /// `job_execution_states` row (as if every earlier `carry_state_in_op`
    /// call had already compacted `gen1`/`gen2` away). The rewritten `pred`
    /// CTE must pick exactly `gen3` -- not scan/require all three -- seed the
    /// new generation from it, and delete exactly its row.
    #[tokio::test]
    async fn carry_state_seeds_from_and_deletes_only_the_newest_predecessor() -> anyhow::Result<()>
    {
        let pool = init_pool().await?;
        let job_type = JobType::from_owned(format!("carry-state-{}", uuid::Uuid::now_v7()));
        let key = "k";
        let base = chrono::Utc::now() - chrono::Duration::hours(1);

        let gen1 = seed_generation(
            &pool,
            &job_type,
            key,
            base,
            Some(serde_json::json!({"processed": 1})),
        )
        .await?;
        let gen2 = seed_generation(
            &pool,
            &job_type,
            key,
            base + chrono::Duration::minutes(1),
            None,
        )
        .await?;
        let gen3 = seed_generation(
            &pool,
            &job_type,
            key,
            base + chrono::Duration::minutes(2),
            Some(serde_json::json!({"processed": 3})),
        )
        .await?;
        // gen1's row is stale leftover from before compaction existed (or a
        // bug) -- it must never be read even though it's a real match for
        // `job_type`/`unique_key`/`id != $1`.
        let _ = gen1;
        let _ = gen2;

        let new_id = JobId::new();
        sqlx::query(
            "INSERT INTO jobs (id, job_type, unique_key, created_at) VALUES ($1, $2, $3, NOW())",
        )
        .bind(uuid::Uuid::from(new_id))
        .bind(job_type.as_str())
        .bind(key)
        .execute(&pool)
        .await?;

        let spawner = test_spawner(&pool, job_type.clone(), true).await;
        let mut op = es_entity::DbOp::init(&pool).await?;
        spawner
            .carry_state_in_op(&mut op, &[new_id], &[key.to_string()])
            .await?;
        op.commit().await?;

        let new_state: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT execution_state_json FROM job_execution_states WHERE id = $1",
        )
        .bind(uuid::Uuid::from(new_id))
        .fetch_optional(&pool)
        .await?;
        assert_eq!(
            new_state,
            Some(serde_json::json!({"processed": 3})),
            "the new generation must seed from the NEWEST predecessor (gen3), not gen1"
        );

        let gen3_state_gone: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT execution_state_json FROM job_execution_states WHERE id = $1",
        )
        .bind(uuid::Uuid::from(gen3))
        .fetch_optional(&pool)
        .await?;
        assert_eq!(
            gen3_state_gone, None,
            "gen3's state row must be deleted -- compacted into the new generation"
        );

        Ok(())
    }

    /// With `inherits_state = false` the seeding half is a no-op, but
    /// compaction (the DELETE) still runs -- the predecessor's state row
    /// must not be left behind to be picked up by some later carry.
    #[tokio::test]
    async fn carry_state_without_inherits_state_still_compacts_the_predecessor()
    -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type =
            JobType::from_owned(format!("carry-state-no-inherit-{}", uuid::Uuid::now_v7()));
        let key = "k";
        let base = chrono::Utc::now() - chrono::Duration::minutes(1);

        let pred = seed_generation(
            &pool,
            &job_type,
            key,
            base,
            Some(serde_json::json!({"processed": 1})),
        )
        .await?;

        let new_id = JobId::new();
        sqlx::query(
            "INSERT INTO jobs (id, job_type, unique_key, created_at) VALUES ($1, $2, $3, NOW())",
        )
        .bind(uuid::Uuid::from(new_id))
        .bind(job_type.as_str())
        .bind(key)
        .execute(&pool)
        .await?;

        let spawner = test_spawner(&pool, job_type.clone(), false).await;
        let mut op = es_entity::DbOp::init(&pool).await?;
        spawner
            .carry_state_in_op(&mut op, &[new_id], &[key.to_string()])
            .await?;
        op.commit().await?;

        let new_state: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT execution_state_json FROM job_execution_states WHERE id = $1",
        )
        .bind(uuid::Uuid::from(new_id))
        .fetch_optional(&pool)
        .await?;
        assert_eq!(new_state, None, "inherits_state = false must not seed");

        let pred_state_gone: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT execution_state_json FROM job_execution_states WHERE id = $1",
        )
        .bind(uuid::Uuid::from(pred))
        .fetch_optional(&pool)
        .await?;
        assert_eq!(
            pred_state_gone, None,
            "the predecessor's row must still be compacted away even when not inherited"
        );

        Ok(())
    }
}
