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
//!
//! A spawn against a LIVE key resolves to its holder and, by default,
//! changes nothing — so a holder parked in the future runs at its own
//! deadline no matter how much work arrives meanwhile.
//! [`KeyedJobSpec::force_reschedule`] opts a spawn out of that: it pulls the
//! holder's `execute_at` forward to the time the spec asks for (its
//! `schedule_at`, or now), monotonically (earlier only) and never over a
//! backoff. See [`KeyedJobSpawner::spawn_all_in_op`].

use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    marker::PhantomData,
    sync::Arc,
};

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
    /// Opt-in "wake no later than now" — see
    /// [`Self::force_reschedule`]. Off by default: a spawn against a
    /// live key stays a pure no-op unless this is set.
    pub force_reschedule: bool,
}

impl<Config> KeyedJobSpec<Config> {
    pub fn new(key: impl Into<String>, config: Config) -> Self {
        Self {
            key: key.into(),
            config,
            schedule_at: None,
            force_reschedule: false,
        }
    }

    /// Schedule this job for a specific time instead of immediately.
    pub fn schedule_at(mut self, schedule_at: DateTime<Utc>) -> Self {
        self.schedule_at = Some(schedule_at);
        self
    }

    /// Treat this spawn as a liveness signal as well as a work request: if
    /// the key is already held by a job scheduled LATER than this spec asks
    /// for, pull that job's `execute_at` forward instead of doing nothing.
    ///
    /// Without it, a spawn against a live key resolves to the holder and has
    /// no effect at all — so a job that parked itself far ahead (say a
    /// keyed subscriber holding a deadline via
    /// [`crate::JobCompletion::RescheduleAt`]) cannot be woken by arriving
    /// work and runs only when its own deadline comes due.
    ///
    /// The target is this spec's own [`Self::schedule_at`], or now when it
    /// has none — the same `execute_at` a job created by this spec would
    /// have been given. So the respawn means *"run no later than when I am
    /// asking for"*: with no `schedule_at` that is "no later than now", and
    /// with one it can pull an hour-long hold in to five minutes and no
    /// further.
    ///
    /// The move is monotone and idempotent: `execute_at` only ever moves
    /// EARLIER, never later, and a repeat of the same request changes
    /// nothing. [`KeyedSpawn::pulled_forward`] reports whether this call was
    /// the one that moved the row.
    ///
    /// # It never shortens a backoff
    ///
    /// A retry scheduled by the type's `RetryPolicy` is left alone —
    /// [`KeyedJobSpawner::spawn_all_in_op`] documents the exact guard. Only
    /// a first-attempt row (a deliberate reschedule, never an exponential
    /// backoff) can be pulled forward, so a high-frequency respawn against a
    /// failing keyed job cannot turn its backoff into a hot loop.
    ///
    /// Note this is deliberately BOTH a property of the call and a property
    /// of the row: the flag says the caller wants a wake, and the guard
    /// decides whether the row it lands on may be woken.
    pub fn force_reschedule(mut self) -> Self {
        self.force_reschedule = true;
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
    /// `true` if this call moved the holder's `execute_at` EARLIER, to the
    /// time this spec asked for — its [`KeyedJobSpec::schedule_at`], or now
    /// when it has none. Only ever set for a
    /// [`KeyedJobSpec::force_reschedule`] spec that resolved to a holder
    /// scheduled later than that and eligible to be woken (see
    /// [`KeyedJobSpawner::spawn_all_in_op`]). Always `false` when `created`
    /// is `true`: a job created here already carries the time it asked for.
    ///
    /// `created == false && pulled_forward == false` is the ordinary
    /// resolve-to-holder outcome: the key was live and nothing was changed.
    pub pulled_forward: bool,
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
    ///
    /// # Waking a live holder ([`KeyedJobSpec::force_reschedule`])
    ///
    /// A spec that resolves to a holder normally has no effect whatsoever.
    /// With the flag set it additionally runs `pull_forward_in_op` —
    /// `execute_at = LEAST(execute_at, target)` for the holder's row, where
    /// the target is the spec's own `schedule_at` or now — turning the
    /// respawn into "run no later than the time I am asking for" for a job
    /// scheduled beyond it.
    ///
    /// Two things make that safe:
    ///
    /// - **It never shortens a backoff.** The row must carry
    ///   `attempt_index <= 1`. `RetryPolicy` retries carry the NEXT attempt
    ///   index (>1, `finalizer.rs`'s retry write), while every deliberate
    ///   reschedule resets it to 1 — so an exponential backoff is invisible
    ///   to this path. Without that guard a keyed job spawned on every
    ///   upstream event (lana's price-shock sweeps are the live example)
    ///   would have its backoff erased by the next event and hot-loop at the
    ///   event rate precisely while it is failing.
    /// - **It only ever moves `execute_at` EARLIER**, so it is monotone and
    ///   idempotent: repeated respawns of a due row are no-ops, and no
    ///   respawn can ever delay a job.
    ///
    /// Only keys that were ALREADY live when the call started reach that
    /// statement. A key repeated within one call — first spec creates it far
    /// ahead, second spec asks for a wake — is resolved in memory instead:
    /// on a row this call is itself inserting every guard holds by
    /// construction, so the wake is just a lower `execute_at` on the insert.
    /// Same row, same reported `pulled_forward`, one statement fewer.
    ///
    /// Rows actually moved are then reported through the SAME two signals a
    /// creation uses (the `ExecutionReady` notify and the local poller's
    /// claim demand). Firing neither would leave the wake to be discovered
    /// by the next ordinary poll tick, which is exactly the latency this
    /// mechanism exists to remove.
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
        // Deduped here rather than by the database: the fan-out this serves
        // maps many upstream events onto a handful of keys, so both
        // statements underneath would otherwise carry one array element per
        // event to describe a handful of rows.
        let mut keys: Vec<String> = specs.iter().map(|s| s.key.clone()).collect();
        keys.sort_unstable();
        keys.dedup();
        let live = self
            .repo
            .lock_and_check_live_keys_in_op(op, &self.job_type, &keys)
            .await?;

        // Key -> (holder, its index into the `new_*` arrays below).
        let mut seen: HashMap<String, (JobId, usize)> = HashMap::new();
        let mut new_jobs = Vec::new();
        let mut new_ids: Vec<JobId> = Vec::new();
        let mut new_keys: Vec<String> = Vec::new();
        let mut new_schedule_times: Vec<DateTime<Utc>> = Vec::new();
        // Wakes against rows that were ALREADY live when this call started —
        // the only ones that need a statement. One entry per key, folded to
        // the earliest target as the specs are read.
        let mut wake: HashMap<String, DateTime<Utc>> = HashMap::new();
        let mut wake_outcomes: Vec<usize> = Vec::new();
        // Wakes against rows THIS call is inserting, by `new_*` index. These
        // never reach the database — see below.
        let mut local_wake: HashMap<usize, DateTime<Utc>> = HashMap::new();
        let mut local_wake_outcomes: Vec<(usize, usize)> = Vec::new();
        let mut outcomes = Vec::with_capacity(specs.len());

        for spec in specs {
            // The `execute_at` this spec asks for: the one a NEW job of it
            // would be created with, and the one a wake pulls a holder to.
            let wanted_at = spec.schedule_at.unwrap_or(default_schedule_at);

            if let Some(id) = live.get(&spec.key) {
                if spec.force_reschedule {
                    wake.entry(spec.key.clone())
                        .and_modify(|t| *t = (*t).min(wanted_at))
                        .or_insert(wanted_at);
                    wake_outcomes.push(outcomes.len());
                }
                outcomes.push(KeyedSpawn {
                    key: spec.key,
                    handle: self.handle(*id),
                    created: false,
                    pulled_forward: false,
                });
                continue;
            }

            if let Some(&(id, idx)) = seen.get(&spec.key) {
                if spec.force_reschedule {
                    local_wake
                        .entry(idx)
                        .and_modify(|t| *t = (*t).min(wanted_at))
                        .or_insert(wanted_at);
                    local_wake_outcomes.push((outcomes.len(), idx));
                }
                outcomes.push(KeyedSpawn {
                    key: spec.key,
                    handle: self.handle(id),
                    created: false,
                    pulled_forward: false,
                });
                continue;
            }

            let id = JobId::new();
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
                    .schedule_at(wanted_at)
                    .build()
                    .expect("Could not build new job"),
            );
            seen.insert(spec.key.clone(), (id, new_ids.len()));
            new_ids.push(id);
            new_keys.push(spec.key.clone());
            new_schedule_times.push(wanted_at);
            outcomes.push(KeyedSpawn {
                key: spec.key,
                handle: self.handle(id),
                created: true,
                pulled_forward: false,
            });
        }

        // A wake against a row this very call is inserting needs no
        // statement at all. Such a row is always `state = 'pending'` at
        // `attempt_index = 1` (the column defaults), so both of
        // `pull_forward_in_op`'s guards hold by construction and its write
        // collapses to a `LEAST` over values already in hand — which is
        // simply the `execute_at` the insert should carry in the first place.
        let mut moved_locally: HashSet<usize> = HashSet::new();
        for (idx, target) in local_wake {
            if target < new_schedule_times[idx] {
                new_schedule_times[idx] = target;
                moved_locally.insert(idx);
            }
        }
        for (outcome_idx, idx) in local_wake_outcomes {
            outcomes[outcome_idx].pulled_forward = moved_locally.contains(&idx);
        }

        // Distinct by construction — they are a `HashMap`'s keys — which is
        // what keeps `pull_forward_in_op`'s join single-rowed per key.
        let (wake_keys, wake_targets): (Vec<String>, Vec<DateTime<Utc>>) = wake.into_iter().unzip();

        if new_jobs.is_empty() && wake_keys.is_empty() {
            return Ok(outcomes);
        }

        if !new_jobs.is_empty() {
            self.repo.create_all_in_op(op, new_jobs).await?;
            self.insert_executions_in_op(op, &new_ids, &new_keys, &new_schedule_times)
                .await?;
            self.carry_state_in_op(op, &new_ids, &new_keys).await?;
        }

        let pulled = self
            .pull_forward_in_op(op, &wake_keys, &wake_targets)
            .await?;
        // Reported only on the specs that ASKED for a wake: a plain spec
        // sharing a key with one of them in the same call still reads as the
        // no-op it requested.
        for i in wake_outcomes {
            outcomes[i].pulled_forward = pulled.contains_key(&outcomes[i].key);
        }

        // Nothing created and nothing moved means nothing to announce: a
        // wake that found its row already due must not add a notify to a
        // funnel that is already this crate's busiest.
        if new_ids.is_empty() && pulled.is_empty() {
            return Ok(outcomes);
        }

        // Fires for a row moved to a FUTURE instant too, not just a due one:
        // a peer poller is asleep on a deadline computed before this write,
        // and `min_wait` reports only strictly-future deadlines, so a row
        // that just became earlier than that sleep target has no other way
        // to be noticed until the sleep expires.
        self.notifier
            .execution_ready_in_op(op, &self.job_type)
            .await?;

        let now = self.clock.now();
        // Claim demand is DUE-now work only, so a row pulled forward to a
        // still-future instant contributes nothing here — the notify above
        // is what covers it.
        let n_due = new_schedule_times.iter().filter(|at| **at <= now).count()
            + pulled.values().filter(|at| **at <= now).count();
        if n_due > 0
            && let Some(poller) = self.poller_ref.get().and_then(|w| w.upgrade())
        {
            poller.register_claim_demand(op, &self.job_type, n_due);
        }

        Ok(outcomes)
    }

    /// `execute_at = LEAST(execute_at, target)` for every one of `keys`
    /// whose live row sits later than that key's `target` and is eligible to
    /// be woken. Returns each moved key with its new `execute_at` — nothing
    /// at all is written for a key already due at or before its target,
    /// already running, or backing off.
    ///
    /// `keys[n]` pairs with `targets[n]`, and `keys` MUST be distinct: the
    /// caller folds several wake specs for one key to their EARLIEST target
    /// before calling, which is both the monotone answer and what keeps this
    /// `UPDATE ... FROM` join single-rowed. A repeated key would leave the
    /// join free to pick either target.
    ///
    /// The target is the spec's own `schedule_at`, or the caller's `now`
    /// when it has none — i.e. exactly the `execute_at` a NEW job of that
    /// spec would have been given, so "wake this key" and "create this key"
    /// agree on when the work is wanted. A wake is therefore *"run no later
    /// than T"*, not just *"run now"*: a caller that knows the work is only
    /// due in five minutes can pull a one-hour hold to five minutes and no
    /// further.
    ///
    /// The guards, in the order they matter:
    ///
    /// - `execute_at > target` makes the write monotone: a row can only move
    ///   EARLIER. `LEAST` restates that in the `SET` so the statement is
    ///   still monotone read on its own.
    /// - `attempt_index <= 1` is the backoff guard. A `RetryPolicy` retry
    ///   carries the next attempt index (>1); every deliberate reschedule
    ///   resets it to 1. So this cannot shorten an exponential backoff — see
    ///   [`Self::spawn_all_in_op`].
    /// - `state = 'pending'` keeps this off rows the poller currently owns.
    ///   A running row's `execute_at` is rewritten by the finalizer when the
    ///   run ends, so moving it would be pointless; more importantly the
    ///   finalizer's disposition writes take `FOR UPDATE` on exactly the rows
    ///   it owns (`poller_instance_id = <instance>`, always `running`), so
    ///   excluding them keeps this statement's row locks disjoint from the
    ///   only other multi-row writer of these rows. Concurrent pull-forwards
    ///   of overlapping key sets cannot interleave at all: every caller holds
    ///   the keys' advisory locks from
    ///   `JobRepo::lock_and_check_live_keys_in_op` for the rest of the
    ///   transaction.
    ///
    /// A `now`-derived target is the caller's own `op.maybe_now()`-or-clock
    /// instant, the same basis the poller compares against, rather than the
    /// server's `NOW()`. Every `execute_at` this crate writes comes from the
    /// injected clock (`NOW()` is used only for `alive_at`/`created_at`), and
    /// a woken row has to look due to a poller reading that clock.
    ///
    /// It is read before the advisory locks are waited on, so a caller that
    /// blocked behind another writer moves the row to a slightly stale
    /// instant. That is deliberate and harmless: the write is monotone, so a
    /// stale `now` can only ever land the row further in the past.
    ///
    /// KNOWN GAP (`job-dev:handoff-keyed-wake-pull-forward.md` Q1, open): a
    /// congestion reschedule leaves `attempt_index` untouched, so a row
    /// delayed by pool congestion on its FIRST attempt is indistinguishable
    /// here from a deliberate hold and is pulled forward. The exposure is
    /// bounded and does not compound — congestion parks a row 2s ± 1s ahead,
    /// flat, and the real load shedding is `poller::budget`'s pool-aware
    /// admission (a saturated pool claims nothing regardless of
    /// `execute_at`) rather than that delay.
    #[instrument(name = "job.pull_keyed_executions_forward", skip_all)]
    async fn pull_forward_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        keys: &[String],
        targets: &[DateTime<Utc>],
    ) -> Result<HashMap<String, DateTime<Utc>>, JobError> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = sqlx::query!(
            r#"
            UPDATE job_executions je
               SET execute_at = LEAST(je.execute_at, t.target)
              FROM UNNEST($2::text[], $3::timestamptz[]) AS t(unique_key, target)
             WHERE je.job_type = $1
               AND je.unique_key = t.unique_key
               AND je.state = 'pending'
               AND je.attempt_index <= 1
               AND je.execute_at > t.target
            RETURNING je.unique_key AS "unique_key!", je.execute_at AS "execute_at!"
            "#,
            &self.job_type as &JobType,
            keys,
            targets,
        )
        .fetch_all(op.as_executor())
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.unique_key, r.execute_at))
            .collect())
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
    /// deleted, so picking the predecessor id from `jobs` first costs a
    /// two-row backward index scan per key (`id != i.id` skips at most the
    /// current row) regardless of how many terminal generations the key has
    /// accumulated. That bound is the point: a long-running key accumulates
    /// thousands of terminal generations, and any form that examines them
    /// all before narrowing to one row pays for the whole history to find
    /// the one row that can exist. `job_execution_states` is then probed
    /// only for that one winning id per key.
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
    use crate::{
        notification_router::JobNotificationRouter, notifier::JobEventNotifier, tracker::JobTracker,
    };
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
    async fn test_spawner(
        pool: &sqlx::PgPool,
        job_type: JobType,
        inherits_state: bool,
    ) -> KeyedJobSpawner<()> {
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

    /// Three terminal predecessor generations exist for the key. The
    /// compaction invariant means only the NEWEST of them (`gen3`) can still
    /// carry a `job_execution_states` row, since every earlier
    /// `carry_state_in_op` call already compacted `gen1`/`gen2` away. The
    /// `pred` CTE must pick exactly `gen3` -- not scan or require all three
    /// -- seed the new generation from it, and delete exactly its row.
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
