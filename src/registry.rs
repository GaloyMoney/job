//! Registry storing job initializers and retry settings.

use es_entity::clock::ClockHandle;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{
    batched::{AnyBatchedJobInitializer, AnyBatchedJobRunner, BatchedJobInitializer},
    entity::*,
    error::JobError,
    keyed::{KeyedJobInitializer, KeyedJobSpawner},
    notification_router::JobNotificationRouter,
    notifier::JobEventNotifier,
    repo::JobRepo,
    resident::{ResidentJobInitializer, ResidentRunnerAdapter},
    runner::*,
    spawner::JobSpawner,
    tracker::JobTracker,
};

/// Internal trait for storing initializers with erased Config type.
/// Only `init` is needed after registration - job_type and retry_settings
/// are extracted before boxing and stored separately.
pub(crate) trait AnyJobInitializer: Send + Sync + 'static {
    fn init(
        &self,
        job: &Job,
        repo: Arc<JobRepo>,
        router: Arc<JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>>;
}

impl<T: JobInitializer> AnyJobInitializer for T {
    fn init(
        &self,
        job: &Job,
        repo: Arc<JobRepo>,
        _router: Arc<JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        // Fan-out spawns made from WITHIN a running job's own runner take
        // the ordinary insert path: this handle is never populated, since
        // `dispatch_job` has no `Arc<JobPoller>` to hand it here.
        let spawner = JobSpawner::<T::Config>::new(
            repo,
            self.job_type(),
            clock,
            notifier,
            Arc::new(std::sync::OnceLock::new()),
        );
        JobInitializer::init(self, job, spawner)
    }
}

/// Erases a [`KeyedJobInitializer`] into an [`AnyJobInitializer`].
///
/// Can't be a second blanket `impl<T: KeyedJobInitializer> AnyJobInitializer
/// for T` — that would conflict with the blanket impl over `JobInitializer`
/// above (the compiler can't rule out one type implementing both traits).
/// This newtype sidesteps the conflict: it's a distinct concrete type that
/// implements `AnyJobInitializer` itself, wrapping the caller's initializer
/// rather than blanket-extending its trait.
pub(crate) struct ErasedKeyedInitializer<I> {
    inner: I,
    inherits_state: bool,
}

impl<I> ErasedKeyedInitializer<I> {
    pub(crate) fn new(inner: I, inherits_state: bool) -> Self {
        Self {
            inner,
            inherits_state,
        }
    }
}

impl<I: KeyedJobInitializer> AnyJobInitializer for ErasedKeyedInitializer<I> {
    fn init(
        &self,
        job: &Job,
        repo: Arc<JobRepo>,
        router: Arc<JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        // Always-empty handle: fan-out spawns of further generations made
        // from WITHIN a running keyed job's own runner take the ordinary
        // insert path, same as the plain and batched fan-out cases above.
        let spawner = KeyedJobSpawner::<I::Config>::new(
            repo,
            self.inner.job_type(),
            router,
            clock,
            notifier,
            self.inherits_state,
            Arc::new(std::sync::OnceLock::new()),
        );
        KeyedJobInitializer::init(&self.inner, job, spawner)
    }
}

/// Erases a [`ResidentJobInitializer`] into an [`AnyJobInitializer`]. See
/// [`ErasedKeyedInitializer`] for why this can't be a blanket impl.
pub(crate) struct ErasedResidentInitializer<I>(pub(crate) I);

impl<I: ResidentJobInitializer> AnyJobInitializer for ErasedResidentInitializer<I> {
    fn init(
        &self,
        job: &Job,
        _repo: Arc<JobRepo>,
        _router: Arc<JobNotificationRouter>,
        _clock: ClockHandle,
        _notifier: Arc<JobEventNotifier>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let runner = ResidentJobInitializer::init(&self.0, job)?;
        Ok(Box::new(ResidentRunnerAdapter(runner)))
    }
}

/// How claims and dispatch are shaped for one batched job type.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchPolicy {
    /// Most jobs handed to a single `run_batch` call.
    pub max_batch_size: usize,
    /// Most batches of this type in flight per process; also the claim
    /// throttle — no rows are claimed for the type while every slot is busy.
    pub max_concurrent_per_process: usize,
}

/// One poll's per-type claim plan.
pub(super) struct ClaimPlan {
    pub types: Vec<JobType>,
    pub row_limits: Vec<i32>,
    /// Whether `unit_budget` (see [`JobRegistry::plan_claim`]) bound any
    /// type's row limit below what it would otherwise have claimed --
    /// specifically "the pool's headroom was the limiting factor."
    /// Telemetry only (recorded on the poll span): since
    /// `JobPoller::pool_unit_budget` floors the budget at one unit, the
    /// pool clamp can shrink a plan but never empty it, so nothing branches
    /// on this.
    pub clamped_by_pool: bool,
}

/// Keeps track of registered job types and their retry behaviour.
pub struct JobRegistry {
    initializers: HashMap<JobType, Box<dyn AnyJobInitializer>>,
    batched_initializers: HashMap<JobType, Box<dyn AnyBatchedJobInitializer>>,
    batch_policies: HashMap<JobType, BatchPolicy>,
    concurrency: HashMap<JobType, Option<usize>>,
    /// Keyed types whose execution state outlives the generation that wrote
    /// it (`KeyedJobInitializer::inherits_state`).
    retains_state: HashSet<JobType>,
    /// Plain (`JobInitializer`) types that opted OUT of the short-circuit
    /// spawn fast path (`JobInitializer::short_circuit() == false`). Absence
    /// from this set means short-circuiting is allowed -- the default -- so
    /// this is a deny-list, not the flag itself.
    short_circuit_disabled: HashSet<JobType>,
    retry_settings: HashMap<JobType, RetrySettings>,
    tracker: Arc<JobTracker>,
}

impl JobRegistry {
    pub(crate) fn new(tracker: Arc<JobTracker>) -> Self {
        Self {
            initializers: HashMap::new(),
            batched_initializers: HashMap::new(),
            batch_policies: HashMap::new(),
            concurrency: HashMap::new(),
            retains_state: HashSet::new(),
            short_circuit_disabled: HashSet::new(),
            retry_settings: HashMap::new(),
            tracker,
        }
    }

    /// Register a [`JobInitializer`] and its associated retry settings.
    /// Returns the job type that was registered.
    pub fn add_initializer<I: JobInitializer>(&mut self, initializer: I) -> JobType {
        let job_type = initializer.job_type();
        let retry_settings = initializer.retry_on_error_settings();
        let concurrency = initializer.max_concurrent_per_process().map(|c| c.max(1));
        if !initializer.short_circuit() {
            self.short_circuit_disabled.insert(job_type.clone());
        }
        self.initializers
            .insert(job_type.clone(), Box::new(initializer));
        self.concurrency.insert(job_type.clone(), concurrency);
        self.retry_settings.insert(job_type.clone(), retry_settings);
        job_type
    }

    /// Register a [`KeyedJobInitializer`] and its associated retry settings.
    /// Returns the job type that was registered. Stored in the same
    /// `initializers` map as [`add_initializer`](Self::add_initializer) —
    /// dispatch is identical once erased.
    pub fn add_keyed_initializer<I: KeyedJobInitializer>(&mut self, initializer: I) -> JobType {
        let job_type = initializer.job_type();
        let retry_settings = initializer.retry_on_error_settings();
        let concurrency = initializer.max_concurrent_per_process().map(|c| c.max(1));
        let inherits_state = initializer.inherits_state();
        if !initializer.short_circuit() {
            self.short_circuit_disabled.insert(job_type.clone());
        }
        if inherits_state {
            self.retains_state.insert(job_type.clone());
        }
        self.initializers.insert(
            job_type.clone(),
            Box::new(ErasedKeyedInitializer::new(initializer, inherits_state)),
        );
        self.concurrency.insert(job_type.clone(), concurrency);
        self.retry_settings.insert(job_type.clone(), retry_settings);
        job_type
    }

    /// Register a [`ResidentJobInitializer`]. Returns the job type that was
    /// registered. Retry is always eternal (`n_attempts: None`) regardless
    /// of what [`ResidentJobInitializer::retry_on_error_settings`] returns —
    /// a resident job can never be exhausted into a terminal error — and no
    /// concurrency entry is created: at most one job of this type ever
    /// exists, so there is nothing to cap.
    pub fn add_resident_initializer<I: ResidentJobInitializer>(
        &mut self,
        initializer: I,
    ) -> JobType {
        let job_type = initializer.job_type();
        let retry_settings = RetrySettings {
            n_attempts: None,
            ..initializer.retry_on_error_settings()
        };
        self.initializers.insert(
            job_type.clone(),
            Box::new(ErasedResidentInitializer(initializer)),
        );
        self.retry_settings.insert(job_type.clone(), retry_settings);
        job_type
    }

    /// Register a [`BatchedJobInitializer`]: jobs of this type are executed in
    /// batches rather than one task per job. Returns the job type registered.
    pub fn add_batched_initializer<I: BatchedJobInitializer>(&mut self, initializer: I) -> JobType {
        let job_type = initializer.job_type();
        let retry_settings = initializer.retry_on_error_settings();
        let policy = BatchPolicy {
            max_batch_size: initializer.max_batch_size().max(1),
            max_concurrent_per_process: initializer.max_concurrent_per_process().max(1),
        };
        if !initializer.short_circuit() {
            self.short_circuit_disabled.insert(job_type.clone());
        }
        self.batched_initializers
            .insert(job_type.clone(), Box::new(initializer));
        self.batch_policies.insert(job_type.clone(), policy);
        self.retry_settings.insert(job_type.clone(), retry_settings);
        job_type
    }

    pub(super) fn init_job(
        &self,
        job: &Job,
        repo: Arc<JobRepo>,
        router: Arc<JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
    ) -> Result<Box<dyn JobRunner>, JobError> {
        self.initializers
            .get(&job.job_type)
            .ok_or(JobError::NoInitializerPresent)?
            .init(job, repo, router, clock, notifier)
            .map_err(|e| JobError::JobInitError(e.to_string()))
    }

    /// Whether this type keeps its execution state past terminal. True only
    /// for keyed types with `inherits_state`; see `dispatcher.rs`.
    pub(super) fn retains_state(&self, job_type: &JobType) -> bool {
        self.retains_state.contains(job_type)
    }

    /// Whether jobs of this type are dispatched in batches.
    pub(super) fn is_batched(&self, job_type: &JobType) -> bool {
        self.batched_initializers.contains_key(job_type)
    }

    /// The claim/dispatch policy of a batched type; `None` for per-job types.
    pub(super) fn batch_policy(&self, job_type: &JobType) -> Option<BatchPolicy> {
        self.batch_policies.get(job_type).copied()
    }

    /// Largest number of jobs of this type handed to one `run_batch` call.
    pub(super) fn max_batch_size(&self, job_type: &JobType) -> usize {
        self.batch_policies
            .get(job_type)
            .map(|policy| policy.max_batch_size)
            .unwrap_or(1)
    }

    pub(super) fn init_batch(
        &self,
        job_type: &JobType,
        repo: Arc<JobRepo>,
        router: Arc<JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
    ) -> Result<Box<dyn AnyBatchedJobRunner>, JobError> {
        self.batched_initializers
            .get(job_type)
            .ok_or(JobError::NoInitializerPresent)?
            .init_erased(repo, router, clock, notifier)
            .map_err(|e| JobError::JobInitError(e.to_string()))
    }

    /// Retrieve retry settings for a given job type.
    pub(super) fn retry_settings(&self, job_type: &JobType) -> &RetrySettings {
        self.retry_settings
            .get(job_type)
            .expect("Retry settings not found")
    }

    /// Get a list of all registered job types, batched and non-batched alike.
    pub(crate) fn registered_job_types(&self) -> Vec<JobType> {
        self.initializers
            .keys()
            .chain(self.batched_initializers.keys())
            .cloned()
            .collect()
    }

    /// Per-process concurrency cap of a PLAIN job type, if any. `None` for
    /// batched types (see [`Self::batch_policy`]) and uncapped plain types.
    pub(super) fn per_process_cap(&self, job_type: &JobType) -> Option<usize> {
        self.concurrency.get(job_type).copied().flatten()
    }

    /// Whether a due-now spawn or completion of `job_type` may take the
    /// head-swap short-circuit path (`JobInitializer`/`KeyedJobInitializer`/
    /// `BatchedJobInitializer::short_circuit`, default `true` on all three).
    /// Meaningful for plain, keyed, and batched types alike -- resident types
    /// never reach this check (`ResidentJobSpawner` holds no poller
    /// reference).
    pub(super) fn short_circuit(&self, job_type: &JobType) -> bool {
        !self.short_circuit_disabled.contains(job_type)
    }

    /// Every job type the tracker must notify on for a freed slot: the plain
    /// types carrying a per-process cap, whose backlog only becomes claimable
    /// again on the next poll.
    pub(super) fn capped_types(&self) -> Vec<JobType> {
        self.concurrency
            .iter()
            .filter(|(_, cap)| cap.is_some())
            .map(|(job_type, _)| job_type.clone())
            .collect()
    }

    /// Row limit for each registered type this poll; a type with no free slot
    /// is dropped. This is the ONLY admission budget the claim query gets —
    /// each type's queued and unqueued scans are bounded by it directly, so
    /// there is no overscan multiplier and no type can crowd out another.
    /// `unit_budget` bounds *dispatch units*, not claimed rows -- see
    /// `JobPoller::pool_unit_budget`'s doc comment for why the
    /// distinction matters (a batched type's whole claim becomes as few as
    /// one `run_batch` call, not one per row). For a batched type, one unit
    /// is one eventual `dispatch_batches` chunk: `free_concurrent_slots =
    /// max_concurrent_per_process - units_in_flight` chunks of up to
    /// `max_batch_size` rows each is the worst case where every claimed row
    /// is "fresh" (attempt 1) -- see `dispatch_batches`' chunking loop.
    /// Retries (attempt > 1) are dispatched one-per-unit on top of that and
    /// aren't accounted for here, since they're the minority case; a poll
    /// with an unusually large retry batch could therefore claim slightly
    /// past `unit_budget` in practice, which is the same direction of
    /// imprecision as under-claiming elsewhere in this clamp -- never an
    /// increase in the STEADY-STATE worst case, only in a scenario already
    /// bounded by `RetrySettings`. For a plain type, one unit is one row
    /// (one `JobDispatcher` each).
    pub(super) fn plan_claim(&self, n_jobs_to_poll: usize, unit_budget: usize) -> ClaimPlan {
        // First pass: every type's UNCONSTRAINED (pool-budget-ignoring)
        // limit and dispatch-unit cost -- everything `plan_claim` already
        // computed before pool-awareness existed, just also carrying the
        // unit cost alongside each row limit now.
        let natural: Vec<(JobType, usize, usize)> = self
            .registered_job_types()
            .into_iter()
            .filter_map(|job_type| {
                let (limit, units) = match self.batch_policy(&job_type) {
                    Some(policy) => {
                        let limit = policy
                            .max_concurrent_per_process
                            .saturating_sub(self.tracker.units_in_flight(&job_type))
                            .saturating_mul(policy.max_batch_size)
                            .min(n_jobs_to_poll);
                        (limit, limit.div_ceil(policy.max_batch_size.max(1)))
                    }
                    None => {
                        let limit = match self.per_process_cap(&job_type) {
                            Some(cap) => {
                                cap.saturating_sub(self.tracker.units_in_flight(&job_type))
                            }
                            None => n_jobs_to_poll,
                        }
                        .min(n_jobs_to_poll);
                        (limit, limit)
                    }
                };
                (limit > 0).then_some((job_type, limit, units))
            })
            .collect();

        // Second pass: spend `unit_budget` across that list, trimming
        // (never growing) each type's row limit to fit -- for a batched
        // type in whole-`max_batch_size`-chunk increments, since a partial
        // chunk is still one full dispatch unit's connection cost.
        //
        // Smallest-demand types first, NOT `registered_job_types`' order
        // (a `HashMap` iteration order, randomized per process): under a
        // scarce budget, spending it in registration order lets whichever
        // type happens to be iterated first -- however large its own
        // uncapped demand -- exhaust the WHOLE budget before a small,
        // explicitly-capped type (e.g. `max_concurrent_per_process:
        // Some(1)`, needing exactly one unit) ever gets a turn. That is
        // exactly the type-starvation failure per-type windowing exists to
        // prevent elsewhere in the claim path (see PERFORMANCE.md,
        // "Contention headroom") -- this clamp must not reintroduce it at
        // the row_limit-assignment level. Smallest-first means a type only
        // ever loses budget to types with EQUAL OR GREATER demand, and a
        // type asking for just 1-2 units is served before an uncapped
        // type's effectively-unbounded appetite can crowd it out.
        let mut natural = natural;
        natural.sort_by_key(|(_, _, units)| *units);
        let clamped_by_pool = natural.iter().map(|(.., units)| units).sum::<usize>() > unit_budget;
        let mut types = Vec::new();
        let mut row_limits = Vec::new();
        let mut units_used = 0;
        for (job_type, limit, units) in natural {
            let remaining_units = unit_budget.saturating_sub(units_used);
            if remaining_units == 0 {
                continue;
            }
            let (limit, units) = if units <= remaining_units {
                (limit, units)
            } else if let Some(policy) = self.batch_policy(&job_type) {
                (
                    remaining_units
                        .saturating_mul(policy.max_batch_size)
                        .min(limit),
                    remaining_units,
                )
            } else {
                (remaining_units, remaining_units)
            };
            if limit == 0 {
                continue;
            }
            types.push(job_type);
            row_limits.push(limit as i32);
            units_used += units;
        }
        ClaimPlan {
            types,
            row_limits,
            clamped_by_pool,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ZeroCapInitializer;

    impl JobInitializer for ZeroCapInitializer {
        type Config = ();

        fn job_type(&self) -> JobType {
            JobType::new("registry-zero-cap-test")
        }

        fn max_concurrent_per_process(&self) -> Option<usize> {
            Some(0)
        }

        fn init(
            &self,
            _job: &Job,
            _: JobSpawner<Self::Config>,
        ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
            unimplemented!("never invoked by this test")
        }
    }

    /// `Some(0)` caps are clamped to 1 rather than silently and
    /// permanently starving a type.
    #[test]
    fn zero_cap_is_clamped_to_one() {
        let mut registry = JobRegistry::new(Arc::new(JobTracker::new(0, 10)));
        let job_type = registry.add_initializer(ZeroCapInitializer);

        assert_eq!(registry.per_process_cap(&job_type), Some(1));
    }
}
