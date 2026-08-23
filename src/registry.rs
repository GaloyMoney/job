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
    /// specifically "the pool's headroom was the limiting factor." Recorded
    /// on the poll span, and read by `poll_and_dispatch`'s empty-plan
    /// branch: an empty-but-clamped plan means due work exists that a zero
    /// budget kept unclaimed, which arms the pool-headroom waiter
    /// (`JobPoller::arm_pool_headroom_waiter`) so recovery does not wait
    /// out the idle-poll fallback.
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
        // unit cost alongside each row limit now, plus whether the type is
        // ELASTIC: an uncapped plain type, whose `n_jobs_to_poll` "cost"
        // below is only the window ceiling standing in for demand this
        // planner has no way to measure (it doesn't know the type's real
        // due-row count) -- never a real obligation the way a capped
        // plain type's free-slot count or a batched type's free-chunk
        // count is. That distinction is why elastic types get a separate
        // allocation step below instead of competing in the same sort.
        let natural: Vec<(JobType, usize, usize, bool)> = self
            .registered_job_types()
            .into_iter()
            .filter_map(|job_type| {
                let (limit, units, elastic) = match self.batch_policy(&job_type) {
                    Some(policy) => {
                        let limit = policy
                            .max_concurrent_per_process
                            .saturating_sub(self.tracker.units_in_flight(&job_type))
                            .saturating_mul(policy.max_batch_size)
                            .min(n_jobs_to_poll);
                        (limit, limit.div_ceil(policy.max_batch_size.max(1)), false)
                    }
                    None => match self.per_process_cap(&job_type) {
                        Some(cap) => {
                            let limit = cap
                                .saturating_sub(self.tracker.units_in_flight(&job_type))
                                .min(n_jobs_to_poll);
                            (limit, limit, false)
                        }
                        None => (n_jobs_to_poll, n_jobs_to_poll, true),
                    },
                };
                (limit > 0).then_some((job_type, limit, units, elastic))
            })
            .collect();
        let clamped_by_pool =
            natural.iter().map(|(.., units, _)| units).sum::<usize>() > unit_budget;

        // Second pass: spend `unit_budget` in two steps.
        //
        // Step 1 -- reserve a floor of ONE unit per elastic type, off the
        // top, before bounded types (batched / capped plain, whose demand
        // is a real, finite obligation) get to compete for anything. This
        // is the mirror image of step 2's smallest-demand-first rule
        // below: without it, bounded types' own worst-case demands --
        // individually small, but potentially summing to the whole
        // budget across many registered types -- can exhaust `unit_budget`
        // before an elastic type's inflated `n_jobs_to_poll` cost ever
        // sorts into reach, excluding it from the plan entirely on every
        // poll (see `elastic_type_is_not_starved_by_many_bounded_types`).
        // A type with due work must never be silently unclaimable while
        // budget remains -- this floor is what guarantees that for
        // elastic types the way step 2's sort guarantees it for bounded
        // ones. (If `unit_budget` is smaller than the number of elastic
        // types, the floor itself is scarce; the deterministic
        // job-type-order tie-break below decides who gets it, same as any
        // other resource exhausted below demand.)
        let (bounded, mut elastic): (Vec<_>, Vec<_>) =
            natural.into_iter().partition(|(.., elastic)| !*elastic);
        elastic.sort_by(|(a, ..), (b, ..)| a.as_str().cmp(b.as_str()));
        let floor_count = elastic.len().min(unit_budget);
        let mut elastic_units: Vec<usize> = (0..elastic.len())
            .map(|i| if i < floor_count { 1 } else { 0 })
            .collect();
        let mut remaining_budget = unit_budget - floor_count;

        // Step 2 -- bounded types spend whatever the floor left, smallest-
        // demand-first, NOT `registered_job_types`' order (a `HashMap`
        // iteration order, randomized per process): under a scarce
        // budget, spending it in registration order lets whichever type
        // happens to be iterated first -- however large its own demand --
        // exhaust the WHOLE remaining budget before a small, explicitly-
        // capped type (e.g. `max_concurrent_per_process: Some(1)`,
        // needing exactly one unit) ever gets a turn. That is exactly the
        // type-starvation failure per-type windowing exists to prevent
        // elsewhere in the claim path (see PERFORMANCE.md, "Contention
        // headroom") -- this clamp must not reintroduce it at the
        // row_limit-assignment level. Smallest-first means a bounded type
        // only ever loses budget to another bounded type with EQUAL OR
        // GREATER demand.
        let mut bounded = bounded;
        bounded.sort_by_key(|(_, _, units, _)| *units);
        let mut types = Vec::new();
        let mut row_limits = Vec::new();
        for (job_type, limit, units, _) in bounded {
            if remaining_budget == 0 {
                continue;
            }
            let (limit, units) = if units <= remaining_budget {
                (limit, units)
            } else if let Some(policy) = self.batch_policy(&job_type) {
                (
                    remaining_budget
                        .saturating_mul(policy.max_batch_size)
                        .min(limit),
                    remaining_budget,
                )
            } else {
                (remaining_budget, remaining_budget)
            };
            if limit == 0 {
                continue;
            }
            types.push(job_type);
            row_limits.push(limit as i32);
            remaining_budget -= units;
        }

        // Step 3 -- grow elastic types beyond their floor with whatever
        // budget the bounded types didn't end up needing, first-come
        // against the same deterministic order used for the floor.
        for ((job_type, limit, _, _), floor) in elastic.into_iter().zip(elastic_units.iter_mut()) {
            if *floor == 0 {
                // Missed the floor entirely: `unit_budget` was smaller
                // than the number of elastic types. Left out of the plan
                // this poll, same as a bounded type that lost the sort.
                continue;
            }
            if remaining_budget > 0 {
                let extra = remaining_budget.min(limit.saturating_sub(*floor));
                *floor += extra;
                remaining_budget -= extra;
            }
            types.push(job_type);
            row_limits.push(*floor as i32);
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

    /// A capped plain type, keyed by its own job type so several can be
    /// registered side by side in one registry.
    struct FixedCapInitializer {
        job_type: JobType,
        cap: Option<usize>,
    }

    impl JobInitializer for FixedCapInitializer {
        type Config = ();

        fn job_type(&self) -> JobType {
            self.job_type.clone()
        }

        fn max_concurrent_per_process(&self) -> Option<usize> {
            self.cap
        }

        fn init(
            &self,
            _job: &Job,
            _: JobSpawner<Self::Config>,
        ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
            unimplemented!("never invoked by this test")
        }
    }

    /// An uncapped ("elastic") plain type -- `max_concurrent_per_process`
    /// defaults to `None`.
    struct UncappedInitializer(JobType);

    impl JobInitializer for UncappedInitializer {
        type Config = ();

        fn job_type(&self) -> JobType {
            self.0.clone()
        }

        fn init(
            &self,
            _job: &Job,
            _: JobSpawner<Self::Config>,
        ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
            unimplemented!("never invoked by this test")
        }
    }

    /// D6 at the `plan_claim` level (mirrors `capped_type_does_not_starve_others`
    /// in `tests/job.rs`): a capped type's small, bounded demand must be met
    /// in full even when an uncapped sibling is also competing for the same
    /// budget.
    #[test]
    fn capped_type_gets_full_demand_despite_uncapped_sibling() {
        let mut registry = JobRegistry::new(Arc::new(JobTracker::new(0, 10)));
        let capped = registry.add_initializer(FixedCapInitializer {
            job_type: JobType::new("registry-plan-claim-capped-sibling"),
            cap: Some(1),
        });
        registry.add_initializer(UncappedInitializer(JobType::new(
            "registry-plan-claim-uncapped-sibling",
        )));

        let plan = registry.plan_claim(50, 6);

        let idx = plan
            .types
            .iter()
            .position(|t| t == &capped)
            .expect("the capped type must be in the plan");
        assert_eq!(
            plan.row_limits[idx], 1,
            "the capped type's whole (small) demand must be met"
        );
    }

    /// The converse of `capped_type_does_not_starve_others`: several small,
    /// correctly-priced bounded (capped) demands that together consume the
    /// entire `unit_budget` must not crowd an UNCAPPED plain type out of the
    /// plan entirely. `plan_claim` has no real due-row count for an uncapped
    /// type -- only the window ceiling `n_jobs_to_poll` standing in for
    /// demand -- so under a pure smallest-demand-first sort that inflated
    /// cost always sorts last and can see a `remaining_units` of zero. This
    /// pins the fix: a floor of at least one row is reserved for the
    /// uncapped type before bounded types compete for anything.
    #[test]
    fn elastic_type_is_not_starved_by_many_bounded_types() {
        let mut registry = JobRegistry::new(Arc::new(JobTracker::new(0, 10)));
        const BOUNDED: [&str; 4] = [
            "registry-plan-claim-bounded-0",
            "registry-plan-claim-bounded-1",
            "registry-plan-claim-bounded-2",
            "registry-plan-claim-bounded-3",
        ];
        for job_type in BOUNDED {
            registry.add_initializer(FixedCapInitializer {
                job_type: JobType::new(job_type),
                cap: Some(1),
            });
        }
        let uncapped = registry.add_initializer(UncappedInitializer(JobType::new(
            "registry-plan-claim-uncapped-starved",
        )));

        // n_jobs_to_poll well above every real demand; unit_budget equals
        // the bounded types' combined real demand exactly, so under the
        // OLD algorithm (smallest-demand-first with no floor) the
        // uncapped type's inflated `n_jobs_to_poll` cost sorts dead last
        // and is excluded outright once the four capped types have spent
        // the whole budget on their genuine one-unit-each demand.
        let plan = registry.plan_claim(50, 4);

        let idx = plan.types.iter().position(|t| t == &uncapped);
        assert!(
            idx.is_some(),
            "an uncapped plain type must not be excluded from the plan while \
             unit_budget > 0, even when bounded types' combined real demand \
             consumes the rest of the budget -- got plan.types = {:?}",
            plan.types
        );
        assert!(
            plan.row_limits[idx.unwrap()] >= 1,
            "the uncapped type must get at least its floor of one claimable row"
        );
    }
}
