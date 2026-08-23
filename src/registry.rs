//! Registry storing job initializers and retry settings.

use es_entity::clock::ClockHandle;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    /// Advanced once per `plan_claim` call; rotates which elastic types
    /// (and, at `unit_budget == 1`, which tier) get a scarce claim slot
    /// across polls instead of the same ones losing out every time.
    claim_tick: AtomicUsize,
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
            claim_tick: AtomicUsize::new(0),
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
    /// `elastic` types (uncapped plain -- no real due-row count, only the
    /// `n_jobs_to_poll` window ceiling) get a per-poll floor split off
    /// `unit_budget` before `bounded` types (batched / capped plain, real
    /// finite demand) compete for the rest, smallest-demand-first. See
    /// `tier_split` for how that split degrades once elastic types
    /// outnumber the budget.
    pub(super) fn plan_claim(&self, n_jobs_to_poll: usize, unit_budget: usize) -> ClaimPlan {
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

        let (mut bounded, mut elastic): (Vec<_>, Vec<_>) =
            natural.into_iter().partition(|(.., elastic)| !*elastic);
        elastic.sort_by(|(a, ..), (b, ..)| a.as_str().cmp(b.as_str()));
        let bounded_demand: usize = bounded.iter().map(|(_, _, units, _)| *units).sum();
        let tick = self.claim_tick.fetch_add(1, Ordering::Relaxed);
        let (elastic_tier_budget, bounded_tier_budget) =
            tier_split(elastic.len(), bounded_demand, unit_budget, tick);
        // tier_split caps each tier at its own (possibly small) demand, so
        // it can leave budget unassigned at the top level -- e.g. no
        // bounded types at all, capping the elastic tier at just its type
        // count instead of the whole budget. That unassigned amount is
        // real, unclaimed headroom and must still reach elastic growth.
        let tier_leftover = unit_budget - bounded_tier_budget - elastic_tier_budget;

        // Bounded types spend their tier's budget smallest-demand-first,
        // same rule #186 used across the whole list -- a type only ever
        // loses budget to another type with equal or greater demand.
        bounded.sort_by_key(|(_, _, units, _)| *units);
        let mut types = Vec::new();
        let mut row_limits = Vec::new();
        let mut bounded_remaining = bounded_tier_budget;
        for (job_type, limit, units, _) in bounded {
            if bounded_remaining == 0 {
                continue;
            }
            let (limit, units) = if units <= bounded_remaining {
                (limit, units)
            } else if let Some(policy) = self.batch_policy(&job_type) {
                (
                    bounded_remaining
                        .saturating_mul(policy.max_batch_size)
                        .min(limit),
                    bounded_remaining,
                )
            } else {
                (bounded_remaining, bounded_remaining)
            };
            if limit == 0 {
                continue;
            }
            types.push(job_type);
            row_limits.push(limit as i32);
            bounded_remaining -= units;
        }

        // Elastic types draw their floor from a window that rotates by
        // `tick`, so a scarce elastic_tier_budget cycles through every
        // elastic type instead of the same ones winning it every poll.
        // Whatever the bounded tier didn't need grows the picked ones
        // beyond their floor.
        let n = elastic.len();
        let take = elastic_tier_budget.min(n);
        let offset = if n == 0 { 0 } else { tick % n };
        let mut growth_budget = bounded_remaining + tier_leftover;
        for i in 0..take {
            let (job_type, limit, ..) = &elastic[(offset + i) % n];
            let mut units = 1;
            let extra = growth_budget.min(limit.saturating_sub(units));
            units += extra;
            growth_budget -= extra;
            types.push(job_type.clone());
            row_limits.push(units as i32);
        }

        ClaimPlan {
            types,
            row_limits,
            clamped_by_pool,
        }
    }
}

/// Splits `unit_budget` between the elastic tier (demand = one turn per
/// elastic type) and the bounded tier (demand = real summed units),
/// smaller demand first -- the same water-filling `plan_claim` already
/// does per-type, one level up. At `unit_budget == 1` neither tier gets
/// even a token share, so priority alternates by `tick` instead of
/// always favoring whichever demand is smaller -- otherwise that tier
/// would win the single unit on every poll.
fn tier_split(
    elastic_demand: usize,
    bounded_demand: usize,
    unit_budget: usize,
    tick: usize,
) -> (usize, usize) {
    if elastic_demand == 0 {
        return (0, bounded_demand.min(unit_budget));
    }
    if bounded_demand == 0 {
        return (elastic_demand.min(unit_budget), 0);
    }
    if unit_budget == 1 {
        return if tick.is_multiple_of(2) {
            (1, 0)
        } else {
            (0, 1)
        };
    }
    let (small, large, small_is_elastic) = if elastic_demand <= bounded_demand {
        (elastic_demand, bounded_demand, true)
    } else {
        (bounded_demand, elastic_demand, false)
    };
    let small_share = small.min(unit_budget / 2);
    let large_share = (unit_budget - small_share).min(large);
    if small_is_elastic {
        (small_share, large_share)
    } else {
        (large_share, small_share)
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

    /// Mirrors `capped_type_does_not_starve_others` at the `plan_claim`
    /// level: a capped type's small demand must be met in full even with
    /// an uncapped sibling also competing for the budget.
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

    /// The converse of `capped_type_does_not_starve_others`: bounded
    /// demand that sums to the whole budget must not crowd an uncapped
    /// type out of the plan entirely.
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

        // unit_budget equals the four bounded types' combined demand
        // exactly, so a floor-less algorithm excludes the uncapped type
        // outright once bounded spends the whole budget.
        let plan = registry.plan_claim(50, 4);

        let idx = plan.types.iter().position(|t| t == &uncapped);
        assert!(
            idx.is_some(),
            "an uncapped plain type must not be excluded while unit_budget > 0, \
             even when bounded demand consumes the rest of it -- got plan.types = {:?}",
            plan.types
        );
        assert!(
            plan.row_limits[idx.unwrap()] >= 1,
            "the uncapped type must get at least its floor of one claimable row"
        );
    }

    /// The mirror-image boundary: when elastic types outnumber
    /// `unit_budget`, a bounded type competing for the same budget must
    /// still make progress -- an unconditional "1 unit per elastic type"
    /// floor would reserve the whole budget before bounded is considered.
    #[test]
    fn bounded_type_is_not_starved_by_many_elastic_types() {
        let mut registry = JobRegistry::new(Arc::new(JobTracker::new(0, 10)));
        let capped = registry.add_initializer(FixedCapInitializer {
            job_type: JobType::new("registry-plan-claim-bounded-vs-many-elastic"),
            cap: Some(1),
        });
        const ELASTIC: [&str; 5] = [
            "registry-plan-claim-elastic-0",
            "registry-plan-claim-elastic-1",
            "registry-plan-claim-elastic-2",
            "registry-plan-claim-elastic-3",
            "registry-plan-claim-elastic-4",
        ];
        for job_type in ELASTIC {
            registry.add_initializer(UncappedInitializer(JobType::new(job_type)));
        }

        // 5 elastic types, unit_budget == 3: a per-type floor reserves
        // the whole budget for elastic before the capped type is ever
        // considered.
        let plan = registry.plan_claim(50, 3);

        let idx = plan.types.iter().position(|t| t == &capped);
        assert!(
            idx.is_some(),
            "a bounded type must not be starved by many elastic types \
             sharing the budget -- got plan.types = {:?}",
            plan.types
        );
        assert!(plan.row_limits[idx.unwrap()] >= 1);
    }

    /// Elastic types that lose out on a scarce floor in one poll must win
    /// it in a later one -- the picked subset rotates rather than always
    /// being the same alphabetically-first types.
    #[test]
    fn elastic_types_rotate_through_a_scarce_floor_across_polls() {
        let mut registry = JobRegistry::new(Arc::new(JobTracker::new(0, 10)));
        let elastic: Vec<JobType> = (0..5)
            .map(|i| {
                registry.add_initializer(UncappedInitializer(JobType::new(Box::leak(
                    format!("registry-plan-claim-rotation-{i}").into_boxed_str(),
                ))))
            })
            .collect();

        // unit_budget == 2 across 5 elastic types and no bounded
        // competitor: only 2 can win the floor per poll.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..elastic.len() {
            let plan = registry.plan_claim(50, 2);
            seen.extend(plan.types);
        }

        assert_eq!(
            seen.len(),
            elastic.len(),
            "every elastic type must be picked within enough polls to cycle through them all"
        );
    }

    /// At `unit_budget == 1` neither tier's demand fits, so a fixed
    /// smaller-demand-first order would hand the single unit to the same
    /// tier forever; it must alternate by `tick` instead.
    #[test]
    fn tier_split_alternates_at_budget_one() {
        assert_eq!(tier_split(5, 1, 1, 0), (1, 0));
        assert_eq!(tier_split(5, 1, 1, 1), (0, 1));
    }

    /// With no bounded competitor, tier_split caps the elastic tier's
    /// floor allocation at its (small) type count, not the whole budget
    /// -- the growth phase must still recover the rest.
    #[test]
    fn elastic_type_alone_grows_to_its_full_window_not_just_its_floor() {
        let mut registry = JobRegistry::new(Arc::new(JobTracker::new(0, 10)));
        let uncapped = registry.add_initializer(UncappedInitializer(JobType::new(
            "registry-plan-claim-elastic-alone",
        )));

        let plan = registry.plan_claim(5, 7);

        let idx = plan.types.iter().position(|t| t == &uncapped).unwrap();
        assert_eq!(
            plan.row_limits[idx], 5,
            "the sole elastic type must use the whole window, not just its floor"
        );
    }
}
