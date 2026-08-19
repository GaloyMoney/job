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
        // Fan-out spawns made from WITHIN a running job's own runner don't
        // get the short-circuit fast path in this pass -- an always-empty
        // handle means they take the ordinary insert path unconditionally,
        // same as before. Deferred: reaching the live poller from here needs
        // an `Arc<JobPoller>` at the dispatch call site, which `dispatch_job`
        // doesn't carry today. See the handoff's PR2 §3.2 scope note.
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
        let spawner = KeyedJobSpawner::<I::Config>::new(
            repo,
            self.inner.job_type(),
            router,
            clock,
            notifier,
            self.inherits_state,
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

    /// Whether a due-now spawn of `job_type` may take the short-circuit
    /// fast path (`JobInitializer::short_circuit`, default `true`). Only
    /// meaningful for plain (non-batched, non-keyed, non-resident) types --
    /// the fast path does not yet extend to the other flavors (see the
    /// handoff this implements for what's deferred).
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
    pub(super) fn plan_claim(&self, n_jobs_to_poll: usize) -> ClaimPlan {
        let mut types = Vec::new();
        let mut row_limits = Vec::new();
        for job_type in self.registered_job_types() {
            let limit = match self.batch_policy(&job_type) {
                Some(policy) => policy
                    .max_concurrent_per_process
                    .saturating_sub(self.tracker.units_in_flight(&job_type))
                    .saturating_mul(policy.max_batch_size),
                None => match self.per_process_cap(&job_type) {
                    Some(cap) => cap.saturating_sub(self.tracker.units_in_flight(&job_type)),
                    None => n_jobs_to_poll,
                },
            };
            let limit = limit.min(n_jobs_to_poll);
            if limit == 0 {
                continue;
            }
            types.push(job_type);
            row_limits.push(limit as i32);
        }
        ClaimPlan { types, row_limits }
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

    /// D18: `Some(0)` caps are clamped to 1 rather than silently and
    /// permanently starving a type.
    #[test]
    fn zero_cap_is_clamped_to_one() {
        let mut registry = JobRegistry::new(Arc::new(JobTracker::new(0, 10)));
        let job_type = registry.add_initializer(ZeroCapInitializer);

        assert_eq!(registry.per_process_cap(&job_type), Some(1));
    }
}
