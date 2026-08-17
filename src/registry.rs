//! Registry storing job initializers and retry settings.

use es_entity::clock::ClockHandle;
use std::collections::HashMap;
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
        let spawner = JobSpawner::<T::Config>::new(repo, self.job_type(), clock, notifier);
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

/// Concurrency bounds of one PLAIN (non-batched) job type.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TypeConcurrency {
    pub per_process: Option<usize>,
    pub global: Option<usize>,
}

/// One poll's per-type claim plan.
pub(super) struct ClaimPlan {
    pub types: Vec<JobType>,
    pub row_limits: Vec<i32>,
    /// The subset of `types` that also carries a global (cross-instance) cap.
    pub global_cap_types: Vec<JobType>,
    pub global_caps: Vec<i32>,
}

/// Keeps track of registered job types and their retry behaviour.
pub struct JobRegistry {
    initializers: HashMap<JobType, Box<dyn AnyJobInitializer>>,
    batched_initializers: HashMap<JobType, Box<dyn AnyBatchedJobInitializer>>,
    batch_policies: HashMap<JobType, BatchPolicy>,
    concurrency: HashMap<JobType, TypeConcurrency>,
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
            retry_settings: HashMap::new(),
            tracker,
        }
    }

    /// Register a [`JobInitializer`] and its associated retry settings.
    /// Returns the job type that was registered.
    pub fn add_initializer<I: JobInitializer>(&mut self, initializer: I) -> JobType {
        let job_type = initializer.job_type();
        let retry_settings = initializer.retry_on_error_settings();
        let concurrency = TypeConcurrency {
            per_process: initializer.max_concurrent_per_process().map(|c| c.max(1)),
            global: initializer.max_concurrent_global().map(|c| c.max(1)),
        };
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
        let concurrency = TypeConcurrency {
            per_process: initializer.max_concurrent_per_process().map(|c| c.max(1)),
            global: initializer.max_concurrent_global().map(|c| c.max(1)),
        };
        let inherits_state = initializer.inherits_state();
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
        self.concurrency.get(job_type).and_then(|c| c.per_process)
    }

    /// Global (cross-process, soft) concurrency cap of a job type, if any.
    pub(super) fn global_cap(&self, job_type: &JobType) -> Option<usize> {
        self.concurrency.get(job_type).and_then(|c| c.global)
    }

    /// Every job type the tracker must notify on for a freed slot: plain
    /// types with a per-process cap, plus every globally-capped type (a freed
    /// global slot also only becomes claimable again on the next poll).
    pub(super) fn capped_types(&self) -> Vec<JobType> {
        self.concurrency
            .iter()
            .filter(|(_, c)| c.per_process.is_some() || c.global.is_some())
            .map(|(job_type, _)| job_type.clone())
            .collect()
    }

    /// Row limit for each registered type this poll. A type with no free
    /// slot is dropped; global caps are split out but resolved in `poll_jobs`.
    pub(super) fn plan_claim(&self, n_jobs_to_poll: usize) -> ClaimPlan {
        let mut types = Vec::new();
        let mut row_limits = Vec::new();
        let mut global_cap_types = Vec::new();
        let mut global_caps = Vec::new();
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
            if let Some(cap) = self.global_cap(&job_type) {
                global_cap_types.push(job_type.clone());
                global_caps.push(cap as i32);
            }
            types.push(job_type);
            row_limits.push(limit as i32);
        }
        ClaimPlan {
            types,
            row_limits,
            global_cap_types,
            global_caps,
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

        fn max_concurrent_global(&self) -> Option<usize> {
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
        assert_eq!(registry.global_cap(&job_type), Some(1));
    }
}
