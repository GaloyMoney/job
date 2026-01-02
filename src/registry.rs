//! Registry storing job initializers and retry settings.

use std::collections::HashMap;
use std::sync::Arc;

use super::{entity::*, error::JobError, repo::JobRepo, runner::*, spawner::JobSpawner};

/// Internal trait for storing initializers with erased Config type.
/// Only `init` is needed after registration - job_type and retry_settings
/// are extracted before boxing and stored separately.
pub(crate) trait AnyJobInitializer: Send + Sync + 'static {
    fn init(
        &self,
        job: &Job,
        repo: Arc<JobRepo>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>>;
}

impl<T: JobInitializer> AnyJobInitializer for T {
    fn init(
        &self,
        job: &Job,
        repo: Arc<JobRepo>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let spawner = JobSpawner::<T::Config>::new(repo, self.job_type());
        JobInitializer::init(self, job, spawner)
    }
}

/// Keeps track of registered job types and their retry behaviour.
pub struct JobRegistry {
    initializers: HashMap<JobType, Box<dyn AnyJobInitializer>>,
    retry_settings: HashMap<JobType, RetrySettings>,
}

impl JobRegistry {
    pub(crate) fn new() -> Self {
        Self {
            initializers: HashMap::new(),
            retry_settings: HashMap::new(),
        }
    }

    /// Register a [`JobInitializer`] and its associated retry settings.
    /// Returns the job type that was registered.
    pub fn add_initializer<I: JobInitializer>(&mut self, initializer: I) -> JobType {
        let job_type = initializer.job_type();
        let retry_settings = initializer.retry_on_error_settings();
        self.initializers
            .insert(job_type.clone(), Box::new(initializer));
        self.retry_settings.insert(job_type.clone(), retry_settings);
        job_type
    }

    pub(super) fn init_job(
        &self,
        job: &Job,
        repo: Arc<JobRepo>,
    ) -> Result<Box<dyn JobRunner>, JobError> {
        self.initializers
            .get(&job.job_type)
            .ok_or(JobError::NoInitializerPresent)?
            .init(job, repo)
            .map_err(|e| JobError::JobInitError(e.to_string()))
    }

    /// Retrieve retry settings for a given job type.
    pub(super) fn retry_settings(&self, job_type: &JobType) -> &RetrySettings {
        self.retry_settings
            .get(job_type)
            .expect("Retry settings not found")
    }

    /// Get a list of all registered job types.
    pub(crate) fn registered_job_types(&self) -> Vec<JobType> {
        self.initializers.keys().cloned().collect()
    }
}
