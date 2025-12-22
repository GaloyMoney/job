//! Registry storing job initializers and retry settings.

use std::collections::HashMap;

use super::{entity::*, error::JobError, runner::*};

/// Keeps track of registered job types and their retry behaviour.
pub struct JobRegistry {
    initializers: HashMap<JobType, Box<dyn JobInitializer>>,
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
    pub fn add_initializer<I: JobInitializer>(&mut self, initializer: I) {
        self.initializers
            .insert(<I as JobInitializer>::job_type(), Box::new(initializer));
        self.retry_settings.insert(
            <I as JobInitializer>::job_type(),
            <I as JobInitializer>::retry_on_error_settings(),
        );
    }

    pub(super) fn init_job(&self, job: &Job) -> Result<Box<dyn JobRunner>, JobError> {
        self.initializers
            .get(&job.job_type)
            .ok_or(JobError::NoInitializerPresent)?
            .init(job)
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
