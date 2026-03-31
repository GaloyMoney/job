//! Types describing the terminal outcome and return value of a completed job.

use serde::{Deserialize, Serialize};

/// Newtype wrapper around a raw JSON value representing the value produced by a
/// job runner via [`CurrentJob::set_result`](crate::CurrentJob::set_result).
///
/// Using a dedicated type instead of bare `serde_json::Value` gives call sites
/// semantic clarity and prevents accidental mix-ups with other JSON payloads
/// (config, execution state, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobReturnValue(serde_json::Value);

impl JobReturnValue {
    /// Consume the wrapper and return the inner JSON value.
    pub fn into_inner(self) -> serde_json::Value {
        self.0
    }

    /// Return a reference to the inner JSON value.
    pub fn as_value(&self) -> &serde_json::Value {
        &self.0
    }

    /// Deserialize the return value into a typed struct.
    pub fn deserialize<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.0.clone())
    }

    /// Serialize a value into a `JobReturnValue`.
    pub fn try_from<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        serde_json::to_value(value).map(Self)
    }
}

/// Outcome returned by [`Jobs::await_completion`](crate::Jobs::await_completion),
/// carrying both the terminal state and an optional return value.
#[derive(Debug, Clone)]
pub struct JobOutcome {
    state: JobTerminalState,
    result: Option<JobReturnValue>,
}

impl JobOutcome {
    pub(crate) fn new(state: JobTerminalState, result: Option<JobReturnValue>) -> Self {
        Self { state, result }
    }

    /// The terminal state the job reached.
    pub fn state(&self) -> JobTerminalState {
        self.state
    }

    /// Deserialize the return value into a typed struct.
    pub fn result<T: serde::de::DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        match &self.result {
            Some(r) => r.deserialize().map(Some),
            None => Ok(None),
        }
    }

    /// Returns `true` if the job completed successfully.
    pub fn is_completed(&self) -> bool {
        matches!(self.state, JobTerminalState::Completed)
    }

    /// Returns `true` if the job exhausted retries and was marked as errored.
    pub fn is_errored(&self) -> bool {
        matches!(self.state, JobTerminalState::Errored)
    }
}

/// Extension trait for inspecting a batch of [`JobOutcome`] values.
pub trait JobOutcomes {
    /// Count how many jobs in the batch ended with an error.
    fn failed_count(&self) -> usize;
    /// Returns `true` when every job in the batch completed successfully.
    fn all_succeeded(&self) -> bool;
}

impl JobOutcomes for Vec<JobOutcome> {
    fn failed_count(&self) -> usize {
        self.iter().filter(|r| r.is_errored()).count()
    }
    fn all_succeeded(&self) -> bool {
        self.iter().all(|r| r.is_completed())
    }
}

impl JobOutcomes for [JobOutcome] {
    fn failed_count(&self) -> usize {
        self.iter().filter(|r| r.is_errored()).count()
    }
    fn all_succeeded(&self) -> bool {
        self.iter().all(|r| r.is_completed())
    }
}

/// Terminal outcome of a job lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobTerminalState {
    /// The job completed successfully.
    Completed,
    /// The job exhausted its retries and was marked as errored.
    Errored,
}
