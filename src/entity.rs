//! Core job entities and events persisted in Postgres.

use chrono::{DateTime, Utc};
use derive_builder::Builder;
use rand::{Rng, rng};
use serde::{Deserialize, Serialize};

use std::{borrow::Cow, time::Duration};

use es_entity::{context::TracingContext, *};

use crate::{JobId, error::JobError, time};

#[derive(Clone, Eq, Hash, PartialEq, Debug, Serialize, Deserialize, sqlx::Type)]
#[sqlx(transparent)]
#[serde(transparent)]
/// Identifier describing a job type or class of work.
///
/// Use `JobType::new` for static name registration.
///
/// # Examples
///
/// ```rust
/// use job::JobType;
///
/// const CLEANUP_JOB: JobType = JobType::new("cleanup-job");
/// ```
pub struct JobType(Cow<'static, str>);
impl JobType {
    pub const fn new(job_type: &'static str) -> Self {
        JobType(Cow::Borrowed(job_type))
    }

    #[cfg(test)]
    pub(crate) fn from_owned(job_type: String) -> Self {
        JobType(Cow::Owned(job_type))
    }
}

impl std::fmt::Display for JobType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(EsEvent, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[es_event(id = "JobId", event_context = false)]
pub enum JobEvent {
    Initialized {
        id: JobId,
        job_type: JobType,
        config: serde_json::Value,
        tracing_context: Option<TracingContext>,
    },
    ExecutionScheduled {
        attempt: u32,
        scheduled_at: DateTime<Utc>,
    },
    ExecutionCompleted,
    ExecutionAborted {
        reason: String,
    },
    ExecutionErrored {
        error: String,
    },
    JobCompleted,
}

#[derive(Debug, Clone)]
pub(crate) struct RetryPolicy {
    pub max_attempts: Option<u32>,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_jitter_pct: u8,
    pub attempt_reset_after_backoff_multiples: u32,
}

#[derive(EsEntity, Builder)]
#[builder(pattern = "owned", build_fn(error = "EsEntityError"))]
/// Entity capturing immutable job metadata and lifecycle events.
pub struct Job {
    pub id: JobId,
    pub job_type: JobType,
    config: serde_json::Value,
    events: EntityEvents<JobEvent>,
}

impl Job {
    /// Decode the stored configuration payload into a typed struct.
    pub fn config<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.config.clone())
    }

    /// Returns `true` once the job has emitted a `JobCompleted` event.
    pub fn completed(&self) -> bool {
        self.events
            .iter_all()
            .rev()
            .any(|event| matches!(event, JobEvent::JobCompleted))
    }

    pub(crate) fn inject_tracing_parent(&self) {
        if let JobEvent::Initialized {
            tracing_context: Some(tracing_context),
            ..
        } = self.events.iter_all().next().expect("first event")
        {
            tracing_context.inject_as_parent();
        }
    }

    pub(super) fn execution_scheduled(&mut self, scheduled_at: DateTime<Utc>) {
        self.events.push(JobEvent::ExecutionScheduled {
            attempt: 1,
            scheduled_at,
        });
    }

    pub(super) fn execution_rescheduled(&mut self, scheduled_at: DateTime<Utc>) {
        self.events.push(JobEvent::ExecutionCompleted);
        self.events.push(JobEvent::ExecutionScheduled {
            attempt: 1,
            scheduled_at,
        });
    }

    pub(super) fn execution_aborted(
        &mut self,
        reason: String,
        scheduled_at: DateTime<Utc>,
        attempt: u32,
    ) {
        self.events.push(JobEvent::ExecutionAborted { reason });
        self.events.push(JobEvent::ExecutionScheduled {
            attempt,
            scheduled_at,
        });
    }

    pub(super) fn job_completed(&mut self) {
        self.events.push(JobEvent::ExecutionCompleted);
        self.events.push(JobEvent::JobCompleted);
    }

    pub(super) fn retry_scheduled(
        &mut self,
        error: String,
        scheduled_at: DateTime<Utc>,
        attempt: u32,
    ) {
        self.events.push(JobEvent::ExecutionErrored { error });
        self.events.push(JobEvent::ExecutionScheduled {
            attempt,
            scheduled_at,
        });
    }

    pub(super) fn job_errored(&mut self, error: String) {
        self.events.push(JobEvent::ExecutionErrored { error });
        self.events.push(JobEvent::JobCompleted);
    }

    pub(super) fn maybe_schedule_retry(
        &mut self,
        attempt: u32,
        retry_policy: &RetryPolicy,
        error: String,
    ) -> Option<(DateTime<Utc>, u32)> {
        let max_attempts = retry_policy.max_attempts.unwrap_or(u32::MAX);
        let mut next_attempt = attempt.max(1).saturating_add(1);
        if self.should_reset_attempt_count(&time::now(), retry_policy) {
            // If the attempt counter has reset, because enough time
            // has passed, that means the current attempt is 1, and the
            // next will be 2.
            next_attempt = 2;
        }

        if next_attempt > max_attempts {
            self.job_errored(error);
            return None;
        }

        let reschedule_at = next_attempt_at(
            // Based on current attempt counter
            next_attempt.saturating_sub(1),
            retry_policy.min_backoff,
            retry_policy.max_backoff,
            retry_policy.backoff_jitter_pct,
        );
        self.retry_scheduled(error, reschedule_at, next_attempt);
        Some((reschedule_at, next_attempt))
    }

    fn should_reset_attempt_count(&self, now: &DateTime<Utc>, retry_policy: &RetryPolicy) -> bool {
        self.latest_execution_scheduled()
            .and_then(|(scheduled_at, recorded_at)| {
                let previous_backoff = scheduled_at
                    .signed_duration_since(recorded_at)
                    .to_std()
                    .ok()?;
                let elapsed_since_scheduled =
                    now.signed_duration_since(scheduled_at).to_std().ok()?;
                let reset_threshold = previous_backoff
                    .checked_mul(retry_policy.attempt_reset_after_backoff_multiples)?;
                Some(elapsed_since_scheduled > reset_threshold)
            })
            .unwrap_or(false)
    }

    fn latest_execution_scheduled(&self) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        self.events
            .iter_persisted()
            .rev()
            .find_map(|persisted| match &persisted.event {
                JobEvent::ExecutionScheduled { scheduled_at, .. } => {
                    Some((*scheduled_at, persisted.recorded_at))
                }
                _ => None,
            })
    }
}

impl TryFromEvents<JobEvent> for Job {
    fn try_from_events(events: EntityEvents<JobEvent>) -> Result<Self, EsEntityError> {
        let mut builder = JobBuilder::default();
        for event in events.iter_all() {
            match event {
                JobEvent::Initialized {
                    id,
                    job_type,
                    config,
                    ..
                } => {
                    builder = builder
                        .id(*id)
                        .job_type(job_type.clone())
                        .config(config.clone())
                }
                JobEvent::ExecutionScheduled { .. } => {}
                JobEvent::ExecutionCompleted => {}
                JobEvent::ExecutionAborted { .. } => {}
                JobEvent::ExecutionErrored { .. } => {}
                JobEvent::JobCompleted => {}
            }
        }
        builder.events(events).build()
    }
}

fn next_attempt_at(
    attempt: u32,
    min_backoff: Duration,
    max_backoff: Duration,
    backoff_jitter_pct: u8,
) -> DateTime<Utc> {
    let backoff_ms = calculate_backoff(attempt, min_backoff, max_backoff, backoff_jitter_pct);
    time::now() + Duration::from_millis(backoff_ms)
}

fn calculate_backoff(
    attempt: u32,
    min_backoff: Duration,
    max_backoff: Duration,
    backoff_jitter_pct: u8,
) -> u64 {
    // Calculate base exponential backoff with overflow protection
    let safe_attempt = attempt.saturating_sub(1).min(30);
    let base_ms = min_backoff.as_millis() as u64;
    let max_ms = max_backoff.as_millis() as u64;

    // Use u64 arithmetic with saturation to prevent overflow
    let backoff = base_ms.saturating_mul(1u64 << safe_attempt).min(max_ms);

    // Apply jitter if configured
    if backoff_jitter_pct == 0 {
        backoff
    } else {
        apply_jitter(backoff, max_ms, backoff_jitter_pct)
    }
}

fn apply_jitter(backoff_ms: u64, max_ms: u64, backoff_jitter_pct: u8) -> u64 {
    let jitter_amount = backoff_ms * backoff_jitter_pct as u64 / 100;
    let jitter = rng().random_range(-(jitter_amount as i64)..=(jitter_amount as i64));

    let jittered = (backoff_ms as i64 + jitter).max(0) as u64;
    jittered.min(max_ms)
}

#[derive(Debug, Builder)]
pub struct NewJob {
    #[builder(setter(into))]
    pub(super) id: JobId,
    #[builder(default)]
    pub(super) unique_per_type: bool,
    pub(super) job_type: JobType,
    #[builder(setter(custom))]
    pub(super) config: serde_json::Value,
    #[builder(default)]
    pub(super) tracing_context: Option<TracingContext>,
}

impl NewJob {
    pub fn builder() -> NewJobBuilder {
        NewJobBuilder::default()
    }
}

impl NewJobBuilder {
    pub fn config<C: serde::Serialize>(&mut self, config: C) -> Result<&mut Self, JobError> {
        self.config =
            Some(serde_json::to_value(config).map_err(JobError::CouldNotSerializeConfig)?);
        Ok(self)
    }
}

impl IntoEvents<JobEvent> for NewJob {
    fn into_events(self) -> EntityEvents<JobEvent> {
        EntityEvents::init(
            self.id,
            [JobEvent::Initialized {
                id: self.id,
                job_type: self.job_type,
                config: self.config,
                tracing_context: self.tracing_context,
            }],
        )
    }
}
