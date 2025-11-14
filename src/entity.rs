//! Core job entities and events persisted in Postgres.

use chrono::{DateTime, Utc};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use std::{borrow::Cow, time::Duration};

use es_entity::{context::TracingContext, *};

use crate::{JobId, error::JobError};

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

    pub(crate) fn recent_failures_in_window(
        &self,
        window: Option<Duration>,
        now: DateTime<Utc>,
    ) -> u32 {
        let mut count = 0u32;
        for persisted in self.events.iter_persisted().rev() {
            if let Some(window) = window {
                let elapsed = match now.signed_duration_since(persisted.recorded_at).to_std() {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                if elapsed > window {
                    break;
                }
            }

            if matches!(
                persisted.event,
                JobEvent::ExecutionErrored { .. } | JobEvent::ExecutionAborted { .. }
            ) {
                count = count.saturating_add(1);
            }
        }
        count
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration as ChronoDuration, TimeZone};
    use es_entity::events::GenericEvent;
    use serde_json::json;

    fn timestamp() -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("valid timestamp")
    }

    fn job_with_history(job_id: JobId, events: Vec<(JobEvent, DateTime<Utc>)>) -> Job {
        let generic_events = events
            .into_iter()
            .enumerate()
            .map(|(idx, (event, recorded_at))| GenericEvent {
                entity_id: job_id.clone(),
                sequence: (idx as i32) + 1,
                event: serde_json::to_value(event).expect("serialize event"),
                context: None,
                recorded_at,
            })
            .collect::<Vec<_>>();

        EntityEvents::<JobEvent>::load_first::<Job>(generic_events).expect("load job")
    }

    #[test]
    fn recent_failures_respect_window() {
        let now = timestamp();
        let job_type = JobType::new("test");
        let job_id = JobId::new();
        let events = vec![
            (
                JobEvent::Initialized {
                    id: job_id.clone(),
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::hours(3),
            ),
            (
                JobEvent::ExecutionScheduled {
                    attempt: 1,
                    scheduled_at: now - ChronoDuration::hours(3),
                },
                now - ChronoDuration::hours(3),
            ),
            (
                JobEvent::ExecutionErrored {
                    error: "old".to_string(),
                },
                now - ChronoDuration::hours(2),
            ),
            (
                JobEvent::ExecutionScheduled {
                    attempt: 2,
                    scheduled_at: now - ChronoDuration::hours(2),
                },
                now - ChronoDuration::hours(2),
            ),
            (
                JobEvent::ExecutionErrored {
                    error: "recent".to_string(),
                },
                now - ChronoDuration::minutes(5),
            ),
        ];
        let job = job_with_history(job_id, events);
        assert_eq!(
            job.recent_failures_in_window(Some(Duration::from_secs(3600)), now),
            1
        );
        assert_eq!(job.recent_failures_in_window(None, now), 2);
    }

    #[test]
    fn recent_failures_include_aborted() {
        let now = timestamp();
        let job_type = JobType::new("test-abort");
        let job_id = JobId::new();
        let events = vec![
            (
                JobEvent::Initialized {
                    id: job_id.clone(),
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::hours(1),
            ),
            (
                JobEvent::ExecutionAborted {
                    reason: "shutdown".to_string(),
                },
                now - ChronoDuration::minutes(10),
            ),
        ];
        let job = job_with_history(job_id, events);
        assert_eq!(
            job.recent_failures_in_window(Some(Duration::from_secs(3600)), now),
            1
        );
    }
}
