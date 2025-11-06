//! Core job entities and events persisted in Postgres.

use chrono::{DateTime, Utc};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use std::borrow::Cow;
use uuid::Uuid;

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

#[derive(Clone, Copy, Eq, Hash, PartialEq, Debug, Serialize, Deserialize, sqlx::Type)]
#[sqlx(transparent)]
#[serde(transparent)]
/// Identifier representing the entity a job is scoped to.
pub struct JobEntityId(Uuid);

impl JobEntityId {
    pub const fn new(id: Uuid) -> Self {
        Self(id)
    }
}

impl From<Uuid> for JobEntityId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl From<JobEntityId> for Uuid {
    fn from(value: JobEntityId) -> Self {
        value.0
    }
}

impl std::fmt::Display for JobEntityId {
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
        entity_id: Option<JobEntityId>,
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
    entity_id: Option<JobEntityId>,
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

    pub fn entity_id(&self) -> Option<&JobEntityId> {
        self.entity_id.as_ref()
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
                    entity_id,
                    config,
                    ..
                } => {
                    builder = builder
                        .id(*id)
                        .job_type(job_type.clone())
                        .entity_id(*entity_id)
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
    #[builder(default)]
    pub(super) entity_id: Option<JobEntityId>,
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
                entity_id: self.entity_id,
                config: self.config,
                tracing_context: self.tracing_context,
            }],
        )
    }
}
