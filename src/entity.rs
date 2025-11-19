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
        now: DateTime<Utc>,
        attempt: u32,
        retry_policy: &RetryPolicy,
        error: String,
    ) -> Option<(DateTime<Utc>, u32)> {
        let max_attempts = retry_policy.max_attempts.unwrap_or(u32::MAX);
        let mut next_attempt = attempt.max(1).saturating_add(1);
        if self.should_reset_attempt_count(now, retry_policy) {
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

    fn should_reset_attempt_count(&self, now: DateTime<Utc>, retry_policy: &RetryPolicy) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    mod retry_scheduling {
        use super::*;
        use chrono::Duration as ChronoDuration;
        use es_entity::events::GenericEvent;
        use serde_json::json;
        use std::time::Duration;

        const TEST_MIN_BACKOFF_SECS: u64 = 30;
        const TEST_MAX_BACKOFF_SECS: u64 = 600;
        const TEST_RESET_MULTIPLE: u32 = 3;

        fn backoff_duration() -> ChronoDuration {
            ChronoDuration::seconds(TEST_MIN_BACKOFF_SECS as i64)
        }

        fn reset_threshold() -> ChronoDuration {
            backoff_duration() * TEST_RESET_MULTIPLE as i32
        }

        fn elapsed_just_under_reset() -> ChronoDuration {
            reset_threshold() - ChronoDuration::seconds(1)
        }

        fn elapsed_just_over_reset() -> ChronoDuration {
            reset_threshold() + ChronoDuration::seconds(1)
        }

        fn schedule_timestamps(
            now: DateTime<Utc>,
            elapsed_since_schedule: ChronoDuration,
        ) -> (DateTime<Utc>, DateTime<Utc>) {
            let scheduled_at = now - elapsed_since_schedule;
            let recorded_at = scheduled_at - backoff_duration();
            (scheduled_at, recorded_at)
        }

        fn job_with_history(job_id: JobId, events: Vec<(JobEvent, DateTime<Utc>)>) -> Job {
            let generic_events = events
                .into_iter()
                .enumerate()
                .map(|(idx, (event, recorded_at))| GenericEvent {
                    entity_id: job_id,
                    sequence: (idx as i32) + 1,
                    event: serde_json::to_value(event).expect("serialize event"),
                    context: None,
                    recorded_at,
                })
                .collect::<Vec<_>>();

            EntityEvents::<JobEvent>::load_first::<Job>(generic_events).expect("load job")
        }

        fn build_retry_policy(max_attempts: Option<u32>) -> RetryPolicy {
            RetryPolicy {
                max_attempts,
                min_backoff: Duration::from_secs(TEST_MIN_BACKOFF_SECS),
                max_backoff: Duration::from_secs(TEST_MAX_BACKOFF_SECS),
                backoff_jitter_pct: 0,
                attempt_reset_after_backoff_multiples: TEST_RESET_MULTIPLE,
            }
        }

        #[test]
        fn maybe_schedule_retry_emits_next_attempt_when_allowed() {
            let now = time::now();
            let job_type = JobType::new("retry-success");
            let job_id = JobId::new();
            let (latest_schedule_at, latest_recorded_at) =
                schedule_timestamps(now, elapsed_just_under_reset());
            let events = vec![
                (
                    JobEvent::Initialized {
                        id: job_id,
                        job_type: job_type.clone(),
                        config: json!({}),
                        tracing_context: None,
                    },
                    now - ChronoDuration::minutes(5),
                ),
                (
                    JobEvent::ExecutionScheduled {
                        attempt: 1,
                        scheduled_at: latest_schedule_at,
                    },
                    latest_recorded_at,
                ),
            ];
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(3));

            let (_, next_attempt) = job
                .maybe_schedule_retry(time::now(), 1, &retry_policy, "boom".to_string())
                .expect("retry expected");

            assert_eq!(next_attempt, 2);
            let events: Vec<_> = job.events.iter_all().collect();
            assert!(matches!(
                events[events.len() - 2],
                JobEvent::ExecutionErrored { .. }
            ));
            assert!(matches!(
                events.last(),
                Some(JobEvent::ExecutionScheduled { attempt: 2, .. })
            ));
        }

        #[test]
        fn maybe_schedule_retry_handles_zero_attempt_index() {
            let now = time::now();
            let job_type = JobType::new("retry-zero");
            let job_id = JobId::new();
            let events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::minutes(5),
            )];
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(3));

            let (_, next_attempt) = job
                .maybe_schedule_retry(time::now(), 0, &retry_policy, "boom".to_string())
                .expect("retry expected when attempt starts at zero");

            assert_eq!(next_attempt, 2);
            let events: Vec<_> = job.events.iter_all().collect();
            assert!(matches!(
                events[events.len() - 2],
                JobEvent::ExecutionErrored { .. }
            ));
            assert!(matches!(
                events.last(),
                Some(JobEvent::ExecutionScheduled { attempt: 2, .. })
            ));
        }

        #[test]
        fn maybe_schedule_retry_records_terminal_failure_when_limit_hit() {
            let now = time::now();
            let job_type = JobType::new("retry-terminal");
            let job_id = JobId::new();
            let (first_schedule_at, first_schedule_recorded_at) =
                schedule_timestamps(now, ChronoDuration::minutes(5));
            let first_error_at = first_schedule_at + ChronoDuration::seconds(1);
            let (second_schedule_at, second_schedule_recorded_at) =
                schedule_timestamps(now, elapsed_just_under_reset());
            let events = vec![
                (
                    JobEvent::Initialized {
                        id: job_id,
                        job_type: job_type.clone(),
                        config: json!({}),
                        tracing_context: None,
                    },
                    now - ChronoDuration::minutes(10),
                ),
                (
                    JobEvent::ExecutionScheduled {
                        attempt: 1,
                        scheduled_at: first_schedule_at,
                    },
                    first_schedule_recorded_at,
                ),
                (
                    JobEvent::ExecutionErrored {
                        error: "first".to_string(),
                    },
                    first_error_at,
                ),
                (
                    JobEvent::ExecutionScheduled {
                        attempt: 2,
                        scheduled_at: second_schedule_at,
                    },
                    second_schedule_recorded_at,
                ),
            ];
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(2));

            assert!(
                job.maybe_schedule_retry(time::now(), 2, &retry_policy, "boom".to_string())
                    .is_none(),
                "should stop retrying when attempts exhausted"
            );

            let events: Vec<_> = job.events.iter_all().collect();
            assert!(matches!(
                events[events.len() - 2],
                JobEvent::ExecutionErrored { .. }
            ));
            assert!(matches!(events.last(), Some(JobEvent::JobCompleted)));
        }

        #[test]
        fn maybe_schedule_retry_resets_attempt_after_healthy_gap() {
            let now = time::now();
            let job_type = JobType::new("retry-reset");
            let job_id = JobId::new();
            let (first_schedule_at, first_schedule_recorded_at) =
                schedule_timestamps(now, ChronoDuration::minutes(15));
            let first_error_at = first_schedule_at + ChronoDuration::seconds(1);
            let (healthy_gap_schedule_at, healthy_gap_schedule_recorded_at) =
                schedule_timestamps(now, elapsed_just_over_reset());
            let events = vec![
                (
                    JobEvent::Initialized {
                        id: job_id,
                        job_type: job_type.clone(),
                        config: json!({}),
                        tracing_context: None,
                    },
                    now - ChronoDuration::minutes(30),
                ),
                (
                    JobEvent::ExecutionScheduled {
                        attempt: 1,
                        scheduled_at: first_schedule_at,
                    },
                    first_schedule_recorded_at,
                ),
                (
                    JobEvent::ExecutionErrored {
                        error: "first".to_string(),
                    },
                    first_error_at,
                ),
                (
                    JobEvent::ExecutionScheduled {
                        attempt: 2,
                        scheduled_at: healthy_gap_schedule_at,
                    },
                    healthy_gap_schedule_recorded_at,
                ),
            ];
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(5));

            let (_, next_attempt) = job
                .maybe_schedule_retry(time::now(), 2, &retry_policy, "boom".to_string())
                .expect("retry expected");

            assert_eq!(
                next_attempt, 2,
                "a healthy gap should treat the next run as the second attempt"
            );
            let events: Vec<_> = job.events.iter_all().collect();
            assert!(matches!(
                events[events.len() - 2],
                JobEvent::ExecutionErrored { .. }
            ));
            assert!(matches!(
                events.last(),
                Some(JobEvent::ExecutionScheduled { attempt: 2, .. })
            ));
        }

        #[test]
        fn maybe_schedule_retry_allows_retry_when_next_attempt_hits_limit() {
            let now = time::now();
            let job_type = JobType::new("retry-max-boundary");
            let job_id = JobId::new();
            let (first_schedule_at, first_schedule_recorded_at) =
                schedule_timestamps(now, ChronoDuration::minutes(5));
            let first_error_at = first_schedule_at + ChronoDuration::seconds(1);
            let (latest_schedule_at, latest_schedule_recorded_at) =
                schedule_timestamps(now, elapsed_just_under_reset());
            let events = vec![
                (
                    JobEvent::Initialized {
                        id: job_id,
                        job_type: job_type.clone(),
                        config: json!({}),
                        tracing_context: None,
                    },
                    now - ChronoDuration::minutes(5),
                ),
                (
                    JobEvent::ExecutionScheduled {
                        attempt: 1,
                        scheduled_at: first_schedule_at,
                    },
                    first_schedule_recorded_at,
                ),
                (
                    JobEvent::ExecutionErrored {
                        error: "first".to_string(),
                    },
                    first_error_at,
                ),
                (
                    JobEvent::ExecutionScheduled {
                        attempt: 2,
                        scheduled_at: latest_schedule_at,
                    },
                    latest_schedule_recorded_at,
                ),
            ];
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(3));

            let (_, next_attempt) = job
                .maybe_schedule_retry(time::now(), 2, &retry_policy, "second failure".to_string())
                .expect("final retry should still be scheduled");

            assert_eq!(next_attempt, 3);
            let events: Vec<_> = job.events.iter_all().collect();
            assert!(matches!(
                events[events.len() - 2],
                JobEvent::ExecutionErrored { .. }
            ));
            assert!(matches!(
                events.last(),
                Some(JobEvent::ExecutionScheduled { attempt: 3, .. })
            ));
        }

        #[test]
        fn maybe_schedule_retry_resets_even_when_retry_limit_reached() {
            let now = time::now();
            let job_type = JobType::new("retry-reset-limit");
            let job_id = JobId::new();
            let (first_schedule_at, first_schedule_recorded_at) =
                schedule_timestamps(now, ChronoDuration::minutes(20));
            let first_error_at = first_schedule_at + ChronoDuration::seconds(1);
            let (second_schedule_at, second_schedule_recorded_at) =
                schedule_timestamps(now, ChronoDuration::minutes(10));
            let second_error_at = second_schedule_at + ChronoDuration::seconds(1);
            let (healthy_gap_schedule_at, healthy_gap_schedule_recorded_at) =
                schedule_timestamps(now, elapsed_just_over_reset());
            let events = vec![
                (
                    JobEvent::Initialized {
                        id: job_id,
                        job_type: job_type.clone(),
                        config: json!({}),
                        tracing_context: None,
                    },
                    now - ChronoDuration::hours(4),
                ),
                (
                    JobEvent::ExecutionScheduled {
                        attempt: 1,
                        scheduled_at: first_schedule_at,
                    },
                    first_schedule_recorded_at,
                ),
                (
                    JobEvent::ExecutionErrored {
                        error: "first".to_string(),
                    },
                    first_error_at,
                ),
                (
                    JobEvent::ExecutionScheduled {
                        attempt: 2,
                        scheduled_at: second_schedule_at,
                    },
                    second_schedule_recorded_at,
                ),
                (
                    JobEvent::ExecutionErrored {
                        error: "second".to_string(),
                    },
                    second_error_at,
                ),
                (
                    JobEvent::ExecutionScheduled {
                        attempt: 3,
                        scheduled_at: healthy_gap_schedule_at,
                    },
                    healthy_gap_schedule_recorded_at,
                ),
            ];
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(3));

            let (_, next_attempt) = job
                .maybe_schedule_retry(time::now(), 3, &retry_policy, "third failure".to_string())
                .expect("a healthy gap should reset attempt even at limit");

            assert_eq!(next_attempt, 2);
            let events: Vec<_> = job.events.iter_all().collect();
            assert!(matches!(
                events[events.len() - 2],
                JobEvent::ExecutionErrored { .. }
            ));
            assert!(matches!(
                events.last(),
                Some(JobEvent::ExecutionScheduled { attempt: 2, .. })
            ));
        }

        #[test]
        fn maybe_schedule_retry_with_unbounded_limit_handles_saturation() {
            let now = time::now();
            let job_type = JobType::new("retry-unbounded");
            let job_id = JobId::new();
            let attempt = u32::MAX;
            let (latest_schedule_at, latest_recorded_at) =
                schedule_timestamps(now, elapsed_just_under_reset());
            let events = vec![
                (
                    JobEvent::Initialized {
                        id: job_id,
                        job_type: job_type.clone(),
                        config: json!({}),
                        tracing_context: None,
                    },
                    now - ChronoDuration::minutes(1),
                ),
                (
                    JobEvent::ExecutionScheduled {
                        attempt,
                        scheduled_at: latest_schedule_at,
                    },
                    latest_recorded_at,
                ),
            ];
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(None);

            let (_, next_attempt) = job
                .maybe_schedule_retry(time::now(), attempt, &retry_policy, "overflow".to_string())
                .expect("unbounded retries should permit another schedule");

            assert_eq!(next_attempt, u32::MAX);
            let events: Vec<_> = job.events.iter_all().collect();
            assert!(matches!(
                events[events.len() - 2],
                JobEvent::ExecutionErrored { .. }
            ));
            match events.last() {
                Some(JobEvent::ExecutionScheduled { attempt, .. }) => {
                    assert_eq!(*attempt, u32::MAX);
                }
                other => panic!("expected execution scheduled event, got {other:?}"),
            }
        }
    }

    mod backoff {
        use std::time::Duration;

        const MAX_BACKOFF_MS: u64 = 60_000;

        fn assert_delay_exact(actual: u64, expected: u64) {
            assert_eq!(
                actual, expected,
                "Expected exactly {expected}ms, got {actual}ms"
            );
        }

        fn assert_delay_in_range(actual: u64, min: u64, max: u64) {
            assert!(
                actual >= min && actual <= max,
                "Expected delay in range {min}-{max}ms, got {actual}ms"
            );
        }

        #[test]
        fn exponential_backoff_grows_correctly() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(60);
            let expected_delays = [100, 200, 400, 800];

            for (attempt, &expected) in (1..=4).zip(&expected_delays) {
                let actual = super::calculate_backoff(attempt, min_backoff, max_backoff, 0);
                assert_delay_exact(actual, expected);
            }
        }

        #[test]
        fn zero_attempt_handled_correctly() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(60);
            let delay = super::calculate_backoff(0, min_backoff, max_backoff, 0);

            assert_delay_exact(delay, 100);
        }

        #[test]
        fn high_attempts_capped_at_max_backoff() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_millis(MAX_BACKOFF_MS);

            for high_attempt in [20, 31, 100, 1000, u32::MAX] {
                let delay = super::calculate_backoff(high_attempt, min_backoff, max_backoff, 0);
                assert_delay_exact(delay, MAX_BACKOFF_MS);
            }
        }

        #[test]
        fn attempts_capped_at_30() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_millis(MAX_BACKOFF_MS);
            let backoff31 = super::calculate_backoff(31, min_backoff, max_backoff, 0);
            let backoff100 = super::calculate_backoff(100, min_backoff, max_backoff, 0);

            assert_eq!(backoff31, backoff100, "Both should be capped at attempt 30");
            assert_eq!(backoff31, MAX_BACKOFF_MS);
            assert_eq!(backoff100, MAX_BACKOFF_MS);
        }

        #[test]
        fn jitter_adds_randomness() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(60);
            let delay = super::calculate_backoff(1, min_backoff, max_backoff, 20);

            assert_delay_in_range(delay, 80, 120);
        }

        #[test]
        fn jitter_never_negative() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(60);

            for _ in 0..10 {
                let delay = super::calculate_backoff(1, min_backoff, max_backoff, 20);
                assert!(delay <= 120, "Delay should be reasonable, got {delay}ms");
            }
        }

        #[test]
        fn deterministic_without_jitter() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(60);

            let backoff1 = super::calculate_backoff(5, min_backoff, max_backoff, 0);
            let backoff2 = super::calculate_backoff(5, min_backoff, max_backoff, 0);

            assert_eq!(
                backoff1, backoff2,
                "Backoffs should be identical without jitter"
            );
        }
    }
}
