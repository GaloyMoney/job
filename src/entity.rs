//! Core job entities and events persisted in Postgres.

use chrono::{DateTime, Utc};
use derive_builder::Builder;
use rand::{RngExt, rng};
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

    pub fn as_str(&self) -> &str {
        &self.0
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
    AttemptCounterReset,
}

#[derive(Debug, Clone)]
pub(crate) struct RetryPolicy {
    pub max_attempts: Option<u32>,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_jitter_pct: u8,
    pub attempt_reset_after_backoff_multiples: u32,
}

impl RetryPolicy {
    fn next_attempt_at(&self, now: DateTime<Utc>, attempt: u32) -> DateTime<Utc> {
        let backoff_ms = self.calculate_backoff(attempt);
        now + Duration::from_millis(backoff_ms)
    }

    fn calculate_backoff(&self, attempt: u32) -> u64 {
        // Calculate base exponential backoff with overflow protection
        let safe_attempt = attempt.saturating_sub(1).min(30);
        let base_ms = self.min_backoff.as_millis() as u64;
        let max_ms = self.max_backoff.as_millis() as u64;

        // Use u64 arithmetic with saturation to prevent overflow
        let backoff = base_ms.saturating_mul(1u64 << safe_attempt).min(max_ms);

        // Apply jitter if configured
        if self.backoff_jitter_pct == 0 {
            backoff
        } else {
            self.apply_jitter(backoff, max_ms)
        }
    }

    fn apply_jitter(&self, backoff_ms: u64, max_ms: u64) -> u64 {
        let jitter_amount = backoff_ms * self.backoff_jitter_pct as u64 / 100;
        let jitter = rng().random_range(-(jitter_amount as i64)..=(jitter_amount as i64));

        let jittered = (backoff_ms as i64 + jitter).max(0) as u64;
        jittered.min(max_ms)
    }

    fn should_reset_attempt_count(&self, now: DateTime<Utc>, window: RetryWindow) -> bool {
        let Some(elapsed_since_scheduled) = window.elapsed_since_retry_schedule(now) else {
            return false;
        };
        let Some(reset_threshold) = window
            .backoff_duration()
            .checked_mul(self.attempt_reset_after_backoff_multiples)
        else {
            return false;
        };
        elapsed_since_scheduled > reset_threshold
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RetryWindow {
    failure_recorded_at: DateTime<Utc>,
    retry_scheduled_at: DateTime<Utc>,
}

impl RetryWindow {
    fn new(failure_recorded_at: DateTime<Utc>, retry_scheduled_at: DateTime<Utc>) -> Option<Self> {
        if failure_recorded_at >= retry_scheduled_at {
            return None;
        }
        Some(Self {
            failure_recorded_at,
            retry_scheduled_at,
        })
    }

    fn backoff_duration(&self) -> Duration {
        self.retry_scheduled_at
            .signed_duration_since(self.failure_recorded_at)
            .to_std()
            .expect("retry window invariants ensure positive backoff duration")
    }

    fn elapsed_since_retry_schedule(&self, now: DateTime<Utc>) -> Option<Duration> {
        if now < self.retry_scheduled_at {
            return None;
        }
        now.signed_duration_since(self.retry_scheduled_at)
            .to_std()
            .ok()
    }
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

    pub(super) fn schedule_execution(&mut self, scheduled_at: DateTime<Utc>) {
        self.events.push(JobEvent::ExecutionScheduled {
            attempt: 1,
            scheduled_at,
        });
    }

    pub(super) fn reschedule_execution(&mut self, scheduled_at: DateTime<Utc>) {
        self.events.push(JobEvent::ExecutionCompleted);
        self.events.push(JobEvent::ExecutionScheduled {
            attempt: 1,
            scheduled_at,
        });
    }

    pub(super) fn abort_execution(
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

    pub(super) fn complete_job(&mut self) {
        self.events.push(JobEvent::ExecutionCompleted);
        self.events.push(JobEvent::JobCompleted);
    }

    pub(super) fn schedule_retry(
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

    pub(super) fn error_job(&mut self, error: String) {
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
        let mut current_attempt = attempt.max(1);
        if self
            .latest_retry_window()
            .map(|window| retry_policy.should_reset_attempt_count(now, window))
            .unwrap_or(false)
        {
            current_attempt = 1;
            self.events.push(JobEvent::AttemptCounterReset);
        }

        let next_attempt = current_attempt.saturating_add(1);
        let max_attempts = retry_policy.max_attempts.unwrap_or(u32::MAX);
        if next_attempt > max_attempts {
            self.error_job(error);
            return None;
        }

        let reschedule_at = retry_policy.next_attempt_at(now, current_attempt);
        self.schedule_retry(error, reschedule_at, next_attempt);
        Some((reschedule_at, next_attempt))
    }

    fn latest_retry_window(&self) -> Option<RetryWindow> {
        for persisted in self.events.iter_persisted().rev() {
            if let JobEvent::ExecutionScheduled {
                attempt,
                scheduled_at,
            } = &persisted.event
            {
                if *attempt > 1 {
                    return RetryWindow::new(persisted.recorded_at, *scheduled_at);
                } else {
                    // if the ExecutionScheduled event has attempt==1
                    // It means it was not scheduled for retry, but simply
                    // a normal scheduled run
                    return None;
                }
            }
        }
        None
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
                JobEvent::AttemptCounterReset => {}
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

    mod job {
        use super::*;
        use chrono::Duration as ChronoDuration;
        use es_entity::clock::Clock;
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

        fn schedule_window(
            now: DateTime<Utc>,
            elapsed_since_schedule: ChronoDuration,
        ) -> RetryWindow {
            let scheduled_at = now - elapsed_since_schedule;
            let recorded_at = scheduled_at - backoff_duration();
            RetryWindow::new(recorded_at, scheduled_at).expect("schedule window must be valid")
        }

        fn scheduled_event(attempt: u32, window: &RetryWindow) -> (JobEvent, DateTime<Utc>) {
            (
                JobEvent::ExecutionScheduled {
                    attempt,
                    scheduled_at: window.retry_scheduled_at,
                },
                window.failure_recorded_at,
            )
        }

        fn errored_event(error: &str, scheduled_at: DateTime<Utc>) -> (JobEvent, DateTime<Utc>) {
            (
                JobEvent::ExecutionErrored {
                    error: error.to_string(),
                },
                scheduled_at + ChronoDuration::seconds(1),
            )
        }

        fn push_attempt(
            history: &mut Vec<(JobEvent, DateTime<Utc>)>,
            attempt: u32,
            window: &RetryWindow,
            error_label: Option<&str>,
        ) {
            history.push(scheduled_event(attempt, window));
            if let Some(label) = error_label {
                history.push(errored_event(label, window.retry_scheduled_at));
            }
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
            let now = Clock::now();
            let job_type = JobType::new("retry-success");
            let job_id = JobId::new();
            let latest_window = schedule_window(now, elapsed_just_under_reset());
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::minutes(5),
            )];
            events.push(scheduled_event(1, &latest_window));
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(3));

            let (_, next_attempt) = job
                .maybe_schedule_retry(Clock::now(), 1, &retry_policy, "boom".to_string())
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
            let now = Clock::now();
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
                .maybe_schedule_retry(Clock::now(), 0, &retry_policy, "boom".to_string())
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
            let now = Clock::now();
            let job_type = JobType::new("retry-terminal");
            let job_id = JobId::new();
            let first_window = schedule_window(now, ChronoDuration::minutes(5));
            let second_window = schedule_window(now, elapsed_just_under_reset());
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::minutes(10),
            )];
            push_attempt(&mut events, 1, &first_window, Some("first"));
            events.push(scheduled_event(2, &second_window));
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(2));

            assert!(
                job.maybe_schedule_retry(Clock::now(), 2, &retry_policy, "boom".to_string())
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
            let now = Clock::now();
            let job_type = JobType::new("retry-reset");
            let job_id = JobId::new();
            let first_window = schedule_window(now, ChronoDuration::minutes(15));
            let healthy_window = schedule_window(now, elapsed_just_over_reset());
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::minutes(30),
            )];
            push_attempt(&mut events, 1, &first_window, Some("first"));
            events.push(scheduled_event(2, &healthy_window));
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(5));

            let (_, next_attempt) = job
                .maybe_schedule_retry(Clock::now(), 2, &retry_policy, "boom".to_string())
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
                events[events.len() - 3],
                JobEvent::AttemptCounterReset
            ));
            assert!(matches!(
                events.last(),
                Some(JobEvent::ExecutionScheduled { attempt: 2, .. })
            ));
        }

        #[test]
        fn maybe_schedule_retry_allows_retry_when_next_attempt_hits_limit() {
            let now = Clock::now();
            let job_type = JobType::new("retry-max-boundary");
            let job_id = JobId::new();
            let first_window = schedule_window(now, ChronoDuration::minutes(5));
            let latest_window = schedule_window(now, elapsed_just_under_reset());
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::minutes(5),
            )];
            push_attempt(&mut events, 1, &first_window, Some("first"));
            events.push(scheduled_event(2, &latest_window));
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(3));

            let (_, next_attempt) = job
                .maybe_schedule_retry(Clock::now(), 2, &retry_policy, "second failure".to_string())
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
            let now = Clock::now();
            let job_type = JobType::new("retry-reset-limit");
            let job_id = JobId::new();
            let first_window = schedule_window(now, ChronoDuration::minutes(20));
            let second_window = schedule_window(now, ChronoDuration::minutes(10));
            let healthy_window = schedule_window(now, elapsed_just_over_reset());
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::hours(4),
            )];
            push_attempt(&mut events, 1, &first_window, Some("first"));
            push_attempt(&mut events, 2, &second_window, Some("second"));
            events.push(scheduled_event(3, &healthy_window));
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(3));

            let (_, next_attempt) = job
                .maybe_schedule_retry(Clock::now(), 3, &retry_policy, "third failure".to_string())
                .expect("a healthy gap should reset attempt even at limit");

            assert_eq!(next_attempt, 2);
            let events: Vec<_> = job.events.iter_all().collect();
            assert!(matches!(
                events[events.len() - 2],
                JobEvent::ExecutionErrored { .. }
            ));
            assert!(matches!(
                events[events.len() - 3],
                JobEvent::AttemptCounterReset
            ));
            assert!(matches!(
                events.last(),
                Some(JobEvent::ExecutionScheduled { attempt: 2, .. })
            ));
        }

        #[test]
        fn maybe_schedule_retry_with_unbounded_limit_handles_saturation() {
            let now = Clock::now();
            let job_type = JobType::new("retry-unbounded");
            let job_id = JobId::new();
            let attempt = u32::MAX;
            let latest_window = schedule_window(now, elapsed_just_under_reset());
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::minutes(1),
            )];
            events.push(scheduled_event(attempt, &latest_window));
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(None);

            let (_, next_attempt) = job
                .maybe_schedule_retry(Clock::now(), attempt, &retry_policy, "overflow".to_string())
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

        #[test]
        fn latest_retry_window_returns_retry_window() {
            let now = Clock::now();
            let job_type = JobType::new("latest-retry");
            let job_id = JobId::new();
            let first_window = schedule_window(now, ChronoDuration::minutes(5));
            let retry_window_schedule = schedule_window(now, ChronoDuration::minutes(1));
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::minutes(20),
            )];
            push_attempt(&mut events, 1, &first_window, Some("first"));
            events.push(scheduled_event(2, &retry_window_schedule));
            let job = job_with_history(job_id, events);

            let window = job.latest_retry_window().expect("expected retry window");

            assert_eq!(
                window.failure_recorded_at,
                retry_window_schedule.failure_recorded_at
            );
            assert_eq!(
                window.retry_scheduled_at,
                retry_window_schedule.retry_scheduled_at
            );
        }

        #[test]
        fn latest_retry_window_returns_none_for_initial_attempt() {
            let now = Clock::now();
            let job_type = JobType::new("latest-no-retry");
            let job_id = JobId::new();
            let initial_window = schedule_window(now, ChronoDuration::minutes(2));
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::minutes(10),
            )];
            events.push(scheduled_event(1, &initial_window));
            let job = job_with_history(job_id, events);

            assert!(
                job.latest_retry_window().is_none(),
                "scheduling the first attempt is not a retry"
            );
        }

        #[test]
        fn latest_retry_window_ignores_older_retries_when_latest_is_initial() {
            let now = Clock::now();
            let job_type = JobType::new("latest-reset-to-initial");
            let job_id = JobId::new();
            let first_window = schedule_window(now, ChronoDuration::minutes(30));
            let retry_window_schedule = schedule_window(now, ChronoDuration::minutes(20));
            let final_window = schedule_window(now, ChronoDuration::minutes(10));
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                },
                now - ChronoDuration::hours(1),
            )];
            push_attempt(&mut events, 1, &first_window, Some("first"));
            push_attempt(&mut events, 2, &retry_window_schedule, Some("second"));
            events.push((
                JobEvent::ExecutionCompleted,
                final_window.failure_recorded_at - ChronoDuration::seconds(1),
            ));
            events.push(scheduled_event(1, &final_window));
            let job = job_with_history(job_id, events);

            assert!(
                job.latest_retry_window().is_none(),
                "the most recent schedule is not a retry"
            );
        }
    }

    mod retry_window {
        use super::*;
        use chrono::Duration as ChronoDuration;
        use es_entity::clock::Clock;
        use std::time::Duration;

        #[test]
        fn allows_future_windows() {
            let now = Clock::now();
            let future_failure = now + ChronoDuration::minutes(5);
            let further_future = future_failure + ChronoDuration::minutes(1);

            assert!(
                RetryWindow::new(future_failure, further_future).is_some(),
                "future timestamps should be accepted"
            );
        }

        #[test]
        fn rejects_inverted_ranges() {
            let now = Clock::now();
            let later_failure = now + ChronoDuration::minutes(1);
            let earlier_run = now;

            assert!(
                RetryWindow::new(later_failure, earlier_run).is_none(),
                "last failure must be before the planned run"
            );
        }

        #[test]
        fn reports_durations() {
            let now = Clock::now();
            let last_failure_at = now - ChronoDuration::minutes(30);
            let planned_run_at = now - ChronoDuration::minutes(20);
            let window =
                RetryWindow::new(last_failure_at, planned_run_at).expect("valid retry window");

            assert_eq!(
                window.backoff_duration(),
                Duration::from_secs(600),
                "planned run minus last failure should be 10 minutes"
            );
            assert_eq!(
                window.elapsed_since_retry_schedule(now),
                Some(Duration::from_secs(1_200)),
                "now minus planned run should be 20 minutes"
            );
        }

        #[test]
        fn elapsed_since_retry_schedule_requires_past() {
            let now = Clock::now();
            let last_failure_at = now - ChronoDuration::minutes(1);
            let planned_run_at = now + ChronoDuration::minutes(1);
            let window =
                RetryWindow::new(last_failure_at, planned_run_at).expect("valid retry window");

            assert!(
                window.elapsed_since_retry_schedule(now).is_none(),
                "elapsed duration only defined once the planned run is in the past"
            );
        }
    }

    mod retry_policy {
        use super::*;
        use chrono::Duration as ChronoDuration;
        use es_entity::clock::Clock;
        use std::time::Duration;

        const MAX_BACKOFF_MS: u64 = 60_000;

        fn retry_policy(
            min_backoff: Duration,
            max_backoff: Duration,
            jitter_pct: u8,
        ) -> RetryPolicy {
            retry_policy_with_reset(min_backoff, max_backoff, jitter_pct, 1)
        }

        fn retry_policy_with_reset(
            min_backoff: Duration,
            max_backoff: Duration,
            jitter_pct: u8,
            reset_multiples: u32,
        ) -> RetryPolicy {
            RetryPolicy {
                max_attempts: None,
                min_backoff,
                max_backoff,
                backoff_jitter_pct: jitter_pct,
                attempt_reset_after_backoff_multiples: reset_multiples,
            }
        }

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

        fn window_with_elapsed(
            backoff_secs: i64,
            elapsed_since_schedule_secs: i64,
        ) -> (RetryWindow, DateTime<Utc>) {
            let now = Clock::now();
            let scheduled_at = now - ChronoDuration::seconds(elapsed_since_schedule_secs);
            let failure_at = scheduled_at - ChronoDuration::seconds(backoff_secs);
            let window = RetryWindow::new(failure_at, scheduled_at).expect("valid window");
            (window, now)
        }

        #[test]
        fn exponential_backoff_grows_correctly() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(60);
            let expected_delays = [100, 200, 400, 800];
            let policy = retry_policy(min_backoff, max_backoff, 0);

            for (attempt, &expected) in (1..=4).zip(&expected_delays) {
                let actual = policy.calculate_backoff(attempt);
                assert_delay_exact(actual, expected);
            }
        }

        #[test]
        fn zero_attempt_handled_correctly() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(60);
            let policy = retry_policy(min_backoff, max_backoff, 0);
            let delay = policy.calculate_backoff(0);

            assert_delay_exact(delay, 100);
        }

        #[test]
        fn high_attempts_capped_at_max_backoff() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_millis(MAX_BACKOFF_MS);
            let policy = retry_policy(min_backoff, max_backoff, 0);

            for high_attempt in [20, 31, 100, 1000, u32::MAX] {
                let delay = policy.calculate_backoff(high_attempt);
                assert_delay_exact(delay, MAX_BACKOFF_MS);
            }
        }

        #[test]
        fn attempts_capped_at_30() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_millis(MAX_BACKOFF_MS);
            let policy = retry_policy(min_backoff, max_backoff, 0);
            let backoff31 = policy.calculate_backoff(31);
            let backoff100 = policy.calculate_backoff(100);

            assert_eq!(backoff31, backoff100, "Both should be capped at attempt 30");
            assert_eq!(backoff31, MAX_BACKOFF_MS);
            assert_eq!(backoff100, MAX_BACKOFF_MS);
        }

        #[test]
        fn jitter_adds_randomness() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(60);
            let policy = retry_policy(min_backoff, max_backoff, 20);
            let delay = policy.calculate_backoff(1);

            assert_delay_in_range(delay, 80, 120);
        }

        #[test]
        fn jitter_never_negative() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(60);
            let policy = retry_policy(min_backoff, max_backoff, 20);

            for _ in 0..10 {
                let delay = policy.calculate_backoff(1);
                assert!(delay <= 120, "Delay should be reasonable, got {delay}ms");
            }
        }

        #[test]
        fn deterministic_without_jitter() {
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(60);
            let policy = retry_policy(min_backoff, max_backoff, 0);
            let backoff1 = policy.calculate_backoff(5);
            let backoff2 = policy.calculate_backoff(5);

            assert_eq!(
                backoff1, backoff2,
                "Backoffs should be identical without jitter"
            );
        }

        #[test]
        fn should_reset_attempt_count_returns_false_when_schedule_in_future() {
            let policy =
                retry_policy_with_reset(Duration::from_secs(30), Duration::from_secs(600), 0, 3);
            let (window, now) = window_with_elapsed(30, -10);

            assert!(
                !policy.should_reset_attempt_count(now, window),
                "Should be false until the scheduled retry time has passed"
            );
        }

        #[test]
        fn should_reset_attempt_count_returns_false_when_within_threshold() {
            let policy =
                retry_policy_with_reset(Duration::from_secs(30), Duration::from_secs(600), 0, 3);
            let (window, now) = window_with_elapsed(30, 80);
            let reset = policy.should_reset_attempt_count(now, window);

            assert!(
                !reset,
                "Elapsed time should not reset attempts when below the threshold"
            );
        }

        #[test]
        fn should_reset_attempt_count_returns_true_when_past_threshold() {
            let policy =
                retry_policy_with_reset(Duration::from_secs(30), Duration::from_secs(600), 0, 3);
            let (window, now) = window_with_elapsed(30, 95);
            let reset = policy.should_reset_attempt_count(now, window);

            assert!(
                reset,
                "Elapsed time beyond the configured threshold should reset attempts"
            );
        }
    }
}
