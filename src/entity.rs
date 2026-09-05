//! Core job entities and events persisted in Postgres.

use chrono::{DateTime, Utc};
use derive_builder::Builder;
use rand::{RngExt, rng};
use serde::{Deserialize, Serialize};

use std::{borrow::Cow, time::Duration};

use es_entity::{context::TracingContext, *};

use crate::{
    JobId,
    error::JobError,
    outcome::{JobReturnValue, JobTerminalState},
};

#[derive(Clone, Eq, Hash, PartialEq, Debug, Serialize, Deserialize)]
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

    pub(crate) fn from_owned(job_type: String) -> Self {
        JobType(Cow::Owned(job_type))
    }
}

impl std::fmt::Display for JobType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Hand-written rather than `#[sqlx(transparent)]`, which delegates `Decode` to
// `Cow<'static, str>` and so demands a `'static` borrow no row can provide.
// Decoding owned lets queries name the type: `AS "job_type!: JobType"`.
impl sqlx::Type<sqlx::Postgres> for JobType {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <String as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl sqlx::postgres::PgHasArrayType for JobType {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for JobType {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for JobType {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        Ok(JobType(Cow::Owned(<String as sqlx::Decode<
            sqlx::Postgres,
        >>::decode(value)?)))
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
        queue_id: Option<String>,
        #[serde(default)]
        unique_key: Option<String>,
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
    ReturnValueUpdated {
        return_value: JobReturnValue,
    },
    JobCompleted,
    AttemptCounterReset,
    /// A batch's write was rescheduled because the *shared connection pool*
    /// was under pressure (`sqlx::Error::PoolTimedOut`), not because
    /// anything about this job was wrong. Deliberately distinct from
    /// `ExecutionErrored`: it must never drive the retry policy's attempt
    /// escalation (see `BatchDispatcher::reschedule_congestion`), so it
    /// can't reuse the event that means "this job's `RetryPolicy` just
    /// spent an attempt."
    CongestionRescheduled {
        error: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct RetryPolicy {
    pub max_attempts: Option<u32>,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_jitter_pct: u8,
    /// Retained for configuration compatibility with `RetrySettings`. The
    /// attempt counter no longer resets on elapsed time; it resets only on
    /// evidence of recovery (issue #163), so this value is currently unused
    /// by the policy.
    #[allow(dead_code)]
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
        // Overflow-safe jitter: compute the magnitude in u128 (clamped to max_ms
        // and i64's range), then add with saturation.
        let jitter_amount = ((backoff_ms as u128) * (self.backoff_jitter_pct as u128) / 100)
            .min(max_ms as u128)
            .min(i64::MAX as u128) as i64;
        let jitter = rng().random_range(-jitter_amount..=jitter_amount);

        backoff_ms.saturating_add_signed(jitter).min(max_ms)
    }

    /// Whether the accumulated attempt count may be forgiven.
    ///
    /// Forgiveness is reserved for a job that *demonstrably recovered* — ran
    /// to completion after its previous failure. It must never fire on the
    /// passage of clock time alone: under a manual/simulated domain clock an
    /// advance is not recovery, and under the real clock scheduler/poller
    /// latency is not recovery either (issue #163). `recovered` is supplied by
    /// [`Job::has_recovered_since_last_failure`].
    ///
    /// The `attempt_reset_after_backoff_multiples` setting is retained on
    /// `RetryPolicy`/`RetrySettings` for configuration compatibility, but
    /// recovery — not elapsed time — is what gates the reset.
    fn should_reset_attempt_count(&self, recovered: bool) -> bool {
        recovered
    }
}

// `RetryWindow` and `Job::latest_retry_window` are no longer used by the
// retry decision (which is now gated on recovery evidence, not elapsed time).
// They are retained for the existing window-construction tests.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RetryWindow {
    failure_recorded_at: DateTime<Utc>,
    retry_scheduled_at: DateTime<Utc>,
}

#[cfg(test)]
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
#[builder(pattern = "owned", build_fn(error = "EntityHydrationError"))]
/// Entity capturing immutable job metadata and lifecycle events.
pub struct Job {
    pub id: JobId,
    pub job_type: JobType,
    pub queue_id: Option<String>,
    pub unique_key: Option<String>,
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

    /// Determine the terminal state of a job, if it has reached one.
    ///
    /// - `Errored` if `JobCompleted` exists and the last execution event before it was
    ///   `ExecutionErrored`
    /// - `Completed` if `JobCompleted` exists (normal completion)
    /// - `None` if the job has not reached a terminal state
    pub fn terminal_state(&self) -> Option<JobTerminalState> {
        let mut rev = self.events.iter_all().rev();
        match rev.next()? {
            JobEvent::JobCompleted => match rev.next() {
                Some(JobEvent::ExecutionErrored { .. }) => Some(JobTerminalState::Errored),
                _ => Some(JobTerminalState::Completed),
            },
            _ => None,
        }
    }

    /// Returns the error string of the latest `ExecutionErrored` event, if any.
    ///
    /// Recorded on **every** failed attempt, not only at terminal: a retry with
    /// attempts remaining pushes `ExecutionErrored { error }` via
    /// `schedule_retry`, and the terminal path pushes it via `error_job` before
    /// `JobCompleted`. So this is `Some` for a mid-retry (still-running) job as
    /// well as a terminally errored one — the signal a wedged, never-terminal
    /// handler needs.
    pub(crate) fn last_error(&self) -> Option<&str> {
        self.events.iter_all().rev().find_map(|event| {
            if let JobEvent::ExecutionErrored { error } = event {
                Some(error.as_str())
            } else {
                None
            }
        })
    }

    /// Returns the raw return value attached to this job, if any.
    ///
    /// Scans for the latest `ReturnValueUpdated` event (last write wins).
    pub(crate) fn raw_return_value(&self) -> Option<&JobReturnValue> {
        self.events.iter_all().rev().find_map(|event| {
            if let JobEvent::ReturnValueUpdated { return_value } = event {
                Some(return_value)
            } else {
                None
            }
        })
    }

    /// Deserialize the return value into a typed struct.
    pub fn return_value<T: serde::de::DeserializeOwned>(
        &self,
    ) -> Result<Option<T>, serde_json::Error> {
        match self.raw_return_value() {
            Some(r) => r.deserialize().map(Some),
            None => Ok(None),
        }
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

    /// Reschedule after a pool-congestion classification
    /// (`Finalizer::maybe_reclassify`, `finalizer.rs`): same shape
    /// as [`Self::schedule_retry`]
    /// but at the SAME `attempt` rather than the next one, and via
    /// `CongestionRescheduled` rather than `ExecutionErrored` -- congestion
    /// carries no evidence of a broken job, so it must not spend a
    /// `RetryPolicy` attempt. Attempt staying unchanged is load-bearing
    /// beyond the retry policy too: the poller's retry-solo rule
    /// (`poller.rs`, `attempt > 1` is dispatched alone, never batched) reads
    /// this same value, so a job that keeps its attempt through a
    /// congestion reschedule stays eligible for batching on its next claim
    /// -- see `congestion_reschedule_keeps_job_batchable` in
    /// `tests/batched_job.rs`.
    ///
    /// Returns the new consecutive-congestion streak (see
    /// [`Self::consecutive_congestion_reschedules`]) for the caller's
    /// stuck-in-congestion WARN.
    pub(super) fn reschedule_congestion(
        &mut self,
        error: String,
        scheduled_at: DateTime<Utc>,
        attempt: u32,
    ) -> u32 {
        self.events.push(JobEvent::CongestionRescheduled { error });
        self.events.push(JobEvent::ExecutionScheduled {
            attempt,
            scheduled_at,
        });
        self.consecutive_congestion_reschedules()
    }

    /// How many times in a row this job has just been rescheduled for pool
    /// congestion, most-recent-first -- including the reschedule just
    /// pushed by [`Self::reschedule_congestion`] above, which is why this
    /// reads `iter_all` rather than `iter_persisted`: the streak needs to
    /// see this call's own not-yet-committed push, or the very first
    /// congestion event of a new streak would report 0.
    ///
    /// Each congestion cycle is the pair `[CongestionRescheduled,
    /// ExecutionScheduled]` (see `reschedule_congestion`). Walking backward
    /// from the latest event, this consumes exactly one such pair per
    /// iteration and stops at the first `ExecutionScheduled` NOT preceded
    /// by `CongestionRescheduled` -- i.e. the first ordinary retry, rescue
    /// reschedule, or fresh dispatch, whichever ends the streak.
    fn consecutive_congestion_reschedules(&self) -> u32 {
        let mut count = 0;
        let mut events = self.events.iter_all().rev();
        while let Some(JobEvent::ExecutionScheduled { .. }) = events.next() {
            match events.next() {
                Some(JobEvent::CongestionRescheduled { .. }) => count += 1,
                _ => break,
            }
        }
        count
    }

    /// Attach or overwrite the return value for this job.
    ///
    /// Returns [`Idempotent::AlreadyApplied`] when the new value is identical
    /// to the current one, allowing callers to skip the DB round-trip.
    pub(crate) fn update_return_value(
        &mut self,
        return_value: JobReturnValue,
    ) -> es_entity::Idempotent<()> {
        idempotency_guard!(
            self.events.iter_all().rev(),
            already_applied: JobEvent::ReturnValueUpdated { return_value: existing } if *existing.as_value() == *return_value.as_value()
        );
        self.events
            .push(JobEvent::ReturnValueUpdated { return_value });
        es_entity::Idempotent::Executed(())
    }

    pub(super) fn maybe_schedule_retry(
        &mut self,
        now: DateTime<Utc>,
        attempt: u32,
        retry_policy: &RetryPolicy,
        error: String,
    ) -> Option<(DateTime<Utc>, u32)> {
        let mut current_attempt = attempt.max(1);
        if retry_policy.should_reset_attempt_count(self.has_recovered_since_last_failure()) {
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

    #[cfg(test)]
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

    /// Whether the job ran to completion at any point after its most recent
    /// failure — the only trustworthy evidence of recovery.
    ///
    /// A job that recovered emits an `ExecutionCompleted` (via
    /// [`Self::reschedule_execution`] for a self-rescheduling run, or
    /// [`Self::complete_job`] for a finishing one). Because that event is only
    /// ever produced by a run that *finished*, its presence after the last
    /// `ExecutionErrored` proves the job worked again between failures —
    /// independent of any clock. A deterministically failing job never
    /// produces one, so its attempt counter climbs to `max_attempts` and the
    /// job terminates (issue #163).
    ///
    /// Note this is deliberately *not* a wall-clock or elapsed-time probe: the
    /// retry window's timestamps and the `now` passed to
    /// [`Self::maybe_schedule_retry`] all live on the injectable domain clock,
    /// which a manual/simulated clock can advance without any run occurring.
    fn has_recovered_since_last_failure(&self) -> bool {
        for event in self.events.iter_all().rev() {
            match event {
                JobEvent::ExecutionErrored { .. } => return false,
                JobEvent::ExecutionCompleted => return true,
                _ => {}
            }
        }
        false
    }
}

impl TryFromEvents<JobEvent> for Job {
    fn try_from_events(events: EntityEvents<JobEvent>) -> Result<Self, EntityHydrationError> {
        let mut builder = JobBuilder::default();
        for event in events.iter_all() {
            match event {
                JobEvent::Initialized {
                    id,
                    job_type,
                    config,
                    queue_id,
                    unique_key,
                    ..
                } => {
                    builder = builder
                        .id(*id)
                        .job_type(job_type.clone())
                        .queue_id(queue_id.clone())
                        .unique_key(unique_key.clone())
                        .config(config.clone())
                }
                JobEvent::ExecutionScheduled { .. } => {}
                JobEvent::ExecutionCompleted => {}
                JobEvent::ExecutionAborted { .. } => {}
                JobEvent::ExecutionErrored { .. } => {}
                JobEvent::ReturnValueUpdated { .. } => {}
                JobEvent::JobCompleted => {}
                JobEvent::AttemptCounterReset => {}
                JobEvent::CongestionRescheduled { .. } => {}
            }
        }
        builder.events(events).build()
    }
}

#[derive(Debug, Builder)]
pub struct NewJob {
    #[builder(setter(into))]
    pub(super) id: JobId,
    pub(super) job_type: JobType,
    #[builder(setter(custom))]
    pub(super) config: serde_json::Value,
    #[builder(default)]
    pub(super) tracing_context: Option<TracingContext>,
    #[builder(default)]
    pub(super) queue_id: Option<String>,
    #[builder(setter(into, strip_option), default)]
    pub(super) unique_key: Option<String>,
    #[builder(default)]
    pub(super) resident: bool,
    /// The job's first `execute_at`. `into_events` appends
    /// `ExecutionScheduled { attempt: 1, scheduled_at }` to the initial
    /// event batch alongside `Initialized`, so a caller never needs a
    /// second `update_in_op` round trip just to record it.
    pub(super) schedule_at: DateTime<Utc>,
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
            [
                JobEvent::Initialized {
                    id: self.id,
                    job_type: self.job_type,
                    config: self.config,
                    tracing_context: self.tracing_context,
                    queue_id: self.queue_id,
                    unique_key: self.unique_key,
                },
                JobEvent::ExecutionScheduled {
                    attempt: 1,
                    scheduled_at: self.schedule_at,
                },
            ],
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
                    forgettable_payload: None,
                })
                .collect::<Vec<_>>();

            EntityEvents::<JobEvent>::load_first::<Job>(generic_events)
                .expect("load job")
                .expect("no events")
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
                    queue_id: None,
                    unique_key: None,
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
                    queue_id: None,
                    unique_key: None,
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
                    queue_id: None,
                    unique_key: None,
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
        fn maybe_schedule_retry_resets_attempt_after_recovery() {
            let now = Clock::now();
            let job_type = JobType::new("retry-reset");
            let job_id = JobId::new();
            let first_window = schedule_window(now, ChronoDuration::minutes(15));
            let retry_window = schedule_window(now, ChronoDuration::minutes(1));
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                    queue_id: None,
                    unique_key: None,
                },
                now - ChronoDuration::minutes(30),
            )];
            push_attempt(&mut events, 1, &first_window, Some("first"));
            // The job recovered: a run completed after the failure. Only this
            // — not the mere passage of clock time — may forgive the attempt
            // count (issue #163).
            events.push((
                JobEvent::ExecutionCompleted,
                now - ChronoDuration::minutes(2),
            ));
            events.push(scheduled_event(2, &retry_window));
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(5));

            let (_, next_attempt) = job
                .maybe_schedule_retry(Clock::now(), 2, &retry_policy, "boom".to_string())
                .expect("retry expected");

            assert_eq!(
                next_attempt, 2,
                "a completed run since the last failure should forgive the attempt count"
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
                    queue_id: None,
                    unique_key: None,
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
            let retry_window = schedule_window(now, ChronoDuration::minutes(1));
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                    queue_id: None,
                    unique_key: None,
                },
                now - ChronoDuration::hours(4),
            )];
            push_attempt(&mut events, 1, &first_window, Some("first"));
            push_attempt(&mut events, 2, &second_window, Some("second"));
            // A genuine recovery (a completed run) after the second failure is
            // what forgives the attempt count even at the limit — not the
            // elapsed time that used to drive this (issue #163).
            events.push((
                JobEvent::ExecutionCompleted,
                now - ChronoDuration::minutes(2),
            ));
            events.push(scheduled_event(3, &retry_window));
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(3));

            let (_, next_attempt) = job
                .maybe_schedule_retry(Clock::now(), 3, &retry_policy, "third failure".to_string())
                .expect("a completed run should reset attempt even at limit");

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
                    queue_id: None,
                    unique_key: None,
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
                    queue_id: None,
                    unique_key: None,
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
                    queue_id: None,
                    unique_key: None,
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
                    queue_id: None,
                    unique_key: None,
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

        // ---- issue #163 regression tests -------------------------------
        // A deterministically failing job under an advancing (simulated or
        // manual) domain clock: each failure is followed by a multi-hour
        // clock jump, so the old elapsed-time check cleared the reset
        // threshold on every pass without any successful run in between.

        /// Load a job from already-serialized events, so a growing history
        /// can be rebuilt into a fresh entity every dispatch cycle (events
        /// are `Clone` as `serde_json::Value`; `JobEvent` itself is not).
        fn job_from_raw_history(job_id: JobId, raw: &[(serde_json::Value, DateTime<Utc>)]) -> Job {
            let generic_events = raw
                .iter()
                .enumerate()
                .map(|(idx, (event, recorded_at))| GenericEvent {
                    entity_id: job_id,
                    sequence: (idx as i32) + 1,
                    event: event.clone(),
                    context: None,
                    recorded_at: *recorded_at,
                    forgettable_payload: None,
                })
                .collect::<Vec<_>>();

            EntityEvents::<JobEvent>::load_first::<Job>(generic_events)
                .expect("load job")
                .expect("no events")
        }

        fn raw_event(event: JobEvent) -> serde_json::Value {
            serde_json::to_value(event).expect("serialize event")
        }

        #[test]
        fn maybe_schedule_retry_terminates_at_max_attempts_despite_domain_clock_jumps() {
            let retry_policy = build_retry_policy(Some(4));
            let job_type = JobType::new("issue-163-loop");
            let job_id = JobId::new();
            let mut now = Clock::now();
            let mut history: Vec<(serde_json::Value, DateTime<Utc>)> = vec![
                (
                    raw_event(JobEvent::Initialized {
                        id: job_id,
                        job_type: job_type.clone(),
                        config: json!({}),
                        tracing_context: None,
                        queue_id: None,
                        unique_key: None,
                    }),
                    now - ChronoDuration::minutes(2),
                ),
                (
                    raw_event(JobEvent::ExecutionScheduled {
                        attempt: 1,
                        scheduled_at: now - ChronoDuration::minutes(1),
                    }),
                    now - ChronoDuration::minutes(2),
                ),
            ];
            let mut attempt = 1u32;
            let mut terminated = false;
            // Reload the entity from persisted history every cycle, exactly
            // like the real dispatch loop (each failure is a separate
            // transaction), then jump the domain clock forward by hours.
            for _ in 0..12 {
                let mut job = job_from_raw_history(job_id, &history);
                match job.maybe_schedule_retry(now, attempt, &retry_policy, "boom".to_string()) {
                    Some((reschedule_at, next_attempt)) => {
                        history.push((
                            raw_event(JobEvent::ExecutionErrored {
                                error: "boom".to_string(),
                            }),
                            now,
                        ));
                        history.push((
                            raw_event(JobEvent::ExecutionScheduled {
                                attempt: next_attempt,
                                scheduled_at: reschedule_at,
                            }),
                            now,
                        ));
                        attempt = next_attempt;
                        now += ChronoDuration::hours(6);
                    }
                    None => {
                        terminated = true;
                        break;
                    }
                }
            }
            assert!(
                terminated,
                "a permanently failing job must terminate at max_attempts; \
                 domain-clock advancement alone must not reset the attempt counter (issue #163)"
            );
        }

        #[test]
        fn maybe_schedule_retry_does_not_reset_when_only_the_clock_advanced() {
            let now = Clock::now();
            let job_type = JobType::new("issue-163-jump");
            let job_id = JobId::new();
            let first_window = schedule_window(now, ChronoDuration::hours(25));
            // The retry was scheduled 24h of domain-clock time ago; no
            // successful run happened in between.
            let jumped_window = schedule_window(now, ChronoDuration::hours(24));
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: job_type.clone(),
                    config: json!({}),
                    tracing_context: None,
                    queue_id: None,
                    unique_key: None,
                },
                now - ChronoDuration::hours(26),
            )];
            push_attempt(&mut events, 1, &first_window, Some("first"));
            events.push(scheduled_event(2, &jumped_window));
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy(Some(5));

            let (_, next_attempt) = job
                .maybe_schedule_retry(Clock::now(), 2, &retry_policy, "boom".to_string())
                .expect("retry expected");

            assert_eq!(
                next_attempt, 3,
                "a domain-clock jump is not recovery evidence; the attempt counter must climb (issue #163)"
            );
            let events: Vec<_> = job.events.iter_all().collect();
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, JobEvent::AttemptCounterReset)),
                "no reset may fire when only the clock advanced"
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
        fn should_reset_attempt_count_requires_recovery() {
            let policy =
                retry_policy_with_reset(Duration::from_secs(30), Duration::from_secs(600), 0, 3);

            assert!(
                !policy.should_reset_attempt_count(false),
                "no completed run => no reset, regardless of elapsed time (issue #163)"
            );
            assert!(
                policy.should_reset_attempt_count(true),
                "a completed run after the last failure is recovery evidence"
            );
        }

        #[test]
        fn apply_jitter_survives_huge_backoff_without_overflow() {
            // The result must stay within [0, max_ms] and never panic, even
            // with a huge max_backoff, across many jitter draws.
            let huge = u64::MAX; // > i64::MAX
            let policy = RetryPolicy {
                max_attempts: None,
                min_backoff: Duration::from_millis(huge),
                max_backoff: Duration::from_millis(huge),
                backoff_jitter_pct: u8::MAX,
                attempt_reset_after_backoff_multiples: 1,
            };
            for _ in 0..256 {
                let backoff = policy.calculate_backoff(1);
                assert!(backoff <= huge, "backoff {backoff} exceeded max_ms {huge}");
            }
            // Exercises the i64::MAX backoff boundary directly without panicking.
            // (The result is a u64, so a `<= u64::MAX` assertion would be
            // vacuously true; calling it is the actual check.)
            let _ = policy.apply_jitter(i64::MAX as u64, u64::MAX);
        }
    }
}
