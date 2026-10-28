//! Core job entities and events persisted in Postgres.

use chrono::{DateTime, Utc};
use rand::{RngExt, rng};
use serde::{Deserialize, Serialize};

use std::{borrow::Cow, time::Duration};

use es_entity::{context::TracingContext, *};

use crate::{
    JobId,
    error::{CouldNotSerialize, JobFault},
    outcome::{JobReturnValue, JobTerminalState},
    runner::Backoff,
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
        lane: FailureLane,
    },
    ReturnValueUpdated {
        return_value: JobReturnValue,
    },
    JobCompleted,
    /// The job itself concluded it cannot succeed (`JobCompletion::Fail`).
    /// The only failure-terminal event.
    JobFailed {
        reason: String,
    },
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

/// The two lanes the job crate acts on. errlanes' `Lane` has no serde
/// derive and carries Rejected/Denied, which `dispatch_job` has already
/// narrowed away by the time a failure reaches the entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureLane {
    Transient,
    Fatal,
}

impl From<&JobFault> for FailureLane {
    fn from(failure: &JobFault) -> Self {
        if failure.is_fatal() {
            FailureLane::Fatal
        } else {
            FailureLane::Transient
        }
    }
}

/// Which part of the retry curve a failed job's next attempt was scheduled
/// on. The transient band logs at `WARN`, the fatal band at `ERROR`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryBand {
    Transient,
    Fatal,
}

impl From<RetryBand> for FailureLane {
    fn from(band: RetryBand) -> Self {
        match band {
            RetryBand::Transient => FailureLane::Transient,
            RetryBand::Fatal => FailureLane::Fatal,
        }
    }
}

impl From<FailureLane> for RetryBand {
    fn from(lane: FailureLane) -> Self {
        match lane {
            FailureLane::Transient => RetryBand::Transient,
            FailureLane::Fatal => RetryBand::Fatal,
        }
    }
}

impl RetryBand {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RetryBand::Transient => "transient",
            RetryBand::Fatal => "fatal",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RetryPolicy {
    pub backoff: Backoff,
    pub attempt_reset_after_healthy_run: Option<Duration>,
}

/// What `Job::maybe_schedule_retry` decided for one failed attempt.
pub(crate) struct ScheduledRetry {
    pub at: DateTime<Utc>,
    pub next_attempt: u32,
    pub band: RetryBand,
    pub fatal_streak: u32,
}

fn doubled_ms(base_ms: u64, exponent: u32) -> u64 {
    if exponent >= 63 {
        u64::MAX
    } else {
        base_ms.saturating_mul(1u64 << exponent)
    }
}

impl RetryPolicy {
    /// The delay and band of the retry that follows a failed attempt
    /// `attempt` (>= 1) of the given `lane`. `fatal_streak` is the number of
    /// consecutive trailing `Fatal` failures INCLUDING this one.
    pub(crate) fn next_retry(
        &self,
        lane: FailureLane,
        attempt: u32,
        fatal_streak: u32,
    ) -> (Duration, RetryBand) {
        let fatal_min_ms = self.backoff.fatal_min().as_millis() as u64;
        let max_ms = self.backoff.max().as_millis() as u64;
        let transient_ms = doubled_ms(
            self.backoff.transient_min().as_millis() as u64,
            attempt.saturating_sub(1),
        );

        let (delay_ms, band) = match lane {
            FailureLane::Transient if transient_ms < fatal_min_ms => {
                (transient_ms, RetryBand::Transient)
            }
            FailureLane::Transient => (transient_ms.min(max_ms), RetryBand::Fatal),
            FailureLane::Fatal => {
                let fatal_ms = doubled_ms(fatal_min_ms, fatal_streak.saturating_sub(1));
                (transient_ms.max(fatal_ms).min(max_ms), RetryBand::Fatal)
            }
        };

        let delay_ms = if self.backoff.jitter_pct() == 0 {
            delay_ms
        } else {
            self.apply_jitter(delay_ms, max_ms)
        };
        (Duration::from_millis(delay_ms), band)
    }

    fn apply_jitter(&self, backoff_ms: u64, max_ms: u64) -> u64 {
        // Overflow-safe jitter: compute the magnitude in u128 (clamped to max_ms
        // and i64's range), then add with saturation.
        let jitter_amount = ((backoff_ms as u128) * (self.backoff.jitter_pct() as u128) / 100)
            .min(max_ms as u128)
            .min(i64::MAX as u128) as i64;
        let jitter = rng().random_range(-jitter_amount..=jitter_amount);

        backoff_ms.saturating_add_signed(jitter).min(max_ms)
    }

    /// Did this execution run long enough to be evidence the job had
    /// recovered?
    ///
    /// `run_duration` is measured on a monotonic `std::time::Instant` bracketed
    /// around the run itself (see `JobDispatcher::execute` and
    /// `BatchDispatcher::execute_batch`), NOT on the injectable domain clock:
    /// advancing an application clock must never forgive an attempt, and
    /// neither must poll latency. Crash repair and liveness make the same
    /// choice for the same reason -- see the module doc on
    /// `crate::poller::recovery`.
    fn should_reset_attempt_count(&self, run_duration: Duration) -> bool {
        self.attempt_reset_after_healthy_run
            .is_some_and(|threshold| run_duration >= threshold)
    }
}

#[derive(EsEntity)]
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
    /// - `Completed` if the last event is `JobCompleted`
    /// - `Failed` if the last event is `JobFailed`
    /// - `None` if the job has not reached a terminal state
    pub fn terminal_state(&self) -> Option<JobTerminalState> {
        match self.events.iter_all().next_back()? {
            JobEvent::JobCompleted => Some(JobTerminalState::Completed),
            JobEvent::JobFailed { .. } => Some(JobTerminalState::Failed),
            _ => None,
        }
    }

    /// The reason the job gave in `JobCompletion::Fail`, if it failed.
    pub(crate) fn failure_reason(&self) -> Option<&str> {
        match self.events.iter_all().next_back()? {
            JobEvent::JobFailed { reason } => Some(reason.as_str()),
            _ => None,
        }
    }

    /// Returns the error string of the latest `ExecutionErrored` event, if any.
    ///
    /// Recorded on **every** failed attempt, so this is `Some` for a
    /// mid-retry (still-running) job -- the signal a wedged handler needs.
    pub(crate) fn last_error(&self) -> Option<&str> {
        self.events.iter_all().rev().find_map(|event| {
            if let JobEvent::ExecutionErrored { error, .. } = event {
                Some(error.as_str())
            } else {
                None
            }
        })
    }

    /// The band the job's pending retry sits in and the error that put it
    /// there, derived from events alone: the last `ExecutionErrored`, if it
    /// is the most recent execution-level event.
    pub(crate) fn pending_retry(&self) -> (RetryBand, Option<String>) {
        for event in self.events.iter_all().rev() {
            match event {
                JobEvent::ExecutionScheduled { .. }
                | JobEvent::CongestionRescheduled { .. }
                | JobEvent::ReturnValueUpdated { .. } => {}
                JobEvent::ExecutionErrored { error, lane } => {
                    return (RetryBand::from(*lane), Some(error.clone()));
                }
                _ => break,
            }
        }
        (RetryBand::Transient, None)
    }

    /// Consecutive trailing `ExecutionErrored { lane: Fatal }` events,
    /// counting from the most recent one. `CongestionRescheduled` and
    /// `ExecutionAborted` are transparent (neither is evidence about the
    /// job), as are the bookkeeping events that accompany a retry. Stops at
    /// `ExecutionCompleted`, `JobCompleted`, `JobFailed`,
    /// `AttemptCounterReset`, or a `Transient` error. 0 when the last error
    /// was not Fatal.
    pub(super) fn fatal_streak(&self) -> u32 {
        let mut streak = 0;
        for event in self.events.iter_all().rev() {
            match event {
                JobEvent::ExecutionErrored {
                    lane: FailureLane::Fatal,
                    ..
                } => streak += 1,
                JobEvent::ExecutionErrored {
                    lane: FailureLane::Transient,
                    ..
                }
                | JobEvent::ExecutionCompleted
                | JobEvent::JobCompleted
                | JobEvent::JobFailed { .. }
                | JobEvent::AttemptCounterReset
                | JobEvent::Initialized { .. } => break,
                JobEvent::ExecutionScheduled { .. }
                | JobEvent::ExecutionAborted { .. }
                | JobEvent::CongestionRescheduled { .. }
                | JobEvent::ReturnValueUpdated { .. } => {}
            }
        }
        streak
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
        lane: FailureLane,
        scheduled_at: DateTime<Utc>,
        attempt: u32,
    ) {
        self.events.push(JobEvent::ExecutionErrored { error, lane });
        self.events.push(JobEvent::ExecutionScheduled {
            attempt,
            scheduled_at,
        });
    }

    pub(super) fn fail_job(&mut self, reason: String) {
        self.events.push(JobEvent::ExecutionCompleted);
        self.events.push(JobEvent::JobFailed { reason });
    }

    /// Reschedule after a pool-congestion classification
    /// (`JobFault::is_congestion`, `finalizer.rs`): same shape
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

    /// Schedule the retry that follows a failed attempt. `failure` is the
    /// classified [`JobFault`] that ended it (a `Denied` was already narrowed
    /// to `Fatal(Denied)` at the job boundary); its lane picks where the
    /// retry enters the backoff curve. Always schedules: nothing here ends a
    /// job.
    pub(super) fn maybe_schedule_retry(
        &mut self,
        now: DateTime<Utc>,
        attempt: u32,
        run_duration: Duration,
        retry_policy: &RetryPolicy,
        failure: &JobFault,
    ) -> ScheduledRetry {
        let mut current_attempt = attempt.max(1);
        // Only a counter that has actually accumulated can be forgiven; every
        // completion path already sets `attempt_index = 1` in its own
        // transaction, so emitting the event at attempt 1 would make
        // `AttemptCounterReset` useless as a diagnostic.
        if current_attempt > 1 && retry_policy.should_reset_attempt_count(run_duration) {
            current_attempt = 1;
            self.events.push(JobEvent::AttemptCounterReset);
        }

        let next_attempt = current_attempt.saturating_add(1);
        let prior_streak = self.fatal_streak();
        let (delay, band) = retry_policy.next_retry(
            FailureLane::from(failure),
            current_attempt,
            prior_streak.saturating_add(1),
        );
        let at = now + delay;
        self.schedule_retry(failure.message(), FailureLane::from(band), at, next_attempt);
        ScheduledRetry {
            at,
            next_attempt,
            band,
            fatal_streak: self.fatal_streak(),
        }
    }
}

impl TryFromEvents<JobEvent> for Job {
    fn try_from_events(events: EntityEvents<JobEvent>) -> Result<Self, EntityHydrationError> {
        let mut h = Job::hydrator();
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
                    h.id(*id)
                        .job_type(job_type.clone())
                        .maybe_queue_id(queue_id.clone())
                        .maybe_unique_key(unique_key.clone())
                        .config(config.clone());
                }
                JobEvent::ExecutionScheduled { .. } => {}
                JobEvent::ExecutionCompleted => {}
                JobEvent::ExecutionAborted { .. } => {}
                JobEvent::ExecutionErrored { .. } => {}
                JobEvent::ReturnValueUpdated { .. } => {}
                JobEvent::JobCompleted => {}
                JobEvent::JobFailed { .. } => {}
                JobEvent::AttemptCounterReset => {}
                JobEvent::CongestionRescheduled { .. } => {}
            }
        }
        h.build(events)
    }
}

#[derive(Debug)]
pub struct NewJob {
    pub(super) id: JobId,
    pub(super) job_type: JobType,
    pub(super) config: serde_json::Value,
    pub(super) tracing_context: Option<TracingContext>,
    pub(super) queue_id: Option<String>,
    pub(super) unique_key: Option<String>,
    pub(super) resident: bool,
    /// The job's first `execute_at`. `into_events` appends
    /// `ExecutionScheduled { attempt: 1, scheduled_at }` to the initial
    /// event batch alongside `Initialized`, so a caller never needs a
    /// second `update_in_op` round trip just to record it.
    pub(super) schedule_at: DateTime<Utc>,
}

#[bon::bon]
impl NewJob {
    #[builder(on(String, into))]
    pub fn new<C: serde::Serialize>(
        #[builder(into)] id: JobId,
        job_type: JobType,
        config: C,
        tracing_context: Option<TracingContext>,
        queue_id: Option<String>,
        unique_key: Option<String>,
        #[builder(default)] resident: bool,
        schedule_at: DateTime<Utc>,
    ) -> Result<Self, JobFault> {
        Ok(Self {
            id,
            job_type,
            config: serde_json::to_value(config).map_err(CouldNotSerialize::Config)?,
            tracing_context,
            queue_id,
            unique_key,
            resident,
            schedule_at,
        })
    }
}

impl IntoEvents<JobEvent> for NewJob {
    type IdSource = FromCaller;

    fn into_events(
        self,
        builder: EntityEventsBuilder<JobEvent, FromCaller>,
    ) -> EntityEvents<JobEvent> {
        builder.initial_events(
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
        use es_entity::errlanes::{Fatal, FatalKind, Transient, TransientKind};
        use es_entity::events::GenericEvent;
        use serde_json::json;
        use std::time::Duration;

        const TEST_MIN_BACKOFF_SECS: u64 = 30;
        const TEST_FATAL_MIN_BACKOFF_SECS: u64 = 480;
        const TEST_MAX_BACKOFF_SECS: u64 = 600;
        const TEST_HEALTHY_RUN_SECS: u64 = 60 * 60;

        /// A run far too short to be evidence of anything.
        const SHORT_RUN: Duration = Duration::from_millis(10);

        /// An ordinary retryable failure -- what `Fault::classify(&*e)
        /// .narrow_denied()` yields at the boundary for anything that isn't
        /// `Fatal`. None of these tests assert on the message text, only on
        /// the attempt-count machinery, so one bare `Transient` stands in for
        /// every call site.
        fn retry_failure() -> JobFault {
            Transient::new(TransientKind::Other).into()
        }

        /// A run that clears `attempt_reset_after_healthy_run`.
        fn healthy_run() -> Duration {
            Duration::from_secs(TEST_HEALTHY_RUN_SECS)
        }

        fn backoff_duration() -> ChronoDuration {
            ChronoDuration::seconds(TEST_MIN_BACKOFF_SECS as i64)
        }

        /// Timestamps for one scheduled attempt in a replayed history: when
        /// the preceding failure was recorded, and when the retry was
        /// scheduled to run. Purely history scaffolding — the retry
        /// policy reads none of it.
        #[derive(Clone, Copy)]
        struct ScheduleWindow {
            failure_recorded_at: DateTime<Utc>,
            retry_scheduled_at: DateTime<Utc>,
        }

        fn schedule_window(
            now: DateTime<Utc>,
            elapsed_since_schedule: ChronoDuration,
        ) -> ScheduleWindow {
            let scheduled_at = now - elapsed_since_schedule;
            ScheduleWindow {
                failure_recorded_at: scheduled_at - backoff_duration(),
                retry_scheduled_at: scheduled_at,
            }
        }

        fn scheduled_event(attempt: u32, window: &ScheduleWindow) -> (JobEvent, DateTime<Utc>) {
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
                    lane: FailureLane::Transient,
                },
                scheduled_at + ChronoDuration::seconds(1),
            )
        }

        fn push_attempt(
            history: &mut Vec<(JobEvent, DateTime<Utc>)>,
            attempt: u32,
            window: &ScheduleWindow,
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

        fn build_retry_policy() -> RetryPolicy {
            build_retry_policy_with_reset(Some(healthy_run()))
        }

        fn build_retry_policy_with_reset(
            attempt_reset_after_healthy_run: Option<Duration>,
        ) -> RetryPolicy {
            RetryPolicy {
                backoff: Backoff::new(
                    Duration::from_secs(TEST_MIN_BACKOFF_SECS),
                    Duration::from_secs(TEST_FATAL_MIN_BACKOFF_SECS),
                    Duration::from_secs(TEST_MAX_BACKOFF_SECS),
                    0,
                ),
                attempt_reset_after_healthy_run,
            }
        }

        fn default_shape_policy() -> RetryPolicy {
            RetryPolicy {
                backoff: Backoff::new(
                    Duration::from_secs(1),
                    Duration::from_secs(512),
                    Duration::from_secs(6 * 3600),
                    0,
                ),
                attempt_reset_after_healthy_run: Some(healthy_run()),
            }
        }

        fn fatal_failure() -> JobFault {
            Fatal::invariant("broken").into()
        }

        fn empty_job() -> Job {
            let job_id = JobId::new();
            job_with_history(
                job_id,
                vec![(
                    JobEvent::Initialized {
                        id: job_id,
                        job_type: JobType::new("banded"),
                        config: json!({}),
                        tracing_context: None,
                        queue_id: None,
                        unique_key: None,
                    },
                    Clock::now() - ChronoDuration::minutes(5),
                )],
            )
        }

        fn schedule(
            job: &mut Job,
            policy: &RetryPolicy,
            attempt: u32,
            failure: &JobFault,
        ) -> (i64, ScheduledRetry) {
            let now = Clock::now();
            let retry = job.maybe_schedule_retry(now, attempt, SHORT_RUN, policy, failure);
            ((retry.at - now).num_seconds(), retry)
        }

        fn last_errored_lane(job: &Job) -> FailureLane {
            job.events
                .iter_all()
                .rev()
                .find_map(|event| match event {
                    JobEvent::ExecutionErrored { lane, .. } => Some(*lane),
                    _ => None,
                })
                .expect("an ExecutionErrored event")
        }

        #[test]
        fn maybe_schedule_retry_emits_next_attempt_when_allowed() {
            let now = Clock::now();
            let job_type = JobType::new("retry-success");
            let job_id = JobId::new();
            let latest_window = schedule_window(now, ChronoDuration::minutes(1));
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
            let retry_policy = build_retry_policy();

            let next_attempt = job
                .maybe_schedule_retry(Clock::now(), 1, SHORT_RUN, &retry_policy, &retry_failure())
                .next_attempt;

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
            let retry_policy = build_retry_policy();

            let next_attempt = job
                .maybe_schedule_retry(Clock::now(), 0, SHORT_RUN, &retry_policy, &retry_failure())
                .next_attempt;

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
        fn forgiveness_still_resets() {
            let now = Clock::now();
            let job_type = JobType::new("retry-reset");
            let job_id = JobId::new();
            let first_window = schedule_window(now, ChronoDuration::minutes(15));
            let healthy_window = schedule_window(now, ChronoDuration::minutes(2));
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
            events.push(scheduled_event(2, &healthy_window));
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy();

            let next_attempt = job
                .maybe_schedule_retry(
                    Clock::now(),
                    2,
                    healthy_run(),
                    &retry_policy,
                    &retry_failure(),
                )
                .next_attempt;

            assert_eq!(
                next_attempt, 2,
                "a healthy run should treat the next run as the second attempt"
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
        fn maybe_schedule_retry_resets_even_when_retry_limit_reached() {
            let now = Clock::now();
            let job_type = JobType::new("retry-reset-limit");
            let job_id = JobId::new();
            let first_window = schedule_window(now, ChronoDuration::minutes(20));
            let second_window = schedule_window(now, ChronoDuration::minutes(10));
            let healthy_window = schedule_window(now, ChronoDuration::minutes(2));
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
            events.push(scheduled_event(3, &healthy_window));
            let mut job = job_with_history(job_id, events);
            let retry_policy = build_retry_policy();

            let next_attempt = job
                .maybe_schedule_retry(
                    Clock::now(),
                    3,
                    healthy_run(),
                    &retry_policy,
                    &retry_failure(),
                )
                .next_attempt;

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

        /// Build a history whose latest scheduled attempt is `attempt`, with
        /// NO `ExecutionCompleted` anywhere: the daemon shape, where healthy
        /// means "still inside `run()`" and no completion is ever emitted.
        fn job_at_attempt_without_completion(attempt: u32) -> Job {
            let now = Clock::now();
            let job_id = JobId::new();
            let mut events = vec![(
                JobEvent::Initialized {
                    id: job_id,
                    job_type: JobType::new("daemon"),
                    config: json!({}),
                    tracing_context: None,
                    queue_id: None,
                    unique_key: None,
                },
                now - ChronoDuration::hours(9),
            )];
            for prior in 1..attempt {
                let window =
                    schedule_window(now, ChronoDuration::minutes((attempt - prior) as i64));
                push_attempt(&mut events, prior, &window, Some("earlier"));
            }
            events.push(scheduled_event(
                attempt,
                &schedule_window(now, ChronoDuration::seconds(1)),
            ));
            let job = job_with_history(job_id, events);
            assert!(
                !job.events
                    .iter_all()
                    .any(|e| matches!(e, JobEvent::ExecutionCompleted)),
                "fixture must contain no completion event"
            );
            job
        }

        fn reset_events(job: &Job) -> usize {
            job.events
                .iter_all()
                .filter(|e| matches!(e, JobEvent::AttemptCounterReset))
                .count()
        }

        /// The #14 case: a daemon that stayed up for hours before failing is
        /// forgiven, with no completion event anywhere in its history. This
        /// is what an `ExecutionCompleted`-based reset (PR #205) could not
        /// do — for a job whose healthy state is "still inside `run()`",
        /// that predicate is structurally false exactly when forgiveness is
        /// needed.
        #[test]
        fn long_running_execution_forgives_attempt_count() {
            let mut job = job_at_attempt_without_completion(20);
            let retry_policy = build_retry_policy();

            let next_attempt = job
                .maybe_schedule_retry(
                    Clock::now(),
                    20,
                    Duration::from_secs(6 * 60 * 60),
                    &retry_policy,
                    &retry_failure(),
                )
                .next_attempt;

            assert_eq!(next_attempt, 2, "a six-hour run forgives the counter");
            assert_eq!(reset_events(&job), 1, "exactly one reset event");
        }

        /// The #163 case at unit level: a fast, deterministic failure is not
        /// forgiven no matter how far the DOMAIN clock has moved.
        #[test]
        fn fast_failure_does_not_forgive_attempt_count() {
            let mut job = job_at_attempt_without_completion(5);
            let retry_policy = build_retry_policy();
            // 24h of domain-clock time since the last schedule.
            let now = Clock::now() + ChronoDuration::hours(24);

            let next_attempt = job
                .maybe_schedule_retry(now, 5, SHORT_RUN, &retry_policy, &retry_failure())
                .next_attempt;

            assert_eq!(next_attempt, 6, "the counter must keep climbing");
            assert_eq!(
                reset_events(&job),
                0,
                "advancing the domain clock must never forgive an attempt"
            );
        }

        /// Noise guard: every completion path already sets `attempt_index`
        /// to 1 in its own transaction, so a reset at attempt 1 resets
        /// nothing and would only make `AttemptCounterReset` useless as a
        /// diagnostic — it is the signal the #163 investigators counted.
        #[test]
        fn reset_does_not_fire_at_first_attempt() {
            let mut job = job_at_attempt_without_completion(1);
            let retry_policy = build_retry_policy();

            let next_attempt = job
                .maybe_schedule_retry(
                    Clock::now(),
                    1,
                    Duration::from_secs(6 * 60 * 60),
                    &retry_policy,
                    &retry_failure(),
                )
                .next_attempt;

            assert_eq!(next_attempt, 2);
            assert_eq!(
                reset_events(&job),
                0,
                "a counter still at 1 has nothing to forgive"
            );
        }

        #[test]
        fn reset_disabled_when_threshold_is_none() {
            let mut job = job_at_attempt_without_completion(20);
            let retry_policy = build_retry_policy_with_reset(None);

            let next_attempt = job
                .maybe_schedule_retry(
                    Clock::now(),
                    20,
                    Duration::from_secs(6 * 60 * 60),
                    &retry_policy,
                    &retry_failure(),
                )
                .next_attempt;

            assert_eq!(next_attempt, 21, "forgiveness is off; the counter climbs");
            assert_eq!(reset_events(&job), 0);
        }

        #[test]
        fn run_duration_at_threshold_forgives() {
            let mut job = job_at_attempt_without_completion(7);
            let retry_policy = build_retry_policy();

            let next_attempt = job
                .maybe_schedule_retry(
                    Clock::now(),
                    7,
                    healthy_run(),
                    &retry_policy,
                    &retry_failure(),
                )
                .next_attempt;

            assert_eq!(next_attempt, 2, "the predicate is `>=`, so equality resets");
            assert_eq!(reset_events(&job), 1);
        }

        // ---- issue #163 regression test --------------------------------
        // A deterministically failing job under an advancing (simulated or
        // manual) domain clock: each failure is followed by a multi-hour
        // clock jump, so the old elapsed-time check cleared the reset
        // threshold on every pass without any successful run in between.
        //
        // Ported from PR #205 (author: the #205 contributor), re-pointed at
        // the run-duration signal.

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
        fn attempt_index_keeps_climbing_despite_domain_clock_jumps() {
            let retry_policy = build_retry_policy();
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
            // Reload the entity from persisted history every cycle, exactly
            // like the real dispatch loop (each failure is a separate
            // transaction), then jump the domain clock forward by hours.
            // The run itself fails fast every time, which is the only
            // evidence the policy now accepts.
            for _ in 0..12 {
                let mut job = job_from_raw_history(job_id, &history);
                let retry = job.maybe_schedule_retry(
                    now,
                    attempt,
                    SHORT_RUN,
                    &retry_policy,
                    &retry_failure(),
                );
                assert_eq!(
                    retry.next_attempt,
                    attempt + 1,
                    "domain-clock advancement alone must not reset the attempt counter (issue #163)"
                );
                assert!(
                    job.terminal_state().is_none(),
                    "nothing terminates a job by retrying"
                );
                history.push((
                    raw_event(JobEvent::ExecutionErrored {
                        error: "boom".to_string(),
                        lane: FailureLane::Transient,
                    }),
                    now,
                ));
                history.push((
                    raw_event(JobEvent::ExecutionScheduled {
                        attempt: retry.next_attempt,
                        scheduled_at: retry.at,
                    }),
                    now,
                ));
                attempt = retry.next_attempt;
                now += ChronoDuration::hours(6);
            }
            assert_eq!(attempt, 13);
        }

        #[test]
        fn transient_climbs_from_transient_min() {
            let policy = default_shape_policy();
            let mut job = empty_job();

            for attempt in 1..=9 {
                let (delay, retry) = schedule(&mut job, &policy, attempt, &retry_failure());
                assert_eq!(delay, 1 << (attempt - 1), "attempt {attempt}");
                assert_eq!(retry.band, RetryBand::Transient);
                assert_eq!(last_errored_lane(&job), FailureLane::Transient);
            }
        }

        #[test]
        fn transient_streak_crosses_at_fatal_min() {
            let policy = default_shape_policy();
            let mut job = empty_job();
            for attempt in 1..=9 {
                schedule(&mut job, &policy, attempt, &retry_failure());
            }

            let (delay, retry) = schedule(&mut job, &policy, 10, &retry_failure());

            assert_eq!(delay, 512);
            assert_eq!(retry.band, RetryBand::Fatal);
            assert_eq!(last_errored_lane(&job), FailureLane::Fatal);
            assert_eq!(retry.fatal_streak, 1);
        }

        #[test]
        fn transient_streak_continues_past_crossing() {
            let policy = default_shape_policy();
            let mut job = empty_job();
            for attempt in 1..=11 {
                schedule(&mut job, &policy, attempt, &retry_failure());
            }

            let (delay, retry) = schedule(&mut job, &policy, 12, &retry_failure());

            assert_eq!(delay, 2048);
            assert_eq!(retry.band, RetryBand::Fatal);
        }

        #[test]
        fn fatal_enters_at_fatal_min() {
            let policy = default_shape_policy();
            let mut job = empty_job();

            let (delay, retry) = schedule(&mut job, &policy, 1, &fatal_failure());

            assert_eq!(delay, 512);
            assert_eq!(retry.band, RetryBand::Fatal);
            assert_eq!(retry.fatal_streak, 1);
            assert_eq!(job.fatal_streak(), 1);
        }

        #[test]
        fn fatal_streak_doubles() {
            let policy = default_shape_policy();
            let mut job = empty_job();

            let delays: Vec<i64> = (1..=3)
                .map(|attempt| schedule(&mut job, &policy, attempt, &fatal_failure()).0)
                .collect();

            assert_eq!(delays, vec![512, 1024, 2048]);
            assert_eq!(job.fatal_streak(), 3);
        }

        #[test]
        fn fatal_late_in_transient_streak_does_not_drop_back() {
            let policy = default_shape_policy();
            let mut job = empty_job();
            for attempt in 1..=12 {
                schedule(&mut job, &policy, attempt, &retry_failure());
            }

            let (delay, retry) = schedule(&mut job, &policy, 13, &fatal_failure());

            assert_eq!(delay, 4096);
            assert_eq!(retry.band, RetryBand::Fatal);
        }

        #[test]
        fn transient_after_fatal_returns_to_its_band() {
            let policy = default_shape_policy();
            let mut job = empty_job();
            schedule(&mut job, &policy, 1, &fatal_failure());

            let (delay, retry) = schedule(&mut job, &policy, 2, &retry_failure());

            assert_eq!(delay, 2);
            assert_eq!(retry.band, RetryBand::Transient);
            assert_eq!(retry.fatal_streak, 0);
            assert_eq!(job.fatal_streak(), 0);
        }

        #[test]
        fn max_caps_both_bands() {
            let policy = default_shape_policy();
            let max = 6 * 3600;

            for attempt in [20, 1000, u32::MAX] {
                let mut job = empty_job();
                let (transient, _) = schedule(&mut job, &policy, attempt, &retry_failure());
                let mut job = empty_job();
                let (fatal, _) = schedule(&mut job, &policy, attempt, &fatal_failure());
                assert_eq!(transient, max, "transient at attempt {attempt}");
                assert_eq!(fatal, max, "fatal at attempt {attempt}");
            }
        }

        #[test]
        fn congestion_and_abort_are_transparent_to_fatal_streak() {
            let policy = default_shape_policy();
            let mut job = empty_job();
            schedule(&mut job, &policy, 1, &fatal_failure());
            job.reschedule_congestion("pool".to_string(), Clock::now(), 2);
            job.abort_execution("shutdown".to_string(), Clock::now(), 2);

            let (_, retry) = schedule(&mut job, &policy, 2, &fatal_failure());

            assert_eq!(retry.fatal_streak, 2);
        }

        #[test]
        fn attempt_counter_reset_ends_fatal_streak() {
            let policy = default_shape_policy();
            let mut job = empty_job();
            for attempt in 1..=3 {
                schedule(&mut job, &policy, attempt, &fatal_failure());
            }
            assert_eq!(job.fatal_streak(), 3);
            job.events.push(JobEvent::AttemptCounterReset);
            assert_eq!(job.fatal_streak(), 0);

            let (_, retry) = schedule(&mut job, &policy, 1, &fatal_failure());

            assert_eq!(retry.fatal_streak, 1);
        }

        #[test]
        fn nothing_terminates_by_retrying() {
            let policy = default_shape_policy();
            let mut job = empty_job();
            let panic: JobFault = Fatal::new(FatalKind::Panic).with_context("boom").into();

            let (_, retry) = schedule(&mut job, &policy, 1000, &panic);

            assert_eq!(retry.next_attempt, 1001);
            assert!(job.terminal_state().is_none());
            assert!(
                !job.events.iter_all().any(|event| matches!(
                    event,
                    JobEvent::JobFailed { .. } | JobEvent::JobCompleted
                ))
            );
        }

        #[test]
        fn fail_job_is_terminal() {
            let mut job = empty_job();

            job.fail_job("r".to_string());

            assert_eq!(job.terminal_state(), Some(JobTerminalState::Failed));
            let snapshot = crate::JobSnapshot::from_parts(job, None, None);
            assert_eq!(
                snapshot.state(),
                crate::JobStatus::Failed {
                    reason: "r".to_string(),
                    queue_id: None,
                }
            );
        }

        #[test]
        fn backoff_invariant() {
            let s = Duration::from_secs;
            assert!(Backoff::try_new(s(1), s(1), s(6 * 3600), 20).is_err());
            assert!(Backoff::try_new(s(1), s(512), s(100), 20).is_err());
            assert!(Backoff::try_new(s(1), s(512), s(6 * 3600), 101).is_err());
            assert!(Backoff::try_new(s(1), s(512), s(6 * 3600), 100).is_ok());
            assert!(Backoff::try_new(Duration::ZERO, s(512), s(6 * 3600), 20).is_err());
            assert!(Backoff::try_new(Duration::from_micros(999), s(512), s(6 * 3600), 20).is_err());
            assert!(Backoff::try_new(Duration::from_millis(1), s(512), s(6 * 3600), 20).is_ok());
        }
    }

    mod retry_policy {
        use super::*;
        use std::time::Duration;

        const MAX_DELAY_MS: u64 = 60_000;
        const SECOND: Duration = Duration::from_secs(1);

        fn retry_policy(first_delay: Duration, max_delay: Duration, jitter_pct: u8) -> RetryPolicy {
            RetryPolicy {
                backoff: Backoff::new(first_delay, max_delay, max_delay, jitter_pct),
                attempt_reset_after_healthy_run: Some(Duration::from_secs(60 * 60)),
            }
        }

        trait TransientDelay {
            fn calculate_backoff(&self, attempt: u32) -> u64;
        }

        impl TransientDelay for RetryPolicy {
            fn calculate_backoff(&self, attempt: u32) -> u64 {
                self.next_retry(FailureLane::Transient, attempt, 0)
                    .0
                    .as_millis() as u64
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
            let first_delay = Duration::from_millis(100);
            let max_delay = Duration::from_secs(60);
            let expected_delays = [100, 200, 400, 800];
            let policy = retry_policy(first_delay, max_delay, 0);

            for (attempt, &expected) in (1..=4).zip(&expected_delays) {
                let actual = policy.calculate_backoff(attempt);
                assert_delay_exact(actual, expected);
            }
        }

        #[test]
        fn zero_attempt_handled_correctly() {
            let first_delay = Duration::from_millis(100);
            let max_delay = Duration::from_secs(60);
            let policy = retry_policy(first_delay, max_delay, 0);
            let delay = policy.calculate_backoff(0);

            assert_delay_exact(delay, 100);
        }

        #[test]
        fn high_attempts_capped_at_max_delay() {
            let first_delay = Duration::from_millis(100);
            let max_delay = Duration::from_millis(MAX_DELAY_MS);
            let policy = retry_policy(first_delay, max_delay, 0);

            for high_attempt in [20, 31, 100, 1000, u32::MAX] {
                let delay = policy.calculate_backoff(high_attempt);
                assert_delay_exact(delay, MAX_DELAY_MS);
            }
        }

        #[test]
        fn attempts_capped_at_30() {
            let first_delay = Duration::from_millis(100);
            let max_delay = Duration::from_millis(MAX_DELAY_MS);
            let policy = retry_policy(first_delay, max_delay, 0);
            let backoff31 = policy.calculate_backoff(31);
            let backoff100 = policy.calculate_backoff(100);

            assert_eq!(backoff31, backoff100, "Both should be capped at attempt 30");
            assert_eq!(backoff31, MAX_DELAY_MS);
            assert_eq!(backoff100, MAX_DELAY_MS);
        }

        #[test]
        fn jitter_adds_randomness() {
            let first_delay = Duration::from_millis(100);
            let max_delay = Duration::from_secs(60);
            let policy = retry_policy(first_delay, max_delay, 20);
            let delay = policy.calculate_backoff(1);

            assert_delay_in_range(delay, 80, 120);
        }

        #[test]
        fn jitter_never_negative() {
            let first_delay = Duration::from_millis(100);
            let max_delay = Duration::from_secs(60);
            let policy = retry_policy(first_delay, max_delay, 20);

            for _ in 0..10 {
                let delay = policy.calculate_backoff(1);
                assert!(delay <= 120, "Delay should be reasonable, got {delay}ms");
            }
        }

        #[test]
        fn deterministic_without_jitter() {
            let first_delay = Duration::from_millis(100);
            let max_delay = Duration::from_secs(60);
            let policy = retry_policy(first_delay, max_delay, 0);
            let backoff1 = policy.calculate_backoff(5);
            let backoff2 = policy.calculate_backoff(5);

            assert_eq!(
                backoff1, backoff2,
                "Backoffs should be identical without jitter"
            );
        }

        #[test]
        fn should_reset_attempt_count_is_false_below_the_threshold() {
            let policy = retry_policy(Duration::from_secs(30), Duration::from_secs(600), 0);

            assert!(
                !policy.should_reset_attempt_count(Duration::from_secs(60 * 60) - SECOND),
                "a run one second short of the threshold is not evidence of recovery"
            );
        }

        #[test]
        fn should_reset_attempt_count_is_true_at_and_above_the_threshold() {
            let policy = retry_policy(Duration::from_secs(30), Duration::from_secs(600), 0);

            assert!(policy.should_reset_attempt_count(Duration::from_secs(60 * 60)));
            assert!(policy.should_reset_attempt_count(Duration::from_secs(60 * 60) + SECOND));
        }

        #[test]
        fn should_reset_attempt_count_is_false_when_disabled() {
            let policy = RetryPolicy {
                backoff: Backoff::new(
                    Duration::from_secs(30),
                    Duration::from_secs(480),
                    Duration::from_secs(600),
                    0,
                ),
                attempt_reset_after_healthy_run: None,
            };

            assert!(
                !policy.should_reset_attempt_count(Duration::from_secs(24 * 60 * 60)),
                "`None` disables forgiveness outright"
            );
        }

        #[test]
        fn apply_jitter_survives_huge_backoff_without_overflow() {
            // The result must stay within [0, max_ms] and never panic, even
            // with a huge max delay, across many jitter draws.
            let huge = u64::MAX; // > i64::MAX
            let policy = RetryPolicy {
                backoff: Backoff::new(
                    Duration::from_millis(huge - 2),
                    Duration::from_millis(huge - 1),
                    Duration::from_millis(huge),
                    100,
                ),
                attempt_reset_after_healthy_run: None,
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
