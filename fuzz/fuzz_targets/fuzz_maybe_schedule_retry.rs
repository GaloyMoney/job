#![no_main]

//! Coverage-guided fuzz target for `Job::maybe_schedule_retry`.
//!
//! `maybe_schedule_retry` is the retry decision: given a job's event history,
//! the current attempt counter, a `RetryPolicy` and "now", it decides whether
//! to reset the attempt counter (healthy gap since the last retry), schedule the
//! next retry (with a freshly-computed backoff), or give up. It touches the
//! backoff/jitter math, the datetime reset logic (`checked_mul`), and pushes
//! new events. We feed arbitrary `(events, now, attempt, policy)` and assert it
//! never panics and keeps its invariants (next attempt ≥ 2; reschedule never in
//! the past). Input is a JSON object so the fuzzer mutates whole structured
//! values; `recorded_at` is pinned to the Unix epoch so seeds are deterministic.

use libfuzzer_sys::fuzz_target;

use chrono::{DateTime, Utc};
use es_entity::*;
use job::testing::{maybe_schedule_retry, RetryPolicy};
use job::{Job, JobEvent, JobId};
use serde::Deserialize;
use std::time::Duration;
use uuid::Uuid;

#[derive(Deserialize)]
struct FuzzPolicy {
    max_attempts: Option<u32>,
    min_ms: u64,
    max_ms: u64,
    jitter_pct: u8,
    reset_multiples: u32,
}

#[derive(Deserialize)]
struct FuzzInput {
    events: Vec<JobEvent>,
    now_ms: i64,
    attempt: u32,
    policy: FuzzPolicy,
}

/// Fixed "recorded_at" so retry-window math is independent of the wall clock.
const RECORDED_AT: DateTime<Utc> = DateTime::UNIX_EPOCH;

fn make_events(events: &[JobEvent]) -> Vec<GenericEvent<JobId>> {
    let stride = (events.len() % 4).max(1);
    events
        .iter()
        .enumerate()
        .map(|(i, e)| GenericEvent {
            entity_id: JobId::from(Uuid::from_u128((i / stride) as u128)),
            sequence: (i % stride) as i32,
            event: serde_json::to_value(e).unwrap_or(serde_json::Value::Null),
            context: None,
            recorded_at: RECORDED_AT,
            forgettable_payload: None,
        })
        .collect()
}

fuzz_target!(|data: &[u8]| {
    let Ok(input) = serde_json::from_slice::<FuzzInput>(data) else {
        return;
    };

    let now = DateTime::<Utc>::from_timestamp_millis(input.now_ms).unwrap_or_else(Utc::now);
    let policy = RetryPolicy {
        max_attempts: input.policy.max_attempts,
        min_backoff: Duration::from_millis(input.policy.min_ms),
        max_backoff: Duration::from_millis(input.policy.max_ms),
        backoff_jitter_pct: input.policy.jitter_pct,
        attempt_reset_after_backoff_multiples: input.policy.reset_multiples,
    };

    let Ok(Some(mut job)) =
        EntityEvents::<JobEvent>::load_first::<Job>(make_events(&input.events))
    else {
        return;
    };

    let outcome = maybe_schedule_retry(&mut job, now, input.attempt, &policy, "fuzz".to_string());

    if let Some((reschedule_at, next_attempt)) = outcome {
        // next_attempt is always incremented (possibly after a reset to 1 → 2),
        // and the reschedule is `now + backoff` with backoff ≥ 0.
        assert!(next_attempt >= 2, "next_attempt {next_attempt} < 2");
        assert!(
            reschedule_at >= now,
            "reschedule_at {reschedule_at} before now {now}"
        );
    }
});
