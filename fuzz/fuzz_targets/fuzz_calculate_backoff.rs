#![no_main]

//! Coverage-guided fuzz target for `RetryPolicy::calculate_backoff` /
//! `apply_jitter`.
//!
//! The backoff/jitter math multiplies millisecond magnitudes by a percentage
//! and does a bit-shift — prime territory for overflow / out-of-range panics
//! when fed extreme `Duration`s. We build a `RetryPolicy` straight from raw
//! fuzzer bytes (min/max backoff in ms, jitter %, reset multiples) plus an
//! attempt number, and assert the result never panics and never exceeds
//! `max_ms` (the cap `apply_jitter`'s final `.min(max_ms)` enforces when the
//! arithmetic is sound).

use libfuzzer_sys::fuzz_target;

use std::time::Duration;

use job::testing::RetryPolicy;

fn u64_le(b: &mut &[u8]) -> Option<u64> {
    let (n, rest) = b.split_first_chunk::<8>()?;
    *b = rest;
    Some(u64::from_le_bytes(*n))
}

fn u32_le(b: &mut &[u8]) -> Option<u32> {
    let (n, rest) = b.split_first_chunk::<4>()?;
    *b = rest;
    Some(u32::from_le_bytes(*n))
}

fn byte(b: &mut &[u8]) -> Option<u8> {
    let (n, rest) = b.split_first()?;
    *b = rest;
    Some(*n)
}

fuzz_target!(|data: &[u8]| {
    let mut b = data;
    let (Some(min_ms), Some(max_ms), Some(jitter_pct), Some(reset_multiples), Some(attempt)) =
        (u64_le(&mut b), u64_le(&mut b), byte(&mut b), u32_le(&mut b), u32_le(&mut b))
    else {
        return;
    };

    let policy = RetryPolicy {
        max_attempts: None,
        min_backoff: Duration::from_millis(min_ms),
        max_backoff: Duration::from_millis(max_ms),
        backoff_jitter_pct: jitter_pct,
        attempt_reset_after_backoff_multiples: reset_multiples,
    };

    let backoff = policy.calculate_backoff(attempt);
    // calculate_backoff caps at max_ms via apply_jitter's final `.min(max_ms)`,
    // so any value above it signals an arithmetic bug (overflow/wrap).
    assert!(
        backoff <= max_ms,
        "calculate_backoff({attempt}) = {backoff} exceeded max_ms {max_ms}"
    );
});
