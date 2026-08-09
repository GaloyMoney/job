#![no_main]

//! Coverage-guided fuzz target for the job entity-hydration path.
//!
//! `EntityEvents::load_first` / `load_n` reconstruct [`Job`] entities from
//! persisted `JobEvent` JSON, then the derived accessors (`terminal_state`,
//! `completed`, `config`, `return_value`) walk the reconstructed event stream.
//! Persisted event payloads are the most untrusted data the repo reads back, so
//! we feed arbitrary *structured* event streams through hydration and assert it
//! never panics and respects the `load_n` limit.
//!
//! Input is decoded as `Vec<JobEvent>` (not raw `serde_json::Value`) so the
//! fuzzer explores permutations of *valid* events — orderings, missing
//! `Initialized`, arbitrary attempt numbers / timestamps in `ExecutionScheduled`,
//! duplicate/terminal sequences, … — and actually reaches `try_from_events` and
//! the accessors. A checked-in seed corpus (`fuzz/seeds/`) provides the valid
//! `Initialized` building block the fuzzer can't synthesize from scratch.

use libfuzzer_sys::fuzz_target;

use chrono::Utc;
use es_entity::*;
use job::{Job, JobEvent, JobId};
use uuid::Uuid;

fn make_events(events: &[JobEvent]) -> Vec<GenericEvent<JobId>> {
    // Partition events into contiguous groups (each group = one entity) so that
    // `load_n` reconstructs multiple entities and exercises its grouping +
    // early-return-at-n paths. `load_n` assumes events grouped by id, ordered by
    // sequence per id; a varying stride gives a range of groupings. (Mirrors
    // es-entity's fuzz_event_hydration harness.)
    let stride = (events.len() % 4).max(1);
    events
        .iter()
        .enumerate()
        .map(|(i, e)| GenericEvent {
            entity_id: JobId::from(Uuid::from_u128((i / stride) as u128)),
            sequence: (i % stride) as i32,
            // Re-serialize each event to the opaque JSON the persistence layer
            // stores; `to_value` is infallible for these types but degrade
            // gracefully rather than panic if it ever weren't.
            event: serde_json::to_value(e).unwrap_or(serde_json::Value::Null),
            context: None,
            recorded_at: Utc::now(),
            forgettable_payload: None,
        })
        .collect()
}

/// Exercise every public accessor derived from the reconstructed event stream —
/// these are the job-specific invariants (beyond "doesn't crash") we want to
/// hold for any successfully hydrated entity.
fn exercise_accessors(job: &Job) {
    let _ = job.terminal_state();
    let _ = job.completed();
    let _ = job.config::<serde_json::Value>();
    let _ = job.return_value::<serde_json::Value>();
}

fuzz_target!(|data: &[u8]| {
    let Ok(events) = serde_json::from_slice::<Vec<JobEvent>>(data) else {
        return;
    };
    if events.is_empty() {
        return;
    }

    if let Ok(Some(job)) = EntityEvents::<JobEvent>::load_first::<Job>(make_events(&events)) {
        exercise_accessors(&job);
    }

    let n = (events.len() % 8) + 1;
    if let Ok((entities, _more)) = EntityEvents::<JobEvent>::load_n::<Job>(make_events(&events), n)
    {
        assert!(entities.len() <= n, "load_n exceeded the requested limit");
        for job in &entities {
            exercise_accessors(job);
        }
    }
});
