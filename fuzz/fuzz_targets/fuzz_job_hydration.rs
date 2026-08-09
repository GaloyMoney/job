#![no_main]

//! Coverage-guided fuzz target for the job entity-hydration path.
//!
//! Mirrors es-entity's `fuzz_event_hydration`: arbitrary persisted JSON streams
//! through `EntityEvents::load_first` / `load_n`, asserting it never panics and
//! respects the `load_n` limit. Persisted event payloads are the most untrusted
//! data the repo reads back. Uses the real (public) `Job` / `JobEvent` types so
//! `try_from_events` and the derived accessors get exercised whenever the
//! fuzzer produces a deserialisable stream; the corpus self-bootstraps and the
//! nightly job grows it in GCS.

use libfuzzer_sys::fuzz_target;

use chrono::Utc;
use es_entity::*;
use job::{Job, JobEvent, JobId};
use uuid::Uuid;

fn make_events(values: &[serde_json::Value]) -> Vec<GenericEvent<JobId>> {
    // Partition events into contiguous groups (each group = one entity) so that
    // `load_n` actually reconstructs multiple entities and exercises its
    // grouping + early-return-at-n paths — previously every event shared one id,
    // so only one entity was ever built and the `len <= n` coverage was vacuous.
    // `load_n` assumes events grouped by id, ordered by sequence per id; a
    // varying stride gives a range of groupings.
    let stride = (values.len() % 4).max(1);
    values
        .iter()
        .enumerate()
        .map(|(i, v)| GenericEvent {
            entity_id: JobId::from(Uuid::from_u128((i / stride) as u128)),
            sequence: (i % stride) as i32,
            event: v.clone(),
            context: None,
            recorded_at: Utc::now(),
            forgettable_payload: None,
        })
        .collect()
}

fuzz_target!(|data: &[u8]| {
    let Ok(values) = serde_json::from_slice::<Vec<serde_json::Value>>(data) else {
        return;
    };
    if values.is_empty() {
        return;
    }

    let _ = EntityEvents::<JobEvent>::load_first::<Job>(make_events(&values));

    let n = (values.len() % 8) + 1;
    if let Ok((entities, _more)) = EntityEvents::<JobEvent>::load_n::<Job>(make_events(&values), n) {
        assert!(entities.len() <= n, "load_n exceeded the requested limit");
    }
});
