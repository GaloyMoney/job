//! Minimal span-field capture for asserting on what
//! `#[es_entity::errlanes::instrument]` records (`error.lane`, `error.code`,
//! etc.) without pulling in a full tracing test harness. Compiled separately
//! into every integration test binary that needs it (`mod span_capture;`),
//! so items unused by a given binary need `#[allow(dead_code)]`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// Field values recorded on spans by name, keyed by span name then field
/// name. Shared (`Clone`) handle into the subscriber's storage.
#[derive(Clone, Default)]
#[allow(dead_code)]
pub struct SpanFields(Arc<Mutex<HashMap<String, HashMap<String, String>>>>);

impl SpanFields {
    /// The last recorded value of `field` on the most recently seen span
    /// named `span_name`, if any. Values from `tracing::field::display(..)`
    /// and plain `&str`/`bool` records all come back as plain strings (e.g.
    /// `"true"`, `"transient"`).
    #[allow(dead_code)]
    pub fn get(&self, span_name: &str, field: &str) -> Option<String> {
        self.0
            .lock()
            .expect("span fields lock")
            .get(span_name)
            .and_then(|fields| fields.get(field).cloned())
    }

    #[allow(dead_code)]
    pub fn debug_dump(&self) -> String {
        format!("{:?}", self.0.lock().expect("span fields lock"))
    }
}

struct FieldVisitor<'a>(&'a mut HashMap<String, String>);

impl tracing::field::Visit for FieldVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

struct CaptureLayer(SpanFields);

impl<S> tracing_subscriber::Layer<S> for CaptureLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else { return };
        let mut storage = self.0.0.lock().expect("span fields lock");
        let entry = storage.entry(span.name().to_string()).or_default();
        attrs.record(&mut FieldVisitor(entry));
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else { return };
        let mut storage = self.0.0.lock().expect("span fields lock");
        let entry = storage.entry(span.name().to_string()).or_default();
        values.record(&mut FieldVisitor(entry));
    }
}

/// Installs a capturing subscriber as the default for the CURRENT THREAD
/// only (not process-global), for the life of the returned guard. Pair with
/// a `#[tokio::test]` using the default `current_thread` flavor so every
/// span the test's job execution creates stays on this thread and is seen.
#[allow(dead_code)]
pub fn install() -> (SpanFields, tracing::subscriber::DefaultGuard) {
    let fields = SpanFields::default();
    let subscriber = tracing_subscriber::registry().with(CaptureLayer(fields.clone()));
    let guard = tracing::subscriber::set_default(subscriber);
    // `tracing`'s per-callsite "interest" cache is process-wide: under
    // parallel test execution, another thread's subscriber (or the
    // no-subscriber default, between tests) can get cached as "never
    // interested" for a callsite shared with this one (the compiled
    // `#[instrument]` span-creation site is a single fixed callsite, hit by
    // every test that exercises it), silently dropping events here even
    // though this thread's `set_default` above is active. Rebuilding
    // forces every callsite to re-register interest against the
    // currently-active (this thread's) dispatch.
    tracing::callsite::rebuild_interest_cache();
    (fields, guard)
}
