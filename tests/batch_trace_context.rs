//! Live-PG proof that tracing context survives job persistence and reaches the
//! dispatcher span: a singleton adopts its producer as parent, while a
//! multi-item batch links every producer without inventing a single parent.

mod helpers;

use async_trait::async_trait;
use job::{
    BatchedJobInitializer, BatchedJobRunner, CurrentBatchedJob, CurrentJob, Job,
    JobBatchCompletion, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSvcConfig,
    JobTerminalState, JobType, Jobs,
};
use opentelemetry::{
    Context,
    trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider,
    },
};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NoopConfig;

struct NoopInitializer {
    job_type: JobType,
}

impl BatchedJobInitializer for NoopInitializer {
    type Config = NoopConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: JobSpawner<Self::Config>,
    ) -> Result<
        Box<dyn BatchedJobRunner<Config = Self::Config>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        Ok(Box::new(NoopRunner))
    }
}

struct NoopRunner;

struct PlainJobInitializer {
    job_type: JobType,
}

impl JobInitializer for PlainJobInitializer {
    type Config = NoopConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(PlainJobRunner))
    }
}

struct PlainJobRunner;

#[async_trait]
impl JobRunner for PlainJobRunner {
    async fn run(
        &self,
        _: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        Ok(JobCompletion::Complete)
    }
}

#[async_trait]
impl BatchedJobRunner for NoopRunner {
    type Config = NoopConfig;

    async fn run_batch(
        &self,
        _: CurrentBatchedJob<Self::Config>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error + Send + Sync>> {
        Ok(JobBatchCompletion::CompleteAll)
    }
}

struct Telemetry {
    provider: SdkTracerProvider,
    exporter: InMemorySpanExporter,
    _subscriber: tracing::subscriber::DefaultGuard,
}

impl Telemetry {
    fn install() -> Self {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer().with_tracer(provider.tracer("job-trace-context-test")),
        );
        let guard = tracing::subscriber::set_default(subscriber);
        tracing::callsite::rebuild_interest_cache();
        Self {
            provider,
            exporter,
            _subscriber: guard,
        }
    }

    fn spans(&self) -> Vec<SpanData> {
        self.provider.force_flush().expect("flush spans");
        self.exporter.get_finished_spans().expect("finished spans")
    }
}

fn producer_span(trace_id: &str, span_id: &str) -> tracing::Span {
    let span = tracing::info_span!(parent: None, "test.producer");
    span.set_parent(Context::new().with_remote_span_context(SpanContext::new(
        TraceId::from_hex(trace_id).expect("producer trace id"),
        SpanId::from_hex(span_id).expect("producer parent span id"),
        TraceFlags::SAMPLED,
        true,
        TraceState::from_key_value([("galoy", "retain")]).expect("retention state"),
    )))
    .expect("remote parent applies before activation");
    span
}

async fn spawn_with_producer(
    spawner: &JobSpawner<NoopConfig>,
    id: JobId,
    trace_id: &str,
    parent_span_id: &str,
) -> anyhow::Result<TraceId> {
    let producer = producer_span(trace_id, parent_span_id);
    let trace_id = producer.context().span().span_context().trace_id();
    spawner.spawn(id, NoopConfig).instrument(producer).await?;
    Ok(trace_id)
}

fn only_span<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    let matching: Vec<_> = spans.iter().filter(|span| span.name == name).collect();
    assert_eq!(matching.len(), 1, "expected exactly one {name} span");
    matching[0]
}

fn producer_spawn_span(spans: &[SpanData], trace_id: TraceId) -> &SpanData {
    spans
        .iter()
        .find(|span| {
            span.name == "job_spawner.spawn_spec_in_op" && span.span_context.trace_id() == trace_id
        })
        .expect("job's persisted producer spawn span")
}

fn linked_job_id(link: &opentelemetry::trace::Link) -> String {
    link.attributes
        .iter()
        .find_map(|attribute| {
            (attribute.key.as_str() == "job.id").then(|| match &attribute.value {
                opentelemetry::Value::String(value) => value.as_str().to_string(),
                other => panic!("job.id must be a string, got {other:?}"),
            })
        })
        .expect("link job.id")
}

#[tokio::test]
async fn ordinary_job_adopts_persisted_parent_and_tracestate() -> anyhow::Result<()> {
    let telemetry = Telemetry::install();
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(PlainJobInitializer {
        job_type: helpers::job_type("ordinary-trace-context"),
    });

    let id = JobId::new();
    let producer_trace_id = spawn_with_producer(
        &spawner,
        id,
        "00000000000000000000000000000004",
        "0000000000000004",
    )
    .await?;

    jobs.start_poll().await?;
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(30))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Completed);
    jobs.shutdown().await?;

    let spans = telemetry.spans();
    let execution = only_span(&spans, "job.execute_job");
    let producer = producer_spawn_span(&spans, producer_trace_id);
    assert_eq!(execution.span_context.trace_id(), producer_trace_id);
    assert_eq!(execution.parent_span_id, producer.span_context.span_id());
    assert_eq!(
        execution.span_context.trace_state().get("galoy"),
        Some("retain")
    );
    assert!(execution.links.links.is_empty());
    Ok(())
}

#[tokio::test]
async fn singleton_batch_adopts_persisted_parent_and_tracestate() -> anyhow::Result<()> {
    let telemetry = Telemetry::install();
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(NoopInitializer {
        job_type: helpers::job_type("singleton-trace-context"),
    });

    let id = JobId::new();
    let producer_trace_id = spawn_with_producer(
        &spawner,
        id,
        "00000000000000000000000000000001",
        "0000000000000001",
    )
    .await?;

    jobs.start_poll().await?;
    let outcome = jobs
        .handle(id)
        .await_completion(Duration::from_secs(30))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Completed);
    jobs.shutdown().await?;

    let spans = telemetry.spans();
    let batch = only_span(&spans, "job.execute_batch");
    let producer = producer_spawn_span(&spans, producer_trace_id);
    assert_eq!(batch.span_context.trace_id(), producer_trace_id);
    assert_eq!(batch.parent_span_id, producer.span_context.span_id());
    assert_eq!(
        batch.span_context.trace_state().get("galoy"),
        Some("retain")
    );
    assert!(batch.links.links.is_empty());
    Ok(())
}

#[tokio::test]
async fn multi_item_batch_links_every_persisted_producer() -> anyhow::Result<()> {
    let telemetry = Telemetry::install();
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(NoopInitializer {
        job_type: helpers::job_type("multi-trace-context"),
    });

    let first = JobId::new();
    let second = JobId::new();
    let first_trace = spawn_with_producer(
        &spawner,
        first,
        "00000000000000000000000000000002",
        "0000000000000002",
    )
    .await?;
    let second_trace = spawn_with_producer(
        &spawner,
        second,
        "00000000000000000000000000000003",
        "0000000000000003",
    )
    .await?;

    jobs.start_poll().await?;
    let outcomes = jobs
        .handles(vec![first, second])
        .await_all(Duration::from_secs(30))
        .await?;
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.state() == JobTerminalState::Completed)
    );
    jobs.shutdown().await?;

    let spans = telemetry.spans();
    let batch = only_span(&spans, "job.execute_batch");
    assert_eq!(batch.parent_span_id, SpanId::INVALID);
    assert_eq!(batch.links.dropped_count, 0);
    assert_eq!(batch.links.links.len(), 2);

    let expected_job_ids: BTreeSet<String> = [first, second].iter().map(JobId::to_string).collect();
    let actual_job_ids: BTreeSet<String> = batch.links.links.iter().map(linked_job_id).collect();
    assert_eq!(actual_job_ids, expected_job_ids);

    let expected_traces: BTreeSet<String> = [first_trace, second_trace]
        .iter()
        .map(TraceId::to_string)
        .collect();
    let actual_traces: BTreeSet<String> = batch
        .links
        .links
        .iter()
        .map(|link| link.span_context.trace_id().to_string())
        .collect();
    assert_eq!(actual_traces, expected_traces);

    for link in &batch.links.links {
        let trace_id = link.span_context.trace_id();
        let producer = producer_spawn_span(&spans, trace_id);
        assert_eq!(link.span_context.span_id(), producer.span_context.span_id());
        assert_eq!(link.span_context.trace_state().get("galoy"), Some("retain"));
    }
    Ok(())
}
