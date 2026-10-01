# Job
[![Crates.io](https://img.shields.io/crates/v/job)](https://crates.io/crates/job)
[![Documentation](https://docs.rs/job/badge.svg)](https://docs.rs/job)
[![Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Unsafe Rust forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](https://github.com/rust-secure-code/safety-dance/)

An async / distributed job runner for Rust applications with Postgres backend.

Uses [sqlx](https://docs.rs/sqlx/latest/sqlx/) for interfacing with the DB.

## Features

- Async job execution with PostgreSQL backend
- Job scheduling and rescheduling
- Configurable retry logic with exponential backoff
- Built-in job tracking and monitoring
- Type-safe job spawning via `JobSpawner`

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
job = "0.1"
```

### Basic Example

```rust
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use job::*;

// Define your job configuration
#[derive(Debug, Serialize, Deserialize)]
struct MyJobConfig {
    delay_ms: u64,
}

// Define your initializer
struct MyJobInitializer;

impl JobInitializer for MyJobInitializer {
    type Config = MyJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("my_job")
    }

    fn init(&self, job: &Job) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: MyJobConfig = job.config()?;
        Ok(Box::new(MyJobRunner { config }))
    }
}

// Define your runner
struct MyJobRunner {
    config: MyJobConfig,
}

#[async_trait]
impl JobRunner for MyJobRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        // Simulate some work
        tokio::time::sleep(tokio::time::Duration::from_millis(self.config.delay_ms)).await;
        println!("Job completed!");
        
        Ok(JobCompletion::Complete)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Connect to database (requires PostgreSQL with migrations applied)
    let pool = sqlx::PgPool::connect("postgresql://user:pass@localhost/db").await?;

    // Create Jobs service
    let config = JobSvcConfig::builder()
        .pg_con("postgresql://user:pass@localhost/db")
        // If you are using sqlx and already have a pool
        // .pool(pool)
        .build()
        .expect("Could not build JobSvcConfig");
    let mut jobs = Jobs::init(config).await?;
    
    // Register job type - returns a type-safe spawner
    let spawner: JobSpawner<MyJobConfig> = jobs.add_initializer(MyJobInitializer);
    
    // Start job processing
    // Must be called after all initializers have been added
    jobs.start_poll().await?;
    
    // Use the spawner to create jobs
    let job_config = MyJobConfig { delay_ms: 1000 };
    let job_id = JobId::new();
    let job = spawner.spawn(job_id, job_config).await?;
    
    // Do some other stuff...
    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;

    // Check if its completed
    let job = jobs.find(job_id).await?;
    assert!(job.completed());
    
    Ok(())
}
```

### Resident Jobs

For a process-wide singleton that just keeps running — a poller, a periodic
sweep — register with `add_resident_initializer` and spawn with
`ResidentJobSpawner::spawn`. At most one job of the type EVER exists, and it
can never complete (only reschedule) — a `ResidentJobRunner` returns
`ResidentJobCompletion`, which has no `Complete` variant. `spawn` consumes
the spawner, enforcing at the type level that only one job of this type can
be created:

```rust
let cleanup_spawner = jobs.add_resident_initializer(CleanupInitializer);

// Consumes spawner - can't accidentally spawn twice
cleanup_spawner.spawn(CleanupConfig::default()).await?;
```

### Keyed Jobs

For at-most-one-LIVE-per-key semantics — respawnable once the current
generation finishes — register with `add_keyed_initializer` and spawn with
`KeyedJobSpawner::spawn`:

```rust
let shard_spawner = jobs.add_keyed_initializer(ShardListenerInitializer);

// One job per shard; re-spawning a live shard is a no-op that returns the
// existing job's handle. Once a shard's job goes terminal, spawning it
// again starts a new generation.
shard_spawner.spawn(format!("shard-{shard_id}"), ShardConfig { shard_id }).await?;
```

### Parameterized Job Types

For cases where the job type is configured at runtime,
store the job type in your initializer:

```rust
struct TenantJobInitializer {
    job_type: JobType,
    tenant_id: String,
}

impl JobInitializer for TenantJobInitializer {
    type Config = TenantJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()  // From instance, not hardcoded
    }

    fn init(&self, job: &Job) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        // ...
    }
}
```

### Setup

In order to use the jobs crate migrations need to run on Postgres to initialize the tables.
You can either let the library run them, copy them into your project or add them to your migrations via code.

Option 1.
Let the library run the migrations - this is useful when you are not using sqlx in the rest of your project.
To avoid compilation errors set `export SQLX_OFFLINE=true` in your dev shell.
```rust
let config = JobSvcConfig::builder()
    .pool(sqlx::PgPool::connect("postgresql://user:pass@localhost/db")
    // set to true by default when passing .pg_con("<con>") - false otherwise
    .exec_migration(true)
    .build()
    .expect("Could not build JobSvcConfig");
```

Option 2.
If you are using sqlx you can copy the migration file into your project:
```
cp ./migrations/20250904065521_job_setup.sql <path>/<to>/<your>/<project>/migrations/
```

> **Upgrading across the `job_waiters` release:** this file changed in place
> rather than gaining a second migration, so its checksum changed. `sqlx
> migrate run` against a database that applied the older copy will fail with a
> version-mismatch error. Re-copy the file **and** recreate the database —
> there is no in-place upgrade path.

Option 3.
You can also add the job migrations in code when you run your own migrations without copying the file:
```rust
use job::IncludeMigrations;
sqlx::migrate!().include_job_migrations().run(&pool).await?;
```

### Optional Features

#### `tokio-task-names`

Enables named tokio tasks for better debugging and observability. **This feature requires both the feature flag AND setting the `tokio_unstable` compiler flag.**

To enable this feature:

```toml
[dependencies]
job = { version = "0.1", features = ["tokio-task-names"] }
```

And in your `.cargo/config.toml`:

```toml
[build]
rustflags = ["--cfg", "tokio_unstable"]
```

**Important:** Both the feature flag AND the `tokio_unstable` cfg must be set. The feature alone will not enable task names - it requires the unstable tokio API which is only available with the compiler flag.

When fully enabled, all spawned tasks will have descriptive names like `job-poller-main-loop`, `job-{type}-{id}`, etc., which can be viewed in tokio-console and other diagnostic tools.

## Telemetry

`job` emits structured telemetry via [`tracing`](https://docs.rs/tracing) spans such as `job.poll_jobs`,
`job.fail_job`, and `job.complete_job`. `JobError` and a runner's own boxed error are both classified
through [`errlanes`](https://github.com/GaloyMoney/es-entity/tree/main/errlanes) lanes (`Transient`,
`Fatal`, or `Denied`), and `#[es_entity::errlanes::instrument]` records that classification as six span
fields -- `error`, `error.lane`, `error.code`, `error.level`, `exception.message`, and
`exception.type` -- plus `will_retry`, so you can stream the events into your existing observability
pipeline without wrapping the runner in additional logging.

A runner error is classified once, at the job boundary, by `Fault::classify`: a lane payload
anywhere in the error's `source()` chain wins; failing that, the first `sqlx::Error` in the chain is
read through errlanes' own table; failing both, it is reported as `Fatal(Dependency)` carrying the
whole `Display` chain, so an error from a runner that knows nothing of errlanes still surfaces as
something you can alert on rather than as nothing. Job is not an authorization boundary -- there is
no caller on the other end of a job to tell no -- so a `Denied` anywhere in the chain is narrowed
with `.narrow_denied()` into `Fatal(Denied)` on the spot: the span records `error.lane = "fatal"`,
`error.code = "denied"`, not `"denied"`/`"FORBIDDEN"`.

**Reporting the lane and acting on it are separate.** The span always says what the lane was. What job
*does* with it is deliberately conservative for now:

- A transient whose `is_congestion()` is true (`PoolTimeout` or `Congestion`) reschedules without
  spending a retry attempt -- pool-wide pressure is no evidence that this particular job is broken.
- Everything else -- `Transient`, `Fatal` (including a narrowed `Denied`), a panic (`Fatal(Panic)`),
  an unlaned error -- goes through the ordinary attempt-count retry policy, exactly as every failure
  did before `job` adopted errlanes. A `Fatal` is reported as fatal immediately but does not by
  itself end the job.
- Set `terminal_on_fatal` on a job type to act on the lane as well: a `Fatal` then ends that job on
  the attempt that produced it.

The asymmetry is intentional. Trusting a `Fatal` ends a job after one attempt, and a `Fatal` that was
really transient would turn a blip into a dead job -- so that trust is opt-in per job type, earned on
the telemetry, which is there from the first deploy.

When attempts run out on the ordinary retry path, the stored error is not just the last transient's
text: the entity narrows it into `Fatal(Exhausted)` first, so `ExecutionErrored.error` reads
`fatal(exhausted): exhausted after N attempts: transient(...): ...` and a reader can tell "retried to
exhaustion" apart from "fatal from the first attempt" without cross-referencing the attempt count.

The [`RetrySettings`](https://docs.rs/job/latest/job/struct.RetrySettings.html) that you configure for each
[`JobInitializer`](https://docs.rs/job/latest/job/trait.JobInitializer.html) directly influence that
telemetry:

- `n_attempts` caps how many times the dispatcher will retry before emitting a terminal `ERROR` and deleting the job execution.
- `n_warn_attempts` controls how many consecutive failures remain `WARN` level events before the crate promotes them to `ERROR`. Setting it to `None` keeps every retry at `WARN`.
- `min_backoff`, `max_backoff`, and `backoff_jitter_pct` determine the delay that is recorded in the `job.fail_job` span before the next retry is scheduled.
- `attempt_reset_after_healthy_run` lets a job be considered healthy again once a single execution has run for at least that long before failing (measured on a monotonic clock over the run itself, so neither an application-clock advance nor scheduler latency can satisfy it); the dispatcher resets the reported attempt counter accordingly. `None` disables forgiveness, and the counter then only ever resets on a completion.
- `terminal_on_fatal` (default `false`) makes a `Fatal` runner error (including a narrowed `Denied`) end the job on the attempt that produced it, instead of taking the ordinary attempt-count retry. Leave it off until you trust how faithfully the crates behind a given job type lane their errors; the lane is reported on the span either way. Resident types force it off -- they have no terminal state to go to.

Together these make the emitted telemetry reflect both the severity and cadence of retryable failures, which is especially helpful when wiring the crate into alerting systems.

## License

Licensed under the Apache License, Version 2.0.
