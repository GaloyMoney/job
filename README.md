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
`job.fail_job`, and `job.complete_job`. Failed attempts push fields like `error`, `error.message`,
`error.level`, and `will_retry`, so you can stream the events into your existing observability pipeline
without wrapping the runner in additional logging.

The [`RetrySettings`](https://docs.rs/job/latest/job/struct.RetrySettings.html) that you configure for each
[`JobInitializer`](https://docs.rs/job/latest/job/trait.JobInitializer.html) directly influence that
telemetry:

- `n_attempts` caps how many times the dispatcher will retry before emitting a terminal `ERROR` and deleting the job execution.
- `n_warn_attempts` controls how many consecutive failures remain `WARN` level events before the crate promotes them to `ERROR`. Setting it to `None` keeps every retry at `WARN`.
- `min_backoff`, `max_backoff`, and `backoff_jitter_pct` determine the delay that is recorded in the `job.fail_job` span before the next retry is scheduled.
- `attempt_reset_after_backoff_multiples` lets a job be considered healthy again after enough idle time (measured as multiples of the last backoff); the dispatcher resets the reported attempt counter accordingly.

Together these make the emitted telemetry reflect both the severity and cadence of retryable failures, which is especially helpful when wiring the crate into alerting systems.

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
impl JobConfig for MyJobConfig {
    type Initializer = MyJobInitializer;
}

struct MyJobInitializer;
impl JobInitializer for MyJobInitializer {
    fn job_type() -> JobType {
        JobType::new("my_job")
    }

    fn init(&self, job: &Job) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: MyJobConfig = job.config()?;
        Ok(Box::new(MyJobRunner { config }))
    }
}

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
    let config = JobsSvcConfig::builder()
        .pg_con("postgresql://user:pass@localhost/db")
        // If you are using sqlx and already have a pool
        // .pool(sqlx::PgPool::connect("postgresql://user:pass@localhost/db")
        .build()
        .expect("Could not build JobSvcConfig");
    let mut jobs = Jobs::init(config).await?;
    
    // Register job type
    jobs.add_initializer(MyJobInitializer);
    
    // Start job processing
    // Must be called after all initializers have been added
    jobs.start_poll().await?;
    
    // Create and spawn a job
    let job_config = MyJobConfig { delay_ms: 1000 };
    let job_id = JobId::new();
    let job = jobs.create_and_spawn(job_id, job_config).await?;
    
    // Do some other stuff...
    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;

    // Check if its completed
    let job = jobs.find(job_id).await?;
    assert!(job.completed());
    
    Ok(())
}
```

### Setup

In order to use the jobs crate migrations need to run on Postgres to initialize the tables.
You can either let the library run them, copy them into your project or add them to your migrations via code.

Option 1.
Let the library run the migrations - this is useful when you are not using sqlx in the rest of your project.
To avoid compilation errors set `export SQLX_OFFLINE=true` in your dev shell.
```rust
let config = JobsSvcConfig::builder()
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

Option 3.
You can also add the job migrations in code when you run your own migrations without copying the file:
```rust
use job::IncludeMigrations;
sqlx::migrate!().include_job_migrations().run(&pool).await?;
```

## License

Licensed under the Apache License, Version 2.0.
