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
