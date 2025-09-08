# Job

An async / distributed job runner for Rust applications.

## Features

- Async job execution with PostgreSQL backend
- Configurable retry logic with exponential backoff
- Job scheduling and rescheduling
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
    
    // Create job system
    let config = JobsConfig::default();
    let mut jobs = Jobs::new(&pool, config);
    
    // Register job type
    jobs.add_initializer(MyJobInitializer);
    
    // Start job processing
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

## License

Licensed under the Apache License, Version 2.0.
