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
use job::{JobConfig, JobInitializer, JobRunner, JobCompletion};
use async_trait::async_trait;

#[derive(serde::Serialize)]
struct MyJobConfig {
    message: String,
}

impl JobConfig for MyJobConfig {
    type Initializer = MyJobInitializer;
}

struct MyJobInitializer;

impl JobInitializer for MyJobInitializer {
    fn job_type() -> job::JobType {
        "my_job".into()
    }

    fn init(&self, job: &job::Job) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: MyJobConfig = serde_json::from_value(job.data_json.clone())?;
        Ok(Box::new(MyJobRunner { config }))
    }
}

struct MyJobRunner {
    config: MyJobConfig,
}

#[async_trait]
impl JobRunner for MyJobRunner {
    async fn run(&self, _current_job: job::CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        println!("Processing job: {}", self.config.message);
        Ok(JobCompletion::Complete)
    }
}
```

## License

Licensed under the Apache License, Version 2.0.
