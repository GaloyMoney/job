use es_entity::AtomicOperation;
use es_entity::clock::ClockHandle;
use es_entity::db;
use serde_json::Value as JsonValue;
use sqlx::Row;
use tracing::{Span, instrument};

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use super::{
    JobId, config::JobPollerConfig, dispatcher::*, error::JobError, handle::OwnedTaskHandle,
    registry::JobRegistry, repo::JobRepo, tracker::JobTracker,
};

/// Helper macro to spawn tasks with optional names based on the tokio-task-names feature
/// Note: Requires both the feature AND tokio_unstable cfg to be set
#[cfg(all(feature = "tokio-task-names", tokio_unstable))]
macro_rules! spawn_named_task {
    ($name:expr, $future:expr) => {
        tokio::task::Builder::new()
            .name($name)
            .spawn($future)
            .expect("failed to spawn task")
    };
}

#[cfg(not(all(feature = "tokio-task-names", tokio_unstable)))]
macro_rules! spawn_named_task {
    ($name:expr, $future:expr) => {
        tokio::spawn($future)
    };
}

pub(crate) struct JobPoller {
    config: JobPollerConfig,
    repo: Arc<JobRepo>,
    registry: JobRegistry,
    tracker: Arc<JobTracker>,
    instance_id: uuid::Uuid,
    shutdown_tx: tokio::sync::broadcast::Sender<
        tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
    >,
    clock: ClockHandle,
}

pub(crate) struct JobPollerHandle {
    #[allow(dead_code)]
    poller: Arc<JobPoller>,
    #[allow(dead_code)]
    handle: OwnedTaskHandle,
    shutdown_tx: tokio::sync::broadcast::Sender<
        tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
    >,
    shutdown_called: Arc<AtomicBool>,
    shutdown_timeout: Duration,
    max_jobs_per_process: usize,
    repo: Arc<JobRepo>,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
}

// Without PgListener (LISTEN/NOTIFY), the poller relies on periodic
// polling. Keep this short so newly inserted jobs are picked up promptly.
const MAX_WAIT: Duration = Duration::from_secs(1);

impl JobPoller {
    pub fn new(
        config: JobPollerConfig,
        repo: Arc<JobRepo>,
        registry: JobRegistry,
        clock: ClockHandle,
    ) -> Self {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<
            tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
        >(1);
        Self {
            tracker: Arc::new(JobTracker::new(
                config.min_jobs_per_process,
                config.max_jobs_per_process,
            )),
            repo,
            config,
            registry,
            instance_id: uuid::Uuid::now_v7(),
            shutdown_tx,
            clock,
        }
    }

    pub async fn start(self) -> Result<JobPollerHandle, sqlx::Error> {
        let lost_handle = self.start_lost_handler();
        let keep_alive_handle = self.start_keep_alive_handler();
        let shutdown_tx = self.shutdown_tx.clone();
        let repo = Arc::clone(&self.repo);
        let instance_id = self.instance_id;
        let shutdown_timeout = self.config.shutdown_timeout;
        let max_jobs_per_process = self.config.max_jobs_per_process;
        let clock = self.clock.clone();
        let executor = Arc::new(self);
        let handle = OwnedTaskHandle::new(spawn_named_task!(
            "job-poller-main-loop",
            Self::main_loop(Arc::clone(&executor), lost_handle, keep_alive_handle,)
        ));
        Ok(JobPollerHandle {
            poller: executor,
            handle,
            shutdown_tx,
            shutdown_called: Arc::new(AtomicBool::new(false)),
            repo,
            instance_id,
            shutdown_timeout,
            max_jobs_per_process,
            clock,
        })
    }

    async fn main_loop(
        self: Arc<Self>,
        _lost_task: OwnedTaskHandle,
        _keep_alive_task: OwnedTaskHandle,
    ) {
        let mut failures = 0;
        let mut woken_up = false;
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            let timeout = match self.poll_and_dispatch(woken_up).await {
                Ok(duration) => {
                    failures = 0;
                    duration
                }
                Err(e) => {
                    failures += 1;
                    tracing::error!(error = %e, failures, "main loop error");
                    Duration::from_millis(50 << failures)
                }
            };

            tokio::select! {
                biased;

                _ = shutdown_rx.recv() => {
                    break;
                }
                result = self.clock.timeout(timeout, self.tracker.notified()) => {
                    woken_up = result.is_ok();
                }
                // Real-time fallback: ensures the poller wakes even when an
                // artificial (manual) clock has already been advanced past
                // the timeout target before it was registered.
                _ = tokio::time::sleep(MAX_WAIT) => {
                    woken_up = false;
                }
            }
        }
    }

    #[instrument(
        name = "job.poll_and_dispatch",
        level = "debug",
        skip(self),
        fields(poller_id, n_jobs_running, n_jobs_to_start, now, next_poll_in),
        err
    )]
    async fn poll_and_dispatch(self: &Arc<Self>, woken_up: bool) -> Result<Duration, JobError> {
        let span = Span::current();
        span.record("poller_id", tracing::field::display(self.instance_id));
        let Some(n_jobs_to_poll) = self.tracker.next_batch_size() else {
            span.record("next_poll_in", tracing::field::debug(MAX_WAIT));
            span.record("n_jobs_to_start", 0);
            return Ok(MAX_WAIT);
        };
        let supported_job_types = self.registry.registered_job_types();
        let rows = match poll_jobs(
            self.repo.pool(),
            n_jobs_to_poll,
            self.instance_id,
            &supported_job_types,
            &self.clock,
        )
        .await?
        {
            JobPollResult::WaitTillNextJob(duration) => {
                span.record("next_poll_in", tracing::field::debug(duration));
                span.record("n_jobs_to_start", 0);
                return Ok(duration);
            }
            JobPollResult::Jobs(jobs) => jobs,
        };
        span.record("n_jobs_to_start", rows.len());
        if !rows.is_empty() {
            for row in rows {
                self.dispatch_job(row).await?;
            }
        }

        span.record("next_poll_in", tracing::field::debug(Duration::ZERO));
        Ok(Duration::ZERO)
    }

    fn start_lost_handler(&self) -> OwnedTaskHandle {
        let job_lost_interval = self.config.job_lost_interval;
        let pool = self.repo.pool().clone();
        let clock = self.clock.clone();
        OwnedTaskHandle::new(spawn_named_task!("job-poller-lost-handler", async move {
            loop {
                clock.sleep(job_lost_interval / 2).await;
                let now = clock.now();
                let check_time = now - job_lost_interval;

                let span = tracing::debug_span!(
                    parent: None,
                    "job.detect_lost_jobs",
                    check_time = %check_time,
                    n_lost_jobs = tracing::field::Empty,
                );
                let _guard = span.enter();

                if let Ok(rows) = sqlx::query(
                    r#"
                    UPDATE job_executions
                    SET state = 'pending', execute_at = ?1, attempt_index = attempt_index + 1, poller_instance_id = NULL
                    WHERE state = 'running' AND alive_at < ?1
                    RETURNING id
                    "#,
                )
                .bind(check_time)
                .fetch_all(&pool)
                .await
                {
                    if !rows.is_empty() {
                        Span::current().record("n_lost_jobs", rows.len());
                        for row in &rows {
                            let id: String = row.get("id");
                            tracing::error!(job_id = %id, "lost job");
                        }
                    } else {
                        Span::current().record("n_lost_jobs", 0);
                    }
                } else {
                    Span::current().record("n_lost_jobs", 0);
                }
            }
        }))
    }

    fn start_keep_alive_handler(&self) -> OwnedTaskHandle {
        let job_lost_interval = self.config.job_lost_interval;
        let pool = self.repo.pool().clone();
        let instance_id = self.instance_id;
        let clock = self.clock.clone();
        OwnedTaskHandle::new(spawn_named_task!(
            "job-poller-keep-alive-handler",
            async move {
                let mut failures = 0;
                loop {
                    let now = clock.now();
                    let span = tracing::debug_span!(
                        parent: None,
                        "job.keep_alive",
                        instance_id = %instance_id,
                        now = %now,
                        failures
                    );
                    let _guard = span.enter();

                    let timeout = match sqlx::query(
                        r#"
                        UPDATE job_executions
                        SET alive_at = ?1
                        WHERE poller_instance_id = ?2 AND state = 'running'
                        "#,
                    )
                    .bind(now)
                    .bind(instance_id.to_string())
                    .execute(&pool)
                    .await
                    {
                        Ok(_) => {
                            failures = 0;
                            job_lost_interval / 4
                        }
                        Err(e) => {
                            failures += 1;
                            tracing::error!(instance_id = %instance_id, error = %e, "keep alive error");
                            Duration::from_millis(50 << failures)
                        }
                    };
                    drop(_guard);
                    clock.sleep(timeout).await;
                }
            }
        ))
    }

    #[instrument(
        name = "job.dispatch_job",
        skip(self, polled_job),
        fields(job_id, job_type, poller_id, attempt, now),
        err
    )]
    async fn dispatch_job(&self, polled_job: PolledJob) -> Result<(), JobError> {
        let span = Span::current();
        span.record("attempt", polled_job.attempt);
        let job = self.repo.find_by_id(polled_job.id).await?;
        span.record("job_id", tracing::field::display(job.id));
        span.record("job_type", tracing::field::display(&job.job_type));
        let runner = self
            .registry
            .init_job(&job, Arc::clone(&self.repo), self.clock.clone())?;
        let retry_settings = self.registry.retry_settings(&job.job_type).clone();
        let repo = Arc::clone(&self.repo);
        let tracker = self.tracker.clone();
        let instance_id = self.instance_id;
        let clock = self.clock.clone();
        span.record("now", tracing::field::display(clock.now()));
        span.record("poller_id", tracing::field::display(instance_id));

        let shutdown_rx = self.shutdown_tx.subscribe();
        let job_id = job.id;
        let job_type = job.job_type.clone();
        #[cfg_attr(
            not(all(feature = "tokio-task-names", tokio_unstable)),
            allow(unused_variables)
        )]
        let task_name = format!("job-{}-{}", job_type, job_id);

        let job_handle = spawn_named_task!(&task_name, async move {
            let id = job.id;
            let attempt = polled_job.attempt;
            if let Err(e) = JobDispatcher::new(
                repo,
                tracker,
                retry_settings,
                job.id,
                runner,
                instance_id,
                clock,
            )
            .execute_job(polled_job, shutdown_rx)
            .await
            {
                tracing::error!(job_id = %id, attempt, error = %e, "job dispatcher error");
            }
        });

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let shutdown_timeout = self.config.shutdown_timeout;
        #[cfg_attr(
            not(all(feature = "tokio-task-names", tokio_unstable)),
            allow(unused_variables)
        )]
        let monitor_task_name = format!("job-{}-monitor-{}", job_type, job_id);

        spawn_named_task!(&monitor_task_name, async move {
            use tracing::Instrument;

            tokio::pin!(job_handle);

            tokio::select! {
                _ = &mut job_handle => {
                    // Job completed - no need for shutdown coordination
                }
                Ok(shutdown_notifier) = shutdown_rx.recv() => {
                    let (send, recv) = tokio::sync::oneshot::channel();

                    async {
                        match shutdown_notifier.send(recv).await {
                            Ok(()) => {
                                tracing::Span::current().record("ack_sent", true);
                                tracing::info!("Acknowledgement sent, waiting for job completion");
                                drop(shutdown_notifier);

                                if tokio::time::timeout(shutdown_timeout, &mut job_handle).await.is_err() {
                                    tracing::Span::current().record("job_completed", false);
                                    tracing::warn!("Job exceeded timeout, aborting");
                                    job_handle.abort();
                                } else {
                                    tracing::Span::current().record("job_completed", true);
                                    tracing::info!("Job completed gracefully");
                                }

                                let _ = send.send(());
                                tracing::info!("Final completion signal sent");
                            }
                            Err(_) => {
                                tracing::Span::current().record("ack_sent", false);
                                tracing::error!("Failed to send acknowledgement - stopped listening");
                            }
                        }
                    }.instrument(tracing::info_span!(
                            parent: None,
                            "job.shutdown_coordination",
                            job_id = %job_id,
                            job_type = %job_type,
                            coordination_path = "shutdown_first",
                            ack_sent = tracing::field::Empty,
                            job_completed = tracing::field::Empty,
                        )
                    ).await;
                }
            }
        });

        Ok(())
    }
}

#[instrument(name = "job.poll_jobs", level = "debug", skip(pool, supported_job_types, clock), fields(n_jobs_to_poll, instance_id = %instance_id, n_jobs_found = tracing::field::Empty), err)]
async fn poll_jobs(
    pool: &db::Pool,
    n_jobs_to_poll: usize,
    instance_id: uuid::Uuid,
    supported_job_types: &[super::entity::JobType],
    clock: &ClockHandle,
) -> Result<JobPollResult, sqlx::Error> {
    let now = clock.now();
    Span::current().record("now", tracing::field::display(now));

    if supported_job_types.is_empty() {
        return Ok(JobPollResult::WaitTillNextJob(MAX_WAIT));
    }

    // Build dynamic IN clause placeholders for supported job types
    // Parameters: ?1 = now, ?2 = instance_id, ?3 = limit, ?4.. = job_types
    let type_placeholders: Vec<String> = (0..supported_job_types.len())
        .map(|i| format!("?{}", i + 4))
        .collect();
    let in_clause = type_placeholders.join(", ");

    // Step 1: Select and update eligible jobs atomically
    let update_query = format!(
        r#"
        UPDATE job_executions
        SET state = 'running', alive_at = ?1, execute_at = NULL, poller_instance_id = ?2
        WHERE id IN (
            SELECT e.id FROM (
                SELECT id, execute_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(queue_id, CAST(id AS TEXT))
                        ORDER BY execute_at
                    ) AS rn
                FROM job_executions
                WHERE state = 'pending'
                    AND execute_at <= ?1
                    AND job_type IN ({in_clause})
                    AND NOT EXISTS (
                        SELECT 1 FROM job_executions AS running
                        WHERE running.state = 'running'
                            AND running.queue_id IS NOT NULL
                            AND running.queue_id = job_executions.queue_id
                    )
            ) e WHERE e.rn = 1
            ORDER BY e.execute_at ASC
            LIMIT ?3
        )
        RETURNING id, execution_state_json, attempt_index
        "#
    );

    let mut q = sqlx::query(&update_query)
        .bind(now)
        .bind(instance_id.to_string())
        .bind(n_jobs_to_poll as i32);
    for jt in supported_job_types {
        q = q.bind(jt.as_str());
    }
    let rows = q.fetch_all(pool).await?;

    if rows.is_empty() {
        // Step 2: No jobs ready now — find the wait time to the next eligible job
        let wait_type_placeholders: Vec<String> = (0..supported_job_types.len())
            .map(|i| format!("?{}", i + 2))
            .collect();
        let wait_in_clause = wait_type_placeholders.join(", ");
        let wait_query = format!(
            r#"
            SELECT MIN(execute_at) as next_execute_at
            FROM job_executions
            WHERE state = 'pending'
                AND job_type IN ({wait_in_clause})
                AND execute_at > ?1
                AND NOT EXISTS (
                    SELECT 1 FROM job_executions AS running
                    WHERE running.state = 'running'
                        AND running.queue_id IS NOT NULL
                        AND running.queue_id = job_executions.queue_id
                )
            "#
        );
        let mut wq = sqlx::query(&wait_query).bind(now);
        for jt in supported_job_types {
            wq = wq.bind(jt.as_str());
        }
        let wait_row = wq.fetch_optional(pool).await?;

        let wait = wait_row
            .and_then(|row| {
                let next: Option<chrono::DateTime<chrono::Utc>> = row.get("next_execute_at");
                next.and_then(|t| (t - now).to_std().ok())
            })
            .unwrap_or(MAX_WAIT);

        Span::current().record("n_jobs_found", 0);
        return Ok(JobPollResult::WaitTillNextJob(wait));
    }

    Span::current().record("n_jobs_found", rows.len());

    let jobs = rows
        .iter()
        .map(|row| {
            let id: JobId = row.get("id");
            let data_json_str: Option<String> = row.get("execution_state_json");
            let data_json = data_json_str
                .as_deref()
                .and_then(|s| serde_json::from_str::<JsonValue>(s).ok());
            let attempt_index: i32 = row.get("attempt_index");
            PolledJob {
                id,
                data_json,
                attempt: attempt_index as u32,
            }
        })
        .collect();

    Ok(JobPollResult::Jobs(jobs))
}

#[derive(Debug)]
enum JobPollResult {
    Jobs(Vec<PolledJob>),
    WaitTillNextJob(Duration),
}

impl JobPollerHandle {
    /// Gracefully shut down the job poller.
    pub async fn shutdown(&self) -> Result<(), JobError> {
        perform_shutdown(
            self.shutdown_tx.clone(),
            Arc::clone(&self.repo),
            self.instance_id,
            self.shutdown_called.clone(),
            self.shutdown_timeout,
            self.max_jobs_per_process,
            self.clock.clone(),
        )
        .await
    }
}

impl Drop for JobPollerHandle {
    fn drop(&mut self) {
        let shutdown_tx = self.shutdown_tx.clone();
        let repo = Arc::clone(&self.repo);
        let instance_id = self.instance_id;
        let shutdown_called = self.shutdown_called.clone();
        let shutdown_timeout = self.shutdown_timeout;
        let max_jobs_per_process = self.max_jobs_per_process;
        let clock = self.clock.clone();

        spawn_named_task!("job-poller-shutdown-on-drop", async move {
            let _ = perform_shutdown(
                shutdown_tx,
                repo,
                instance_id,
                shutdown_called,
                shutdown_timeout,
                max_jobs_per_process,
                clock,
            )
            .await;
        });
    }
}

#[instrument(
    name = "jobs.perform_shutdown",
    skip(shutdown_tx, repo, clock),
    fields(n_jobs, instance_id = %instance_id, broadcast_ok, n_responses)
)]
async fn perform_shutdown(
    shutdown_tx: tokio::sync::broadcast::Sender<
        tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
    >,
    repo: Arc<JobRepo>,
    instance_id: uuid::Uuid,
    shutdown_called: Arc<AtomicBool>,
    shutdown_timeout: Duration,
    max_jobs_per_process: usize,
    clock: ClockHandle,
) -> Result<(), JobError> {
    if shutdown_called
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Ok(());
    }

    let (send, mut recv) =
        tokio::sync::mpsc::channel::<tokio::sync::oneshot::Receiver<()>>(max_jobs_per_process);

    let broadcast_ok = shutdown_tx.send(send).is_ok();
    tracing::Span::current().record("broadcast_ok", broadcast_ok);

    if broadcast_ok {
        let mut receivers = Vec::with_capacity(max_jobs_per_process);
        let receive_timeout = Duration::from_millis(100);

        tracing::info!("Starting to collect shutdown acknowledgements from job monitors");

        loop {
            match tokio::time::timeout(receive_timeout, recv.recv()).await {
                Ok(Some(oneshot_rx)) => {
                    receivers.push(oneshot_rx);
                    tracing::info!(
                        n_collected = receivers.len(),
                        "Received acknowledgement from monitor task"
                    );
                }
                Ok(None) => {
                    tracing::info!(
                        n_collected = receivers.len(),
                        "Channel closed, all monitors responded"
                    );
                    break;
                }
                Err(_) => {
                    tracing::warn!(
                        n_collected = receivers.len(),
                        "Receive timeout expired, moving on with collected responses"
                    );
                    break;
                }
            }
        }

        tracing::Span::current().record("n_responses", receivers.len());

        tracing::info!(
            n_responses = receivers.len(),
            "Waiting for all acknowledged jobs to complete"
        );

        if tokio::time::timeout(shutdown_timeout, futures::future::join_all(receivers))
            .await
            .is_err()
        {
            tracing::warn!("Some jobs did not signal completion within shutdown timeout");
        } else {
            tracing::info!("All acknowledged jobs completed");
        }
    } else {
        // No active subscribers - wait for the shutdown timeout anyway
        tracing::warn!("No active shutdown subscribers, waiting for shutdown timeout");
        tokio::time::sleep(shutdown_timeout).await;
    }

    kill_remaining_jobs(repo, instance_id, clock).await
}

#[instrument(name = "jobs.kill_remaining_jobs", skip(repo, clock), fields(instance_id = %instance_id, n_killed = tracing::field::Empty), err)]
async fn kill_remaining_jobs(
    repo: Arc<JobRepo>,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
) -> Result<(), JobError> {
    let mut op = repo.begin_op_with_clock(&clock).await?;
    let now = clock.now();
    let rows = sqlx::query(
        r#"
        UPDATE job_executions
        SET state = 'pending',
            execute_at = ?1,
            poller_instance_id = NULL
        WHERE poller_instance_id = ?2 AND state = 'running'
        RETURNING id, attempt_index
        "#,
    )
    .bind(now)
    .bind(instance_id.to_string())
    .fetch_all(op.as_executor())
    .await?;

    let n_killed = rows.len();
    tracing::Span::current().record("n_killed", n_killed);

    if n_killed == 0 {
        return Ok(());
    }

    let attempt_map: std::collections::HashMap<JobId, u32> = rows
        .iter()
        .map(|r| {
            let id: JobId = r.get("id");
            let attempt_index: i32 = r.get("attempt_index");
            (id, attempt_index as u32)
        })
        .collect();

    let ids: Vec<JobId> = attempt_map.keys().copied().collect();
    let entities = repo.find_all::<crate::Job>(&ids).await?;

    for (job_id, mut job) in entities {
        let attempt_index = attempt_map[&job_id];

        tracing::error!(
            job_id = %job_id,
            job_type = %job.job_type,
            attempt = attempt_index,
            "Job still running after shutdown timeout, forcing reschedule"
        );

        job.abort_execution("killed job".to_string(), now, attempt_index);
        repo.update_in_op(&mut op, &mut job).await?;
    }
    op.commit().await?;
    Ok(())
}
