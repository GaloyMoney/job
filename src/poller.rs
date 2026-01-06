use es_entity::AtomicOperation;
use es_entity::clock::ClockHandle;
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgListener, PgPool, types::PgInterval};
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

const MAX_WAIT: Duration = Duration::from_secs(60);

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
        let listener_handle = self.start_listener().await?;
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
            Self::main_loop(
                Arc::clone(&executor),
                listener_handle,
                lost_handle,
                keep_alive_handle,
            )
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
        _listener_task: OwnedTaskHandle,
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

    async fn start_listener(&self) -> Result<OwnedTaskHandle, sqlx::Error> {
        let mut listener = PgListener::connect_with(self.repo.pool()).await?;
        listener.listen("job_execution").await?;
        let tracker = self.tracker.clone();
        let supported_job_types = self.registry.registered_job_types();
        Ok(OwnedTaskHandle::new(spawn_named_task!(
            "job-poller-listener",
            async move {
                loop {
                    match listener.recv().await {
                        Ok(notification) => {
                            let job_type = notification.payload();
                            // Only wake the tracker if this is a job type we support
                            if supported_job_types.iter().any(|jt| jt.as_str() == job_type) {
                                tracker.job_execution_inserted();
                            }
                        }
                        Err(_) => {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        )))
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

                if let Ok(rows) = sqlx::query!(
                        r#"
                        UPDATE job_executions
                        SET state = 'pending', execute_at = $1, attempt_index = attempt_index + 1, poller_instance_id = NULL
                        WHERE state = 'running' AND alive_at < $1::timestamptz
                        RETURNING id as id
                        "#,
                        check_time,
                    )
                    .fetch_all(&pool)
                    .await
                        && !rows.is_empty()
                    {
                        Span::current().record("n_lost_jobs", rows.len());
                        for row in rows {
                            tracing::error!(job_id = %row.id, "lost job");
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

                    let timeout = match sqlx::query!(
                        r#"
                        UPDATE job_executions
                        SET alive_at = $1
                        WHERE poller_instance_id = $2 AND state = 'running'
                        "#,
                        now,
                        instance_id,
                    )
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
        let runner = self.registry.init_job(&job, Arc::clone(&self.repo), self.clock.clone())?;
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
    pool: &PgPool,
    n_jobs_to_poll: usize,
    instance_id: uuid::Uuid,
    supported_job_types: &[super::entity::JobType],
    clock: &ClockHandle,
) -> Result<JobPollResult, sqlx::Error> {
    let now = clock.now();
    Span::current().record("now", tracing::field::display(now));

    let rows = sqlx::query_as!(
        JobPollRow,
        r#"
        WITH min_wait AS (
            SELECT MIN(execute_at) - $2::timestamptz AS wait_time
            FROM job_executions
            WHERE state = 'pending'
            AND execute_at > $2::timestamptz
            AND job_type = ANY($4)
        ),
        selected_jobs AS (
            SELECT id, execution_state_json AS data_json, attempt_index
            FROM job_executions
            WHERE execute_at <= $2::timestamptz
            AND state = 'pending'
            AND job_type = ANY($4)
            ORDER BY execute_at ASC
            LIMIT $1
            FOR UPDATE
        ),
        updated AS (
            UPDATE job_executions AS je
            SET state = 'running', alive_at = $2, execute_at = NULL, poller_instance_id = $3
            FROM selected_jobs
            WHERE je.id = selected_jobs.id
            RETURNING je.id, selected_jobs.data_json, je.attempt_index
        )
        SELECT * FROM (
            SELECT
                u.id AS "id?: JobId",
                u.data_json AS "data_json?: JsonValue",
                u.attempt_index AS "attempt_index?",
                NULL::INTERVAL AS "max_wait?: PgInterval"
            FROM updated u
            UNION ALL
            SELECT
                NULL::UUID AS "id?: JobId",
                NULL::JSONB AS "data_json?: JsonValue",
                NULL::INT AS "attempt_index?",
                mw.wait_time AS "max_wait?: PgInterval"
            FROM min_wait mw
            WHERE NOT EXISTS (SELECT 1 FROM updated)
        ) AS result
        "#,
        n_jobs_to_poll as i32,
        now,
        instance_id,
        supported_job_types as _,
    )
    .fetch_all(pool)
    .await?;

    Span::current().record("n_jobs_found", rows.len());
    Ok(JobPollResult::from_rows(rows))
}

#[derive(Debug)]
enum JobPollResult {
    Jobs(Vec<PolledJob>),
    WaitTillNextJob(Duration),
}

#[derive(Debug)]
struct JobPollRow {
    id: Option<JobId>,
    data_json: Option<JsonValue>,
    attempt_index: Option<i32>,
    max_wait: Option<PgInterval>,
}

impl JobPollResult {
    /// Convert raw query rows into a JobPollResult
    pub fn from_rows(rows: Vec<JobPollRow>) -> Self {
        if rows.is_empty() {
            JobPollResult::WaitTillNextJob(MAX_WAIT)
        } else if rows.len() == 1 && rows[0].id.is_none() {
            if let Some(interval) = &rows[0].max_wait {
                JobPollResult::WaitTillNextJob(pg_interval_to_duration(interval))
            } else {
                JobPollResult::WaitTillNextJob(MAX_WAIT)
            }
        } else {
            let jobs = rows
                .into_iter()
                .filter_map(|row| {
                    if let (Some(id), Some(attempt_index)) = (row.id, row.attempt_index) {
                        Some(PolledJob {
                            id,
                            data_json: row.data_json,
                            attempt: attempt_index as u32,
                        })
                    } else {
                        None
                    }
                })
                .collect();
            JobPollResult::Jobs(jobs)
        }
    }
}

fn pg_interval_to_duration(interval: &PgInterval) -> Duration {
    const SECONDS_PER_DAY: u64 = 24 * 60 * 60;
    if interval.microseconds < 0 || interval.days < 0 || interval.months < 0 {
        Duration::default()
    } else {
        let days = (interval.days as u64) + (interval.months as u64) * 30;
        Duration::from_micros(interval.microseconds as u64)
            + Duration::from_secs(days * SECONDS_PER_DAY)
    }
}

impl JobPollerHandle {
    /// Gracefully shut down the job poller.
    ///
    /// This method is idempotent and can be called multiple times safely.
    /// It will:
    /// 1. Send shutdown signal to all running job tasks
    /// 2. Wait briefly for tasks to complete naturally
    /// 3. Reschedule any jobs still running for this instance
    ///
    /// If not called manually, it will be called automatically when the handle is dropped.
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
        // to give jobs a chance to complete gracefully
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
    let mut op = repo.begin_op().await?;
    let now = clock.now();
    let rows = sqlx::query!(
        r#"
        UPDATE job_executions
        SET state = 'pending',
            execute_at = $1,
            poller_instance_id = NULL
        WHERE poller_instance_id = $2 AND state = 'running'
        RETURNING id as "id!: JobId", attempt_index
        "#,
        now,
        instance_id
    )
    .fetch_all(op.as_executor())
    .await?;

    let n_killed = rows.len();
    tracing::Span::current().record("n_killed", n_killed);

    if n_killed == 0 {
        return Ok(());
    }

    let attempt_map: std::collections::HashMap<JobId, u32> = rows
        .into_iter()
        .map(|r| (r.id, r.attempt_index as u32))
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
