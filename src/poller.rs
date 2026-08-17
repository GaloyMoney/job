use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use es_entity::clock::ClockHandle;
use serde_json::Value as JsonValue;
use sqlx::postgres::PgPool;
use tracing::{Instrument, Span, instrument};

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use super::{
    JobId,
    batch_dispatcher::BatchDispatcher,
    batched::RawBatchItem,
    config::JobPollerConfig,
    dispatcher::*,
    entity::{Job, JobType},
    error::JobError,
    notification_router::JobNotificationRouter,
    notifier::JobEventNotifier,
    registry::JobRegistry,
    repo::JobRepo,
    task::OwnedTaskHandle,
    tracker::JobTracker,
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
    router: Arc<JobNotificationRouter>,
    notifier: Arc<JobEventNotifier>,
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
    #[allow(dead_code)]
    router_listener_handle: OwnedTaskHandle,
    #[allow(dead_code)]
    router_waiter_handle: OwnedTaskHandle,
    shutdown: Arc<ShutdownCoordinator>,
}

/// Drives the shutdown sequence for one poller instance.
///
/// Shared behind an `Arc` so the explicit [`JobPollerHandle::shutdown`] call and
/// the drop path run the identical sequence, guarded by the same
/// `shutdown_called` flag.
struct ShutdownCoordinator {
    shutdown_tx: tokio::sync::broadcast::Sender<
        tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
    >,
    /// Tells `main_loop` to stop polling. Separate from `shutdown_tx` on
    /// purpose: the poll loop must be stopped and *drained* before the monitors
    /// are signalled (see [`ShutdownCoordinator::perform`]).
    poll_stop_tx: tokio::sync::watch::Sender<bool>,
    /// Flipped by `main_loop` once it has left the loop. A dropped sender (the
    /// task was aborted or panicked) counts as "exited" too.
    poll_exited_rx: tokio::sync::watch::Receiver<bool>,
    shutdown_called: AtomicBool,
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
        tracker: Arc<JobTracker>,
        router: Arc<JobNotificationRouter>,
        notifier: Arc<JobEventNotifier>,
        clock: ClockHandle,
    ) -> Self {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<
            tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
        >(1);
        Self {
            tracker,
            notifier,
            repo,
            config,
            registry,
            router,
            instance_id: uuid::Uuid::now_v7(),
            shutdown_tx,
            clock,
        }
    }

    pub fn registered_job_types(&self) -> Vec<JobType> {
        self.registry.registered_job_types()
    }

    pub fn start(
        self,
        router_listener_handle: OwnedTaskHandle,
        router_waiter_handle: OwnedTaskHandle,
    ) -> JobPollerHandle {
        let lost_handle = self.start_lost_handler();
        let keep_alive_handle = self.start_keep_alive_handler();
        let stale_jobs_handle = self.start_stale_jobs_handler();
        let shutdown_tx = self.shutdown_tx.clone();
        let repo = Arc::clone(&self.repo);
        let instance_id = self.instance_id;
        let shutdown_timeout = self.config.shutdown_timeout;
        let max_jobs_per_process = self.config.max_jobs_per_process;
        let clock = self.clock.clone();
        let (poll_stop_tx, poll_stop_rx) = tokio::sync::watch::channel(false);
        let (poll_exited_tx, poll_exited_rx) = tokio::sync::watch::channel(false);
        let executor = Arc::new(self);
        let handle = OwnedTaskHandle::new(spawn_named_task!(
            "job-poller-main-loop",
            Self::main_loop(
                Arc::clone(&executor),
                poll_stop_rx,
                poll_exited_tx,
                lost_handle,
                keep_alive_handle,
                stale_jobs_handle,
            )
        ));
        JobPollerHandle {
            poller: executor,
            handle,
            router_listener_handle,
            router_waiter_handle,
            shutdown: Arc::new(ShutdownCoordinator {
                shutdown_tx,
                poll_stop_tx,
                poll_exited_rx,
                shutdown_called: AtomicBool::new(false),
                repo,
                instance_id,
                shutdown_timeout,
                max_jobs_per_process,
                clock,
            }),
        }
    }

    /// Claim-and-dispatch loop.
    ///
    /// Stopping is driven by `poll_stop_rx` — a `watch`, not the
    /// `shutdown_tx` broadcast, because the stop must *latch*: a
    /// `poll_and_dispatch()` already in flight when the signal lands has to see
    /// it on the very next check rather than miss a one-shot notification. The
    /// loop leaves an in-flight poll intact (every row it claimed still gets
    /// dispatched, so no claim is stranded in `state='running'`) and then flips
    /// `poll_exited_tx`, which is what
    /// [`ShutdownCoordinator::perform`] waits for before signalling the
    /// monitors.
    async fn main_loop(
        self: Arc<Self>,
        mut poll_stop_rx: tokio::sync::watch::Receiver<bool>,
        poll_exited_tx: tokio::sync::watch::Sender<bool>,
        _lost_task: OwnedTaskHandle,
        _keep_alive_task: OwnedTaskHandle,
        _stale_jobs_task: OwnedTaskHandle,
    ) {
        let mut failures = 0;
        let mut woken_up = false;
        let debounce = self.config.poll_debounce;
        let mut last_poll = std::time::Instant::now();

        loop {
            if *poll_stop_rx.borrow_and_update() {
                break;
            }
            if woken_up {
                let since = last_poll.elapsed();
                if since < debounce {
                    tokio::time::sleep(debounce - since).await;
                }
            }
            last_poll = std::time::Instant::now();

            let timeout = match self.poll_and_dispatch(woken_up).await {
                Ok(duration) => {
                    failures = 0;
                    duration
                }
                Err(e) => {
                    failures += 1;
                    tracing::error!(
                        exception.message = %e,
                        exception.type = std::any::type_name_of_val(&e),
                        failures,
                        "main loop error"
                    );
                    Duration::from_millis(50 << failures.min(12))
                }
            };

            tokio::select! {
                biased;

                _ = poll_stop_rx.changed() => {
                    break;
                }
                result = self.clock.timeout(timeout, self.tracker.notified()) => {
                    woken_up = result.is_ok();
                }
            }
        }

        // Reported after the loop is provably done claiming rows.
        let _ = poll_exited_tx.send(true);
    }

    #[instrument(
        name = "job.poll_and_dispatch",
        level = "debug",
        skip(self),
        fields(poller_id, n_jobs_running, n_jobs_to_start, now, next_poll_in)
    )]
    async fn poll_and_dispatch(self: &Arc<Self>, woken_up: bool) -> Result<Duration, JobError> {
        let span = Span::current();
        span.record("poller_id", tracing::field::display(self.instance_id));
        let Some(n_jobs_to_poll) = self.tracker.next_batch_size() else {
            span.record("next_poll_in", tracing::field::debug(MAX_WAIT));
            span.record("n_jobs_to_start", 0);
            return Ok(MAX_WAIT);
        };
        let plan = self.registry.plan_claim(n_jobs_to_poll);
        if plan.types.is_empty() {
            // Every type is saturated or capped and nothing else is registered.
            span.record("next_poll_in", tracing::field::debug(MAX_WAIT));
            span.record("n_jobs_to_start", 0);
            return Ok(MAX_WAIT);
        }

        let (rows, window) = match poll_jobs(
            self.repo.pool(),
            n_jobs_to_poll,
            self.instance_id,
            &plan.types,
            &plan.row_limits,
            &plan.global_cap_types,
            &plan.global_caps,
            &self.clock,
        )
        .await?
        {
            JobPollResult::WaitTillNextJob(window) => {
                // Fresh clock read: a duration captured earlier can go stale under a manual clock.
                let duration = window.sleep_for(self.clock.now());
                span.record("next_poll_in", tracing::field::debug(duration));
                span.record("n_jobs_to_start", 0);
                return Ok(duration);
            }
            JobPollResult::Jobs { jobs, window } => (jobs, window),
        };
        let jobs_len = rows.len();
        span.record("n_jobs_to_start", jobs_len);
        if !rows.is_empty() {
            let ids: Vec<JobId> = rows.iter().map(|row| row.id).collect();
            let mut entities = self.repo.find_all::<Job>(&ids).await?;
            // Claims for batched types are collected here rather than
            // dispatched one by one: the poll query guarantees at most one row
            // per queue_id, so a type's claims from a single poll are exactly
            // the set that may be executed together.
            let mut batched: HashMap<JobType, Vec<RawBatchItem>> = HashMap::new();
            for row in rows {
                let Some(job) = entities.remove(&row.id) else {
                    tracing::error!(
                        job_id = %row.id,
                        "claimed job row has no entity; skipping dispatch"
                    );
                    continue;
                };
                if self.registry.is_batched(&job.job_type) {
                    batched
                        .entry(job.job_type.clone())
                        .or_default()
                        .push(RawBatchItem {
                            attempt: row.attempt,
                            queue_id: row.queue_id,
                            execution_state_json: row.data_json,
                            job,
                        });
                } else {
                    self.dispatch_job(job, row).await?;
                }
            }
            for (job_type, items) in batched {
                self.dispatch_batches(job_type, items).await?;
            }
        }

        // Full claim: budget was the limit, drain immediately. Partial claim: sleep,
        // unless `may_have_more` says the due backlog wasn't fully seen this poll.
        let next_poll_in = if jobs_len == n_jobs_to_poll {
            Duration::ZERO
        } else {
            window.sleep_for(self.clock.now())
        };
        span.record("next_poll_in", tracing::field::debug(next_poll_in));
        Ok(next_poll_in)
    }

    fn start_lost_handler(&self) -> OwnedTaskHandle {
        let job_lost_interval = self.config.job_lost_interval;
        let pool = self.repo.pool().clone();
        let clock = self.clock.clone();
        let supported_job_types = self.registry.registered_job_types();
        let instance_id = self.instance_id;
        let tracker = Arc::clone(&self.tracker);
        let notifier = Arc::clone(&self.notifier);
        OwnedTaskHandle::new(spawn_named_task!("job-poller-lost-handler", async move {
            loop {
                // Liveness is a wall-clock question — a manual application clock
                // can be frozen between operator-driven advances while the OS
                // process holding a job either is or isn't actually alive.
                tokio::time::sleep(job_lost_interval / 2).await;
                let alive_threshold = chrono::Utc::now() - job_lost_interval;
                let reschedule_at = clock.now();

                let self_live_ids = tracker.live_job_ids();

                let span = tracing::debug_span!(
                    parent: None,
                    "job.detect_lost_jobs",
                    alive_threshold = %alive_threshold,
                    reschedule_at = %reschedule_at,
                    instance_id = %instance_id,
                    n_live_jobs = self_live_ids.len(),
                    n_lost_jobs = tracing::field::Empty,
                );

                async {
                    match reclaim_lost_jobs(
                        &pool,
                        instance_id,
                        &supported_job_types,
                        alive_threshold,
                        reschedule_at,
                        &self_live_ids,
                    )
                    .await
                    {
                        Ok(reclaimed) => {
                            Span::current().record("n_lost_jobs", reclaimed.len());
                            let mut reported: HashSet<&JobType> = HashSet::new();
                            for (id, job_type) in &reclaimed {
                                tracing::error!(job_id = %id, "lost job");
                                if reported.insert(job_type) {
                                    notifier.execution_ready(job_type);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                exception.message = %e,
                                exception.type = std::any::type_name_of_val(&e),
                                "lost-handler failed to reclaim lost jobs"
                            );
                            Span::current().record("n_lost_jobs", 0);
                        }
                    }
                }
                .instrument(span)
                .await;
            }
        }))
    }

    fn start_keep_alive_handler(&self) -> OwnedTaskHandle {
        let job_lost_interval = self.config.job_lost_interval;
        let pool = self.repo.pool().clone();
        let instance_id = self.instance_id;
        let tracker = Arc::clone(&self.tracker);
        OwnedTaskHandle::new(spawn_named_task!(
            "job-poller-keep-alive-handler",
            async move {
                let mut failures = 0;
                loop {
                    let live_ids = tracker.live_job_ids();

                    // alive_at is a wall-clock liveness heartbeat (see lost-handler).
                    let now = chrono::Utc::now();
                    let span = tracing::debug_span!(
                        parent: None,
                        "job.keep_alive",
                        instance_id = %instance_id,
                        now = %now,
                        n_live_jobs = live_ids.len(),
                        failures
                    );

                    let timeout = async {
                        if live_ids.is_empty() {
                            failures = 0;
                            return job_lost_interval / 4;
                        }
                        match sqlx::query!(
                            r#"
                        UPDATE job_executions
                        SET alive_at = $1
                        WHERE poller_instance_id = $2
                          AND state = 'running'
                          AND id = ANY($3)
                        "#,
                            now,
                            instance_id,
                            &live_ids,
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
                                tracing::error!(
                                    instance_id = %instance_id,
                                    exception.message = %e,
                                    exception.type = std::any::type_name_of_val(&e),
                                    "keep alive error"
                                );
                                Duration::from_millis(50 << failures.min(12))
                            }
                        }
                    }
                    .instrument(span)
                    .await;

                    tokio::time::sleep(timeout).await;
                }
            }
        ))
    }

    fn start_stale_jobs_handler(&self) -> OwnedTaskHandle {
        let pending_jobs_check_interval = self.config.pending_jobs_check_interval;
        let pool = self.repo.pool().clone();
        let clock = self.clock.clone();
        let supported_job_types = self.registry.registered_job_types();
        OwnedTaskHandle::new(spawn_named_task!(
            "job-poller-stale-jobs-handler",
            async move {
                loop {
                    // Staleness reporting is a wall-clock concern — a manual clock
                    // advance should not immediately fire the stale checker before
                    // the poller has had a chance to pick up newly-eligible jobs.
                    tokio::time::sleep(pending_jobs_check_interval).await;
                    let now = clock.now();

                    let span = tracing::info_span!(
                        parent: None,
                        "job.check_stale_pending_jobs",
                        n_stale_pending = tracing::field::Empty,
                        max_pending_duration_secs = tracing::field::Empty,
                    );

                    async {
                        match sqlx::query!(
                            r#"
                        SELECT
                            job_type,
                            COUNT(*)::INT4 AS "count!: i32",
                            EXTRACT(EPOCH FROM ($1::timestamptz - MIN(execute_at)))::FLOAT8
                                AS "max_pending_duration_secs!: f64"
                        FROM job_executions
                        WHERE state = 'pending'
                        AND execute_at <= $1::timestamptz
                        AND job_type = ANY($2)
                        GROUP BY job_type
                        "#,
                            now,
                            &supported_job_types as _,
                        )
                        .fetch_all(&pool)
                        .await
                        {
                            Ok(rows) => {
                                let mut total_stale: i64 = 0;
                                let mut max_pending_secs: f64 = 0.0;

                                for row in &rows {
                                    total_stale += row.count as i64;
                                    if row.max_pending_duration_secs > max_pending_secs {
                                        max_pending_secs = row.max_pending_duration_secs;
                                    }
                                    tracing::warn!(
                                        job_type = %row.job_type,
                                        count = row.count,
                                        max_pending_duration_secs = row.max_pending_duration_secs,
                                        "stale pending jobs detected"
                                    );
                                }

                                Span::current().record("n_stale_pending", total_stale);
                                Span::current()
                                    .record("max_pending_duration_secs", max_pending_secs);
                            }
                            Err(e) => {
                                tracing::error!(
                                    exception.message = %e,
                                    exception.type = std::any::type_name_of_val(&e),
                                    "failed to check stale pending jobs"
                                );
                            }
                        }
                    }
                    .instrument(span)
                    .await;
                }
            }
        ))
    }

    /// Split one poll's claims for a batched type into batches, then dispatch
    /// each batch as a single unit of work.
    ///
    /// Two rules are encoded here:
    ///
    /// - **Canonical order.** Items are sorted by `queue_id` (job id when
    ///   unqueued) so that every batch in the process reaches shared domain
    ///   rows in the same order, which is what keeps concurrent batch
    ///   transactions from deadlocking against each other.
    /// - **Retries run alone.** A job on its second or later attempt is
    ///   dispatched as a batch of one. The first failure of a poisonous job is
    ///   shared with its batch-mates (they are all retried), but from then on
    ///   it can only ever fail by itself.
    #[instrument(
        name = "job.dispatch_batches",
        skip(self, items),
        fields(job_type = %job_type, n_items = items.len(), max_batch_size, n_batches)
    )]
    async fn dispatch_batches(
        &self,
        job_type: JobType,
        mut items: Vec<RawBatchItem>,
    ) -> Result<(), JobError> {
        let span = Span::current();
        let max_batch_size = self.registry.max_batch_size(&job_type);
        span.record("max_batch_size", max_batch_size);

        items.sort_by(
            |a, b| match (a.queue_id.as_deref(), b.queue_id.as_deref()) {
                (Some(x), Some(y)) => x.cmp(y),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => uuid::Uuid::from(a.job.id).cmp(&uuid::Uuid::from(b.job.id)),
            },
        );

        let (retries, mut fresh): (Vec<_>, Vec<_>) =
            items.into_iter().partition(|item| item.attempt > 1);

        let mut n_batches = 0;
        for retry in retries {
            n_batches += 1;
            self.dispatch_batch(job_type.clone(), vec![retry]).await?;
        }
        while !fresh.is_empty() {
            let take = max_batch_size.min(fresh.len());
            let chunk: Vec<RawBatchItem> = fresh.drain(..take).collect();
            n_batches += 1;
            self.dispatch_batch(job_type.clone(), chunk).await?;
        }
        span.record("n_batches", n_batches);
        Ok(())
    }

    #[instrument(
        name = "job.dispatch_batch",
        skip(self, items),
        fields(job_type = %job_type, n_items = items.len(), poller_id, now)
    )]
    async fn dispatch_batch(
        &self,
        job_type: JobType,
        items: Vec<RawBatchItem>,
    ) -> Result<(), JobError> {
        if items.is_empty() {
            return Ok(());
        }
        let span = Span::current();
        let runner = self.registry.init_batch(
            &job_type,
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            self.clock.clone(),
            Arc::clone(&self.notifier),
        )?;
        let retry_settings = self.registry.retry_settings(&job_type).clone();
        let repo = Arc::clone(&self.repo);
        let tracker = self.tracker.clone();
        let notifier = Arc::clone(&self.notifier);
        let instance_id = self.instance_id;
        let clock = self.clock.clone();
        span.record("now", tracing::field::display(clock.now()));
        span.record("poller_id", tracing::field::display(instance_id));

        let shutdown_rx_batch = self.shutdown_tx.subscribe();
        let mut shutdown_rx_monitor = self.shutdown_tx.subscribe();
        let shutdown_timeout = self.config.shutdown_timeout;
        let n_items = items.len();
        let dispatched_type = job_type.clone();
        #[cfg_attr(
            not(all(feature = "tokio-task-names", tokio_unstable)),
            allow(unused_variables)
        )]
        let task_name = format!("job-batch-{}-{}", job_type, n_items);

        // Built here, not in the task: constructing the dispatcher claims the
        // type's batch slot, and that must happen before the poll loop's next
        // iteration or it would claim rows against a slot already spoken for.
        let dispatcher = BatchDispatcher::new(
            repo,
            tracker,
            notifier,
            retry_settings,
            dispatched_type.clone(),
            runner,
            instance_id,
            clock,
            &items,
        );

        spawn_named_task!(&task_name, async move {
            use tracing::Instrument;

            let batch_fut = dispatcher.execute_batch(items, shutdown_rx_batch);
            tokio::pin!(batch_fut);

            tokio::select! {
                res = &mut batch_fut => {
                    if let Err(e) = res {
                        tracing::error!(
                            job_type = %dispatched_type,
                            n_items,
                            exception.message = %e,
                            exception.type = std::any::type_name_of_val(&e),
                            "batch dispatcher error"
                        );
                    }
                }
                Ok(shutdown_notifier) = shutdown_rx_monitor.recv() => {
                    let (send, recv) = tokio::sync::oneshot::channel();

                    async {
                        match shutdown_notifier.send(recv).await {
                            Ok(()) => {
                                tracing::Span::current().record("ack_sent", true);
                                tracing::info!("Acknowledgement sent, waiting for batch completion");
                                drop(shutdown_notifier);

                                match tokio::time::timeout(shutdown_timeout, &mut batch_fut).await {
                                    Ok(res) => {
                                        tracing::Span::current().record("job_completed", true);
                                        tracing::info!("Batch completed gracefully");
                                        if let Err(e) = res {
                                            tracing::error!(
                                                n_items,
                                                exception.message = %e,
                                                exception.type = std::any::type_name_of_val(&e),
                                                "batch dispatcher error"
                                            );
                                        }
                                    }
                                    Err(_) => {
                                        tracing::Span::current().record("job_completed", false);
                                        tracing::warn!("Batch exceeded timeout, aborting");
                                    }
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
                            job_type = %dispatched_type,
                            n_items,
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

    #[instrument(
        name = "job.dispatch_job",
        skip(self, job, polled_job),
        fields(job_id, job_type, poller_id, attempt, now)
    )]
    async fn dispatch_job(&self, job: Job, polled_job: PolledJob) -> Result<(), JobError> {
        let span = Span::current();
        span.record("attempt", polled_job.attempt);
        span.record("job_id", tracing::field::display(job.id));
        span.record("job_type", tracing::field::display(&job.job_type));
        let runner = self.registry.init_job(
            &job,
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            self.clock.clone(),
            Arc::clone(&self.notifier),
        )?;
        let retry_settings = self.registry.retry_settings(&job.job_type).clone();
        let repo = Arc::clone(&self.repo);
        let tracker = self.tracker.clone();
        let notifier = Arc::clone(&self.notifier);
        let instance_id = self.instance_id;
        let clock = self.clock.clone();
        span.record("now", tracing::field::display(clock.now()));
        span.record("poller_id", tracing::field::display(instance_id));

        let shutdown_rx_job = self.shutdown_tx.subscribe();
        let mut shutdown_rx_monitor = self.shutdown_tx.subscribe();
        let shutdown_timeout = self.config.shutdown_timeout;
        let job_id = job.id;
        let job_type = job.job_type.clone();
        let globally_capped = self.registry.global_cap(&job_type).is_some();
        #[cfg_attr(
            not(all(feature = "tokio-task-names", tokio_unstable)),
            allow(unused_variables)
        )]
        let task_name = format!("job-{}-{}", job_type, job_id);

        // Built here, not in the task: constructing the dispatcher claims the
        // type's per-process slot, and that must happen before the poll
        // loop's next iteration or it would claim rows against a slot
        // already spoken for (mirrors `dispatch_batch`).
        let dispatcher = JobDispatcher::new(
            repo,
            tracker,
            notifier,
            retry_settings,
            job_id,
            job_type.clone(),
            globally_capped,
            runner,
            instance_id,
            clock,
        );

        spawn_named_task!(&task_name, async move {
            use tracing::Instrument;

            let attempt = polled_job.attempt;
            let job_fut = dispatcher.execute_job(job, polled_job, shutdown_rx_job);
            tokio::pin!(job_fut);

            tokio::select! {
                res = &mut job_fut => {
                    if let Err(e) = res {
                        tracing::error!(
                            job_id = %job_id,
                            attempt,
                            exception.message = %e,
                            exception.type = std::any::type_name_of_val(&e),
                            "job dispatcher error"
                        );
                    }
                }
                Ok(shutdown_notifier) = shutdown_rx_monitor.recv() => {
                    let (send, recv) = tokio::sync::oneshot::channel();

                    async {
                        match shutdown_notifier.send(recv).await {
                            Ok(()) => {
                                tracing::Span::current().record("ack_sent", true);
                                tracing::info!("Acknowledgement sent, waiting for job completion");
                                drop(shutdown_notifier);

                                match tokio::time::timeout(shutdown_timeout, &mut job_fut).await {
                                    Ok(res) => {
                                        tracing::Span::current().record("job_completed", true);
                                        tracing::info!("Job completed gracefully");
                                        if let Err(e) = res {
                                            tracing::error!(
                                                job_id = %job_id,
                                                attempt,
                                                exception.message = %e,
                                                exception.type = std::any::type_name_of_val(&e),
                                                "job dispatcher error"
                                            );
                                        }
                                    }
                                    Err(_) => {
                                        tracing::Span::current().record("job_completed", false);
                                        tracing::warn!("Job exceeded timeout, aborting");
                                    }
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

async fn reclaim_lost_jobs(
    pool: &PgPool,
    instance_id: uuid::Uuid,
    supported_job_types: &[JobType],
    alive_threshold: DateTime<Utc>,
    reschedule_at: DateTime<Utc>,
    self_live_ids: &[uuid::Uuid],
) -> Result<Vec<(JobId, JobType)>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        UPDATE job_executions
        SET state = 'pending', execute_at = $3, attempt_index = attempt_index + 1, poller_instance_id = NULL
        WHERE state = 'running'
          AND alive_at < $1::timestamptz
          AND job_type = ANY($2)
          AND (poller_instance_id IS DISTINCT FROM $4 OR id <> ALL($5))
        RETURNING id AS "id!: JobId", job_type AS "job_type!: JobType"
        "#,
        alive_threshold,
        supported_job_types as _,
        reschedule_at,
        instance_id,
        self_live_ids,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| (r.id, r.job_type)).collect())
}

#[instrument(
    name = "job.poll_jobs",
    level = "debug",
    skip(pool, pollable_types, row_limits, global_cap_types, global_caps, clock),
    fields(n_jobs_to_poll, instance_id = %instance_id, n_jobs_found = tracing::field::Empty)
)]
#[allow(clippy::too_many_arguments)]
async fn poll_jobs(
    pool: &PgPool,
    n_jobs_to_poll: usize,
    instance_id: uuid::Uuid,
    pollable_types: &[super::entity::JobType],
    row_limits: &[i32],
    global_cap_types: &[super::entity::JobType],
    global_caps: &[i32],
    clock: &ClockHandle,
) -> Result<JobPollResult, sqlx::Error> {
    // sim_now drives execute_at scheduling (whatever clock the application uses);
    // wall_now drives the initial alive_at heartbeat so liveness is always
    // measured in real time, independent of manual-clock advances.
    let sim_now = clock.now();
    let wall_now = chrono::Utc::now();
    Span::current().record("now", tracing::field::display(sim_now));

    // Force the generic plan: auto never picks it for this statement, so
    // every poll re-plans the 6-CTE query (35-54ms vs 0.3ms exec). SET LOCAL
    // keeps the override off other statements on the caller-shared pool.
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
        .execute(&mut *tx)
        .await?;

    let rows = sqlx::query_as!(
        JobPollRow,
        r#"
        -- Narrowest set first: only due jobs, in execute_at order,
        -- bounded by a small overscan of the poll limit.
        -- idx_job_executions_pending_job_type_execute_at serves this as
        -- an ordered index scan; future-scheduled jobs never reach the
        -- anti-join / window-function work below.
        WITH global_running AS (
            -- Running-execution count of the globally-capped subset of $4
            -- ($7, almost always empty or tiny — batched types never appear
            -- here, see `JobInitializer::max_concurrent_global`). Feeds
            -- `limits` below. `job_executions` is small (bounded by the
            -- fleet's slot budgets — hundreds of rows), and the partial index
            -- idx_job_executions_poller_instance already covers
            -- `state = 'running'`, so with $7 empty this is a no-op scan of
            -- zero rows; with $7 populated it's still a cheap index scan, not
            -- a sequential one.
            SELECT job_type, COUNT(*) AS n
            FROM job_executions
            WHERE state = 'running' AND job_type = ANY($7)
            GROUP BY job_type
        ),
        limits AS (
            -- One row per pollable type carrying how many rows this poll may
            -- claim for it. For a batched type that is
            -- `max_batch_size * free batch slots`, so no more rows are taken
            -- than a batch is free to execute immediately; the rest of the
            -- backlog stays unclaimed for other pollers and accumulates into
            -- fuller later batches. A plain type's per-process concurrency
            -- cap (`JobInitializer::max_concurrent_per_process`) is folded
            -- into `row_limit` client-side, before this query runs, via $6 —
            -- `units_in_flight` is in-process state no query can see. A
            -- global cap (`max_concurrent_global`) is folded in right here,
            -- against `global_running`, in the SAME round trip as everything
            -- else: types not in $7 are unaffected (`g.cap IS NULL`).
            SELECT l.job_type,
                   CASE WHEN g.cap IS NOT NULL
                        THEN LEAST(l.row_limit, GREATEST(g.cap - COALESCE(gr.n, 0), 0))
                        ELSE l.row_limit
                   END AS row_limit
            FROM UNNEST($4::text[], $6::int4[]) AS l(job_type, row_limit)
            LEFT JOIN UNNEST($7::text[], $8::int4[]) AS g(job_type, cap)
                   ON g.job_type = l.job_type
            LEFT JOIN global_running gr ON gr.job_type = l.job_type
        ),
        pollable AS (
            -- Types with zero effective row_limit — per-process/batch-slot
            -- saturated (never reach here, already absent from $4) or
            -- global-budget saturated (computed above) — are dropped here,
            -- before `due`: exactly like today, a saturated type's backlog
            -- never floods the overscan window below. A plain row set, not
            -- an aggregated array: `= ANY(subquery)` compares against each
            -- ROW of a subquery, so an array-typed aggregate here would
            -- compare `job_type` against whole arrays instead of elements.
            SELECT job_type FROM limits WHERE row_limit > 0
        ),
        due AS (
            SELECT id, queue_id, execute_at, job_type
            FROM job_executions
            WHERE state = 'pending'
            AND job_type IN (SELECT job_type FROM pollable)
            AND execute_at <= $2::timestamptz
            ORDER BY execute_at
            LIMIT $1 * 4
        ),
        candidates AS (
            -- The 1-at-a-time-per-queue anti-join and the queue-dedup
            -- window run only over the bounded due set instead of every
            -- pending row.
            SELECT id, execute_at, job_type,
                   ROW_NUMBER() OVER (
                       PARTITION BY COALESCE(queue_id, id::text)
                       ORDER BY execute_at
                   ) AS rn
            FROM due
            WHERE NOT EXISTS (
                SELECT 1 FROM job_executions AS running
                WHERE running.state = 'running'
                AND running.queue_id IS NOT NULL
                AND running.queue_id = due.queue_id
            )
        ),
        locked AS (
            -- execution_state_json comes from job_execution_states via this
            -- LEFT JOIN, fetched only for the ~$1 winners.
            --
            -- Every queue-head is eligible here, deliberately: the per-type
            -- cap is applied *after* this lock, never before it. Filtering to
            -- each type's cap first would make every instance rank the same
            -- global candidate set and target an identical head slice, so a
            -- poller that lost the race would skip that whole slice and fall
            -- through to nothing while due work sat unclaimed. SKIP LOCKED can
            -- only route around a concurrent poller if there is something
            -- past its rows left to see.
            --
            -- FOR UPDATE OF je: bare FOR UPDATE errors on a nullable join side.
            SELECT je.id, cp.execution_state_json AS data_json, je.attempt_index,
                   c.job_type, je.execute_at
            FROM candidates c
            JOIN job_executions je ON je.id = c.id
            LEFT JOIN job_execution_states cp ON cp.id = c.id
            WHERE c.rn = 1
            ORDER BY je.execute_at ASC
            LIMIT $1
            FOR UPDATE OF je SKIP LOCKED
        ),
        selected_jobs AS (
            -- Enforce each type's cap on the rows this poller actually holds.
            -- Rows over the cap are simply not claimed: they stay `pending`
            -- and their lock is released when this short poll transaction
            -- commits, so they remain visible to other instances and
            -- accumulate into fuller later batches.
            --
            -- Every type `locked` can contain came through `due`, which only
            -- admits types `pollable` kept — i.e. row_limit >= 1 in `limits`
            -- (`limits` itself may still hold saturated, row_limit = 0 rows;
            -- `pollable` is what filters those out before `due`). So the
            -- first row of any type present here always survives —
            -- `selected_jobs` is never empty while `locked` isn't.
            SELECT t.id, t.data_json, t.attempt_index
            FROM (
                SELECT l.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY l.job_type ORDER BY l.execute_at
                       ) AS type_rn
                FROM locked l
            ) t
            JOIN limits lim ON lim.job_type = t.job_type
            WHERE t.type_rn <= lim.row_limit
        ),
        updated AS (
            -- queue_id rides along in the projection (it is already read by
            -- `due`): batch formation needs it as the canonical ordering key,
            -- and batched runners surface it per item.
            UPDATE job_executions AS je
            SET state = 'running', alive_at = $5, execute_at = NULL, poller_instance_id = $3
            FROM selected_jobs
            WHERE je.id = selected_jobs.id
              AND je.state = 'pending'
            RETURNING je.id, selected_jobs.data_json, je.attempt_index, je.queue_id
        ),
        min_wait AS (
            -- Index-only scan over the pending partial index, no
            -- anti-join. May wake slightly early when the nearest
            -- future job is queue-blocked — one wasted wake-up at
            -- worst, only while its queue stays busy.
            --
            -- Absolute timestamp; excludes already-due rows (see overscan_status).
            SELECT MIN(execute_at) AS next_due_at
            FROM job_executions
            WHERE state = 'pending'
            AND job_type = ANY($4)
            AND execute_at > $2::timestamptz
        ),
        overscan_status AS (
            -- Whether `due` or `locked` hit its LIMIT — if so, re-poll rather than trust next_due_at.
            SELECT
                (SELECT COUNT(*) FROM due) >= $1 * 4
                OR (SELECT COUNT(*) FROM locked) >= $1
                AS may_have_more
        )
        SELECT * FROM (
            SELECT
                u.id AS "id?: JobId",
                u.data_json AS "data_json?: JsonValue",
                u.attempt_index AS "attempt_index?",
                u.queue_id AS "queue_id?",
                NULL::TIMESTAMPTZ AS "next_due_at?",
                os.may_have_more AS "may_have_more!"
            FROM updated u, overscan_status os
            UNION ALL
            SELECT
                NULL::UUID AS "id?: JobId",
                NULL::JSONB AS "data_json?: JsonValue",
                NULL::INT AS "attempt_index?",
                NULL::VARCHAR AS "queue_id?",
                mw.next_due_at AS "next_due_at?",
                os.may_have_more AS "may_have_more!"
            FROM min_wait mw, overscan_status os
        ) AS result
        "#,
        n_jobs_to_poll as i32,
        sim_now,
        instance_id,
        pollable_types as _,
        wall_now,
        row_limits,
        global_cap_types as _,
        global_caps,
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;

    Span::current().record("n_jobs_found", rows.len());
    Ok(JobPollResult::from_rows(rows))
}

/// Whether the poller may sleep on `next_due_at`, or must re-poll
/// immediately because this poll couldn't see the full due backlog.
#[derive(Debug, Clone, Copy)]
struct PollWindow {
    next_due_at: Option<DateTime<Utc>>,
    may_have_more: bool,
}

impl PollWindow {
    /// Sleep duration for a fresh `now` read at the point of use.
    fn sleep_for(&self, now: DateTime<Utc>) -> Duration {
        if self.may_have_more {
            Duration::ZERO
        } else {
            duration_until(self.next_due_at, now)
        }
    }
}

#[derive(Debug)]
enum JobPollResult {
    /// `window.next_due_at` is `None` when nothing else is pending for these types.
    Jobs {
        jobs: Vec<PolledJob>,
        window: PollWindow,
    },
    WaitTillNextJob(PollWindow),
}

#[derive(Debug)]
struct JobPollRow {
    id: Option<JobId>,
    data_json: Option<JsonValue>,
    attempt_index: Option<i32>,
    queue_id: Option<String>,
    next_due_at: Option<DateTime<Utc>>,
    may_have_more: bool,
}

impl JobPollResult {
    /// Convert raw query rows into a JobPollResult. The min-wait row (`id`
    /// NULL) is present in every result set; row order is not assumed.
    pub fn from_rows(rows: Vec<JobPollRow>) -> Self {
        let mut jobs = Vec::with_capacity(rows.len());
        let mut window = PollWindow {
            next_due_at: None,
            may_have_more: false,
        };
        for row in rows {
            window.may_have_more = row.may_have_more;
            match (row.id, row.attempt_index) {
                (Some(id), Some(attempt_index)) => jobs.push(PolledJob {
                    id,
                    data_json: row.data_json,
                    attempt: attempt_index as u32,
                    queue_id: row.queue_id,
                }),
                _ => window.next_due_at = row.next_due_at,
            }
        }
        if jobs.is_empty() {
            JobPollResult::WaitTillNextJob(window)
        } else {
            JobPollResult::Jobs { jobs, window }
        }
    }
}

/// Sleep duration until `deadline`, clamped to zero if already past. `None`
/// falls back to `MAX_WAIT`. Pass a `now` read as close as possible to use.
fn duration_until(deadline: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Duration {
    match deadline {
        Some(at) => (at - now).to_std().unwrap_or(Duration::ZERO),
        None => MAX_WAIT,
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
        self.shutdown.perform().await
    }
}

impl Drop for JobPollerHandle {
    fn drop(&mut self) {
        let shutdown = Arc::clone(&self.shutdown);
        spawn_named_task!("job-poller-shutdown-on-drop", async move {
            let _ = shutdown.perform().await;
        });
    }
}

impl ShutdownCoordinator {
    /// Shut this instance's poller down, in an order that keeps the drain
    /// honest:
    ///
    /// 1. **Stop the poll loop and wait for it to exit.** Nothing new can be
    ///    claimed or dispatched after this point, so the set of live executions
    ///    is final. Doing this *before* step 2 is what makes the ack collection
    ///    complete: `tokio::sync::broadcast` only delivers to receivers that
    ///    subscribed before `send`, so a generation dispatched after the
    ///    broadcast would never see the signal, never ack, never be waited
    ///    for — and would then be force-aborted mid-flight by
    ///    [`kill_remaining_jobs`], racing its own completion write on the same
    ///    `Job` aggregate (`ConcurrentModification`, with the loser's execution
    ///    outcome discarded).
    /// 2. Broadcast to the monitor tasks and collect their acks.
    /// 3. Wait for every acked execution to finish.
    /// 4. Force-reschedule whatever is genuinely still `running`.
    #[instrument(
        name = "jobs.perform_shutdown",
        skip(self),
        fields(
            instance_id = %self.instance_id,
            poll_loop_stopped,
            broadcast_ok,
            n_responses
        )
    )]
    async fn perform(&self) -> Result<(), JobError> {
        if self
            .shutdown_called
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }

        let poll_loop_stopped = self.stop_poll_loop().await;
        tracing::Span::current().record("poll_loop_stopped", poll_loop_stopped);

        let (send, mut recv) = tokio::sync::mpsc::channel::<tokio::sync::oneshot::Receiver<()>>(
            self.max_jobs_per_process,
        );

        let broadcast_ok = self.shutdown_tx.send(send).is_ok();
        tracing::Span::current().record("broadcast_ok", broadcast_ok);

        if broadcast_ok {
            let mut receivers = Vec::with_capacity(self.max_jobs_per_process);
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

            if tokio::time::timeout(self.shutdown_timeout, futures::future::join_all(receivers))
                .await
                .is_err()
            {
                tracing::warn!("Some jobs did not signal completion within shutdown timeout");
            } else {
                tracing::info!("All acknowledged jobs completed");
            }
        } else {
            // No subscribers left. With the poll loop already stopped and
            // drained (step 1) that means there is no live execution to wait
            // for — every monitor task holds a subscription for as long as its
            // execution runs — so there is nothing to give a grace period to.
            tracing::info!("No live job monitors at shutdown, nothing to drain");
        }

        kill_remaining_jobs(Arc::clone(&self.repo), self.instance_id, self.clock.clone()).await
    }

    /// Signal `main_loop` to stop and wait until it has actually exited.
    ///
    /// Returns `false` if the loop did not report back within
    /// `shutdown_timeout` — the shutdown then continues regardless, since
    /// [`kill_remaining_jobs`] still releases whatever the poller left claimed;
    /// a wedged poll must not wedge shutdown.
    ///
    /// A dropped `poll_exited` sender resolves this immediately: the loop's task
    /// is gone (aborted with the handle, or panicked), which is as stopped as it
    /// gets.
    async fn stop_poll_loop(&self) -> bool {
        let _ = self.poll_stop_tx.send(true);

        let mut exited_rx = self.poll_exited_rx.clone();
        let exited = async {
            loop {
                let already_exited = *exited_rx.borrow_and_update();
                if already_exited {
                    return;
                }
                if exited_rx.changed().await.is_err() {
                    return;
                }
            }
        };

        match tokio::time::timeout(self.shutdown_timeout, exited).await {
            Ok(()) => {
                tracing::info!("Poll loop stopped, no further jobs will be dispatched");
                true
            }
            Err(_) => {
                tracing::warn!(
                    "Poll loop did not stop within shutdown timeout, continuing shutdown"
                );
                false
            }
        }
    }
}

/// Release every execution this instance still holds, and record the forced
/// reschedule on each `Job`.
///
/// The `UPDATE` runs first and inside `op`, so by the time anything is read the
/// transaction already holds a row lock on every execution it is about to
/// abort. The entity read then happens **in the same op** (not on a separate
/// pool connection): every execution-path writer — `complete_job`,
/// `reschedule_job`, the retry branch of `fail_job` — writes its
/// `job_executions` row before appending its events, so those locks fence them
/// out and the version snapshot read here cannot go stale under them.
///
/// Writers that touch a `Job` *without* its execution row (`set_result`) are not
/// fenced by those locks, so each entity write additionally gets its own
/// `SAVEPOINT`: a lost race rolls back that one row's audit events instead of
/// failing the whole shutdown, and the release itself — the part that decides
/// whether the job is schedulable again — is already durable in the same
/// transaction either way.
#[instrument(name = "jobs.kill_remaining_jobs", skip(repo, clock), fields(instance_id = %instance_id, n_killed = tracing::field::Empty, n_conflicts = tracing::field::Empty))]
async fn kill_remaining_jobs(
    repo: Arc<JobRepo>,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
) -> Result<(), JobError> {
    let mut op = repo.begin_op_with_clock(&clock).await?;
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
    let entities: std::collections::HashMap<JobId, crate::Job> =
        repo.find_all_in_op(&mut op, &ids).await?;

    let mut n_conflicts = 0usize;
    for (job_id, mut job) in entities {
        let attempt_index = attempt_map[&job_id];

        tracing::warn!(
            job_id = %job_id,
            job_type = %job.job_type,
            attempt = attempt_index,
            "Job still running after shutdown timeout, forcing reschedule"
        );

        job.abort_execution("killed job".to_string(), now, attempt_index);
        if let Err(e) = op
            .with_savepoint(async |sp| repo.update_in_op(sp, &mut job).await)
            .await?
        {
            // The row is released regardless (that write is outside this
            // savepoint), so the job stays schedulable; only its abort audit
            // trail is missing.
            n_conflicts += 1;
            tracing::warn!(
                job_id = %job_id,
                attempt = attempt_index,
                exception.message = %e,
                exception.type = std::any::type_name_of_val(&e),
                "Could not record forced reschedule; execution row released anyway"
            );
        }
    }
    tracing::Span::current().record("n_conflicts", n_conflicts);
    op.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn init_pool() -> anyhow::Result<PgPool> {
        let pg_con = std::env::var("PG_CON").unwrap();
        Ok(sqlx::PgPool::connect(&pg_con).await?)
    }

    /// Seed a real `Job` aggregate (events and all) plus a `running` execution
    /// row owned by `instance_id` — what a live execution looks like to
    /// [`kill_remaining_jobs`].
    async fn seed_running_entity(
        pool: &PgPool,
        repo: &JobRepo,
        job_type: &str,
        instance_id: uuid::Uuid,
    ) -> anyhow::Result<JobId> {
        let id = JobId::new();
        let new_job = crate::entity::NewJob::builder()
            .id(id)
            .job_type(JobType::from_owned(job_type.to_string()))
            .config(serde_json::json!({}))?
            .build()
            .expect("build NewJob");
        repo.create(new_job).await?;

        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, state, poller_instance_id, attempt_index, alive_at, created_at) \
             VALUES ($1, $2, 'running', $3, 1, $4, $4)",
        )
        .bind(uuid::Uuid::from(id))
        .bind(job_type)
        .bind(instance_id)
        .bind(now)
        .execute(pool)
        .await?;
        Ok(id)
    }

    /// Block until some backend on this database is waiting on a lock.
    ///
    /// The synchronisation point for the test below: it makes the interleaving
    /// an observed fact rather than a timing assumption — no sleeping until the
    /// race "probably" happened.
    async fn wait_for_blocked_backend(pool: &PgPool) -> anyhow::Result<()> {
        for _ in 0..600 {
            let blocked: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event_type = 'Lock'",
            )
            .fetch_one(pool)
            .await?;
            if blocked > 0 {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        anyhow::bail!("no backend ever blocked on a lock");
    }

    /// `kill_remaining_jobs` must survive losing a version race on a `Job` it is
    /// force-rescheduling — the failure lana PR #8282 saw escape
    /// `Jobs::shutdown()` as `JobModifyError - ConcurrentModification`.
    ///
    /// The concurrent writer here has `set_result`'s shape: it appends to the
    /// `Job` without touching the execution row, so the row locks
    /// `kill_remaining_jobs` holds do not fence it out. It claims the entity's
    /// next event sequence first and commits while the kill is mid-flight, so
    /// the kill's own append is the one that collides.
    ///
    /// Releasing the execution row is what must survive: the job has to stay
    /// schedulable, and shutdown must not fail because one audit append lost a
    /// race.
    #[tokio::test]
    async fn kill_remaining_jobs_survives_losing_a_concurrent_entity_write() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = Arc::new(JobRepo::new(&pool));
        let clock = ClockHandle::realtime();
        let instance_id = uuid::Uuid::now_v7();
        let job_type = format!("kill-race-{}", uuid::Uuid::now_v7());

        let id = seed_running_entity(&pool, &repo, &job_type, instance_id).await?;

        // The competing writer: entity append staged, not yet committed, so it
        // owns the next event sequence.
        let mut writer_op = repo.begin_op_with_clock(&clock).await?;
        let mut job = repo.find_by_id_in_op(&mut writer_op, id).await?;
        let return_value = crate::outcome::JobReturnValue::try_from(&"progress")?;
        assert!(job.update_return_value(return_value).did_execute());
        repo.update_in_op(&mut writer_op, &mut job).await?;

        // The kill blocks on that staged sequence...
        let kill = tokio::spawn(kill_remaining_jobs(
            Arc::clone(&repo),
            instance_id,
            clock.clone(),
        ));
        wait_for_blocked_backend(&pool).await?;

        // ...and only now does the competing write become the winner.
        writer_op.commit().await?;

        kill.await?
            .expect("shutdown must not fail because a forced-reschedule append lost a race");

        let row: (String, Option<uuid::Uuid>) = sqlx::query_as(
            "SELECT state::text, poller_instance_id FROM job_executions WHERE id = $1",
        )
        .bind(uuid::Uuid::from(id))
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.0, "pending", "execution must be released for reclaim");
        assert_eq!(row.1, None, "released execution must not stay owned");

        Ok(())
    }

    async fn seed_running_job(
        pool: &PgPool,
        job_type: &str,
        instance_id: uuid::Uuid,
        alive_at: DateTime<Utc>,
    ) -> anyhow::Result<JobId> {
        let id = JobId::new();
        let uuid = uuid::Uuid::from(id);
        let now = chrono::Utc::now();
        sqlx::query("INSERT INTO jobs (id, job_type, created_at) VALUES ($1, $2, $3)")
            .bind(uuid)
            .bind(job_type)
            .bind(now)
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, state, poller_instance_id, attempt_index, alive_at, created_at) \
             VALUES ($1, $2, 'running', $3, 1, $4, $5)",
        )
        .bind(uuid)
        .bind(job_type)
        .bind(instance_id)
        .bind(alive_at)
        .bind(now)
        .execute(pool)
        .await?;
        Ok(id)
    }

    async fn seed_pending_job(
        pool: &PgPool,
        job_type: &str,
        execute_at: DateTime<Utc>,
    ) -> anyhow::Result<JobId> {
        let id = JobId::new();
        let uuid = uuid::Uuid::from(id);
        let now = chrono::Utc::now();
        sqlx::query("INSERT INTO jobs (id, job_type, created_at) VALUES ($1, $2, $3)")
            .bind(uuid)
            .bind(job_type)
            .bind(now)
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, state, attempt_index, execute_at, alive_at, created_at) \
             VALUES ($1, $2, 'pending', 1, $3, $4, $5)",
        )
        .bind(uuid)
        .bind(job_type)
        .bind(execute_at)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await?;
        Ok(id)
    }

    /// A capped type's backlog can saturate the `due`/`locked` windows and
    /// hide an uncapped type's due row; `may_have_more` must catch that.
    #[tokio::test]
    async fn may_have_more_when_capped_type_saturates_the_overscan_window() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let instance_id = uuid::Uuid::now_v7();
        let type_a = format!("overscan-dominant-{}", uuid::Uuid::now_v7());
        let type_b = format!("overscan-starved-{}", uuid::Uuid::now_v7());

        // n_jobs_to_poll = 2 -> due overscan LIMIT = 8, locked LIMIT = 2.
        let n_jobs_to_poll = 2usize;

        // A: 10 due rows, all older than B's — saturates the overscan window alone.
        let base = chrono::Utc::now() - chrono::Duration::seconds(3600);
        let mut a_ids = Vec::new();
        for i in 0..10i64 {
            a_ids
                .push(seed_pending_job(&pool, &type_a, base + chrono::Duration::seconds(i)).await?);
        }
        // B: one due row, younger than all of A's but still due.
        let b_id = seed_pending_job(
            &pool,
            &type_b,
            chrono::Utc::now() - chrono::Duration::seconds(1),
        )
        .await?;

        let pollable_types = vec![
            JobType::from_owned(type_a.clone()),
            JobType::from_owned(type_b.clone()),
        ];
        // A capped to 1; B uncapped (row_limit = n_jobs_to_poll).
        let row_limits = vec![1, n_jobs_to_poll as i32];
        let clock = ClockHandle::realtime();

        let result = poll_jobs(
            &pool,
            n_jobs_to_poll,
            instance_id,
            &pollable_types,
            &row_limits,
            &[],
            &[],
            &clock,
        )
        .await?;

        match result {
            JobPollResult::Jobs { jobs, window } => {
                assert_eq!(
                    jobs.len(),
                    1,
                    "only A's single capped slot should be claimed this poll"
                );
                assert!(
                    a_ids.contains(&jobs[0].id),
                    "the one claimed row must be A's (oldest); B's row must \
                     stay unclaimed, pushed out of the overscan window"
                );
                assert_ne!(jobs[0].id, b_id);
                assert!(
                    window.may_have_more,
                    "the due/locked windows were saturated by A alone — B's \
                     due row (and A's own remaining 9) are still out there, \
                     unseen by this poll; min_wait cannot detect that, only \
                     may_have_more can"
                );
            }
            other => panic!("expected a partial Jobs claim, got {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn self_reclaim_skips_live_jobs() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let self_id = uuid::Uuid::now_v7();
        let other_id = uuid::Uuid::now_v7();
        let job_type = format!("reclaim-gate-{}", uuid::Uuid::now_v7());
        let stale = chrono::Utc::now() - chrono::Duration::seconds(600);

        let live_self = seed_running_job(&pool, &job_type, self_id, stale).await?;
        let orphan_self = seed_running_job(&pool, &job_type, self_id, stale).await?;
        let other_instance = seed_running_job(&pool, &job_type, other_id, stale).await?;

        let threshold = chrono::Utc::now() - chrono::Duration::seconds(60);
        let reschedule_at = chrono::Utc::now();
        let self_live_ids = vec![uuid::Uuid::from(live_self)];
        let types = vec![JobType::from_owned(job_type.clone())];

        let reclaimed: std::collections::HashSet<JobId> = reclaim_lost_jobs(
            &pool,
            self_id,
            &types,
            threshold,
            reschedule_at,
            &self_live_ids,
        )
        .await?
        .into_iter()
        .map(|(id, _)| id)
        .collect();

        assert!(
            reclaimed.contains(&orphan_self),
            "self-owned orphan (no live future) must be reclaimed"
        );
        assert!(
            reclaimed.contains(&other_instance),
            "another instance's stale row must be reclaimed"
        );
        assert!(
            !reclaimed.contains(&live_self),
            "self-owned row with a live runner must NOT be reclaimed"
        );

        let row: (String, Option<uuid::Uuid>, i32) = sqlx::query_as(
            "SELECT state::text, poller_instance_id, attempt_index \
             FROM job_executions WHERE id = $1",
        )
        .bind(uuid::Uuid::from(live_self))
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.0, "running");
        assert_eq!(row.1, Some(self_id));
        assert_eq!(row.2, 1);

        Ok(())
    }
}
