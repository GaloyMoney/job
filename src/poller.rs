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

/// How far past its admission budget a poll gathers candidates, so
/// `FOR UPDATE ... SKIP LOCKED` has somewhere to fall through when a peer
/// instance holds locks on the rows this poll would target. Sized for
/// contention, not for filtering. See PERFORMANCE.md, "Contention headroom".
///
/// This is the resting value. The candidate window bounds ROWS while the
/// budget counts QUEUES, so a poll that lands on deep queues under-fills;
/// `MAX_CONTENTION_HEADROOM` is how far the poll loop may widen in response.
const CONTENTION_HEADROOM: i32 = 4;

/// Ceiling for the adaptive widening described on [`CONTENTION_HEADROOM`].
/// Reached only where every widening step still came up short -- queues deep
/// enough that eight rows are scanned per claimable head. See PERFORMANCE.md,
/// "Contention headroom".
const MAX_CONTENTION_HEADROOM: i32 = 32;

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
        // Candidate-window multiplier, in memory and private to this instance.
        // Rests at `CONTENTION_HEADROOM` and widens only while polls report
        // that the window itself was the binding constraint.
        let mut headroom = CONTENTION_HEADROOM;
        loop {
            if *poll_stop_rx.borrow_and_update() {
                break;
            }

            let timeout = match self.poll_and_dispatch(woken_up, &mut headroom).await {
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
    async fn poll_and_dispatch(
        self: &Arc<Self>,
        woken_up: bool,
        headroom: &mut i32,
    ) -> Result<Duration, JobError> {
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

        let result = poll_jobs(
            self.repo.pool(),
            n_jobs_to_poll,
            self.instance_id,
            &plan.types,
            &plan.row_limits,
            *headroom,
            &self.clock,
        )
        .await?;

        // Widen only while the WINDOW was the binding constraint -- it filled
        // and still produced fewer candidates than the budget, which is what
        // deep or clustered queues do to a row-bounded scan. A budget lost to
        // peers or to type caps does not set this, because widening would only
        // buy a bigger scan for the same result. One saturated poll drops it
        // straight back, so the common shallow-queue case never pays for the
        // deep one. See PERFORMANCE.md, "Contention headroom".
        let short = result.window().candidates_short;
        // A poll that is about to widen must not sleep first. A full window
        // read only a PREFIX of the due pollable rows, so claimable heads may
        // sit past it -- already due, and therefore invisible to
        // `next_due_at`. Re-polling at the SAME width would spin at zero
        // yield, so the re-poll is conditioned on the width actually growing:
        // that bounds it to the widening ladder (4 -> 8 -> 16 -> 32), and each
        // step looks strictly further than the last.
        let widening = short && *headroom < MAX_CONTENTION_HEADROOM;
        *headroom = if short {
            (*headroom * 2).min(MAX_CONTENTION_HEADROOM)
        } else {
            CONTENTION_HEADROOM
        };

        let (rows, mut window) = match result {
            JobPollResult::WaitTillNextJob(mut window) => {
                window.may_have_more |= widening;
                // Fresh clock read: a duration captured earlier can go stale under a manual clock.
                let duration = window.sleep_for(self.clock.now());
                span.record("next_poll_in", tracing::field::debug(duration));
                span.record("n_jobs_to_start", 0);
                return Ok(duration);
            }
            JobPollResult::Jobs { jobs, window } => (jobs, window),
        };
        window.may_have_more |= widening;
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
        let retains_state = self.registry.retains_state(&job_type);
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
            retains_state,
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
    skip(pool, pollable_types, row_limits, clock),
    fields(n_jobs_to_poll, instance_id = %instance_id, n_jobs_found = tracing::field::Empty)
)]
#[allow(clippy::too_many_arguments)]
async fn poll_jobs(
    pool: &PgPool,
    n_jobs_to_poll: usize,
    instance_id: uuid::Uuid,
    pollable_types: &[super::entity::JobType],
    row_limits: &[i32],
    headroom: i32,
    clock: &ClockHandle,
) -> Result<JobPollResult, sqlx::Error> {
    // sim_now drives execute_at scheduling (whatever clock the application uses);
    // wall_now drives the initial alive_at heartbeat so liveness is always
    // measured in real time, independent of manual-clock advances.
    let sim_now = clock.now();
    let wall_now = chrono::Utc::now();
    Span::current().record("now", tracing::field::display(sim_now));

    // Both overrides are scoped to this transaction so they never leak onto
    // other statements on the caller-shared pool.
    //
    // Generic plan: auto never picks it here, so every poll would otherwise
    // re-plan the whole CTE tower (35-54ms vs sub-ms exec). No bitmap scans:
    // the candidate window and the unqueued scan depend on ORDERED index
    // access, and a bitmap scan returns heap order instead. See PERFORMANCE.md, "Ordered
    // index access is mandatory".
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL enable_bitmapscan = off")
        .execute(&mut *tx)
        .await?;

    let rows = sqlx::query_as!(
        JobPollRow,
        r#"
        -- Claim admission. Queued and unqueued rows are gathered separately so
        -- that neither can starve the other, and each type is bounded by its
        -- own budget. See PERFORMANCE.md ("Claim admission") for the
        -- measurements behind this shape.
        WITH limits AS (
            SELECT l.job_type, l.row_limit
            FROM UNNEST($4::text[], $6::int4[]) AS l(job_type, row_limit)
            WHERE l.row_limit > 0
        ),
        queued_window AS (
            -- WHICH QUEUES TO EXAMINE, bounded by what this poll can ADMIT
            -- ($1 x $7 rows), never by how much is pending: cost is O(budget),
            -- flat in backlog and in queue count.
            --
            -- The blocked-queue anti-join runs INSIDE the window, below the
            -- LIMIT. That placement is the whole design: a queue whose job is
            -- already running contributes no rows at all, so its backlog can
            -- never crowd claimable work out of the budget. Filtering after a
            -- LIMIT instead is what produced the zero-claim cliff. See
            -- PERFORMANCE.md, "Queued rows: two steps, both bounded by the budget".
            --
            -- Ordering is `(execute_at, id)` and the tiebreak is load-bearing:
            -- it makes the order TOTAL, so the window is a well-defined prefix
            -- rather than an arbitrary cut through a group of rows sharing a
            -- timestamp (bulk spawns give a whole batch one).
            --
            -- Filtering by type is safe HERE, and only here: this decides
            -- which queues get looked at, not which row of a queue is its
            -- head. $4 is instance-dependent -- `plan_claim` drops saturated
            -- types -- so two instances legitimately examine different queues.
            SELECT je.queue_id, je.execute_at, je.id
            FROM job_executions je
            WHERE je.state = 'pending'
              AND je.queue_id IS NOT NULL
              AND je.execute_at <= $2::timestamptz
              AND je.job_type = ANY($4)
              AND NOT EXISTS (
                    SELECT 1 FROM job_executions r
                    WHERE r.state = 'running' AND r.queue_id = je.queue_id)
            ORDER BY je.execute_at, je.id
            LIMIT $1::int4 * $7::int4
        ),
        due_queued AS (
            -- One row per examined queue: its oldest due row, chosen WITHOUT
            -- reference to type and only then dropped if that type is not
            -- pollable here. That order matters -- picking the oldest
            -- *pollable* row instead would let two instances with different
            -- saturated types select DIFFERENT rows of the same queue and both
            -- claim, breaking queue exclusion. Deciding the row identically
            -- everywhere is what makes a peer's lock on it visible as "this
            -- queue is taken" rather than "try the next row down".
            SELECT h.id, h.execute_at, h.job_type
            FROM (SELECT DISTINCT queue_id FROM queued_window) e
            CROSS JOIN LATERAL (
                SELECT je.id, je.execute_at, je.job_type
                FROM job_executions je
                WHERE je.state = 'pending' AND je.queue_id = e.queue_id
                  AND je.execute_at <= $2::timestamptz
                ORDER BY je.execute_at, je.id
                LIMIT 1
            ) h
            WHERE h.job_type IN (SELECT job_type FROM limits)
        ),
        due_plain AS (
            -- Unqueued rows can never be blocked by a sibling, so they come
            -- straight off their own partial index in execute_at order,
            -- bounded per type. Pre-filtering by type is safe here precisely
            -- because there is no queue exclusion to agree on.
            SELECT d.id, d.execute_at, d.job_type
            FROM limits t
            CROSS JOIN LATERAL (
                SELECT je.id, je.execute_at, je.job_type
                FROM job_executions je
                WHERE je.state = 'pending' AND je.queue_id IS NULL
                  AND je.job_type = t.job_type
                  AND je.execute_at <= $2::timestamptz
                ORDER BY je.execute_at
                LIMIT LEAST(t.row_limit, $1::int4) * $7::int4
            ) d
        ),
        ordered_candidates AS (
            -- Interleave types round-robin: every type's oldest candidate
            -- ranks ahead of any type's second, so the global LIMIT below
            -- cannot be consumed end-to-end by one backlogged type. Within a
            -- rank it is still oldest-first.
            SELECT id, execute_at, job_type,
                   ROW_NUMBER() OVER (
                       PARTITION BY job_type ORDER BY execute_at
                   ) AS type_rn
            FROM (SELECT * FROM due_plain UNION ALL SELECT * FROM due_queued) u
        ),
        locked AS (
            -- The join to job_executions sits BELOW the LIMIT so it runs
            -- lazily: only rows LockRows actually pulls get probed. The sort
            -- above is a blocking node, so the full candidate set is still
            -- materialised and SKIP LOCKED falls through a contended head
            -- exactly as before.
            --
            -- FOR UPDATE OF je: bare FOR UPDATE errors on a nullable join side.
            SELECT je.id, je.attempt_index, c.job_type, c.execute_at
            FROM ordered_candidates c
            JOIN job_executions je ON je.id = c.id
            ORDER BY c.type_rn ASC, c.execute_at ASC
            LIMIT $1
            FOR UPDATE OF je SKIP LOCKED
        ),
        selected_jobs AS (
            -- The budget is enforced HERE, on rows actually held: the scans
            -- above deliberately over-gather (see $7) so there is something to
            -- fall through to when a peer holds the head. Rows over a type's
            -- cap are simply not claimed; their locks release at commit.
            -- execution_state_json is joined after the LIMIT, so it is fetched
            -- only for winners.
            SELECT t.id, cp.execution_state_json AS data_json, t.attempt_index
            FROM (
                SELECT l.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY l.job_type ORDER BY l.execute_at
                       ) AS type_rn
                FROM locked l
            ) t
            JOIN limits lim ON lim.job_type = t.job_type
            LEFT JOIN job_execution_states cp ON cp.id = t.id
            WHERE t.type_rn <= lim.row_limit
        ),
        updated AS (
            UPDATE job_executions AS je
            SET state = 'running', alive_at = $5, execute_at = NULL, poller_instance_id = $3
            FROM selected_jobs
            WHERE je.id = selected_jobs.id
              AND je.state = 'pending'
            RETURNING je.id, selected_jobs.data_json, je.attempt_index, je.queue_id
        ),
        min_wait AS (
            SELECT MIN(execute_at) AS next_due_at
            FROM job_executions
            WHERE state = 'pending'
            AND job_type = ANY($4)
            AND execute_at > $2::timestamptz
        ),
        poll_status AS (
            -- Re-poll immediately only when this poll provably left claimable
            -- work behind: it filled its budget, or it saw candidates it could
            -- not take because a peer held them.
            --
            -- A window that came back short means every claimable due row was
            -- examined, so `next_due_at` is the honest next deadline. Blocked
            -- queues are covered by a wake rather than a spin:
            -- `delete_execution_in_op` reports `execution_ready` when it frees
            -- one.
            --
            -- A FULL window that yielded no pollable head does NOT report here
            -- -- but it must not sleep either, and `poll_and_dispatch` is what
            -- keeps it awake. Hitting the LIMIT only means a PREFIX of due
            -- pollable rows was read: rows sitting behind a head this instance
            -- has saturated still consume the window, so claimable heads past
            -- it went unseen. Those are already due, so `next_due_at` does not
            -- cover them. Re-polling at the same width would spin at zero
            -- yield, which is why the answer is to widen and re-poll rather
            -- than either sleep or spin -- see `candidates_short` below.
            --
            -- `candidates_short` drives the adaptive window (see
            -- `poll_and_dispatch`). The window bounds ROWS but candidates are
            -- QUEUES, so its yield is divided by the average depth of the
            -- queues it lands on. When those rows cluster into few queues the
            -- poll under-fills its budget while claimable work sits just past
            -- the window; widening is the only thing that helps, and this is
            -- the signal that distinguishes it from a budget lost to peers or
            -- to type caps, which widening would not fix.
            SELECT ((SELECT COUNT(*) FROM locked) >= $1
                 OR ((SELECT COUNT(*) FROM queued_window) >= $1::int4 * $7::int4
                     AND (SELECT COUNT(*) FROM due_queued) > 0)) AS may_have_more,
                   ((SELECT COUNT(*) FROM queued_window) >= $1::int4 * $7::int4
                AND (SELECT COUNT(*) FROM ordered_candidates) < $1) AS candidates_short
        )
        SELECT * FROM (
            SELECT
                u.id AS "id?: JobId",
                u.data_json AS "data_json?: JsonValue",
                u.attempt_index AS "attempt_index?",
                u.queue_id AS "queue_id?",
                NULL::TIMESTAMPTZ AS "next_due_at?",
                ps.may_have_more AS "may_have_more!",
                ps.candidates_short AS "candidates_short!"
            FROM updated u, poll_status ps
            UNION ALL
            SELECT
                NULL::UUID AS "id?: JobId",
                NULL::JSONB AS "data_json?: JsonValue",
                NULL::INT AS "attempt_index?",
                NULL::VARCHAR AS "queue_id?",
                mw.next_due_at AS "next_due_at?",
                ps.may_have_more AS "may_have_more!",
                ps.candidates_short AS "candidates_short!"
            FROM min_wait mw, poll_status ps
        ) AS result
        "#,
        n_jobs_to_poll as i32,
        sim_now,
        instance_id,
        pollable_types as _,
        wall_now,
        row_limits,
        headroom,
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
    /// The candidate window hit its LIMIT and still yielded fewer candidates
    /// than the budget -- the poll was bounded by the window, not by the work.
    candidates_short: bool,
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
    candidates_short: bool,
}

impl JobPollResult {
    /// The poll window, whichever variant this is.
    fn window(&self) -> &PollWindow {
        match self {
            JobPollResult::Jobs { window, .. } => window,
            JobPollResult::WaitTillNextJob(window) => window,
        }
    }

    /// Convert raw query rows into a JobPollResult. The min-wait row (`id`
    /// NULL) is present in every result set; row order is not assumed.
    pub fn from_rows(rows: Vec<JobPollRow>) -> Self {
        let mut jobs = Vec::with_capacity(rows.len());
        let mut window = PollWindow {
            next_due_at: None,
            may_have_more: false,
            candidates_short: false,
        };
        for row in rows {
            window.may_have_more = row.may_have_more;
            window.candidates_short = row.candidates_short;
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

    /// A backlogged capped type must NOT be able to consume another type's
    /// admission budget.
    ///
    /// This is the regression that the per-type budget replaced the old
    /// `n_claim * 4` overscan window to fix: previously every pollable type
    /// competed for one shared, globally-ordered candidate window, so type A's
    /// 10 older rows filled it completely and type B's due row was never even
    /// seen — the poll returned only A's single capped row and left B starved
    /// behind it. Each type now scans under its own budget, so both are
    /// claimed in the same poll.
    #[tokio::test]
    async fn capped_type_backlog_does_not_starve_another_type() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let instance_id = uuid::Uuid::now_v7();
        let type_a = format!("budget-dominant-{}", uuid::Uuid::now_v7());
        let type_b = format!("budget-starved-{}", uuid::Uuid::now_v7());

        let n_jobs_to_poll = 2usize;

        // A: 10 due rows, ALL older than B's single row.
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
            CONTENTION_HEADROOM,
            &clock,
        )
        .await?;

        match result {
            JobPollResult::Jobs { jobs, .. } => {
                let claimed: std::collections::HashSet<JobId> = jobs.iter().map(|j| j.id).collect();
                assert!(
                    claimed.contains(&b_id),
                    "B's due row must be claimed: A's older backlog can no \
                     longer consume the window B's budget entitles it to"
                );
                assert_eq!(
                    claimed.iter().filter(|id| a_ids.contains(id)).count(),
                    1,
                    "A is capped at 1 and must claim exactly one row"
                );
                assert_eq!(claimed.len(), 2, "one row per type, both claimed");
            }
            other => panic!("expected a Jobs claim, got {other:?}"),
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
    async fn seed_queued_job(
        pool: &PgPool,
        job_type: &str,
        queue_id: &str,
        execute_at: DateTime<Utc>,
        state: &str,
    ) -> anyhow::Result<JobId> {
        let id = JobId::new();
        let uuid = uuid::Uuid::from(id);
        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO jobs (id, job_type, queue_id, created_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(uuid)
        .bind(job_type)
        .bind(queue_id)
        .bind(now)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, queue_id, state, attempt_index, execute_at, alive_at, \
              poller_instance_id, created_at) \
             VALUES ($1, $2, $3, $4::JobExecutionState, 1, \
                     CASE WHEN $4 = 'running' THEN NULL ELSE $5 END, $6, \
                     CASE WHEN $4 = 'running' THEN gen_random_uuid() END, $7)",
        )
        .bind(uuid)
        .bind(job_type)
        .bind(queue_id)
        .bind(state)
        .bind(execute_at)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await?;
        Ok(id)
    }

    /// A blocked queue's backlog must not consume the admission budget.
    ///
    /// This is the zero-claim cliff, and it is the reason the blocked-queue
    /// anti-join sits INSIDE the candidate window rather than after its LIMIT.
    /// A queue with a job already running yields nothing no matter how many
    /// due rows it holds, so if its rows are allowed to fill the window first
    /// and get filtered afterwards, a single deep queue starves every other
    /// queue — and worse, it re-fills the window on the next poll, so the
    /// poller spins at zero yield. See PERFORMANCE.md, "Queued rows: two
    /// steps, both bounded by the budget".
    #[tokio::test]
    async fn blocked_queue_backlog_does_not_consume_the_budget() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let instance_id = uuid::Uuid::now_v7();
        let job_type = format!("cliff-{}", uuid::Uuid::now_v7());
        let hot_queue = format!("hot-{}", uuid::Uuid::now_v7());

        let n_jobs_to_poll = 2usize;
        let base = chrono::Utc::now() - chrono::Duration::seconds(3600);

        // The blocked queue already has a job running, and holds far more due
        // rows than the whole candidate window (n * CONTENTION_HEADROOM), all
        // of them OLDER than the claimable work below.
        seed_queued_job(&pool, &job_type, &hot_queue, base, "running").await?;
        for i in 0..(n_jobs_to_poll as i64 * CONTENTION_HEADROOM as i64 * 3) {
            seed_queued_job(
                &pool,
                &job_type,
                &hot_queue,
                base + chrono::Duration::seconds(i),
                "pending",
            )
            .await?;
        }

        // Two ordinary queues, younger than every row above.
        let recent = chrono::Utc::now() - chrono::Duration::seconds(1);
        let mut claimable = Vec::new();
        for _ in 0..2 {
            let q = format!("cold-{}", uuid::Uuid::now_v7());
            claimable.push(seed_queued_job(&pool, &job_type, &q, recent, "pending").await?);
        }

        let pollable_types = vec![JobType::from_owned(job_type.clone())];
        let row_limits = vec![n_jobs_to_poll as i32];
        let clock = ClockHandle::realtime();

        let result = poll_jobs(
            &pool,
            n_jobs_to_poll,
            instance_id,
            &pollable_types,
            &row_limits,
            CONTENTION_HEADROOM,
            &clock,
        )
        .await?;

        match result {
            JobPollResult::Jobs { jobs, .. } => {
                let claimed: std::collections::HashSet<JobId> = jobs.iter().map(|j| j.id).collect();
                assert_eq!(
                    claimed.len(),
                    2,
                    "the blocked queue's backlog must not crowd out claimable queues"
                );
                for id in &claimable {
                    assert!(
                        claimed.contains(id),
                        "every unblocked queue head is claimed"
                    );
                }
            }
            other => panic!("expected a Jobs claim, got {other:?}"),
        }

        Ok(())
    }

    /// A queue's head is resolved WITHOUT reference to which types this
    /// instance can currently poll — the exclusion-critical property.
    ///
    /// Instances saturate different types at different moments, so `$4` and
    /// `limits` are instance-local. If the head were picked as "oldest
    /// *pollable* row", an instance with type X saturated would pick a queue's
    /// X-blocked second row while a peer picked its first, and both would
    /// claim from one queue. Picking the oldest row outright and only then
    /// dropping it if its type is unpollable makes a peer's lock read as "this
    /// queue is taken" rather than "try the next row down".
    #[tokio::test]
    async fn queue_head_is_resolved_independently_of_saturated_types() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let saturated = format!("sat-{}", uuid::Uuid::now_v7());
        let pollable = format!("poll-{}", uuid::Uuid::now_v7());
        let queue = format!("shared-{}", uuid::Uuid::now_v7());

        let base = chrono::Utc::now() - chrono::Duration::seconds(600);
        // The queue's HEAD belongs to the type this instance cannot poll.
        let head = seed_queued_job(&pool, &saturated, &queue, base, "pending").await?;
        // Behind it, a row this instance could otherwise run.
        let behind = seed_queued_job(
            &pool,
            &pollable,
            &queue,
            base + chrono::Duration::seconds(1),
            "pending",
        )
        .await?;

        // Only the pollable type is offered — exactly what `plan_claim`
        // produces once `saturated` is at its cap.
        let pollable_types = vec![JobType::from_owned(pollable.clone())];
        let row_limits = vec![4i32];
        let clock = ClockHandle::realtime();

        let result = poll_jobs(
            &pool,
            4,
            uuid::Uuid::now_v7(),
            &pollable_types,
            &row_limits,
            CONTENTION_HEADROOM,
            &clock,
        )
        .await?;

        let claimed: std::collections::HashSet<JobId> = match result {
            JobPollResult::Jobs { ref jobs, .. } => jobs.iter().map(|j| j.id).collect(),
            JobPollResult::WaitTillNextJob(_) => Default::default(),
        };
        assert!(
            !claimed.contains(&behind),
            "claiming past an unpollable head would let a peer that CAN poll \
             that head claim the same queue concurrently"
        );
        assert!(
            !claimed.contains(&head),
            "the head's type is not pollable here"
        );

        Ok(())
    }

    /// Rows sharing an `execute_at` — which bulk spawns produce by the batch —
    /// must still resolve one stable head per queue, whatever admission budget
    /// the poll is running with.
    #[tokio::test]
    async fn tied_execute_at_resolves_one_stable_queue_head() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("tie-{}", uuid::Uuid::now_v7());
        let queue = format!("tied-{}", uuid::Uuid::now_v7());

        // One queue, several rows, all sharing an execute_at to the microsecond.
        let tied = chrono::Utc::now() - chrono::Duration::seconds(600);
        let mut ids = Vec::new();
        for _ in 0..8 {
            ids.push(seed_queued_job(&pool, &job_type, &queue, tied, "pending").await?);
        }
        // Younger filler queues, so the window boundary lands inside the tie
        // group for the smaller budgets.
        for _ in 0..40 {
            let q = format!("filler-{}", uuid::Uuid::now_v7());
            seed_queued_job(
                &pool,
                &job_type,
                &q,
                tied + chrono::Duration::seconds(1),
                "pending",
            )
            .await?;
        }

        let pollable_types = vec![JobType::from_owned(job_type.clone())];
        let clock = ClockHandle::realtime();
        let expected = ids.iter().copied().min().expect("seeded rows");

        for n in [1usize, 3, 10] {
            let result = poll_jobs(
                &pool,
                n,
                uuid::Uuid::now_v7(),
                &pollable_types,
                &[n as i32],
                CONTENTION_HEADROOM,
                &clock,
            )
            .await?;
            let JobPollResult::Jobs { jobs, .. } = result else {
                panic!("expected a claim at budget {n}");
            };
            let from_queue: Vec<JobId> = jobs
                .iter()
                .map(|j| j.id)
                .filter(|id| ids.contains(id))
                .collect();
            assert_eq!(
                from_queue.len(),
                1,
                "budget {n} must take exactly one row from the tied queue"
            );
            assert_eq!(
                from_queue[0], expected,
                "budget {n} must resolve the same head as every other budget"
            );
            sqlx::query(
                "UPDATE job_executions SET state = 'pending', execute_at = $2, \
                 poller_instance_id = NULL WHERE id = $1",
            )
            .bind(uuid::Uuid::from(from_queue[0]))
            .bind(tied)
            .execute(&pool)
            .await?;
        }

        Ok(())
    }
    /// Deep queues under-fill a ROW-bounded window, and the poll must say so.
    ///
    /// The window admits `n_claim * headroom` rows but candidates are queues,
    /// so its yield is divided by the depth of the queues it lands on. At
    /// depth > headroom the poll comes back short with claimable work sitting
    /// just past the window — the one case where widening is the only thing
    /// that helps, and the one `candidates_short` has to identify. Widening is
    /// then verified to actually recover the budget. See PERFORMANCE.md,
    /// "Contention headroom".
    #[tokio::test]
    async fn deep_queues_report_a_short_window_and_widening_recovers_it() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("depth-{}", uuid::Uuid::now_v7());
        let n_jobs_to_poll = 4usize;
        // Depth per queue is double the resting headroom, so a window of
        // `n * CONTENTION_HEADROOM` rows lands on half as many queues as the
        // budget wants. Every queue is unblocked — a blocked one contributes
        // no rows at all, so depth there costs nothing.
        let depth = (CONTENTION_HEADROOM * 2) as i64;
        let base = chrono::Utc::now() - chrono::Duration::seconds(3600);
        // Rows of a queue are CONSECUTIVE in execute_at order: the clustering
        // is what makes depth bite. Scattered rows draw from many queues and
        // the window saturates regardless of depth.
        let mut seq = 0i64;
        for q in 0..(n_jobs_to_poll as i64 * 2) {
            let queue = format!("deep-{q}-{}", uuid::Uuid::now_v7());
            for _ in 0..depth {
                seed_queued_job(
                    &pool,
                    &job_type,
                    &queue,
                    base + chrono::Duration::milliseconds(seq),
                    "pending",
                )
                .await?;
                seq += 1;
            }
        }

        let pollable_types = vec![JobType::from_owned(job_type.clone())];
        let row_limits = vec![n_jobs_to_poll as i32];
        let clock = ClockHandle::realtime();

        let narrow = poll_jobs(
            &pool,
            n_jobs_to_poll,
            uuid::Uuid::now_v7(),
            &pollable_types,
            &row_limits,
            CONTENTION_HEADROOM,
            &clock,
        )
        .await?;
        let JobPollResult::Jobs { jobs, window } = narrow else {
            panic!("expected a claim at the resting headroom");
        };
        assert!(
            jobs.len() < n_jobs_to_poll,
            "a window of {} rows over depth-{depth} queues cannot fill a \
             budget of {n_jobs_to_poll}",
            n_jobs_to_poll as i32 * CONTENTION_HEADROOM
        );
        assert!(
            window.candidates_short,
            "the poll must report that the WINDOW bound it, not the work"
        );

        // Release the claim and re-poll wider: the same data must now saturate.
        sqlx::query(
            "UPDATE job_executions SET state = 'pending', execute_at = $2, \
             poller_instance_id = NULL WHERE state = 'running' AND job_type = $1",
        )
        .bind(&job_type)
        .bind(base)
        .execute(&pool)
        .await?;

        let wide = poll_jobs(
            &pool,
            n_jobs_to_poll,
            uuid::Uuid::now_v7(),
            &pollable_types,
            &row_limits,
            MAX_CONTENTION_HEADROOM,
            &clock,
        )
        .await?;
        let JobPollResult::Jobs { jobs, window } = wide else {
            panic!("expected a claim at the widened headroom");
        };
        assert_eq!(
            jobs.len(),
            n_jobs_to_poll,
            "widening the window must recover the budget"
        );
        assert!(
            !window.candidates_short,
            "a saturated poll must drop the headroom back to resting"
        );

        Ok(())
    }
    /// A window filled entirely by rows behind SATURATED heads must not read
    /// as "nothing more to see".
    ///
    /// Step 1 admits rows of pollable types, but step 2 resolves each queue's
    /// head type-agnostically and drops it when that head belongs to a type
    /// this instance has saturated. Those queues therefore consume window
    /// slots and yield nothing, and if enough of them do it the window fills
    /// having read only a PREFIX of the due pollable rows — claimable heads
    /// past it are unseen. They are already due, so `next_due_at` does not
    /// cover them, and sleeping here strands them until an unrelated wake.
    ///
    /// `candidates_short` is what keeps the loop awake: `poll_and_dispatch`
    /// re-polls while it can still widen. See PERFORMANCE.md, "When the poller
    /// may sleep".
    #[tokio::test]
    async fn window_full_of_saturated_heads_does_not_read_as_exhausted() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let saturated = format!("sat-{}", uuid::Uuid::now_v7());
        let pollable = format!("poll-{}", uuid::Uuid::now_v7());
        let n_jobs_to_poll = 2usize;

        // Every queue: an unpollable head, then a pollable row behind it. The
        // pollable rows are what enter the window; none of them can be claimed
        // because each queue's head is the saturated type.
        let base = chrono::Utc::now() - chrono::Duration::seconds(3600);
        let mut seq = 0i64;
        let queues = (n_jobs_to_poll as i32 * CONTENTION_HEADROOM) as i64;
        for q in 0..queues {
            let queue = format!("blocked-head-{q}-{}", uuid::Uuid::now_v7());
            seed_queued_job(
                &pool,
                &saturated,
                &queue,
                base + chrono::Duration::milliseconds(seq),
                "pending",
            )
            .await?;
            seq += 1;
            seed_queued_job(
                &pool,
                &pollable,
                &queue,
                base + chrono::Duration::milliseconds(seq),
                "pending",
            )
            .await?;
            seq += 1;
        }

        // Only the pollable type is offered — `plan_claim`'s output once
        // `saturated` is at its cap.
        let pollable_types = vec![JobType::from_owned(pollable.clone())];
        let row_limits = vec![n_jobs_to_poll as i32];
        let clock = ClockHandle::realtime();

        let result = poll_jobs(
            &pool,
            n_jobs_to_poll,
            uuid::Uuid::now_v7(),
            &pollable_types,
            &row_limits,
            CONTENTION_HEADROOM,
            &clock,
        )
        .await?;

        let window = result.window();
        // The query alone WOULD sleep here: it saw no claimable head, and
        // `may_have_more` is deliberately false so that re-polling at the same
        // width cannot spin at zero yield.
        assert!(
            !window.may_have_more,
            "re-polling at the same width would spin at zero yield"
        );
        // `candidates_short` is the whole safety net -- `poll_and_dispatch`
        // ORs it into the sleep decision while the width can still grow. If
        // this ever goes false, the loop sleeps on `next_due_at` while
        // already-due claimable work sits just past the window.
        assert!(
            window.candidates_short,
            "a full window that yielded no claimable head must say so, or the \
             loop sleeps on next_due_at while already-due work sits past it"
        );
        match result {
            JobPollResult::WaitTillNextJob(_) => {}
            JobPollResult::Jobs { ref jobs, .. } => {
                assert!(
                    jobs.is_empty(),
                    "every head here belongs to a saturated type"
                );
            }
        }

        Ok(())
    }
}
