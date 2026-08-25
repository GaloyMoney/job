//! Claimed row -> running task. Plain jobs dispatch one task each; a
//! batched type's claims from one poll are split into canonical-order
//! batches (sorted by `queue_id`, job id when unqueued, so concurrent
//! batch transactions reach shared domain rows in one order) with retries
//! always batched alone -- a poisonous job shares its first failure with
//! its batch-mates, never a second. Dispatchers are built synchronously
//! with the poll loop, not inside the spawned task: construction claims
//! the type's tracker slot (which the very next poll's plan reads), and
//! the shutdown subscription must exist before any broadcast (a
//! `tokio::sync::broadcast` never delivers to late subscribers). Each task
//! pairs the execution future with a monitor that acks shutdown and grants
//! the drain timeout; the `_from_reservation` entry points are the
//! short-circuit path's (see `hook`), dispatching through an
//! already-taken reservation.

use tracing::{Span, instrument};

use std::collections::HashMap;
use std::sync::Arc;

use super::hook::ClaimedRow;
use crate::{
    JobId,
    batch_dispatcher::BatchDispatcher,
    batched::{RawBatchItem, ShutdownRx},
    dispatcher::*,
    entity::{Job, JobType},
    error::JobError,
    tracker::UnitReservation,
};

use super::JobPoller;

/// The two independently-subscribed shutdown receivers one dispatch task
/// needs; the short-circuit path must obtain these before its claiming
/// transaction commits (see `hook`).
pub(super) struct ShutdownSubs {
    pub(super) job: ShutdownRx,
    pub(super) monitor: ShutdownRx,
}

impl JobPoller {
    pub(super) async fn load_and_dispatch_claimed(
        self: &Arc<Self>,
        rows: Vec<PolledJob>,
    ) -> Result<(), JobError> {
        let ids: Vec<JobId> = rows.iter().map(|row| row.id).collect();
        let mut entities = self.repo.find_all::<Job>(&ids).await?;
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
        Ok(())
    }

    #[instrument(
        name = "job.dispatch_batches",
        skip(self, items),
        fields(job_type = %job_type, n_items = items.len(), max_batch_size, n_batches)
    )]
    async fn dispatch_batches(
        self: &Arc<Self>,
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
        self: &Arc<Self>,
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
        span.record("now", tracing::field::display(self.clock.now()));
        span.record("poller_id", tracing::field::display(self.instance_id));

        let dispatcher = BatchDispatcher::new(
            Arc::downgrade(self),
            Arc::clone(&self.repo),
            Arc::clone(&self.tracker),
            Arc::clone(&self.notifier),
            retry_settings,
            job_type,
            runner,
            self.instance_id,
            self.clock.clone(),
            &items,
        );
        let subs = ShutdownSubs {
            job: self.shutdown_tx.subscribe(),
            monitor: self.shutdown_tx.subscribe(),
        };
        self.spawn_batch_dispatch_task(dispatcher, items, subs);
        Ok(())
    }

    pub(super) fn spawn_batch_dispatch_task(
        &self,
        dispatcher: BatchDispatcher,
        items: Vec<RawBatchItem>,
        subs: ShutdownSubs,
    ) {
        let job_type = dispatcher.job_type().clone();
        let ShutdownSubs {
            job: shutdown_rx_batch,
            monitor: mut shutdown_rx_monitor,
        } = subs;
        let shutdown_timeout = self.config.shutdown_timeout;
        let n_items = items.len();
        let dispatched_type = job_type.clone();
        #[cfg_attr(
            not(all(feature = "tokio-task-names", tokio_unstable)),
            allow(unused_variables)
        )]
        let task_name = format!("job-batch-{}-{}", job_type, n_items);

        spawn_named_task!(&task_name, async move {
            use tracing::Instrument;

            let batch_fut = dispatcher.execute_batch(items, shutdown_rx_batch);
            tokio::pin!(batch_fut);

            tokio::select! {
                // `execute_batch` emits `batch dispatcher error` itself.
                res = &mut batch_fut => {
                    let _ = res;
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
                                        let _ = res;
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
    }

    #[instrument(
        name = "job.dispatch_job",
        skip(self, job, polled_job),
        fields(job_id, job_type, poller_id, attempt, now)
    )]
    async fn dispatch_job(
        self: &Arc<Self>,
        job: Job,
        polled_job: PolledJob,
    ) -> Result<(), JobError> {
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
        let retains_state = self.registry.retains_state(&job.job_type);
        span.record("now", tracing::field::display(self.clock.now()));
        span.record("poller_id", tracing::field::display(self.instance_id));

        let dispatcher = JobDispatcher::new(
            Arc::downgrade(self),
            Arc::clone(&self.repo),
            Arc::clone(&self.tracker),
            Arc::clone(&self.notifier),
            retry_settings,
            job.id,
            job.job_type.clone(),
            retains_state,
            runner,
            self.instance_id,
            self.clock.clone(),
        );
        let subs = ShutdownSubs {
            job: self.shutdown_tx.subscribe(),
            monitor: self.shutdown_tx.subscribe(),
        };
        self.spawn_dispatch_task(dispatcher, job, polled_job, subs);
        Ok(())
    }

    pub(super) async fn dispatch_job_from_reservation(
        self: &Arc<Self>,
        reservation: UnitReservation,
        row: ClaimedRow,
        subs: ShutdownSubs,
    ) -> Result<(), JobError> {
        let job = self.repo.find_by_id(row.id).await?;
        let polled_job = PolledJob {
            id: row.id,
            data_json: row.data_json,
            attempt: row.attempt as u32,
            queue_id: row.queue_id,
        };
        let runner = self.registry.init_job(
            &job,
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            self.clock.clone(),
            Arc::clone(&self.notifier),
        )?;
        let retry_settings = self.registry.retry_settings(&job.job_type).clone();
        let retains_state = self.registry.retains_state(&job.job_type);
        let dispatcher = JobDispatcher::from_reservation(
            reservation,
            Arc::downgrade(self),
            Arc::clone(&self.repo),
            Arc::clone(&self.tracker),
            Arc::clone(&self.notifier),
            retry_settings,
            job.id,
            job.job_type.clone(),
            retains_state,
            runner,
            self.instance_id,
            self.clock.clone(),
        );
        self.spawn_dispatch_task(dispatcher, job, polled_job, subs);
        Ok(())
    }

    pub(super) async fn dispatch_batch_from_reservation(
        self: &Arc<Self>,
        reservation: UnitReservation,
        job_type: JobType,
        rows: Vec<ClaimedRow>,
        subs: ShutdownSubs,
    ) -> Result<(), JobError> {
        let ids: Vec<JobId> = rows.iter().map(|row| row.id).collect();
        let mut entities = self.repo.find_all::<Job>(&ids).await?;
        let mut items: Vec<RawBatchItem> = Vec::with_capacity(rows.len());
        for row in rows {
            let Some(job) = entities.remove(&row.id) else {
                tracing::error!(
                    job_id = %row.id,
                    "claimed job row has no entity; skipping dispatch"
                );
                continue;
            };
            items.push(RawBatchItem {
                attempt: row.attempt as u32,
                queue_id: row.queue_id,
                execution_state_json: row.data_json,
                job,
            });
        }
        if items.is_empty() {
            reservation.release();
            return Ok(());
        }
        let runner = self.registry.init_batch(
            &job_type,
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            self.clock.clone(),
            Arc::clone(&self.notifier),
        )?;
        let retry_settings = self.registry.retry_settings(&job_type).clone();
        let dispatcher = BatchDispatcher::from_reservation(
            reservation,
            Arc::downgrade(self),
            Arc::clone(&self.repo),
            Arc::clone(&self.tracker),
            Arc::clone(&self.notifier),
            retry_settings,
            job_type,
            runner,
            self.instance_id,
            self.clock.clone(),
            &items,
        );
        self.spawn_batch_dispatch_task(dispatcher, items, subs);
        Ok(())
    }

    pub(super) fn spawn_dispatch_task(
        &self,
        dispatcher: JobDispatcher,
        job: Job,
        polled_job: PolledJob,
        subs: ShutdownSubs,
    ) {
        let ShutdownSubs {
            job: shutdown_rx_job,
            monitor: mut shutdown_rx_monitor,
        } = subs;
        let shutdown_timeout = self.config.shutdown_timeout;
        let job_id = job.id;
        let job_type = job.job_type.clone();
        #[cfg_attr(
            not(all(feature = "tokio-task-names", tokio_unstable)),
            allow(unused_variables)
        )]
        let task_name = format!("job-{}-{}", job_type, job_id);

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
    }
}
