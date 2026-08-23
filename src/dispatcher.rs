use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use futures::FutureExt;
use serde_json::Value as JsonValue;
use tracing::{Span, instrument};

use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Weak},
};

use super::{
    JobId,
    current::CurrentJob,
    entity::{Job, JobType},
    error::JobError,
    finalizer::{ClaimDisposition, Disposition, Finalizer},
    notifier::JobEventNotifier,
    poller::JobPoller,
    repo::JobRepo,
    runner::*,
    tracker::{JobTracker, UnitReservation},
};

#[derive(Debug)]
pub struct PolledJob {
    pub id: JobId,
    pub data_json: Option<JsonValue>,
    pub attempt: u32,
    /// The queue this row was claimed from. At most one row per queue is
    /// claimed per poll, so this doubles as the canonical ordering key when
    /// forming batches.
    pub queue_id: Option<String>,
}

pub(crate) struct JobDispatcher {
    /// Reaches this process's poller for the head-swap completion-recycle
    /// claim (`recycle_into_claim`). `Weak` so a dispatcher never keeps
    /// the poller alive on its own -- mirrors `spawner.rs`'s `PollerHandle`.
    poller: Weak<JobPoller>,
    repo: Arc<JobRepo>,
    retry_settings: RetrySettings,
    runner: Option<Box<dyn JobRunner>>,
    tracker: Arc<JobTracker>,
    job_type: JobType,
    rescheduled: bool,
    dispatched: bool,
    /// Whether this job's unit has already been handed to a recycle
    /// reservation. See [`Self::recycle_into_claim`] -- it must happen at
    /// most once, including across [`Finalizer::finalize`]'s retried
    /// attempts.
    recycled: bool,
    id: JobId,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
    finalizer: Finalizer,
}
impl JobDispatcher {
    /// Claims the type's per-process slot **synchronously**, at construction.
    ///
    /// The poller builds this before spawning the execution task, so the very
    /// next poll already sees the slot occupied. Doing it inside
    /// `execute_job` instead leaves a window in which the poll loop re-polls
    /// — immediately, since a non-empty poll returns `Duration::ZERO` — still
    /// reads zero units in flight, and claims a second round of rows: exactly
    /// the over-claiming a per-process cap exists to prevent. Mirrors
    /// `BatchDispatcher::new`. The slot is released by `Drop`, so it cannot
    /// leak once taken.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        poller: Weak<JobPoller>,
        repo: Arc<JobRepo>,
        tracker: Arc<JobTracker>,
        notifier: Arc<JobEventNotifier>,
        retry_settings: RetrySettings,
        id: JobId,
        job_type: JobType,
        retains_state: bool,
        runner: Box<dyn JobRunner>,
        instance_id: uuid::Uuid,
        clock: ClockHandle,
    ) -> Self {
        tracker.dispatch_job(id, &job_type);
        let finalizer = Finalizer::new(
            poller.clone(),
            repo.clone(),
            notifier,
            retry_settings.clone(),
            retains_state,
            instance_id,
            clock.clone(),
        );
        Self {
            poller,
            repo,
            retry_settings,
            runner: Some(runner),
            tracker,
            job_type,
            rescheduled: false,
            dispatched: true,
            recycled: false,
            id,
            instance_id,
            clock,
            finalizer,
        }
    }

    /// Build from an already-taken [`UnitReservation`] (the head-swap
    /// short-circuit fast path): the reservation already accounted for this
    /// unit, so this consumes it via [`UnitReservation::into_live`] instead
    /// of calling `tracker.dispatch_job` a second time.
    #[allow(clippy::too_many_arguments)]
    pub fn from_reservation(
        reservation: UnitReservation,
        poller: Weak<JobPoller>,
        repo: Arc<JobRepo>,
        tracker: Arc<JobTracker>,
        notifier: Arc<JobEventNotifier>,
        retry_settings: RetrySettings,
        id: JobId,
        job_type: JobType,
        retains_state: bool,
        runner: Box<dyn JobRunner>,
        instance_id: uuid::Uuid,
        clock: ClockHandle,
    ) -> Self {
        reservation.into_live(id);
        let finalizer = Finalizer::new(
            poller.clone(),
            repo.clone(),
            notifier,
            retry_settings.clone(),
            retains_state,
            instance_id,
            clock.clone(),
        );
        Self {
            poller,
            repo,
            retry_settings,
            runner: Some(runner),
            tracker,
            job_type,
            rescheduled: false,
            dispatched: true,
            recycled: false,
            id,
            instance_id,
            clock,
            finalizer,
        }
    }

    /// Detach this dispatcher's unit from the ordinary Drop-triggered
    /// release: [`Self::recycle_into_claim`] is handing the about-to-be-freed
    /// unit to [`JobTracker::recycle`] instead of releasing it plainly.
    fn recycle_unit(&mut self) {
        self.dispatched = false;
        self.tracker
            .mark_finished_without_releasing_unit(&[self.id]);
    }

    #[instrument(name = "job.execute_job", skip_all,
        fields(job_id, job_type, attempt, error, error.level, error.message, conclusion, now,
               claim_disposition)
    )]
    #[cfg_attr(feature = "es-entity", es_entity::es_event_context)]
    pub async fn execute_job(
        mut self,
        job: Job,
        polled_job: PolledJob,
        shutdown_rx: tokio::sync::broadcast::Receiver<
            tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
        >,
    ) -> Result<(), JobError> {
        let span = Span::current();
        span.record("job_id", tracing::field::display(job.id));
        span.record("job_type", tracing::field::display(&job.job_type));
        span.record("poller_id", tracing::field::display(self.instance_id));
        span.record("attempt", polled_job.attempt);
        span.record("now", tracing::field::display(self.clock.now()));
        job.inject_tracing_parent();
        #[cfg(feature = "es-entity")]
        {
            let mut ctx = es_entity::EventContext::current();
            ctx.insert(
                "job",
                &serde_json::json!({
                    "job_id": job.id,
                    "job_type": job.job_type,
                    "attempt": polled_job.attempt,
                    "poller_id": self.instance_id
                }),
            )
            .expect("EventContext insert job data");
        }
        let current_job = CurrentJob::new(
            polled_job.id,
            polled_job.attempt,
            self.repo.pool().clone(),
            polled_job.data_json,
            shutdown_rx,
            self.clock.clone(),
            Arc::clone(&self.repo),
        );
        let runner = self.runner.take().expect("runner");
        let completion = Self::dispatch_job(&self.finalizer, runner, current_job).await;
        let disposition: Result<(), JobError> = async {
            match completion {
                Err(e) => {
                    span.record(
                        "conclusion",
                        if matches!(e, JobError::PoolCongestion(_)) {
                            "Congestion"
                        } else {
                            "Error"
                        },
                    );
                    self.fail_job(job.id, e, polled_job.attempt).await?
                }
                Ok(JobCompletion::Complete) => {
                    span.record("conclusion", "Complete");
                    let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
                    self.complete_job(&mut op, job.id).await?;
                    op.commit().await?;
                }
                #[cfg(feature = "es-entity")]
                Ok(JobCompletion::CompleteWithOp(mut op)) => {
                    span.record("conclusion", "CompleteWithOp");
                    self.complete_job(&mut op, job.id).await?;
                    op.commit().await?;
                }
                Ok(JobCompletion::CompleteWithTx(mut tx)) => {
                    span.record("conclusion", "CompleteWithTx");
                    self.complete_job(&mut tx, job.id).await?;
                    tx.commit().await?;
                }
                Ok(JobCompletion::RescheduleNow) => {
                    span.record("conclusion", "RescheduleNow");
                    let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
                    let t = op.maybe_now().unwrap_or_else(|| self.clock.now());
                    self.reschedule_job(&mut op, job.id, t).await?;
                    op.commit().await?;
                }
                #[cfg(feature = "es-entity")]
                Ok(JobCompletion::RescheduleNowWithOp(mut op)) => {
                    span.record("conclusion", "RescheduleNowWithOp");
                    let t = op.maybe_now().unwrap_or_else(|| self.clock.now());
                    self.reschedule_job(&mut op, job.id, t).await?;
                    op.commit().await?;
                }
                Ok(JobCompletion::RescheduleNowWithTx(mut tx)) => {
                    span.record("conclusion", "RescheduleNowWithTx");
                    let t = self.clock.now();
                    self.reschedule_job(&mut tx, job.id, t).await?;
                    tx.commit().await?;
                }
                Ok(JobCompletion::RescheduleIn(d)) => {
                    span.record("conclusion", "RescheduleIn");
                    let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
                    let t = op.maybe_now().unwrap_or_else(|| self.clock.now());
                    let t = t + d;
                    self.reschedule_job(&mut op, job.id, t).await?;
                    op.commit().await?;
                }
                #[cfg(feature = "es-entity")]
                Ok(JobCompletion::RescheduleInWithOp(mut op, d)) => {
                    span.record("conclusion", "RescheduleInWithOp");
                    let t = op.maybe_now().unwrap_or_else(|| self.clock.now());
                    let t = t + d;
                    self.reschedule_job(&mut op, job.id, t).await?;
                    op.commit().await?;
                }
                Ok(JobCompletion::RescheduleInWithTx(mut tx, d)) => {
                    span.record("conclusion", "RescheduleInWithOp");
                    let t = self.clock.now() + d;
                    self.reschedule_job(&mut tx, job.id, t).await?;
                    tx.commit().await?;
                }
                Ok(JobCompletion::RescheduleAt(t)) => {
                    span.record("conclusion", "RescheduleAt");
                    let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
                    self.reschedule_job(&mut op, job.id, t).await?;
                    op.commit().await?;
                }
                #[cfg(feature = "es-entity")]
                Ok(JobCompletion::RescheduleAtWithOp(mut op, t)) => {
                    span.record("conclusion", "RescheduleAtWithOp");
                    self.reschedule_job(&mut op, job.id, t).await?;
                    op.commit().await?;
                }
                Ok(JobCompletion::RescheduleAtWithTx(mut tx, t)) => {
                    span.record("conclusion", "RescheduleAtWithTx");
                    self.reschedule_job(&mut tx, job.id, t).await?;
                    tx.commit().await?;
                }
            }
            Ok(())
        }
        .await;

        // Every disposition failure funnels here, and every one of them
        // leaves this job's row still `running` under this instance --
        // mirror of `BatchDispatcher::execute_batch`'s rescue funnel and
        // for the same reason: without handing the row back, it sits
        // frozen until the lost-handler notices it a full
        // `job_lost_interval` later. Hand it back instead, so the
        // lost-handler is a true backstop rather than the primary
        // recovery path.
        if let Err(e) = disposition {
            let claim_disposition = self.rescue_claimed_row().await;
            span.record(
                "claim_disposition",
                tracing::field::display(claim_disposition),
            );
            tracing::error!(
                job_id = %self.id,
                job_type = %self.job_type,
                exception.message = %e,
                exception.type = std::any::type_name_of_val(&e),
                claim_disposition = %claim_disposition,
                "job disposition failed"
            );
            return Err(e);
        }
        Ok(())
    }

    /// Best-effort last resort when a disposition write itself failed: hand
    /// the claimed row back as `pending` with `execute_at = now` and
    /// `attempt_index = 1` ([`Disposition::Fresh`] -- "we don't know what
    /// happened, start fresh"), so the next poll re-dispatches it. The
    /// solo counterpart of `BatchDispatcher::rescue_claimed_rows`.
    async fn rescue_claimed_row(&mut self) -> ClaimDisposition {
        let items = [(
            self.id,
            Disposition::Fresh {
                at: self.clock.now(),
            },
        )];
        match self.finalizer.clone().finalize(&items, |_, _| {}).await {
            // The row write filters on `poller_instance_id`, so a job whose
            // row was already dispositioned matches nothing and still lands
            // here -- harmless either way.
            Ok(_) => {
                self.rescheduled = true;
                ClaimDisposition::Rescheduled
            }
            Err(e) => {
                tracing::error!(
                    job_id = %self.id,
                    job_type = %self.job_type,
                    exception.message = %e,
                    exception.type = std::any::type_name_of_val(&e),
                    "could not release the claimed row; it stays running until \
                     the lost-handler reclaims it"
                );
                ClaimDisposition::Leaked
            }
        }
    }

    async fn dispatch_job(
        finalizer: &Finalizer,
        runner: Box<dyn JobRunner>,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, JobError> {
        match AssertUnwindSafe(runner.run(current_job))
            .catch_unwind()
            .await
        {
            Ok(Ok(completion)) => Ok(completion),
            Ok(Err(e)) => Err(finalizer.maybe_reclassify(e)),
            Err(panic) => {
                let span = Span::current();
                let message = if let Some(s) = panic.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic payload".to_string()
                };

                span.record("error", true);
                span.record(
                    "error.message",
                    tracing::field::display(&format!("Panic: {message}")),
                );
                span.record(
                    "error.level",
                    tracing::field::display(tracing::Level::ERROR),
                );

                tracing::error!(
                    target: "job.panic",
                    panic_message = %message,
                    panic_backtrace = ?std::backtrace::Backtrace::capture(),
                    "Job panicked during execution"
                );

                Err(JobError::JobExecutionError(format!(
                    "Job panicked: {message}"
                )))
            }
        }
    }

    #[instrument(
        name = "job.fail_job",
        skip(self),
        fields(
            job_id = tracing::field::Empty,
            job_type = tracing::field::Empty,
            poller_id = tracing::field::Empty,
            attempt,
            will_retry = tracing::field::Empty,
            error = tracing::field::Empty,
            error.level = tracing::field::Empty,
            error.message = tracing::field::Empty
        )
    )]
    async fn fail_job(&mut self, id: JobId, error: JobError, attempt: u32) -> Result<(), JobError> {
        let span = Span::current();
        span.record("job_id", tracing::field::display(id));
        span.record("job_type", tracing::field::display(&self.job_type));
        span.record("poller_id", tracing::field::display(self.instance_id));

        let error_str = match error {
            JobError::PoolCongestion(message) => {
                self.rescheduled = true;
                return self
                    .finalizer
                    .reschedule_congested_one(id, attempt, message)
                    .await;
            }
            other => other.to_string(),
        };
        span.record("error", true);
        span.record("error.message", tracing::field::display(&error_str));

        let items = [(
            id,
            Disposition::Fail {
                error: error_str,
                attempt,
            },
        )];
        let finalizer = self.finalizer.clone();
        let outcome = finalizer
            .finalize(&items, |op, outcome| {
                // The exhausted-retries terminal delete frees this job's
                // unit -- recycle it into an immediate same-type claim,
                // exactly like a completion does.
                if !outcome.errored_terminal.is_empty() {
                    self.recycle_into_claim(op);
                }
            })
            .await?;

        if let Some((_, next_attempt)) = outcome.retried.first() {
            let exceeded_warn_attempts = self
                .retry_settings
                .n_warn_attempts
                .is_some_and(|limit| *next_attempt > limit);
            let level = if exceeded_warn_attempts {
                tracing::Level::ERROR
            } else {
                tracing::Level::WARN
            };
            span.record("error.level", tracing::field::display(level));
            span.record("will_retry", true);
            self.rescheduled = true;
        } else {
            span.record(
                "error.level",
                tracing::field::display(tracing::Level::ERROR),
            );
            span.record("will_retry", false);
        }
        Ok(())
    }

    /// Hand this job's about-to-free unit of capacity to a `ClaimHook` so
    /// it can be spent immediately on this SAME type's oldest due backlog
    /// -- registered on `op`, claimed at its pre-commit (subject to the
    /// pool-aware budget gate there). `ClaimHook` runs after
    /// `PromoteHeadsHook` (its `RUNS_AFTER`), so a sibling the finalizer
    /// promoted on this same op is already claimable by the short-circuit.
    ///
    /// "At most once" has to hold across [`Finalizer::finalize`]'s retried
    /// attempts, not just call sites: a rolled-back attempt drops its
    /// `UnitReservation`, and dropping one *releases* the unit -- handing
    /// out a second reservation for the same unit would release it twice
    /// and hand the tracker capacity it does not have. A retried attempt
    /// therefore forfeits the recycle optimisation, which costs at most one
    /// extra poll: `recycle_unit` has already cleared `dispatched`, so
    /// `Drop` correctly declines to release the unit a second time.
    /// Mirrors `BatchDispatcher::try_recycle_own_type`.
    fn recycle_into_claim(&mut self, op: &mut impl es_entity::AtomicOperation) {
        if self.recycled {
            return;
        }
        let Some(poller) = self.poller.upgrade() else {
            return;
        };
        self.recycled = true;
        self.recycle_unit();
        let reservation = self.tracker.recycle(&self.job_type);
        poller.register_claim_recycle(op, &self.job_type, reservation);
    }

    /// Complete via [`Disposition::Complete`] on the caller's op (the
    /// runner's own transaction for the `*WithOp`/`*WithTx` completions).
    /// The finalizer deletes the execution row, promotes the freed queue's
    /// oldest parked sibling, and emits the terminal notification; the
    /// freed unit recycles into an immediate same-type claim when the row
    /// was actually this instance's to delete.
    #[instrument(name = "job.complete_job", skip(self, op), fields(id = %id))]
    async fn complete_job(
        &mut self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
    ) -> Result<(), JobError> {
        let items = [(id, Disposition::Complete)];
        let outcome = self.finalizer.finalize_in_op(op, &items).await?;
        if !outcome.completed.is_empty() {
            self.recycle_into_claim(op);
        }
        Ok(())
    }

    /// Runner-requested reschedule via [`Disposition::Fresh`] on the
    /// caller's op: back to `pending` at `reschedule_at` with
    /// `attempt_index = 1` (an explicit reschedule has no notion of "which
    /// attempt"), plus the finalizer's invariant-B promote registration.
    #[instrument(name = "job.reschedule_job", skip(self, op), fields(id = %id, reschedule_at = %reschedule_at, attempt = 1))]
    async fn reschedule_job(
        &mut self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
        reschedule_at: DateTime<Utc>,
    ) -> Result<(), JobError> {
        self.rescheduled = true;
        let items = [(id, Disposition::Fresh { at: reschedule_at })];
        self.finalizer.finalize_in_op(op, &items).await?;
        Ok(())
    }
}

impl Drop for JobDispatcher {
    fn drop(&mut self) {
        if self.dispatched {
            self.tracker
                .job_completed(self.id, &self.job_type, self.rescheduled);
        }
    }
}
