use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
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
    congestion::CongestionHandler,
    current::CurrentJob,
    entity::{Job, JobType, RetryPolicy},
    error::JobError,
    execution_hooks::PromoteHeadsHook,
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
    /// claim (`delete_execution_in_op`). `Weak` so a dispatcher never keeps
    /// the poller alive on its own -- mirrors `spawner.rs`'s `PollerHandle`.
    poller: Weak<JobPoller>,
    repo: Arc<JobRepo>,
    retry_settings: RetrySettings,
    runner: Option<Box<dyn JobRunner>>,
    tracker: Arc<JobTracker>,
    notifier: Arc<JobEventNotifier>,
    job_type: JobType,
    /// Whether this type keeps its execution state past terminal, i.e. it is
    /// keyed with `inherits_state`. See `delete_execution_in_op`.
    retains_state: bool,
    rescheduled: bool,
    dispatched: bool,
    id: JobId,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
    congestion: CongestionHandler,
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
        let congestion = CongestionHandler::new(
            poller.clone(),
            repo.clone(),
            notifier.clone(),
            instance_id,
            clock.clone(),
        );
        Self {
            poller,
            repo,
            retry_settings,
            runner: Some(runner),
            tracker,
            notifier,
            job_type,
            retains_state,
            rescheduled: false,
            dispatched: true,
            id,
            instance_id,
            clock,
            congestion,
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
        let congestion = CongestionHandler::new(
            poller.clone(),
            repo.clone(),
            notifier.clone(),
            instance_id,
            clock.clone(),
        );
        Self {
            poller,
            repo,
            retry_settings,
            runner: Some(runner),
            tracker,
            notifier,
            job_type,
            retains_state,
            rescheduled: false,
            dispatched: true,
            id,
            instance_id,
            clock,
            congestion,
        }
    }

    /// Detach this dispatcher's unit from the ordinary Drop-triggered
    /// release: `delete_execution_in_op` is handing the about-to-be-freed
    /// unit to [`JobTracker::recycle`] instead of releasing it plainly.
    fn recycle_unit(&mut self) {
        self.dispatched = false;
        self.tracker
            .mark_finished_without_releasing_unit(&[self.id]);
    }

    #[instrument(name = "job.execute_job", skip_all,
        fields(job_id, job_type, attempt, error, error.level, error.message, conclusion, now)
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
        match Self::dispatch_job(&self.congestion, runner, current_job).await {
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

    async fn dispatch_job(
        congestion: &CongestionHandler,
        runner: Box<dyn JobRunner>,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, JobError> {
        match AssertUnwindSafe(runner.run(current_job))
            .catch_unwind()
            .await
        {
            Ok(Ok(completion)) => Ok(completion),
            Ok(Err(e)) => Err(congestion.maybe_reclassify(e)),
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
        let error_str = match error {
            JobError::PoolCongestion(message) => {
                return self.reschedule_congestion(id, message, attempt).await;
            }
            other => other.to_string(),
        };

        // A terminal-write op (the internal pool), not one on `self.repo`'s
        // shared pool: this is the LAST write deciding this job's
        // disposition, and it may run precisely when the shared pool is the
        // thing under pressure that failed the job in the first place. See
        // `CongestionHandler::begin_terminal_write_op` for the full
        // rationale -- the op carries the connection, so the `_in_op` calls
        // below go through `self.repo` as usual.
        let mut op = self.congestion.begin_terminal_write_op().await?;
        let mut job = self.repo.find_by_id_in_op(&mut op, id).await?;

        let span = Span::current();
        span.record("job_id", tracing::field::display(id));
        span.record("job_type", tracing::field::display(&job.job_type));
        span.record("poller_id", tracing::field::display(self.instance_id));
        span.record("error", true);
        span.record("error.message", tracing::field::display(&error_str));

        let retry_policy = RetryPolicy::from(&self.retry_settings);

        if let Some((reschedule_at, next_attempt)) =
            job.maybe_schedule_retry(self.clock.now(), attempt, &retry_policy, error_str)
        {
            let exceeded_warn_attempts = self
                .retry_settings
                .n_warn_attempts
                .is_some_and(|limit| next_attempt > limit);

            let level = if exceeded_warn_attempts {
                tracing::Level::ERROR
            } else {
                tracing::Level::WARN
            };
            span.record("error.level", tracing::field::display(level));
            self.rescheduled = true;
            span.record("will_retry", true);

            sqlx::query!(
                r#"
                UPDATE job_executions
                SET state = 'pending', execute_at = $2, attempt_index = $3, poller_instance_id = NULL
                WHERE id = $1 AND poller_instance_id = $4
              "#,
                id as JobId,
                reschedule_at,
                next_attempt as i32,
                self.instance_id
            )
            .execute(op.as_executor())
            .await?;
            // Invariant B: the retrying row keeps its queue's active slot,
            // but an older parked sibling should run first during the
            // backoff.
            PromoteHeadsHook::register(
                &mut op,
                &self.notifier,
                [job.job_type.clone()],
                vec![uuid::Uuid::from(id)],
            )
            .await?;
        } else {
            span.record(
                "error.level",
                tracing::field::display(tracing::Level::ERROR),
            );
            span.record("will_retry", false);

            self.delete_execution_in_op(&mut op, id).await?;
        }

        self.repo.update_in_op(&mut op, &mut job).await?;
        op.commit().await?;
        Ok(())
    }

    /// Reschedule after a `PoolCongestion` classification, via the shared
    /// [`CongestionHandler`] (a single job is just a batch of one there) --
    /// see `congestion.rs`'s module doc for the full rationale (attempt
    /// preserved, fixed jittered delay, `CongestionRescheduled` event,
    /// terminal-write repo).
    async fn reschedule_congestion(
        &mut self,
        id: JobId,
        message: String,
        attempt: u32,
    ) -> Result<(), JobError> {
        self.rescheduled = true;
        self.congestion.reschedule_one(id, attempt, message).await
    }

    /// Delete the execution row and report what its removal makes true: the
    /// job is terminal, and — if it held a `queue_id` — that queue's oldest
    /// parked sibling (if any) gets promoted to `pending` by a
    /// [`PromoteHeadsHook`] freed-queue registration on this same
    /// transaction. Promoting matters because the queue's backlog was, until
    /// this moment, entirely unclaimable — `parked` rows never appear in the
    /// claim scan — and the hook's notify is also the only wake for that
    /// backlog: the poll loop sleeps on `next_due_at`, and a promoted row is
    /// already due. The hook wakes the PROMOTED job's type, not this job's
    /// own type — a poller only wakes for types it polls, and the two are
    /// frequently different.
    ///
    /// Also hands this job's about-to-free unit of capacity to a `ClaimHook`
    /// so it can be spent immediately on this SAME type's own oldest due
    /// backlog, independent of whichever type got promoted above.
    /// `ClaimHook` runs after `PromoteHeadsHook` (its `RUNS_AFTER`), so a
    /// sibling promoted here is already claimable by the short-circuit.
    ///
    /// This `DELETE` is what a spawner's `FOR KEY SHARE` on the same row
    /// blocks (see `execution_hooks::insert`'s `lock_queue_occupants`): a
    /// completion racing an in-flight spawn into this queue waits out that
    /// spawn's commit tail. The promote deliberately runs as its OWN
    /// later statement (the hook's), never as a CTE of the `DELETE` — see
    /// [`PromoteHeadsHook`] for why folding it in silently orphans
    /// the freshly parked row. Unlike `batch_dispatcher.rs`'s
    /// multi-row completers this needs no id-ordered pre-lock -- a
    /// single-row `DELETE` takes exactly one lock and so cannot be one side
    /// of an ordering cycle.
    ///
    /// The `job_execution_states` row is deleted with the execution unless
    /// this type sets [`KeyedJobInitializer::inherits_state`], which is the
    /// only reason to keep one: the next generation of that key seeds from it
    /// (and the key's next spawn compacts older rows away). Retention is
    /// therefore something a type opts into, not a side effect of being keyed
    /// — a keyed type that does not inherit state cleans up exactly like every
    /// other flavor.
    ///
    /// [`KeyedJobInitializer::inherits_state`]: crate::KeyedJobInitializer::inherits_state
    async fn delete_execution_in_op(
        &mut self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
    ) -> Result<(), JobError> {
        let deleted = sqlx::query!(
            r#"
          WITH deleted AS (
              DELETE FROM job_executions
              WHERE id = $1 AND poller_instance_id = $2
              RETURNING id, queue_id
          ), cleanup AS (
              DELETE FROM job_execution_states s USING deleted d
              WHERE s.id = d.id AND NOT $3::boolean
          )
          SELECT d.queue_id AS "queue_id?" FROM deleted d
        "#,
            id as JobId,
            self.instance_id,
            self.retains_state,
        )
        .fetch_optional(op.as_executor())
        .await?;

        let Some(row) = deleted else {
            return Ok(());
        };

        self.notifier.job_terminal_in_op(op, id).await?;

        if let Some(queue_id) = row.queue_id {
            PromoteHeadsHook::register_freed_queues(op, &self.notifier, vec![queue_id]).await?;
        }

        if let Some(poller) = self.poller.upgrade() {
            self.recycle_unit();
            let reservation = self.tracker.recycle(&self.job_type);
            poller.register_claim_recycle(op, &self.job_type, reservation);
        }

        Ok(())
    }

    #[instrument(name = "job.complete_job", skip(self, op), fields(id = %id))]
    async fn complete_job(
        &mut self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
    ) -> Result<(), JobError> {
        let mut job = self.repo.find_by_id_in_op(&mut *op, &id).await?;
        self.delete_execution_in_op(op, id).await?;
        job.complete_job();
        self.repo.update_in_op(op, &mut job).await?;
        Ok(())
    }

    #[instrument(name = "job.reschedule_job", skip(self, op), fields(id = %id, reschedule_at = %reschedule_at, attempt = 1))]
    async fn reschedule_job(
        &mut self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
        reschedule_at: DateTime<Utc>,
    ) -> Result<(), JobError> {
        self.rescheduled = true;
        let mut job = self.repo.find_by_id_in_op(&mut *op, &id).await?;
        sqlx::query!(
            r#"
          UPDATE job_executions
          SET state = 'pending', execute_at = $2, attempt_index = 1, poller_instance_id = NULL
          WHERE id = $1 AND poller_instance_id = $3
        "#,
            id as JobId,
            reschedule_at,
            self.instance_id
        )
        .execute(op.as_executor())
        .await?;
        // Invariant B: same ordering fixup as the retry branch of
        // `fail_job` — the rescheduled row keeps its queue's active slot,
        // but an older parked sibling should run first.
        PromoteHeadsHook::register(
            op,
            &self.notifier,
            [job.job_type.clone()],
            vec![uuid::Uuid::from(id)],
        )
        .await?;
        job.reschedule_execution(reschedule_at);
        self.repo.update_in_op(op, &mut job).await?;
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
