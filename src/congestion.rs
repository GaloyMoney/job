//! Pool-congestion rescheduling: the one path both dispatchers route a
//! [`JobError::PoolCongestion`] classification (`error::is_pool_congestion`)
//! through at the end of a job, instead of the ordinary `RetryPolicy` fail
//! path. A batch is just N ids and a single job is N = 1 -- the write, the
//! delay, the entity event, and the stuck-streak WARN are identical, so they
//! live here once.
//!
//! Why congestion is its own path, not a retry:
//!
//! - **`attempt_index` stays UNCHANGED**, in the row and in the entity
//!   event. Congestion carries no evidence the JOB is broken -- the pool
//!   could not hand out a connection, a pool-wide condition -- so it must
//!   not spend a `RetryPolicy` attempt and walk the job toward
//!   `max_attempts`. That's load-bearing beyond the retry budget: the
//!   poller's retry-solo rule (`poller.rs`, `attempt > 1` is dispatched
//!   alone, never batched) reads the same column, so resetting or bumping
//!   it here would silently change how the job is dispatched on its next
//!   claim. See `congestion_reschedule_keeps_job_batchable` in
//!   `tests/batched_job.rs`. This is also why the write is its own SQL and
//!   not a call to the dispatchers' ordinary reschedule methods -- those
//!   hardcode `attempt_index = 1` (right for their callers), while this
//!   write leaves `attempt_index` out of the `SET` list entirely.
//! - **Fixed short delay +/- jitter**, not `RetryPolicy`'s exponential
//!   schedule: the pool that just timed out needs a moment to drain, and
//!   the jitter keeps every job congested in the same poll from
//!   synchronizing on the exact same next claim instant.
//! - **A `CongestionRescheduled` entity event**, not `ExecutionErrored`
//!   (see [`Job::reschedule_congestion`]), which is also how the
//!   consecutive-congestion streak is counted for the stuck-forever WARN.
//! - **The write opens on [`CongestionHandler::terminal_write_repo`]** (the
//!   internal pool): it may run precisely when the shared pool is the thing
//!   under pressure that congested the job in the first place.

use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use es_entity::clock::ClockHandle;
use rand::{RngExt, rng};
use tracing::{Span, instrument};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

use super::{
    JobId,
    entity::{Job, JobType},
    error::{CONFLICT_MAX_ATTEMPTS, JobError, is_retryable_conflict},
    execution_hooks::PromoteHeadsHook,
    notifier::JobEventNotifier,
    poller::JobPoller,
    repo::JobRepo,
};

/// Base delay before a pool-congestion reschedule becomes due again. Fixed
/// and short, not the type's exponential `RetryPolicy` schedule: congestion
/// is a pool-wide condition expected to clear on a query-duration timescale,
/// not a per-job failure that compounds.
const CONGESTION_DELAY_MS: i64 = 2_000;

/// +/- jitter applied to `CONGESTION_DELAY_MS`, so every job congestion hit
/// in the same poll doesn't come due on the exact same next claim instant.
const CONGESTION_JITTER_MS: i64 = 1_000;

/// A job whose consecutive congestion-reschedule streak exceeds this gets a
/// WARN: rescheduling stays non-punitive (this is a signal, not a cap), but
/// "stuck in congestion forever" is no longer invisible. Counted from the
/// event stream by [`Job::consecutive_congestion_reschedules`].
const CONGESTION_WARN_STREAK: u32 = 10;

/// The stateful home of the congestion-reschedule machinery: built once per
/// dispatcher from state it already holds, so the reschedule call sites only
/// pass what varies per failure (ids, attempts, the congestion message).
/// Cheap to clone -- every field is a handle (`Weak`, `Arc`, pool-handle
/// clones).
#[derive(Clone)]
pub(crate) struct CongestionHandler {
    /// Reaches this process's poller for `terminal_write_repo`'s internal
    /// pool. `Weak` for the same reason the dispatchers hold their poller
    /// `Weak`: a dispatcher must never keep the poller alive on its own.
    poller: Weak<JobPoller>,
    /// The shared-pool repo, as `terminal_write_repo`'s shutdown fallback.
    repo: Arc<JobRepo>,
    notifier: Arc<JobEventNotifier>,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
}

impl CongestionHandler {
    pub(crate) fn new(
        poller: Weak<JobPoller>,
        repo: Arc<JobRepo>,
        notifier: Arc<JobEventNotifier>,
        instance_id: uuid::Uuid,
        clock: ClockHandle,
    ) -> Self {
        Self {
            poller,
            repo,
            notifier,
            instance_id,
            clock,
        }
    }

    /// A [`JobRepo`] to open terminal-write ops on -- the congestion
    /// reschedule below, and the dispatchers' fail / rescue paths, all need
    /// to record a disposition precisely when the shared pool `self.repo`
    /// wraps may be the thing under pressure that caused the failure in the
    /// first place. Bound to `JobPoller::internal_pool` instead -- the same
    /// dedicated pool the claim query uses, sized to absorb both (see
    /// `build_internal_pool`).
    ///
    /// `JobRepo::new` is a cheap pool-handle clone, not a new connection --
    /// safe to call per attempt. Falls back to `self.repo` (the shared pool)
    /// only if the poller has already been dropped: at that point the
    /// process is shutting down and there is no internal pool handle left to
    /// reach, so the write is best-effort either way.
    pub(crate) fn terminal_write_repo(&self) -> JobRepo {
        match self.poller.upgrade() {
            Some(poller) => JobRepo::new(poller.internal_pool()),
            None => (*self.repo).clone(),
        }
    }

    /// [`Self::reschedule`] for a single job -- a batch of one.
    pub(crate) async fn reschedule_one(
        &self,
        id: JobId,
        attempt: u32,
        message: String,
    ) -> Result<(), JobError> {
        let attempts = HashMap::from([(id, attempt)]);
        self.reschedule(&[id], &attempts, message).await
    }

    /// Reschedule `ids` after a `PoolCongestion` classification: every row
    /// goes back to `pending` at now + [`CONGESTION_DELAY_MS`] +/-
    /// [`CONGESTION_JITTER_MS`], `attempt_index` untouched (see the module
    /// doc), on a fresh `CongestionRescheduled` entity event. Retried on
    /// deadlock / serialization abort like the batch seal paths
    /// ([`CONFLICT_MAX_ATTEMPTS`]).
    ///
    /// `attempts` maps each id to its in-flight attempt number, recorded
    /// unchanged on the entity's next `ExecutionScheduled` event; an id
    /// missing from the map defaults to attempt 1.
    #[instrument(name = "job.congestion_reschedule", skip_all,
        fields(job_type = tracing::field::Empty, n_jobs = ids.len(), congestion_streak)
    )]
    pub(crate) async fn reschedule(
        &self,
        ids: &[JobId],
        attempts: &HashMap<JobId, u32>,
        message: String,
    ) -> Result<(), JobError> {
        let span = Span::current();
        let mut attempt_no = 1;
        loop {
            let result = self.reschedule_once(ids, attempts, &message).await;
            match result {
                Ok((job_type, streak)) => {
                    if let Some(job_type) = job_type {
                        span.record("job_type", tracing::field::display(&job_type));
                    }
                    span.record("congestion_streak", streak);
                    if streak > CONGESTION_WARN_STREAK {
                        tracing::warn!(
                            job_ids = %display_ids(ids),
                            streak,
                            "stuck in congestion-reschedule; the pool may not be recovering"
                        );
                    }
                    return Ok(());
                }
                Err(e) if attempt_no < CONFLICT_MAX_ATTEMPTS && is_retryable_conflict(&e) => {
                    tracing::warn!(
                        job_ids = %display_ids(ids),
                        attempt_no,
                        exception.message = %e,
                        "congestion-reschedule lost a lock conflict; retrying"
                    );
                    attempt_no += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// One attempt of the congestion write: row update, entity events,
    /// freed-slot promotion registration, commit. Returns the jobs' type
    /// (for the caller's span; `None` only if every row was already gone)
    /// and the highest post-reschedule consecutive-congestion streak (for
    /// the stuck-forever WARN).
    async fn reschedule_once(
        &self,
        ids: &[JobId],
        attempts: &HashMap<JobId, u32>,
        message: &str,
    ) -> Result<(Option<JobType>, u32), JobError> {
        let repo = self.terminal_write_repo();
        let mut op = repo.begin_op_with_clock(&self.clock).await?;
        let now = op.maybe_now().unwrap_or_else(|| self.clock.now());
        let jitter_ms = rng().random_range(-CONGESTION_JITTER_MS..=CONGESTION_JITTER_MS);
        let scheduled_at: DateTime<Utc> =
            now + chrono::Duration::milliseconds(CONGESTION_DELAY_MS + jitter_ms);

        let uuids: Vec<uuid::Uuid> = ids.iter().map(|id| uuid::Uuid::from(*id)).collect();

        // `(queue_id, id)`-ordered lock before the write, same reason and
        // same `MATERIALIZED` shape as `BatchDispatcher::reschedule_in_op`/
        // `fail_in_op` -- see `reschedule_in_op`'s doc comment.
        // `attempt_index` is deliberately absent from the `SET` list: this
        // write must not touch it either way (see the module doc).
        sqlx::query!(
            r#"
            WITH to_reschedule AS MATERIALIZED (
                SELECT id FROM job_executions
                WHERE id = ANY($1) AND poller_instance_id = $2
                ORDER BY queue_id, id
                FOR UPDATE
            )
            UPDATE job_executions AS je
            SET state = 'pending', execute_at = $3, poller_instance_id = NULL
            FROM to_reschedule t
            WHERE je.id = t.id
            "#,
            &uuids,
            self.instance_id,
            scheduled_at,
        )
        .execute(op.as_executor())
        .await?;

        let mut entities = repo.find_all_in_op::<Job>(&mut op, ids).await?;
        let mut jobs = Vec::with_capacity(ids.len());
        let mut own_types = HashSet::new();
        let mut max_streak = 0;
        for id in ids {
            if let Some(mut job) = entities.remove(id) {
                let attempt = attempts.get(id).copied().unwrap_or(1);
                let streak = job.reschedule_congestion(message.to_string(), scheduled_at, attempt);
                max_streak = max_streak.max(streak);
                own_types.insert(job.job_type.clone());
                jobs.push(job);
            }
        }
        repo.update_all_in_op(&mut op, &mut jobs).await?;
        let job_type = own_types.iter().next().cloned();

        // Same freed-queue promotion registration as the dispatchers'
        // ordinary reschedules -- a congestion reschedule only ever demotes
        // rows back to `pending`, it never frees a queue, so this is the
        // retry-side registration only ("invariant B": the rescheduled rows
        // keep their queues' active slots, but an older parked sibling
        // should run first during the delay).
        PromoteHeadsHook::register(&mut op, &self.notifier, own_types, uuids).await?;
        op.commit().await?;
        Ok((job_type, max_streak))
    }
}

/// Renders the ids as a comma-separated list for one log field, so a warn
/// line can be tied to the jobs it concerns.
fn display_ids(ids: &[JobId]) -> String {
    let mut out = String::new();
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&id.to_string());
    }
    out
}
