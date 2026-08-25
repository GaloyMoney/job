//! `plan_claim`'s rotation -- the elastic floor window, and (handoff-0138) the
//! bounded tier's tie-group rotation -- excludes some types when demand
//! outstrips a tier's budget share, leaving an excluded type's due rows
//! invisible to both that poll's claim and its sleep deadline (reported as
//! `excluded_due` by `claim_query`). This module closes that blind spot with
//! a zero-sleep re-poll: every poll advances rotation one slot in whichever
//! tier is scarce, so the window reaches any excluded type within one lap.
//! `Duration::ZERO` fires even under a frozen manual clock, since the manual
//! `ClockSleep` completes once `now >= wake_at`, while any positive
//! clock-relative duration would not.
//!
//! Because "due" doesn't imply "claimable" (a peer's in-flight claim can hold
//! a row's lock), two unclaimable due rows further apart in rotation order
//! than the window is wide would otherwise spin forever, so zero-sleep
//! re-polls are granted only while a streak of consecutive claim-nothing
//! `excluded_due` polls stays within two full rotation laps
//! (`ClaimPlan::rotation_lap`; one lap guarantees every excluded type in
//! either tier gets a turn). Past that bound the poll falls back to the
//! honest window-derived sleep plus a one-shot real-time backoff waiter
//! (10ms doubling to 1s, CAS-guarded to at most one live waiter, since lock
//! release emits no notification and clock-relative sleeps are inert under
//! manual clocks); any claim resets the streak.
//!
//! The `2 x rotation_lap` threshold is load-bearing, not a tuning knob:
//! measured, an unbounded zero-sleep spin against unclaimable rows runs at
//! ~250 polls/s (a full DB core), which the bound caps at ~1 poll/s under the
//! backoff waiter. `rotation_lap` sizes off the bounded tier's own type
//! count (not the narrower widest-tie-group figure) specifically so the
//! `plan()` aging fallback (see `plan`'s doc) always gets to force a starved
//! type in BEFORE this bound trips and backs off -- reversing that ordering
//! would stop re-polling exactly when aging was about to rescue the type.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use crate::tracker::JobTracker;

const RECHECK_INITIAL_BACKOFF: Duration = Duration::from_millis(10);
const RECHECK_MAX_BACKOFF: Duration = Duration::from_secs(1);

pub(super) struct Recheck {
    tracker: Arc<JobTracker>,
    armed: Arc<AtomicBool>,
    streak: AtomicUsize,
    backoff_ms: AtomicUsize,
    #[cfg(test)]
    pub(super) waiter_spawns: AtomicUsize,
}

impl Recheck {
    pub(super) fn new(tracker: Arc<JobTracker>) -> Self {
        Self {
            tracker,
            armed: Arc::new(AtomicBool::new(false)),
            streak: AtomicUsize::new(0),
            backoff_ms: AtomicUsize::new(RECHECK_INITIAL_BACKOFF.as_millis() as usize),
            #[cfg(test)]
            waiter_spawns: AtomicUsize::new(0),
        }
    }

    pub(super) fn bounded_sleep(
        &self,
        base: Duration,
        jobs_claimed: usize,
        excluded_due: bool,
        rotation_lap: usize,
    ) -> Duration {
        if !excluded_due {
            self.streak.store(0, Ordering::Relaxed);
            self.backoff_ms.store(
                RECHECK_INITIAL_BACKOFF.as_millis() as usize,
                Ordering::Relaxed,
            );
            return base;
        }
        if jobs_claimed > 0 {
            self.streak.store(0, Ordering::Relaxed);
            return Duration::ZERO;
        }
        let streak = self.streak.fetch_add(1, Ordering::Relaxed) + 1;
        if streak <= rotation_lap.saturating_mul(2) {
            return Duration::ZERO;
        }
        let backoff_ms = self.backoff_ms.load(Ordering::Relaxed);
        self.backoff_ms.store(
            backoff_ms
                .saturating_mul(2)
                .min(RECHECK_MAX_BACKOFF.as_millis() as usize),
            Ordering::Relaxed,
        );
        self.arm(Duration::from_millis(backoff_ms as u64));
        base
    }

    pub(super) fn arm(&self, delay: Duration) {
        if self.armed.swap(true, Ordering::AcqRel) {
            return;
        }
        #[cfg(test)]
        self.waiter_spawns.fetch_add(1, Ordering::SeqCst);
        let armed = Arc::clone(&self.armed);
        let tracker = Arc::clone(&self.tracker);
        spawn_named_task!("job-claim-recheck-waiter", async move {
            tokio::time::sleep(delay).await;
            armed.store(false, Ordering::Release);
            tracker.wake();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{
        ElasticInitializer, build_poller, init_pool, seed_pending_job,
    };
    use super::*;
    use crate::JobType;
    use crate::registry::JobRegistry;

    #[tokio::test]
    async fn recheck_waiter_arm_is_idempotent_while_pending() -> anyhow::Result<()> {
        let recheck = Recheck::new(Arc::new(JobTracker::new(0, 10)));

        for _ in 0..50 {
            recheck.arm(RECHECK_MAX_BACKOFF);
        }

        assert_eq!(
            recheck.waiter_spawns.load(Ordering::SeqCst),
            1,
            "many arm calls while a waiter is already pending must spawn \
             at most one task, not one per call"
        );

        Ok(())
    }

    #[tokio::test]
    async fn unclaimable_excluded_due_rows_do_not_spin_unbounded() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let tracker = Arc::new(JobTracker::new(1, 10));
        let mut registry = JobRegistry::new();
        let run = uuid::Uuid::now_v7();
        let n_elastic = 24usize;
        let mut type_names = Vec::new();
        for i in 0..n_elastic {
            let name = format!("spin-bound-{run}-{i:02}");
            registry.add_initializer(ElasticInitializer {
                job_type: JobType::from_owned(name.clone()),
            });
            type_names.push(name);
        }
        let poller = build_poller(&pool, registry, tracker).await?;

        let due = chrono::Utc::now() - chrono::Duration::seconds(60);
        let stuck_a = seed_pending_job(&pool, &type_names[0], due).await?;
        let stuck_b = seed_pending_job(&pool, &type_names[12], due).await?;

        let mut lock_tx = pool.begin().await?;
        sqlx::query("SELECT id FROM job_executions WHERE id = ANY($1) FOR UPDATE")
            .bind(vec![uuid::Uuid::from(stuck_a), uuid::Uuid::from(stuck_b)])
            .fetch_all(&mut *lock_tx)
            .await?;

        let bound = 2 * n_elastic;
        let mut zeros = 0usize;
        let mut first_non_zero = None;
        for i in 0..(bound + 10) {
            let sleep = poller.poll_and_dispatch(false).await?;
            if sleep == Duration::ZERO {
                zeros += 1;
            } else {
                first_non_zero = Some((i, sleep));
                break;
            }
        }

        let (polls_before_stop, sleep) = first_non_zero.unwrap_or_else(|| {
            panic!(
                "{} consecutive zero-sleep polls against unclaimable rows and \
                 still spinning: the bound never engaged",
                bound + 10
            )
        });
        assert!(
            polls_before_stop <= bound,
            "zero-sleep re-polls must stop within two rotation laps \
             ({bound}): got {polls_before_stop}"
        );
        assert_eq!(zeros, polls_before_stop);
        assert!(
            sleep > Duration::ZERO,
            "the tripped poll must fall back to an honest sleep"
        );
        assert!(
            poller.recheck.waiter_spawns.load(Ordering::SeqCst) >= 1,
            "tripping the bound must arm the real-time recheck waiter -- \
             the honest sleep alone is inert under a manual clock and the \
             lock releasing emits no notification"
        );

        lock_tx.rollback().await?;
        Ok(())
    }
}
