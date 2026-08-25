//! Pool-aware admission: a poll may claim only as many dispatch units as the shared pool
//! has headroom for (`connections_per_job` connections per unit, floor-rounded) --
//! claiming into a saturated pool strands rows `running` on an instance that cannot run
//! them while healthy peers are locked out by `SKIP LOCKED`, so a zero budget claims
//! nothing.
//!
//! Because `tracker.notified()` cannot observe other users of a shared pool freeing
//! connections, a clamped-to-zero poll arms a real-time backoff waiter (10ms doubling to
//! 1s) that wakes the poll loop once the unit budget -- not raw headroom, which can still
//! round down to zero units -- recovers. The unit is a heuristic priced at the crate's own
//! claim/dispatch cost (one connection); what a runner does inside its own code is opaque
//! and deliberately not priced, with congestion reschedules absorbing the misses.

use sqlx::postgres::PgPool;

use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use crate::tracker::JobTracker;

const POOL_WAITER_INITIAL_BACKOFF: Duration = Duration::from_millis(10);
const POOL_WAITER_MAX_BACKOFF: Duration = Duration::from_secs(1);

pub(crate) fn pool_connection_headroom(main_pool: &PgPool) -> usize {
    let max_connections = main_pool.options().get_max_connections() as usize;
    let in_use = (main_pool.size() as usize).saturating_sub(main_pool.num_idle());
    max_connections.saturating_sub(in_use)
}

fn unit_budget(headroom: usize, connections_per_job: f64) -> usize {
    (headroom as f64 / connections_per_job).floor() as usize
}

pub(super) struct PoolBudget {
    inner: Arc<Inner>,
}

struct Inner {
    pool: PgPool,
    connections_per_job: f64,
    tracker: Arc<JobTracker>,
    armed: AtomicBool,
}

impl PoolBudget {
    pub(super) fn new(pool: &PgPool, connections_per_job: f64, tracker: Arc<JobTracker>) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool: pool.clone(),
                connections_per_job,
                tracker,
                armed: AtomicBool::new(false),
            }),
        }
    }

    pub(super) fn unit_budget(&self) -> usize {
        unit_budget(
            pool_connection_headroom(&self.inner.pool),
            self.inner.connections_per_job,
        )
    }

    pub(super) fn arm_waiter(&self) {
        if self.inner.armed.swap(true, Ordering::AcqRel) {
            return;
        }
        let inner: Weak<Inner> = Arc::downgrade(&self.inner);
        spawn_named_task!("job-pool-headroom-waiter", async move {
            let mut backoff = POOL_WAITER_INITIAL_BACKOFF;
            loop {
                {
                    let Some(inner) = inner.upgrade() else {
                        return;
                    };
                    if unit_budget(
                        pool_connection_headroom(&inner.pool),
                        inner.connections_per_job,
                    ) > 0
                    {
                        inner.armed.store(false, Ordering::Release);
                        inner.tracker.wake();
                        return;
                    }
                };
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(POOL_WAITER_MAX_BACKOFF);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn settle(pool: &PgPool, expected_idle: u32) {
        for _ in 0..1000 {
            if pool.num_idle() as u32 == expected_idle {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn pool_headroom_tracks_live_connections() -> anyhow::Result<()> {
        let pg_con = std::env::var("PG_CON").unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(3)
            .connect(&pg_con)
            .await?;

        assert_eq!(pool_connection_headroom(&pool), 3);

        let c1 = pool.acquire().await?;
        let c2 = pool.acquire().await?;
        assert_eq!(pool_connection_headroom(&pool), 1);

        let c3 = pool.acquire().await?;
        assert_eq!(pool_connection_headroom(&pool), 0);

        drop(c3);
        settle(&pool, 1).await;
        assert_eq!(pool_connection_headroom(&pool), 1);

        drop(c1);
        drop(c2);
        settle(&pool, 3).await;
        assert_eq!(pool_connection_headroom(&pool), 3);

        Ok(())
    }

    #[test]
    fn unit_budget_applies_connections_per_job_factor() {
        for headroom in [0, 1, 5, 50] {
            assert_eq!(unit_budget(headroom, 1.0), headroom);
        }
        assert_eq!(unit_budget(5, 0.5), 10);
        assert_eq!(unit_budget(5, 1.5), 3);
        assert_eq!(unit_budget(5, 2.0), 2);
        assert_eq!(unit_budget(1, 1.5), 0);
        assert_eq!(unit_budget(0, 0.5), 0);
        assert_eq!(unit_budget(0, 2.0), 0);
    }
}
