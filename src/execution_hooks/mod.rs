//! Commit hooks that do database work in `pre_commit`: [`PromoteHeadsHook`]
//! and [`ExecutionInsertHook`]. Each centralizes the SQL for one concern and
//! stages further hooks re-entrantly (`crate::notifier::NotifierHook` for
//! notify, [`crate::poller::JobPoller::register_claim_demand`] for the
//! head-swap claim) rather than doing that work inline.
//!
//! # The hook DAG
//!
//! Every `impl CommitHook` in this crate, and its declared
//! [`CommitHook::runs_after`] dependencies -- hard-specified here in one
//! place per the crate's operator requirement that no inter-hook edge rest
//! on registration order at a call site:
//!
//! ```text
//! ExecutionInsertHook  (execution_hooks::insert)  runs_after: []
//! PromoteHeadsHook      (execution_hooks::promote)  runs_after: [ExecutionInsertHook]
//! ClaimHook             (poller)                    runs_after: [ExecutionInsertHook, PromoteHeadsHook]
//! NotifierHook          (notifier)                  runs_after: [ExecutionInsertHook, PromoteHeadsHook, ClaimHook]
//! ```
//!
//! `ExecutionInsertHook` is the producer: it inserts, promotes swapped-in
//! occupants inline (via [`PromoteHeadsHook::apply`], a plain fn call, not a
//! registered hook), and re-entrantly stages a `ClaimHook` (spawn-side fresh
//! demand) and a `NotifierHook` execution-ready-netting contribution (added
//! ids + forces, via `JobEventNotifier::register_execution_ready_in_op`).
//! `ClaimHook` consumes that demand (plus any completion-side recycled
//! capacity) and stages its own netting contribution (claimed ids) once it
//! knows what it actually claimed. The `NotifierHook` instance those merge
//! into -- deferred behind all three producers by its own `runs_after`,
//! declared unconditionally on every instance of the type -- resolves
//! `added` against `claimed`, unions in `forces`, and is the only path left
//! that ends up firing an `execution_ready` notification for a spawn/claim
//! commit pass. `PromoteHeadsHook`, when registered standalone (not via
//! `ExecutionInsertHook`'s inline `apply` call -- e.g. retry backoff,
//! freed-queue promotion from a completion), still notifies directly from
//! its own `pre_commit` via `execution_ready_in_op` (a plain, pre-decided
//! `NotifierHook` registration -- same hook type, same unconditional
//! `runs_after`, just no netting data to resolve); its own `runs_after`
//! edge exists so a hand-composed op that spawns AND promotes in one
//! transaction sees the spawn's rows before it looks for a parked sibling
//! to swap.
//!
//! Over-declaring is free by the framework's own rules: a listed type that
//! never registers on a given op, or whose instances have all already
//! executed, imposes no delay (see the `es_entity` hook-ordering contract).
//! A genuine cycle among declared dependencies fails the commit loudly
//! instead of hanging. See `hook_ordering_tests` below for a test that
//! registers `ExecutionInsertHook` and a standalone `PromoteHeadsHook` on
//! one op in deliberately wrong (registration) order and asserts the
//! execution order the DAG above dictates -- exercised with real hook
//! instances and a live DB, not stand-ins, since `runs_after` dispatches on
//! the REAL types' `TypeId`s. `tests/notify_suppression.rs` and
//! `tests/lock_ordering.rs` cover the `ClaimHook`/`NotifierHook` netting
//! half of the DAG end-to-end through the public spawn API.

mod insert;
mod promote;

pub(crate) use insert::{ExecutionInsertHook, NewExecutionRow};
pub(crate) use promote::{PromoteHeadsHook, PromotedRow};

#[cfg(test)]
mod hook_ordering_tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use chrono::Utc;
    use es_entity::AtomicOperation;
    use es_entity::clock::ClockHandle;

    use crate::entity::JobType;
    use crate::notifier::JobEventNotifier;
    use crate::repo::JobRepo;
    use crate::tracker::JobTracker;

    use super::{ExecutionInsertHook, NewExecutionRow, PromoteHeadsHook};

    async fn init_pool() -> anyhow::Result<sqlx::PgPool> {
        let pg_con = std::env::var("PG_CON").unwrap();
        Ok(sqlx::PgPool::connect(&pg_con).await?)
    }

    fn notifier(pool: &sqlx::PgPool) -> Arc<JobEventNotifier> {
        let tracker = Arc::new(JobTracker::new(0, 0));
        let (terminal_tx, _) = tokio::sync::broadcast::channel(1);
        JobEventNotifier::spawn(pool, tracker, terminal_tx)
    }

    /// A standalone `PromoteHeadsHook` registered on the SAME op as an
    /// `ExecutionInsertHook`, in deliberately WRONG (reversed) registration
    /// order, must still run AFTER it -- per `PromoteHeadsHook::RUNS_AFTER`.
    ///
    /// Observable, not inferred: seed an orphaned older `parked` sibling
    /// (`old_parked`) behind a queue with no current active row, then in one
    /// op (1) register a standalone `PromoteHeadsHook` swap for the id of a
    /// row THIS SAME op's `ExecutionInsertHook` is about to insert
    /// (`new_row`, newer than `old_parked`), registered FIRST, and (2)
    /// register the `ExecutionInsertHook` itself SECOND. `PromoteHeadsHook`'s
    /// swap only finds a candidate once `new_row` exists and is `pending` --
    /// if it ran before the insert (the pre-fix default: registration
    /// order), it finds nothing and both rows are left exactly as inserted
    /// (Invariant B violated: the newer row `pending`, the older `parked`
    /// behind it -- `ExecutionInsertHook`'s OWN inline promote does not
    /// self-heal this, since `new_row` lands `pending` directly, never
    /// `parked` with an `occupant_id`). If it runs after (the fix), it finds
    /// `new_row` and swaps it behind `old_parked`.
    #[tokio::test]
    async fn standalone_promote_hook_runs_after_execution_insert_hook() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = JobRepo::new(&pool);
        let clock = ClockHandle::realtime();
        let notifier = notifier(&pool);
        let jt = JobType::new(Box::leak(
            format!("hook-order-{}", uuid::Uuid::now_v7()).into_boxed_str(),
        ));
        let queue = format!("hook-order-queue-{}", uuid::Uuid::now_v7());

        // An orphaned older parked sibling: no active row in this queue yet.
        let old_parked = crate::JobId::new();
        sqlx::query("INSERT INTO jobs (id, job_type, created_at) VALUES ($1, $2, NOW())")
            .bind(uuid::Uuid::from(old_parked))
            .bind(jt.as_str())
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, queue_id, state, attempt_index, execute_at, alive_at, created_at) \
             VALUES ($1, $2, $3, 'parked', 1, $4, NOW(), NOW())",
        )
        .bind(uuid::Uuid::from(old_parked))
        .bind(jt.as_str())
        .bind(&queue)
        .bind(Utc::now() - chrono::Duration::seconds(100))
        .execute(&pool)
        .await?;

        let new_row_id = crate::JobId::new();
        sqlx::query("INSERT INTO jobs (id, job_type, created_at) VALUES ($1, $2, NOW())")
            .bind(uuid::Uuid::from(new_row_id))
            .bind(jt.as_str())
            .execute(&pool)
            .await?;

        let mut op = repo.begin_op_with_clock(&clock).await?;

        // Registered FIRST -- the deliberately wrong order: a standalone
        // promote for a row the insert below hasn't created yet.
        op.add_commit_hook(PromoteHeadsHook {
            notifier: Arc::clone(&notifier),
            own_types: HashSet::new(),
            ids: vec![uuid::Uuid::from(new_row_id)],
            freed_queues: Vec::new(),
        })
        .map_err(|_| anyhow::anyhow!("op must support commit hooks"))?;

        // Registered SECOND.
        op.add_commit_hook(ExecutionInsertHook {
            notifier: Arc::clone(&notifier),
            poller: Arc::new(std::sync::OnceLock::new()),
            clock: clock.clone(),
            rows: vec![NewExecutionRow {
                id: new_row_id,
                job_type: jt.clone(),
                schedule_at: Utc::now(),
                queue_id: Some(queue.clone()),
            }],
        })
        .map_err(|_| anyhow::anyhow!("op must support commit hooks"))?;

        op.commit().await?;

        let (new_state, old_state): (String, String) = sqlx::query_as(
            "SELECT \
               (SELECT state::text FROM job_executions WHERE id = $1), \
               (SELECT state::text FROM job_executions WHERE id = $2)",
        )
        .bind(uuid::Uuid::from(new_row_id))
        .bind(uuid::Uuid::from(old_parked))
        .fetch_one(&pool)
        .await?;

        assert_eq!(
            old_state, "pending",
            "the older parked sibling must win the slot -- \
             PromoteHeadsHook must have run AFTER ExecutionInsertHook \
             despite registering first"
        );
        assert_eq!(
            new_state, "parked",
            "the newer row must yield to the older sibling"
        );

        Ok(())
    }
}
