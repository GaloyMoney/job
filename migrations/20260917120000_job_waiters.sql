-- A job waiting on another. `waiter_job_id` parked itself (`RescheduleAt`)
-- until `job_id` reaches a terminal state; the finalizer that deletes
-- `job_id`'s execution row pulls the waiter's `execute_at` forward
-- (`finalizer.rs::wake_waiters_in_op`), under the same guards as the keyed
-- pull-forward: pending, first attempt, scheduled later than now.
--
-- A waiter that is still RUNNING when its callee lands cannot be moved (the
-- finalizer's disposition write owns that row); its rows are marked
-- `woken_at` instead, and the waiter's own park write (`Disposition::Fresh`)
-- consults the mark and lands the row due. Rows are deleted when consumed
-- by a wake, and unconditionally when the waiter itself goes terminal, so
-- the table stays O(live waits).
--
-- No FK to `jobs`: `jobs` rows are never deleted, and the finalizer's
-- `DELETE ... RETURNING` is the only consumer.
CREATE TABLE job_waiters (
  job_id UUID NOT NULL,
  waiter_job_id UUID NOT NULL,
  woken_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (job_id, waiter_job_id)
);

-- The waiter-side probes: the park-time mark consult and terminal cleanup.
CREATE INDEX idx_job_waiters_waiter ON job_waiters (waiter_job_id);
