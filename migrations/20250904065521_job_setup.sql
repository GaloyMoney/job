CREATE TABLE jobs (
  id UUID PRIMARY KEY,
  unique_key VARCHAR,
  resident BOOLEAN NOT NULL DEFAULT FALSE,
  job_type VARCHAR NOT NULL,
  queue_id VARCHAR,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- The two singleton flavors are fully orthogonal: `unique_key IS NOT NULL`
-- means keyed, `resident` means resident — a job is never both, and neither
-- uses a sentinel value in the other's column.
--
-- `jobs` accumulates one row per generation of a keyed job (liveness for
-- those is enforced on `job_executions`, see
-- `idx_job_executions_job_type_unique_key`), so this index is a read path
-- only: it resolves the latest generation of a `(job_type, unique_key)` for
-- `find_keyed`/`keyed_handles`.
CREATE INDEX idx_jobs_job_type_unique_key_created_at
  ON jobs (job_type, unique_key, created_at DESC)
  WHERE unique_key IS NOT NULL;

-- `ResidentJobSpawner::spawn` enforcement: absolutely unique, at most one job
-- of `job_type` EVER exists where `resident` is set. `jobs` rows are never
-- deleted, so once that job reaches a terminal state the type can never be
-- spawned again (in practice a resident job never reaches one — see
-- `resident.rs` — but the index doesn't depend on that). A dedicated boolean
-- column rather than a sentinel `unique_key` value — DB-level enforcement
-- doesn't depend on a magic string staying in sync with application code,
-- and ordinary keyed jobs (which never set this flag) are entirely outside
-- this index's partial predicate, getting LIVE-only enforcement on
-- `job_executions` instead.
CREATE UNIQUE INDEX idx_jobs_job_type_resident
  ON jobs (job_type)
  WHERE resident;

CREATE TABLE job_events (
  id UUID NOT NULL,
  sequence INT NOT NULL,
  event_type VARCHAR NOT NULL,
  event JSONB NOT NULL,
  context JSONB DEFAULT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(id, sequence)
);

-- `parked`: a queued row (`queue_id IS NOT NULL`) whose queue already has a
-- live row. Unqueued rows are never parked -- nothing to block them.
--
-- Invariant A (exclusion): per `queue_id`, at most one row in state
-- `pending` or `running` -- enforced below by `idx_job_executions_queue_active`.
-- Invariant B (order): the active (`pending`/`running`) row of a queue is
-- its min-`(execute_at, id)` live-or-parked row. Maintained by the write
-- paths (insert-swap in `execution_hooks.rs`, retry/reschedule/reclaim-swap
-- in `dispatcher.rs`/`batch_dispatcher.rs`/`poller.rs`); claim correctness
-- does not depend on it (exclusion is Invariant A alone), but scheduling
-- semantics do -- a queue's backlog must still drain oldest-first.
CREATE TYPE JobExecutionState AS ENUM ('pending', 'parked', 'running');

CREATE TABLE job_executions (
  id UUID NOT NULL UNIQUE,
  job_type VARCHAR NOT NULL,
  queue_id VARCHAR,
  unique_key VARCHAR,
  poller_instance_id UUID,
  attempt_index INT NOT NULL DEFAULT 1,
  state JobExecutionState NOT NULL DEFAULT 'pending',
  execute_at TIMESTAMPTZ,
  alive_at TIMESTAMPTZ NOT NULL,
  -- Set by a wake (`waiters.rs::wake_in_op`) that found this row and could
  -- NOT move it: `running` (a claim nulls `execute_at`, so there is no field
  -- to write a time into), `parked` behind a queue sibling (lowering a parked
  -- row's `execute_at` would break Invariant B), or in retry backoff
  -- (`attempt_index > 1`, which a wake must never shorten).
  --
  -- It is the record that the callee finished, for whichever write next makes
  -- this row runnable to honour: the `Disposition::Fresh` park write, or a
  -- parked-to-pending promote (`execution_hooks::promote`). Each consults and
  -- CLEARS it in the same statement -- so the mark is consumed strictly
  -- before `PromoteHeadsHook` computes swaps, and a row is therefore never
  -- visible to the hook carrying a deadline the mark is about to override.
  -- The retry-backoff path clears it without honouring it: the wake is spent
  -- (the callee did finish) and the job will run at its backoff, so leaving
  -- the mark would let attempt-count forgiveness resurrect it much later.
  woken_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL
);

-- Execution rows exist iff a job is pending/parked/running (deleted on
-- terminal, see `dispatcher.rs`/`batch_dispatcher.rs`), so this index makes
-- "at most one LIVE job per (job_type, unique_key)" structural and
-- index-exact at spawn time -- the enforcement point for
-- `KeyedJobSpawner::spawn`. A key becomes respawnable the instant its live
-- row is deleted. Spans all states deliberately (no `state` predicate): a
-- keyed row is never parked in practice (keyed spawns never set `queue_id`),
-- but the index does not rely on that -- it must keep blocking re-spawn
-- regardless of what value `state` holds. Resident jobs never carry a
-- `unique_key` (they're enforced absolutely by `idx_jobs_job_type_resident`
-- above), so they're entirely outside this index's partial predicate.
CREATE UNIQUE INDEX idx_job_executions_job_type_unique_key
  ON job_executions (job_type, unique_key)
  WHERE unique_key IS NOT NULL;

-- CLAIM PATH, the only index it needs. `state = 'pending'` contains ONLY
-- already-claimable rows -- every queue's blocked backlog sits in `parked`
-- instead -- so a single ordered prefix scan serves queued and unqueued
-- rows together, bounded by what the poll can admit. Leads with `job_type`
-- because every consumer probes per type (the poll's per-type window,
-- `claim_due_heads_in_op`, `min_wait`, the stale-pending reporter) --
-- without it, each probe filter-scans the whole pending set instead of its
-- own type's slice. `id` trails `execute_at` to make the per-type order
-- total, so that prefix is well defined instead of an arbitrary cut through
-- a group of rows sharing a timestamp (bulk spawns give a whole batch one).
-- See PERFORMANCE.md ("Claim admission") for the measurements behind this
-- shape.
CREATE INDEX idx_job_executions_pending_execute_at
  ON job_executions(job_type, execute_at, id)
  WHERE state = 'pending';

-- Exclusion constraint (Invariant A) AND the insert-time occupancy probe:
-- `spawner.rs`'s park-or-take insert infers this index via
-- `ON CONFLICT (queue_id) WHERE state IN ('pending','running') AND queue_id
-- IS NOT NULL`. A queue's active slot -- pending or running -- is unique by
-- construction; every other spawn to that queue lands `parked` instead.
CREATE UNIQUE INDEX idx_job_executions_queue_active
  ON job_executions (queue_id)
  WHERE state IN ('pending', 'running') AND queue_id IS NOT NULL;

-- Promote path: given a queue whose active row just vacated (completed,
-- reschedule-swap, reclaim-swap), find its oldest parked sibling. One
-- index-only descent per queue, independent of that queue's parked depth.
-- `id` trails `execute_at` for the same total-order reason as the claim
-- index above -- every instance/completer must resolve the same sibling.
CREATE INDEX idx_job_executions_parked_queue_head
  ON job_executions(queue_id, execute_at, id)
  WHERE state = 'parked';

ALTER TABLE job_executions SET (
  fillfactor = 70,
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 50,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_vacuum_cost_delay = 0,
  log_autovacuum_min_duration = 0
);

-- Written by running jobs (attempt-recovery state, id-addressed). Deleted
-- alongside the execution row on terminal (`dispatcher.rs`'s
-- `delete_execution_in_op`), EXCEPT for keyed types that opt into
-- `KeyedJobInitializer::inherits_state`: those rows are kept so the next
-- generation of the key can seed from them, and older generations are
-- compacted away at that key's next spawn (`keyed.rs`). The table therefore
-- stays O(live jobs + inheriting keys), not O(all generations ever).
CREATE TABLE job_execution_states (
  id UUID PRIMARY KEY,
  execution_state_json JSONB NOT NULL
);
ALTER TABLE job_execution_states SET (
  fillfactor = 50,
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 50,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_vacuum_cost_delay = 0
);

-- A job waiting on another. `waiter_job_id` parked itself (`RescheduleAt`)
-- until `job_id` reaches a terminal state; the finalizer that deletes
-- `job_id`'s execution row pulls the waiter's `execute_at` forward
-- (`waiters.rs::wake_in_op`), under the same guards as the keyed
-- pull-forward: pending, first attempt, scheduled later than now.
--
-- A waiter that cannot be moved when its callee lands is marked `woken_at`
-- instead, and whichever write next makes the row runnable consults the
-- mark: its own park write (`Disposition::Fresh`) if it was RUNNING, or a
-- parked-to-pending promote (`execution_hooks::promote`) if it was sitting
-- `parked` behind a queue sibling. Rows are deleted when consumed by a wake,
-- and unconditionally when the waiter itself goes terminal, so the table
-- stays O(live waits).
--
-- No FK to `jobs`: `jobs` rows are never deleted, and the finalizer's
-- `DELETE ... RETURNING` is the only consumer.
-- Pure edges. The "you were woken" MARK is deliberately NOT here: it is a
-- property of the WAITER, not of the (callee, waiter) pair -- a wake dedups
-- to distinct waiters and every consumer read it that way -- so it lives on
-- `job_executions.woken_at` instead. That keeps it on the row every consumer
-- is already updating, which is what makes the two hot-path consults
-- (`execution_hooks::promote`'s parked-head promote, and the
-- `Disposition::Fresh` park write) local column tests rather than a probe
-- into a table that is O(live waits).
CREATE TABLE job_waiters (
  job_id UUID NOT NULL,
  waiter_job_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (job_id, waiter_job_id)
);

-- Terminal cleanup (`delete_waits_of_in_op`): every wait a job registered.
CREATE INDEX idx_job_waiters_waiter ON job_waiters (waiter_job_id);
