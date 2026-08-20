# Poller performance

The claim query is the single hottest statement in this crate: every poller
instance runs it on every wake, and its cost is paid whether or not there is
work to do. The design rule everything below follows from is that **what a
poll scans and what a poll can admit are the same number** — cost is
`O(admission budget)`, flat in the size of the backlog and in the number of
queues.

Numbers quoted here were measured against a local PostgreSQL 18 on seed data
shaped like production (high queue cardinality, a minority of hot queues,
~85% of rows queued). They are here to explain *why* a mechanism exists, not
as a benchmark suite.

---

## Workload shape this is tuned for

Most jobs carry a `queue_id`: they are commands mutating an entity, and two
commands for the same entity must not run concurrently. That means:

- **High queue cardinality** is normal — one queue per entity, most queues
  holding a single row.
- **Deep queues happen** — a hot entity accumulates a backlog, and while one
  of its commands runs, every other row in that queue is unclaimable.
- **Unqueued jobs are a minority** — fire-and-forget work with no exclusion.

Both cardinality extremes are real, so the design has to hold up at 50 deep
queues *and* at 15,000 shallow ones.

---

## Row states and the two invariants

`job_executions.state` is `pending`, `parked`, or `running`.

- **`parked`** is a queued row whose queue already has an active row.
  Unqueued rows are never parked — nothing can block them.
- **Invariant A (exclusion).** Per `queue_id`, at most one row in state
  `pending` or `running`. This is a database constraint —
  `idx_job_executions_queue_active`, a partial unique index — not an
  emergent property of claim-time locking. Pinned by
  `queue_active_unique_index_enforces_exclusion`.
- **Invariant B (order).** A queue's active row is its
  min-`(execute_at, id)` live-or-parked row. Maintained by the write paths,
  not re-derived at claim time. Claim correctness does not depend on it —
  exclusion is Invariant A alone — but scheduling semantics do: a queue's
  backlog must still drain oldest-first.

The consequence that shapes everything else: **`state = 'pending'` contains
only rows that are actually claimable, at most one per queue.** A queue's
blocked backlog is physically outside the claim scan, so its depth costs the
poll nothing — not a wasted probe, not a buffer touch. Pinned by
`blocked_queue_backlog_does_not_consume_the_budget`.

What this buys is bounded claim cost in the presence of hot queues. What it
costs:

- **Work moved to the write and completion paths.** Every insert of a queued
  row pays an `ON CONFLICT` probe against the queue's active slot; every
  completion of a queued job pays a promote of that queue's oldest parked
  sibling, in the same statement that deletes the terminal row.
- **Invariant B is a standing obligation on every write path.** Anything
  that moves a row back to `pending` — retry backoff, voluntary reschedule,
  lost-job reclaim — must re-check for an older parked sibling and swap if
  one exists. A path that forgets leaves a queue draining out of order.
  Covered by `retry_backoff_yields_to_an_older_parked_sibling`,
  `reclaim_lets_an_older_parked_sibling_run_first`, and
  `backdated_spawn_swaps_ahead_of_a_younger_pending_head`.
- **An orphan race that needs sweeping.** A spawn can conflict against a
  queue's active slot and land `parked` while that occupant is concurrently
  completing — the occupant promotes nothing, because the parked row was not
  visible to it yet. `sweep_orphaned_parked_rows` promotes any parked row
  whose queue has no active row, piggybacked on the lost-job reclaim cadence
  (`job_lost_interval / 2`) rather than a timer of its own. Pinned by
  `orphan_sweeper_recovers_orphaned_parked_row`.
- **`parked` is invisible outside the crate.** `load_snapshot_by_id` masks it
  back to `Pending`, keeping the public `JobExecutionState`/`JobStatus`
  contract two-valued. The stale-pending reporter
  (`job.check_stale_pending_jobs`) counts only `state = 'pending'`, so a deep
  hot queue contributes its head and nothing else to that gauge — it reports
  *claimable* backlog, not total backlog.

---

## Claim admission

One statement, no anti-join, no per-queue `LATERAL`. Because `pending` is
already the claimable set, queued and unqueued rows share a single ordered
scan.

**Step 1 — one bounded prefix per type.** For each pollable type, a `LATERAL`
prefix of `(execute_at, id)` order over that type's due `pending` rows,
limited to `LEAST(row_limit, n_jobs_to_poll) * CONTENTION_HEADROOM`. The
budget is the type's *own*, never a shared global one: a backlogged type
cannot crowd another type's due row out of the window entirely. Pinned by
`capped_type_backlog_does_not_starve_another_type` and, for batched types,
`claims_are_capped_by_free_batch_slots`.

**Step 2 — round-robin across types.** `ordered_candidates` ranks every
type's oldest ahead of any type's second, and only then oldest-first within a
rank. Without this the global `LIMIT` could be consumed end-to-end by whichever
type happens to hold the oldest rows.

**Step 3 — lock, then cap.** The join back to `job_executions` sits *below*
the `LIMIT` so it runs lazily: only rows `LockRows` actually pulls get probed.
Per-type budgets are enforced *after* locking, on rows actually held — rows
over a type's cap are simply not claimed and their locks release at commit.
`job_execution_states` is joined after the `LIMIT`, so the state payload is
fetched only for winners.

Cost is `O(n_jobs_to_poll × CONTENTION_HEADROOM)` index entries plus the
claim `UPDATE`, regardless of how many rows or queues are pending.

### The ordering must be total

`(execute_at, id)`, never `execute_at` alone, and the same tiebreak in every
place a "head" is resolved: the claim window, the completion-time promote,
the Invariant-B swap, the orphan sweep. Bulk spawns give a whole batch one
timestamp, so ordering by `execute_at` alone leaves the head of a tie group
ambiguous — the claim window becomes an arbitrary cut through a tie group
rather than a well-defined prefix, and two writers can disagree about which
parked sibling is next, promoting a different row (and so notifying a
different *type*) than a peer would have.

### Contention headroom

`CONTENTION_HEADROOM` is 4: how far past its budget a poll gathers
candidates. Its only job is to give `FOR UPDATE ... SKIP LOCKED` somewhere to
fall through when a peer instance holds the rows this poll would target.
Without it, a type budget of 5 against a peer holding exactly those 5 rows
gathers 5 candidates, skips all 5, and claims nothing — pinned by
`poller_falls_through_locked_head_rows_to_later_due_jobs`.

It is **fixed, not adaptive**, and that is a direct consequence of parked
rows: every window row is by construction a candidate, so the window can only
under-deliver through lock contention, which a small constant overscan
absorbs. There is no filtering left for the multiplier to survive and nothing
for an adaptive ladder to recover.

The cost is that a poll reads up to 4× its budget in index entries when the
backlog is deep enough to fill the window. That is bounded by the budget, not
by the backlog.

### Ordered index access is mandatory

The claim query runs on a dedicated two-connection pool (`build_poll_pool`)
whose connections carry `plan_cache_mode = force_generic_plan` and
`enable_bitmapscan = off` from `after_connect`.

- **`enable_bitmapscan = off`** is not a micro-optimisation. A bitmap scan
  returns rows in heap order, destroying the index ordering the prefix scan
  depends on and forcing a sort of every candidate: **10.3 ms vs 1.4 ms** on
  identical data.
- **A dedicated pool, not `SET LOCAL`.** Session-level overrides must never
  leak onto the shared application pool, and setting them once per connection
  instead of inside a `BEGIN`/`COMMIT` on every poll turns the claim into a
  single autocommit statement — 5 round trips down to 1.

---

## When the poller may sleep

`may_have_more` decides whether the poll loop re-polls immediately or sleeps
until `next_due_at`. It is exact rather than heuristic: a poll re-polls
immediately only when it filled its budget, or when some type's own window
came back full while the poll still yielded at least one candidate (rows past
that type's window are unseen and already due). A window that came back short
for every type means every claimable due row was examined — nothing can be
discarded from the window after the fact, so there is nothing left to miss.

Sleeping accurately only works if every transition that creates a claimable
row wakes a poller that covers that row's **type**, and type is the subtle
part: a poller only wakes for types it polls, and a queue's next job is
frequently a different type from the one that just vacated it. So every
promotion notifies the *promoted* row's type, not the completing row's:

- `delete_execution_in_op` resolves the freed queue's oldest parked sibling
  and notifies its type (pinned by
  `reclaim_reports_a_promoted_sibling_of_a_different_type` for the reclaim
  equivalent).
- `PromoteHeadsHook` notifies its caller's own types plus every distinct
  promoted type — deliberately generous, because after hooks merge there is
  no way to attribute a promoted type back to one registration. A redundant
  notify costs one empty poll; a missed one costs a stranded row.
- `ExecutionInsertHook` notifies every type that landed a `pending` row or
  gained a promotion, *without* due-gating: a plain notify only wakes the
  ordinary poll, which re-checks `execute_at <= now` itself.

`next_due_at` is computed over `pending` rows only. A parked row is
unclaimable until promoted, and promotion always emits a notify, so excluding
parked rows from the sleep deadline is sound rather than an approximation.

---

## Queue fairness: oldest-first

Fairness is a byproduct of the window's ordering rather than a separate
mechanism. Because each type's window is a prefix of `(execute_at, id)`
order, a due row is claimed once no more than `budget × CONTENTION_HEADROOM`
*older* claimable rows of its type remain, and rows age monotonically toward
the front of that order. No row can be passed over indefinitely.

The trade-off is that **queue service order is not equalised**. A queue whose
work keeps arriving is served as often as its work is old — not more, not
less. Per-queue round-robin would equalise queue service rate instead, which
advantages a queue holding one old job over a queue holding ten and lets an
old row wait behind newer rows in other queues; it also requires visiting
every queue, which is exactly the `O(pending queues)` cost this design
exists to avoid. Age order is the guarantee this system wants and the one
users perceive.

---

## Short-circuit dispatch: head-swap claiming

A due-now spawn, or a job/batch completion, does not wait for the next poll.
It also never dispatches a *specific* row.

The mechanism: the event obtains one unit of capacity for a type `T` — a
fresh reservation for a spawn, a **recycled** unit for a completion (the
completing job's own slot is about to free regardless) — then claims `T`'s
oldest due `pending` row(s), whoever they are, via `claim_due_heads_in_op`.
That claim is a later statement in the *same* transaction as the caller's own
write, so it sees that write with guaranteed statement ordering. If nothing is
due, the reservation releases and the caller's row (already inserted or
promoted normally) waits for the ordinary poll.

- **Fairness is structural, not a trade-off.** Within one type, admission is
  always `(execute_at, id)`-ordered, on the fast path exactly as on the poll
  path. A short-circuited event can and does dispatch a different row than
  the one that triggered it whenever an older due row of the same type
  exists — pinned by `spawn_yields_to_an_older_pending_row_of_the_same_type`.
  `short_circuit()` (on `JobInitializer`, `KeyedJobInitializer` and
  `BatchedJobInitializer`, default `true` on all three) is a per-type opt-out
  for latency-indifferent types that would rather not pay the extra claim
  statement, not a correctness lever.
- **Batched types are included.** A batch slot is one tracker unit regardless
  of row count, so a reservation maps onto a batched type unchanged; the
  claim uses `limit = max_batch_size` and excludes `attempt_index > 1` rows
  (retries always run alone, mirroring how `dispatch_batches` splits an
  ordinary poll claim). Partial batches under light load are already normal
  behavior — there is no linger and no minimum — and under sustained load a
  completion's recycle claims a full batch in one statement, because the
  backlog is deep at exactly that moment.
- **Completions recycle into their own type.** `delete_execution_in_op`
  (per job) and `seal`/`fail_batch` (per batch, exactly once per
  `execute_batch` no matter how many sub-outcomes it disposed) hand the
  about-to-free unit to a `ClaimHook`. This is independent of which queue was
  freed and of whether the promoted sibling is even the same type. Pinned by
  `completion_recycles_into_a_promoted_sibling_with_no_poll_needed`.
- **Shutdown suppresses recycling.** `ClaimHook::pre_commit` checks
  `JobPoller::is_shutting_down()`, so a completion during an in-flight drain
  never re-admits work that would miss the shutdown broadcast and get
  force-aborted. Pinned by
  `completion_during_shutdown_does_not_recycle_into_new_work`.
- **Shutdown receivers are subscribed before commit.** A `tokio::sync::
  broadcast` never delivers to a late subscriber, so a shutdown broadcast
  landing between commit and the dispatch task starting would be invisible to
  it. `pre_commit` subscribes synchronously, mirroring how `dispatch_job`
  subscribes before spawning. Pinned by
  `short_circuit_spawn_dispatch_survives_a_shutdown_race`.
- **Bulk spawns are included**, as a count: the insert hook reports how many
  due-now rows landed pending per type and the claim hook converts that into
  `n_due.div_ceil(rows_per_reservation)` reservations, claiming for all of
  them in one statement per type. Under-reservation is not a failure — every
  row landed pending or parked via the ordinary insert regardless, so
  whatever is not claimed here the ordinary poll picks up.
- **Fan-out spawns from inside a running job's runner do not
  short-circuit.** The `JobSpawner`/`KeyedJobSpawner` handed to `init()`
  carries a never-populated poller handle, so those spawns take the ordinary
  insert path. Only spawners returned by `Jobs::add_initializer` /
  `add_batched_initializer` / `add_keyed_initializer` short-circuit. This is
  a plumbing gap (`init` has no `PollerHandle` to hand through), not a
  correctness issue: the ordinary poll covers it.
- **Cross-type fairness is explicitly out of scope**, permanently. A
  completion only ever recycles into its own type's backlog; comparing
  across types at claim time would reintroduce most of the poll query's
  machinery. Cross-type sharing stays the ordinary poll's job.

### When the claiming transaction fails

`ClaimHook::on_rollback` cannot distinguish "a later hook errored and
everything rolled back" from "the `COMMIT` itself errored after the rows
actually landed `running`". `ClaimReconciler` resolves it by checking rather
than assuming: it resets any row that really landed (restoring its original
`execute_at`, re-running the Invariant-B swap, and notifying), retrying over
250 ms / 1 s / 4 s before abandoning the row to `reclaim_lost_jobs`' slower
backstop. Cost is zero on every successful commit — nothing spawns — and one
indexed statement in the common rollback case. Pinned by
`reconciler_resets_a_row_that_actually_landed_running`,
`reconciler_is_a_noop_for_a_row_that_never_landed`, and
`reconciler_swaps_an_older_parked_sibling_ahead_of_the_reset_row`.

---

## The commit-hook pipeline

The insert, promote, and claim statements do not run inline at the call site
mid-transaction. They run in three `CommitHook`s' `pre_commit`, at
`op.commit()` time:

- **`ExecutionInsertHook`** — one combined
  `INSERT ... ON CONFLICT DO NOTHING` / fallback-`INSERT 'parked'` statement
  for every row registered on this `op` (single, bulk, and resident spawns;
  several `spawn_in_op` calls sharing one `op` merge into ONE multi-row
  statement), then any Invariant-B swap via `PromoteHeadsHook::apply`, then
  notify and due-now claim demand, both staged re-entrantly. Keyed spawns
  insert through their own liveness-enforcing statement
  (`insert_keyed_execution`) and register claim demand directly — they never
  set a `queue_id`, so they can never park.
- **`PromoteHeadsHook`** — the retry/reschedule/reclaim swap statement,
  registered by those call sites, or invoked directly as `apply` where no
  hook buffer exists (`reclaim_lost_jobs` runs on a raw pool transaction).
- **`ClaimHook`** — reserves capacity and runs `claim_due_heads_in_op`, one
  statement per type covering however many units that op's spawn and
  completion demand asked for.

Ordering — Insert, then Promote, then Claim — is enforced two ways:
`ClaimHook::runs_after` declares a dependency on `PromoteHeadsHook`'s type
(the commit-hook queue defers a hook behind any still-pending instance of a
declared dependency type, regardless of registration order), and within
`ExecutionInsertHook::pre_commit` itself by plain sequential Rust — insert,
promote, then re-entrant claim registration — so by the time a re-entrantly
staged `ClaimHook` runs, both statements before it already have.

**Implications worth knowing:**

- **Lock-hold time is bounded by the commit pass, not the caller.** A claimed
  row's `FOR UPDATE SKIP LOCKED` head is locked only from the claim to the
  `COMMIT`, regardless of how long the caller's own transaction ran
  beforehand. A long-running transaction that spawns early does not hold a
  claimed head hostage against the poller for its whole duration.
- **Insert errors surface at `commit()`.** An insert that would have failed
  inline at a `_in_op` call (constraint violation, connection error) now
  fails at `op.commit()`; the entity `create_in_op` has already landed
  (uncommitted) by the time the hook's `pre_commit` runs. Public
  `spawn()`/`spawn_all()` are unaffected — same `Result<_, JobError>`, just
  returned from a different internal await point — but `_in_op` callers
  composing their own transaction should expect the failure point to be the
  commit.
- **Hook registration failure degrades safely, in opposite directions.**
  `ExecutionInsertHook` and `PromoteHeadsHook` force inline execution if the
  `op` carries no hook buffer — their work must not be dropped.
  `ClaimHook` does the opposite and drops itself: forcing it inline would
  claim rows with no `post_commit` pass to dispatch them, stranding them
  `running` until `reclaim_lost_jobs` recovered them. No claim at all is the
  safer failure.

**Never collapse Promote and Claim, or Insert and Promote, into one statement
to save a round trip.** CTEs within a single Postgres statement share one
snapshot: a row a sibling CTE just promoted to `pending` is invisible to
another CTE in the same statement scanning `state = 'pending'`, and two
independent writes to the same table with no data-dependency edge between
them have no ordering guarantee at all. That is not hypothetical — it is why
the swap statement's promote `UPDATE` reads `FROM demote` rather than from
`swaps`: without the forced data dependency, both writes can transiently make
two rows active for one queue and violate `idx_job_executions_queue_active`.
Separate sequential statements plus `runs_after` is the sanctioned way to get
"sees the prior write, with guaranteed ordering".

---

## Capacity accounting: a bounded, self-correcting overshoot

`JobTracker::try_reserve` (the short-circuit path's capacity check) and
`JobRegistry::plan_claim` (the poll's per-type row-limit computation) both
read `units_in_flight`, but nothing makes those reads atomic with the DB
claim that follows. A short-circuit reservation landing in the window between
a poll's `plan_claim` snapshot and its claim query executing is invisible to
that poll — the `row_limit` was already baked into the query as a parameter —
so a per-type cap can be transiently exceeded by however many concurrent
short-circuit reservations land inside that one window.

The overshoot is bounded and self-correcting: `units_in_flight` is
authoritative again by the very next poll. Closing it would need either a
lock spanning the whole plan-to-claim window — serializing every
short-circuit reservation behind poll latency, a regression for exactly the
paths this design exists to speed up — or a post-claim backstop that
re-validates and releases over-claimed rows before dispatch. A per-type cap
that must never be exceeded even transiently should not rely on this path
alone.

---

## Indexes and the write-path trade-off

Claim-side gains are paid for on insert, so `job_executions` carries only the
indexes that earn their keep:

| index | serves |
|---|---|
| `idx_job_executions_pending_execute_at` on `(execute_at, id) WHERE state = 'pending'` | the claim window (the only ordered access path to due work), `min_wait`, and the stale-pending reporter |
| `idx_job_executions_queue_active` — UNIQUE on `(queue_id) WHERE state IN ('pending','running') AND queue_id IS NOT NULL` | Invariant A itself, and the insert-time occupancy probe the park-or-take `ON CONFLICT` arbiter infers |
| `idx_job_executions_parked_queue_head` on `(queue_id, execute_at, id) WHERE state = 'parked'` | the promote path: one index-only descent per freed queue, independent of that queue's parked depth |
| `idx_job_executions_job_type_unique_key` — UNIQUE, all states | keyed-job liveness at spawn time |

Three things this settles:

- **`INCLUDE` payloads are not worth it.** Covering the claim index with
  `INCLUDE (id, job_type)` costs roughly 9% more insert time and 25% more
  index bytes for no measurable claim-side gain — a poll touches only about
  `n_jobs_to_poll` rows, so the heap fetches are free.
- **`id` trailing `execute_at` is a correctness requirement**, not a
  covering trick — see "The ordering must be total". On
  `idx_job_executions_parked_queue_head` it also makes head resolution an
  index-only scan.
- **The claim index is not droppable on any terms.** Serving `min_wait`
  per-type instead has been tried and measured slower, and the claim path has
  no other ordered access to due work.

### Why `poller_instance_id` has no index

The claim `UPDATE` is a large share of every poll's cost and writes to every
index on the table, so carrying one fewer index makes the poll cheaper as
well as the write path. Every hot-path query filtering on
`poller_instance_id` is id-led and served by the unique index on `id`;
`reclaim_lost_jobs` never uses it as a leading predicate. Only
`kill_remaining_jobs` (once per process shutdown) loses an index lookup, and
it measured *faster* scanning instead.

Restore it if `job_executions` grows large: the shutdown scan is O(heap
pages), so **bloat is the trigger, not poller count**.

### HOT updates, fillfactor, and vacuum

The claim sets `state`, and `state` is in the predicate of every partial
index on the table, so **no claim can ever be a HOT update** — measured HOT
is 0.0% for claims under every index set tried. The production figure of
around 1.3% is inherent to the pending/parked/running design, not something
an index change moves. `fillfactor = 70` is there for the updates that *can*
go HOT — chiefly the keep-alive heartbeat, which touches only the unindexed
`alive_at`.

Heap bloat is identical across index configurations (about 8,512 kB per 30k
jobs); only *index* bloat responds. Roughly half of a poll's buffer touches
on a production-shaped table are bloat rather than query shape (2,109 → 1,094
after `VACUUM FULL`), so vacuum health matters as much as the query here.

That said, **this table's autovacuum settings are not known to need
changing**, and are deliberately left alone. A 30k-job churn burst reclaimed
nothing, but that burst completed in 909 ms against a 60 s
`autovacuum_naptime` — which proves only that a sub-second burst finishes
before the launcher looks. The table-level knobs are already near maximal
(`scale_factor` 0.01, `threshold` 50, `cost_delay` 0 — no throttling); the
remaining levers, `autovacuum_naptime` and `autovacuum_max_workers`, are
cluster GUCs that cannot be set from a migration and belong to the
deployment. If a stress run shows index bloat growing across a *sustained*
window, that is where to look — and it needs measuring over minutes, not
milliseconds.

---

## Concurrency safety

Queue exclusion does not depend on the claim query being clever: Invariant A
is a unique index, so a second active row for a queue is a constraint
violation, not a race that got through. What the claim path adds on top:

- **`SKIP LOCKED` never blocks**, and the `LIMIT` sits above `LockRows`, so
  skipped rows do not consume the limit — the executor keeps pulling until it
  has locked enough or exhausted candidates. No lock waiting is structural
  rather than lucky.
- **The lock join sits below the `LIMIT`** so it runs lazily; the sort above
  it is a blocking node, so the full candidate set is still materialised and
  fall-through behaviour is unchanged. Worst case — every candidate locked —
  it does exactly the work an eager form would, never more.
- **The short-circuit claim races the poll claim harmlessly.** Both use
  `FOR UPDATE SKIP LOCKED` on the same `pending` head rows and produce
  byte-identical writes (same `state`, `poller_instance_id`, `alive_at`,
  `execute_at`), so whichever gets there first wins and the other skips.

---

## Deliberately absent

- **`poll_debounce`.** Spacing out notify-triggered polls reduced the poll
  query's CPU share roughly in proportion to the delay — and reduced
  end-to-end throughput by about as much. Poll frequency is the price of fast
  pickup, and short-circuit dispatch has since removed most of the polls that
  debouncing was trying to suppress.
- **`max_concurrent_global`.** Enforcing it required *every* poll to
  pre-count the fleet's running executions, so every job type paid a scan of
  every running row whether or not any type was capped — a cost that grows
  with fleet size, for a bound that was only ever soft (concurrent polls on
  different instances could still overshoot). Use
  `max_concurrent_per_process`: exact, free at the database, and multiplied
  by a known instance count it gives a real fleet-wide ceiling.

---

## Validating a change to the claim path

The behaviour above is pinned by committed tests, and a change to the claim
query, the invariants, or the write paths should be judged against them:

- `src/poller.rs` unit tests — admission budgets, blocked-queue exclusion,
  Invariant A enforcement, orphan sweeping, reclaim swaps, and the claim
  reconciler.
- `tests/parked_rows.rs` — park/swap/promote across spawn, bulk spawn,
  backdated spawn, retry backoff, keyed spawn, and the full short-circuit
  matrix including the shutdown races.
- `tests/poll_contention.rs` — `SKIP LOCKED` fall-through and the
  sleep-versus-re-poll decision.
- `tests/batched_job.rs` — batch slot accounting and claim caps.
