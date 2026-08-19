# Poller performance

Why the claim query looks the way it does. Every number below was measured on
a local PostgreSQL 18 against seeded datasets shaped like production; the
harness lives in `.claude/bench/` (not committed) and is described at the end.

---

## Update — parked rows (current design)

Everything from here to "Reproducing" describes the design that shipped as
`f304fb1` (#172, "bound the claim scan by admission, not by backlog") and was
superseded by this change. It stays below for its reasoning — several of its
conclusions (the total-order tiebreak, per-type budgets, round-robin
interleaving, `SET enable_bitmapscan = off`) carry forward unchanged — but its
central claim-time mechanism (the blocked-queue anti-join and per-queue
`due_queued` LATERAL) no longer exists, replaced by the design below. Treat
this section as the current source of truth; the rest is historical context
for *why*.

### The problem #172 didn't fix

#172's claim query bounds the *candidate window* by admission (`n_claim *
headroom` rows), which stops one type's or one poll's backlog from crowding
out another's — but the blocked-queue anti-join (`NOT EXISTS (... state =
'running' ...)`) still runs **inside** that window, evaluated per row as the
index scan walks `(execute_at, id)` order. A queue whose job is already
running contributes rows that are scanned, anti-join-probed, and discarded —
never counted toward the window's `LIMIT`, but not free either. At 0 blocked
rows the window fills in `n_claim * headroom` probes; at depth, every blocked
row ahead of the next claimable one costs a probe that produces nothing. Cost
tracks the *position* of claimable work within the blocked backlog, not just
the backlog's size — measured locally at 228 buf/poll with no blocked rows,
climbing to 5,026 buf/poll at 1,600 (churned, single hot queue). This is the
exact shape of failure the "Two failures this replaced" section below
describes for shape 1 and the queue-walk, recurring one level down: the
window-bounding fix stopped the scan from being unbounded, but a blocked
queue's rows are still read.

### The fix: park blocked rows out of `pending` entirely

New state `parked`: a queued row (`queue_id IS NOT NULL`) whose queue already
has a live row. **Invariant A** — per `queue_id`, at most one row in state
`pending` or `running`, ever — is now a database constraint
(`idx_job_executions_queue_active`, a partial unique index), not an emergent
property of claim-time locking. **Invariant B** — the active row of a queue is
its min-`(execute_at, id)` live-or-parked row — is maintained by the write
paths (insert-time park-or-swap, retry/reschedule/reclaim-swap) rather than
being re-derived at claim time.

With blocked rows physically out of `state = 'pending'`, the claim query needs
no anti-join and no per-queue LATERAL: `pending` already contains only
claimable rows, so a single per-type LATERAL prefix scan (`window_rows`,
bounded by each type's own admission budget — kept per-type, not merged into
one global scan, to preserve the existing "a backlogged type cannot starve
another type's due row" guarantee) serves queued and unqueued rows together.
`CONTENTION_HEADROOM` (still 4) exists solely so `FOR UPDATE ... SKIP LOCKED`
has somewhere to fall through when a peer holds the rows this poll would
target — not, as before, to compensate for a row-bounded window under-filling
against a queue-counted budget: every window row is now a candidate, so that
compensation has nothing left to do. The adaptive widening ladder
(`CONTENTION_HEADROOM` → `MAX_CONTENTION_HEADROOM`, `candidates_short`) is
gone with it.

`may_have_more` simplifies correspondingly: re-poll immediately only when the
claim filled its budget, or some type's own window came back full while still
yielding a candidate overall. A window that comes back short means every
claimable due row was examined — exact now, not a heuristic, since nothing can
be discarded from the window after the fact.

### What moved to completion / write time

- **Promotion.** `delete_execution_in_op` (`dispatcher.rs`) now promotes the
  freed queue's oldest `parked` sibling to `pending` in the same statement
  that deletes the terminal row, using the identical `(execute_at, id)`
  tiebreak the claim used to resolve via `due_queued`. The batch completion
  paths (`batch_dispatcher.rs`) do the same per freed queue in one commit,
  notifying each promoted row's actual type (a batch can free several queues
  whose next job is a different type than the batch's own).
- **Ordering edges.** A backdated `spawn`/`spawn_at` that sorts before the
  occupying `pending` row swaps in ahead of it (demoting the occupant to
  `parked`) at insert time. The same swap runs after a retry reschedule, a
  voluntary reschedule, and a reclaim, so an older parked sibling is never
  stuck behind a row that just re-entered backoff.
- **Orphan sweep.** Piggybacked on the existing lost-job reclaim cadence: a
  `parked` row whose queue has no active row (a real, bounded race between a
  conflicting insert and the occupant's concurrent completion) is promoted.

### Bench gates

Not re-run with fresh numbers as part of this change (the harness needs
regenerating per "Reproducing" below, from the prototype in the handoff this
implements). Owed before the validation run referenced in the PR: `exp2.sh`
parity across `nblocked ∈ {0..1600}`, the seed matrix with blocked siblings
seeded as `parked`, and `writecost.sql` for the insert-path delta.

### Short-circuit dispatch: head-swap claiming (supersedes the born-claimed design below)

**Superseded design note.** An earlier pass of this change (still visible in
the git history of this PR) short-circuited a due-now spawn by inserting it
**born-claimed** — `running`-by-this-instance directly, unconditionally. That
design had a real fairness defect (see the retired write-up a few paragraphs
down) and has been replaced by **head-swap claiming**, described here. The
mechanism, scope, and trade-offs below are the current, authoritative state.

**The idea:** a short-circuit event — a due-now spawn, or a job/batch
completing — never dispatches a *specific* row. It obtains one unit of
capacity for a type `T` (a fresh reservation for a spawn; a **recycled**
unit for a completion, since the completing job's own slot is about to free
regardless), then claims `T`'s oldest due `pending` row(s) — whoever they
are — via `claim_due_heads_in_op`, a second statement in the SAME
transaction as the caller's own write. If the claim finds nothing, the
reservation releases and the caller's row (already inserted/promoted
normally) simply waits for the ordinary poll like today.

- **Fairness is structural, not a trade-off.** Within one type, admission is
  always `(execute_at, id)`-ordered — on the fast path exactly like the poll
  path. A short-circuited event can dispatch a DIFFERENT row than the one
  that triggered it, whenever an older due row of the same type exists.
  `short_circuit()` (on `JobInitializer`, `KeyedJobInitializer`, and
  `BatchedJobInitializer` — default `true` on all three) remains a per-type
  opt-out, but it is no longer covering for a fairness defect; it exists for
  latency-indifferent types that would rather never pay the extra claim
  statement.
- **Batched types are included.** A batch slot is already one tracker unit
  regardless of row count (`JobTracker::dispatch_batch`), so a reservation
  maps onto a batched type unchanged; the claim just uses `limit =
  max_batch_size` and excludes `attempt_index > 1` rows (retries always run
  alone, mirroring `dispatch_batches`' own split of an ordinary poll claim).
  No accumulator/collector is needed: partial batches under light load are
  already normal behavior (`dispatch_batches` chunks with
  `take = max_batch_size.min(fresh.len())`, no linger, no minimum), and under
  sustained load a completion's recycle claims a FULL batch in one statement,
  since the backlog is deep at exactly that moment.
- **Keyed types are included.** `KeyedJobSpawner::spawn`'s `Inserted` branch
  attempts the same claim after its ordinary insert.
- **The completion-time chain hop is implemented**, in
  `dispatcher.rs::delete_execution_in_op` (per-job) and
  `batch_dispatcher.rs`'s `seal`/`fail_batch` (per-batch, exactly once per
  `execute_batch` regardless of how many sub-outcomes it disposed — a batch
  is one execution unit no matter how many rows terminate inside it, so
  recycling from more than one of its internal branches would try to spend
  the same freed unit twice). A completing job/batch recycles its
  about-to-free unit into its OWN type's oldest due backlog — independent of
  which queue it happened to free, and independent of whether the promoted
  sibling (unchanged logic, still promoted unconditionally either way) is
  even the same type. Gated on `JobPoller::is_shutting_down()`: a completion
  during an in-flight shutdown drain never recycles, so it cannot re-admit
  work that would miss the shutdown broadcast and get force-aborted (#169).
- **Bulk `spawn_all` is included.** `try_claim_after_bulk_spawn` greedily
  reserves as many units as the batch's own due-row count could plausibly
  use and claims after each; under-reservation is not a failure — every row
  already landed pending/parked via the ordinary bulk insert regardless, so
  whatever isn't claimed here is picked up by the ordinary poll exactly as
  before this design.
- **Fan-out spawns from inside a running job's own runner still do not
  short-circuit** — the one piece carried over unchanged from the
  born-claimed design's scope cuts. Only spawns made from the
  application-level spawner returned by `Jobs::add_initializer`/
  `add_batched_initializer`/`add_keyed_initializer` do; a job calling
  `spawner.spawn(...)` on the spawner its own `init()` was handed gets the
  ordinary path. Contained, not a correctness issue — the plumbing (`init`/
  `init_erased` would need a live `PollerHandle` threaded through, which
  their signatures don't carry today) just wasn't judged worth it in this
  pass.
- **Cross-type fairness is explicitly not addressed**, and this is a
  permanent design line, not a scope cut: a completion only ever recycles
  into its OWN type's backlog. Comparing across types at claim time would
  reintroduce most of the poll query's own machinery; cross-type sharing
  stays the ordinary poll's job (a claim's reservation releases when nothing
  of its type is due, and the ordinary notify still wakes other types'
  pollers as before).

**Cost worth naming:** a claimed row's `SELECT ... FOR UPDATE SKIP LOCKED`
head stays row-locked for the remainder of the caller's own transaction
(the insert/promote plus the claim, both in one transaction) — bounded and
small in the ordinary case, but a caller that does unrelated slow work
between its write and its commit extends that window. `claim_due_heads_in_op`
is deliberately a SECOND, separate statement rather than folded into the
write CTE — the two genuine bugs found in this PR's write paths
(`swap_older_parked_siblings_in_op`'s CTE-ordering hazard; a missing
ordering check in the now-deleted born-claimed conflict path) both came from
collapsing independent writes into one statement with no guaranteed
ordering between them. Don't re-introduce that shape here to save a round
trip.

---

<details>
<summary>Retired: the born-claimed design's scope cuts and fairness trade-off (superseded above)</summary>

A due-now spawn of a non-batched, non-keyed, non-resident type whose
initializer allowed it skipped the pending queue entirely: capacity was
claimed synchronously, the row was inserted `running`-by-this-instance
directly, and a commit hook handed it to the dispatcher the instant the
transaction committed. Deliberately scoped down at the time: batched and
keyed types were excluded, the completion-time chain hop was not
implemented, fan-out spawns and bulk `spawn_all` never short-circuited.

**The fairness trade-off that motivated the head-swap replacement:** a
born-claimed spawn dispatched the instant capacity allowed, independent of
how much *older* due backlog of the same (or any) type was still sitting
`pending` — the ordinary claim path's oldest-first admission (see "Queue
fairness: oldest-first" above) did not apply to it. That was a real behavior
change for any type that ever accumulated backlog while spawning more work,
not just a latency improvement, which is why `short_circuit()` existed as a
per-type opt-out in the first place. Head-swap claiming removes the
trade-off structurally instead of documenting around it.

</details>

---

The short version: the poll query is the single hottest statement in the
system, and it has failed twice for the same underlying reason — the scan was
bounded by something other than what the poll can actually admit. One shape
returned **zero** rows while ~20k jobs sat ready and then immediately
re-polled, burning CPU at zero yield. Its replacement never returned zero, but
spent up to **395 buffers per job claimed** where the current one spends 23,
because its cost tracked the size of the backlog instead of the size of the
batch. The current shape bounds both: what it scans, and what it can claim, are
the same number.

---

## Workload shape this is tuned for

Most jobs carry a `queue_id`: they are commands mutating an entity, and two
commands for the same entity must not run concurrently. That means:

- **High queue cardinality** is normal — one queue per entity, most queues
  holding a single pending row.
- **Deep queues happen** — a hot entity accumulates a backlog, and while one of
  its commands runs, every other row in that queue is unclaimable.
- **Unqueued jobs are a minority** — fire-and-forget work with no exclusion.

Both cardinality extremes are real, so the design has to hold up at 50 deep
queues *and* at 15,000 shallow ones.

---

## The two failures this replaced

Both previous shapes lost work to the same mistake in different places:
**deciding what is claimable after bounding the scan, or bounding the scan by
something other than what the poll can admit.**

**Shape 1 — filter after the limit.** It took the `n_claim * 4` oldest **rows**,
then discarded the ones that were queue-blocked or not their queue's head. A
hot entity's blocked backlog is simultaneously the oldest work and entirely
unclaimable, so it filled the window and the poll returned nothing.
`may_have_more` then correctly fired and the poller immediately re-polled —
into the same clogged window.

8 pollers × 200 polls, realistic seed (85% queued, 14.9k queues, hot entities):

| design | claimed of 19,720 claimable |
|---|---|
| filter-after-limit | **0** |
| current | **19,720 (100%)** |

Not only a pathological case: on a uniform seed with no hot queues at all it
still claimed just 2,142 of 30,130 (7%).

**Shape 2 — bound the scan by queues examined.** The fix for shape 1 walked
`queue_id` space queue-by-queue, tallying claimable queues and stopping once it
had `n_claim * CONTENTION_HEADROOM` of them. That removed the cliff, but
`queue_id` order carries no information about *dueness*, so every not-yet-due
queue was visited and rejected on every poll — and because the tally counts
*claimable* queues, a low due fraction meant the budget was never reached and
the walk ran the entire pending-queue keyspace every time. With the job gate at
250/150, `n_claim` is 100–250 and the target 400–1000 claimable queues, which
production never has: the exit condition was dead code.

Cost per job claimed, measured (600 queues, `n_claim = 150`):

| fraction of queue heads due | queue walk | current |
|---|---|---|
| 100% | 55 buffers | 35 |
| 20% | 55 | 20 |
| 5% | 168 | 21 |
| 2% | **395** | **23** |

The walk's cost is flat in *work done* and linear in *backlog*. That is the
wrong way round, and it is what production saw: two live sandboxes measured
231 → 494 and 126 → 218 buffers per job claimed against the shape it replaced.


---

## Claim admission

The query gathers candidates that are **already claimable**, so the limit
counts useful work rather than raw rows. Queued and unqueued rows are gathered
by separate paths because their admission rules have nothing in common.

### Queued rows: two steps, both bounded by the budget

A queued row is claimable only if its queue has nothing running *and* it is
that queue's oldest pending row. Those are two different questions, and
answering them in one pass is what went wrong twice. They are now separate:

**Step 1 — which queues to examine.** A prefix of `(execute_at, id)` order over
due, *unblocked*, pollable rows, `LIMIT n_claim * CONTENTION_HEADROOM`. The
blocked-queue anti-join sits **inside** this window, below the `LIMIT`, so a
queue with a job already running contributes nothing no matter how deep its
backlog. That placement is the whole design — it is the difference between
this and shape 1.

**Step 2 — which row of each queue.** One `LATERAL` probe per examined queue
against `idx_job_executions_pending_queue_head`, taking its oldest due row.
One index-only descent per queue, independent of that queue's depth.

Cost is therefore `O(n_claim)`, flat in backlog and in queue count. Measured
against the queue walk, same claims in every case:

| queues | due heads | queue walk | current |
|---|---|---|---|
| 600 | 100% | 8,316 buf / 3.8 ms | **5,290 / 2.3 ms** |
| 600 | 20% | 6,697 / 3.1 ms | **2,445 / 1.1 ms** |
| 600 | 5% | 5,062 / 2.5 ms | **641 / 0.5 ms** |
| 600 | 2% | 4,746 / 2.4 ms | **285 / 0.4 ms** |
| 3,000 | 20% | 28,879 / 12.5 ms | **7,117 / 2.6 ms** |
| 3,000 | 2% | 23,962 / 11.0 ms | **1,411 / 0.7 ms** |

`DISTINCT ON (queue_id)` was rejected as the step-2 mechanism: a `Unique` node
cannot skip duplicates, so it reads every index entry and a 500-deep queue
costs 500 of them. It also dedups per *(type, queue)* when run inside a
per-type lateral, which yielded 27 queues with more than one running job.

#### The one case that still degrades, and why it is left alone

A single **unblocked** queue holding more due rows than the whole window
(> `n_claim * 4`) fills step 1 by itself, so that poll claims one row. It then
self-heals in the next poll, because claiming that row blocks the queue and
step 1's anti-join drops all of its rows:

| poll | claimed | `may_have_more` |
|---|---|---|
| 1 | 1 | true |
| 2 | 150 | true |
| 3 | 50 | false |

Identical at depth 1,000 and depth 5,000 — the cost is exactly one extra poll
of 133 buffers, not a function of depth, and `may_have_more` keeps the poller
from sleeping through it.

This is the degenerate end of the depth effect in "Why it has to adapt", and
adaptive widening does fire here — one step, on a poll that then saturates and
snaps it back. Widening cannot fix *this* shape (a wider window over one queue
is still one candidate), but it costs a single wider scan, not a spiral.

Closing it properly needs an "is this row its queue's head" anti-join inside
step 1, which was measured and **rejected**: it fixes the case at 18,523
buffers and 30 ms for depth 5,000 — unbounded in depth, worse than the walk it
replaced — and taxes the ordinary workload 25%.

### Picking a queue's row deterministically

Every instance must choose the *same* row for a given queue, or a peer's lock
on it reads as "try the row below" instead of "this queue is taken", and two
instances claim from one queue. Two things guarantee it:

- **Step 2 is type-agnostic.** The head is the queue's oldest due row outright;
  the pollable-type filter is applied only *after*. `plan_claim` drops
  saturated types, so the type set is instance-local — picking the oldest
  *pollable* row would let two instances that have saturated different types
  select different rows of one queue. Pinned by
  `queue_head_is_resolved_independently_of_saturated_types`. Filtering by type
  in step 1 is safe precisely because it only decides which queues get looked
  at, never which row within one.
- **The ordering is total.** `(execute_at, id)`, not `execute_at`. Bulk spawns
  give a whole batch one timestamp, and ordering by `execute_at` alone leaves
  the head of a tie group ambiguous. This is not theoretical: 8–16 concurrent
  pollers against 5,000 queues of tied rows produced a **real exclusion
  breach** without the tiebreak, and none with it (3 runs each). Pinned by
  `tied_execute_at_resolves_one_stable_queue_head`.

### Unqueued rows

They can never be blocked by a sibling, so they come straight off
`idx_job_executions_pending_unqueued` in `execute_at` order, bounded per type.

### Ordered index access is mandatory

`SET LOCAL enable_bitmapscan = off` is not a micro-optimisation. A bitmap scan
returns rows in heap order, destroying the index ordering that step 1 and the
unqueued scan depend on, and forcing a sort of every candidate: **10.3 ms vs
1.4 ms** on identical data. It is scoped to the poll transaction, alongside the
`plan_cache_mode` override that already lived there.

---

## Contention headroom, and the adaptive window

`CONTENTION_HEADROOM` (resting value 4) is how far past its budget a poll
gathers candidates. Its original job is to give `FOR UPDATE ... SKIP LOCKED`
somewhere to fall through when a peer holds the rows this poll would target.
Removing it entirely regresses a real case: with a type budget of 5 and a peer
holding exactly those 5 rows, a poll gathers 5 candidates, skips all 5, and
claims nothing (`poller_falls_through_locked_head_rows_to_later_due_jobs`).

It reads like shape 1's `* 4` overscan and is a different quantity. That
multiplier was sized to survive *filtering* — it had to guess how many gathered
rows would turn out unclaimable, and a deep blocked queue could exhaust any
guess. Step 1 now gathers only rows that are already unblocked.

### Why it has to adapt

The window bounds **rows**; the budget counts **queues**. Its yield is
therefore `window ÷ average depth of the queues it lands on`, and a fixed
multiplier of 4 saturates only while that depth stays ≤ 4. Measured on 2,000
queues at `n_claim = 150` (window 600):

| clustered depth | claims per poll |
|---|---|
| 1, 2, 4 | 150 |
| 8 | 75 |
| 16 | 38 |
| 32 | 19 |

Exactly `600 ÷ depth`. Depth alone is not enough to trigger it — the rows must
also be **adjacent in `execute_at` order**. With a queue's rows scattered
across time the window draws from ~600 distinct queues and saturates at every
depth tested, up to 32. And the queues must be **unblocked**: a blocked queue
contributes no rows at all, so depth there is free.

No work is lost — `may_have_more` fires and the loop re-polls — so the cost is
**polls**, not throughput. Draining a 3,000-job backlog:

| clustered depth | walk | fixed ×4 | fixed ×32 | **adaptive** |
|---|---|---|---|---|
| 1 | 14 polls | 14 | 14 | **14** (never widens) |
| 8 | 20 | 24 | 20 | **22** (peak ×8) |
| 32 | 20 | **86** | 20 | **34** (peak ×32) |

A fixed ×32 is robust but costs 42% more buffers at depth 1 — the common case,
and what production actually looks like at 1.26 rows per queue. So the window
adapts instead: `candidates_short` reports that the window **filled and still
produced fewer candidates than the budget**, and the poll loop doubles the
multiplier up to `MAX_CONTENTION_HEADROOM` (32), snapping straight back to 4 on
any saturated poll.

The signal deliberately does *not* fire for a budget lost to peers or to type
caps. Widening would buy a bigger scan for the same result in both cases. It is
specifically "the window was the binding constraint", which is the only thing
widening fixes. Pinned by
`deep_queues_report_a_short_window_and_widening_recovers_it`.

The regime this exists for is **recovery from a stall or a cold start** — every
queue unblocked with its full backlog due at once, which is when throughput
matters most and when a fixed ×4 degrades furthest.

---

## Per-type budgets and interleaving

Each type is bounded by its own budget at gather time, so no type's backlog can
consume the scan another type needs. That alone is not sufficient: the final
`LIMIT` is global, so a type with older rows could still take the whole batch.
`ordered_candidates` therefore ranks candidates **round-robin across types** —
every type's oldest ranks ahead of any type's second — and only then oldest
first within a rank. Pinned by
`capped_type_backlog_does_not_starve_another_type`.

---

## Queue fairness: oldest-first

Fairness is a byproduct of step 1's ordering rather than a separate mechanism.
Because the window is a prefix of `(execute_at, id)` order, a due row is
claimed once no more than `n_claim * CONTENTION_HEADROOM` *older* claimable
rows remain — so a job's wait is bounded by how much older work exists, and
rows age monotonically into the front of that order. No row can be passed over
indefinitely.

This replaced a per-poller round-robin cursor over `queue_id` space. That
cursor gave a strictly bounded per-*queue* service gap (one sweep) where
oldest-first does not, and the earlier simulation measured queue-service gaps
of up to 316 rounds for oldest-first against 40 for the cursor. It was dropped
anyway, for two reasons:

- The cursor only had that property because the walk visited every queue, which
  is exactly the `O(pending queues)` cost that had to go. Its fairness
  guarantee and its cost were the same mechanism.
- Per-queue round-robin is the wrong target. It equalises *queue* service rate,
  which advantages a queue with one old job over a queue with ten, and lets an
  old row wait behind newer rows in other queues. Age order is the guarantee
  this system actually wants, and it is the one users perceive.

What was given up: queue service order is not equalised, and a queue whose work
keeps arriving is served as often as its work is old — not more.


---

## When the poller may sleep

`may_have_more` decides whether the poll loop re-polls immediately or sleeps on
`next_due_at`. Under the queue walk it was true almost always — the walk only
reported completeness if it ran off the end of `queue_id` space having started
at the beginning — so the loop leaned on re-polling to find anything it had
missed. That is affordable only when a poll is cheap, and it was not.

It is now exact. A poll re-polls immediately only when it provably left
claimable work behind:

- it filled its budget, or
- step 1's window came back full *and* yielded at least one pollable head, so
  there is claimable work past the window, or
- the window was the binding constraint and **the width just grew**.

A short window means every claimable due row was examined, and `next_due_at` is
the honest next deadline.

The third condition is the awkward case. A full window that yielded **no**
pollable head is not evidence of an empty queue — hitting the LIMIT only means
a *prefix* of the due pollable rows was read, and rows sitting behind a head
this instance has saturated still consume window slots. Claimable heads past it
went unseen, they are already due, and `next_due_at` therefore does not cover
them: sleeping strands them. But re-polling at the same width reads the same
prefix — a spin at zero yield, the original disease.

So the answer is neither. `candidates_short` fires, `poll_and_dispatch` widens,
and the re-poll is conditioned on the widen having actually happened. That
bounds it to the ladder — 4 → 8 → 16 → 32, three extra polls at most — and
every step reads strictly further than the last. At the ceiling with still
nothing claimable the loop does sleep, and the event that unblocks those
saturated heads is a capped-type completion, which `job_completed` already
wakes for. Pinned by
`window_full_of_saturated_heads_does_not_read_as_exhausted`.

Sleeping accurately only works if every transition that creates claimable work
actually wakes someone, and one of them did not. `delete_execution_in_op`
reported a freed queue under the **completing** job's type, but a poller only
wakes for types it polls, and a queue's next job is frequently a different type
from the one that just vacated it. It now resolves and reports the freed
queue's next job type instead — using the same `(execute_at, id)` head
definition the claim uses, since ordering by `execute_at` alone would let it
name a different row, and so a different type, than the one step 2 treats as
the head.

That bug was invisible while `may_have_more` was permanently true. Fixing the
notify took the test suite from 60 s to 7.5 s — seven tests had been sitting on
timeouts waiting for a wake that never came, and passing anyway because a
later poll happened to pick the work up.

---

## What was dropped, and why

### `poll_debounce`

Removed. Spacing out notify-triggered polls reduced the poll query's CPU share
roughly in proportion to the delay — and reduced end-to-end throughput by about
as much. The notify waker is already near-optimal; poll frequency is the price
of fast pickup.

### `max_concurrent_global`

Removed. It was enforced by having **every** poll pre-count the fleet's running
executions, so every job type paid a scan of every running row whether or not
any type was capped. That cost grows with fleet size and was paid at full
notify frequency once debounce was gone. The bound it bought was only ever
soft — concurrent polls on different instances could still overshoot.

Use `max_concurrent_per_process` instead: exact, free at the database, and
multiplied by a known instance count it gives a real fleet-wide ceiling.

### A known, bounded race between reservation and poll (not fixed here)

Flagged by automated review on PR #173. `JobTracker::try_reserve` (the
head-swap short-circuit path's capacity check) and `JobRegistry::plan_claim`
(the ordinary poll's per-type row-limit computation) both read
`units_in_flight`, but nothing makes those two reads atomic with the DB
claim that follows. A short-circuit reservation that lands in the narrow
window between a poll's `plan_claim` snapshot and its claim query actually
executing against Postgres is invisible to that poll — the `row_limit` it
claims against was already baked into the query as a stale parameter — so a
per-type cap can be transiently exceeded by however many concurrent
short-circuit reservations land inside that one window.

The overshoot is bounded and self-correcting: `units_in_flight` is
authoritative again by the very next poll, and this is the same class of
soft accounting imperfection `max_jobs`' own unit-vs-row mismatch already
carries (filed, not fixed, in the original handoff). A real fix needs either
a lock spanning the whole plan-to-claim window — which would serialize every
short-circuit reservation behind poll latency, a real regression for
exactly the paths this design exists to speed up — or a post-claim backstop
that re-validates and releases any row a poll over-claimed before
dispatching it. Neither was judged worth bundling into the PR that
introduced short-circuit dispatch. A per-type cap that must never be
exceeded even transiently should not rely on this path alone until one of
those lands.

---

## Indexes: the write-path trade-off

Claim-side gains are paid for on insert, so the index set was measured, not
assumed. Poll numbers are 8 pollers × 200 polls; write numbers are the average
of 3 × 30k rows.

| index set | n | insert 30k | flip 30k | poll (realistic) | poll (uniform) |
|---|---|---|---|---|---|
| previous | 6 | 237 ms | 292 ms | 0 | 2,142 |
| **current** | **7** | **323 ms** | **398 ms** | **19,720** | **30,130** |
| + keep old type index | 8 | 670 ms | 592 ms | — | — |
| single merged index | 6 | 165 ms | 134 ms | 15,502 | 30,130 but **72.9 s** |

Net: **+1 index, ~+36% on the write path** (≈ +2.9 µs per job insert). Given
the claim path goes from zero throughput to full drain, that is the right side
of the trade.

Three things this settles:

- **Zero net new indexes is not achievable.** Merging both claim indexes into
  one collapses the unqueued path: without `job_type` leading, every registered
  type with no pending work scans the entire unqueued range. With 35 resident
  types that is ~35× the work — 72.9 s to drain what otherwise takes 1.9 s.
- **Dropping `idx_job_executions_pending_job_type_execute_at` is required.**
  Keeping it alongside the new pair nearly triples insert cost; the new indexes
  supersede it.
- **`INCLUDE` payloads are not worth it.** Covering the claim indexes with
  `INCLUDE (id, job_type)` cost ~9% more insert time and 25% more index bytes
  for no measurable claim-side gain — the poll touches only ~`n_claim` rows, so
  the heap fetches are free.

`idx_job_executions_pending_execute_at` carries `(execute_at, id)` and is now
the claim path's step-1 index, as well as what `min_wait` and the stale-pending
reporter read. Serving `min_wait` per-type instead — to drop it and get back to
the previous index count — was tried against the earlier design and
**rejected**: it cost 2.3× on the poll (4,342 vs 1,908 buffers, 1.39 vs 0.61
ms), with the whole regression in `min_wait` itself. It is no longer droppable
on any terms: step 1 is the only ordered access path to due work.

Both claim indexes carry `id` as their trailing column. That is a correctness
requirement, not a covering trick (see "Picking a queue's row
deterministically"); on `idx_job_executions_pending_queue_head` it also turns
step 2 into an index-only scan, worth 10% of the poll's buffers and 21% of its
time.

### Why `poller_instance_id` has no index

Dropping it is what pays for the two claim indexes. Measured over 30k churned
jobs, and on the poll itself:

| index set | n | index bloat | poll median | poll buffers |
|---|---|---|---|---|
| previous | 6 | 5,320 kB | — | — |
| claim indexes added | 7 | 7,064 kB (+33%) | 0.697 ms | 2,078 |
| **…and `poller_instance` dropped** | **6** | **6,088 kB (+14%)** | **0.627 ms** | **1,807** |

It is *better than free*: the claim `UPDATE` is ~42% of a poll and writes to
every index, so carrying one fewer makes the poll cheaper as well as the write
path. Every hot-path query filtering on `poller_instance_id` is id-led and
served by the unique index on `id`; `reclaim_lost_jobs` never uses it as a
leading predicate. Only `kill_remaining_jobs` (once per process shutdown) loses
an index lookup, and it was measured *faster* scanning instead.

Restore it if `job_executions` grows large: the shutdown scan is O(heap pages),
so **bloat is the trigger, not poller count**.

### HOT updates are structurally impossible here

Worth knowing before optimising for them: the claim sets `state`, and `state`
is in the predicate of every pending-partial index, so no claim can ever be a
HOT update. Measured HOT is 0.0% under every index set tried — the production
figure of ~1.3% is inherent to the pending/running design, not something an
index change moves. Heap bloat is likewise identical across all configs
(8,512 kB per 30k jobs); only *index* bloat responds.

Roughly half of a poll's buffer touches on a production-shaped table are bloat
rather than query shape (2,109 → 1,094 after `VACUUM FULL`), so vacuum health
matters as much as the query here.

That said, **this table's autovacuum settings are not known to need changing**,
and were deliberately left alone. A 30k-job churn burst reclaimed nothing, but
that burst completed in 909 ms against a 60 s `autovacuum_naptime` — the
measurement proves only that a sub-second burst finishes before the launcher
looks, not that vacuum falls behind. The table-level knobs are already near
maximal (`scale_factor` 0.01, `threshold` 50, `cost_delay` 0 — no throttling);
the remaining levers, `autovacuum_naptime` and `autovacuum_max_workers`, are
cluster GUCs that cannot be set from a migration and belong to the deployment.
If a stress run shows index bloat growing across a *sustained* window, that is
where to look — and it needs measuring over minutes, not milliseconds.

---

## Concurrency safety

Verified under 8, 12 and 16 concurrent pollers across every seed, including
5,000 queues whose rows all share one `execute_at`:

- **0** double-claims
- **0** queues with more than one running job
- **0** deadlocks
- **0** lock waits and **0** ungranted locks, sampled every 1 ms

The tied-timestamp case is not decoration. Run against an `execute_at`-only
ordering it produced a real breach on the first attempt, and zero across three
runs once the ordering was made total. A queue-exclusion bug is invisible on
any seed with distinct timestamps.

No lock waiting is structural rather than lucky: `SKIP LOCKED` never blocks,
and the `LIMIT` sits above `LockRows`, so skipped rows do not consume the limit
— the executor keeps pulling until it has locked enough or exhausts candidates.

The lock-join sits **below** the `LIMIT` so it runs lazily: only rows
`LockRows` actually pulls get probed. The sort above it is a blocking node, so
the full candidate set is still materialised and fall-through behaviour is
unchanged. Worst case — every candidate locked — it does exactly the work the
eager form did, never more.

Taking only each queue's **head** row is what upholds queue exclusion against a
concurrent poller: a peer that has already locked that head finds it locked and
skips the entire queue, instead of claiming the row behind it. This only holds
because every instance resolves the same head — see "Picking a queue's row
deterministically" for the two things that guarantee it.

---

## Reproducing

The harness under `.claude/bench/` is intentionally untracked; it needs a local
dev database (`make start-deps`).

Seeds:

- `seed_real.sql` — realistic: 85% queued, ~15k queues, hot entities, ~34.5k pending
- `seed_lowcard.sql` — 50 queues × 600 rows (deep-queue extreme)
- `seed.sql` — uniform, no skew
- `seed_due.sql` — `-v nq=N -v duepct=P`: N shallow queues with P% of heads due.
  The due fraction is the variable the queue walk was blind to; sweep it.
- `seed_cliff.sql` — `-v depth=D -v blocked=0|1`: one pathological queue ahead
  of ordinary ones, for the zero-claim cliff
- `seed_depth.sql` / `seed_clustered.sql` — `-v nq=N -v depth=D`: uniform depth,
  with each queue's rows scattered across `execute_at` or consecutive in it.
  The clustered variant is the one that under-fills a row-bounded window; the
  scattered one saturates at every depth, and the pair is what shows that
  clustering rather than depth is the trigger.

Drivers:

- `ab2.sh` — A/B against the previous shape, each arm on its own fresh seed
- `cliff.sh` / `selfheal.sh` — cliff matrix, and the multi-poll recovery sequence
- `depth2.sh` — claims per poll against clustered depth
- `drain.sh` / `adaptive2.sh` — polls and buffers to drain a fixed backlog at a
  fixed headroom, and under the real `candidates_short` adaptive loop
- `race.sh` — N concurrent pollers on tied `execute_at`; asserts zero queues with
  more than one running job. This is what caught the missing `id` tiebreak, and
  it caught it on the *first* run — do not skip it.
- `stress.sh` — N concurrent pollers; claims, double-claims, deadlocks, lock waits
- `idx_sets.sql` / `writecost.sql` — index-set switcher and write-path cost

When changing the claim query, re-run every seed. Each of them has caught a
bug the others missed: `seed_lowcard` the per-type `DISTINCT ON` breach,
`seed_due` the walk's blindness to dueness, `seed_cliff` the zero-claim cliff,
`race.sh` the ordering tiebreak.
