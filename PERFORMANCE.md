# Poller performance

Why the claim query looks the way it does. Every number below was measured on
a local PostgreSQL 18 against seeded datasets shaped like production; the
harness lives in `.claude/bench/` (not committed) and is described at the end.

The short version: the poll query is the single hottest statement in the
system, and the thing that dominates its cost is not how *expensive* it is but
how much claimable work it *misses*. An earlier shape could return **zero**
rows while ~20k jobs sat ready, and then immediately re-poll — burning CPU at
zero yield.

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

## The failure this replaced

The previous query took the `n_claim * 4` oldest **rows**, then discarded the
ones that were queue-blocked or not their queue's head. Filtering *after* the
window is the bug: a hot entity's blocked backlog is simultaneously the oldest
work and entirely unclaimable, so it filled the window completely and the poll
returned nothing. `may_have_more` then correctly fired and the poller
immediately re-polled — into the same clogged window.

8 pollers × 200 polls, realistic seed (85% queued, 14.9k queues, hot entities):

| design | claimed of 19,720 claimable |
|---|---|
| old window | **0** |
| current | **19,720 (100%)** |

It was not only a pathological-case problem. On a uniform seed with no hot
queues at all, the old shape still claimed just 2,142 of 30,130 (7%).

---

## Claim admission

The query gathers candidates that are **already claimable**, so the limit
counts useful work rather than raw rows. Queued and unqueued rows are gathered
by separate paths because their admission rules have nothing in common.

### Queued rows: enumerate queues, not rows

A queued row is claimable only if its queue has nothing running *and* it is
that queue's oldest pending row. Evaluating that per row costs O(pending rows).
Instead `walk` seeks queue-to-queue through
`idx_job_executions_pending_queue_head` — one index descent per queue,
regardless of that queue's depth — and carries a running tally of eligible
queues so the recursion **terminates itself** once there are enough.

That self-termination is what makes it work at both extremes. Two rejected
alternatives:

| approach | 50 deep queues | 14,920 queues |
|---|---|---|
| `DISTINCT ON (queue_id)` | 17.4 ms | 1.4 ms |
| loose scan, no early stop | 0.24 ms | 19.1 ms (walks all 14,921) |
| **loose scan, self-terminating** | **0.49 ms** | **0.33 ms** (stops at 50) |

`DISTINCT ON` looks tempting but a `Unique` node cannot skip duplicates — it
reads every index entry, so a 500-deep queue costs 500 entries.

It also has a **correctness** trap. Running `DISTINCT ON` inside the per-type
lateral dedups per *(type, queue)*, so a queue carrying three job types yields
three concurrently-running jobs. Measured: 27 queues with more than one running
job. The queue walk is deliberately **global**, not per type, because queue
exclusion is global.

Two things the walk must get right, both of which reintroduce starvation if
they are treated as details:

- **The budget counts *claimable* queues, not merely unblocked ones.** A queue
  whose oldest due row is future-scheduled, or belongs to a type this instance
  has saturated, yields no candidate. Counting it lets such queues exhaust the
  budget while claimable work further along the sweep is never examined — the
  original disease in a new place. Eligibility is evaluated against the queue
  already in hand rather than the one being advanced to, so a step still costs
  one seek to advance plus its checks.
- **`may_have_more` must account for the sweep, not just the claim.** A walk
  that stopped on its budget leaves queues past it unexamined; one that wrapped
  from a mid-space cursor never looked at the queues *before* that cursor. In
  both cases there is due work this poll could not see, and reporting otherwise
  lets the poller sleep on `next_due_at` while it sits there. Only a sweep that
  ran off the end having started from the beginning covered everything.

### Picking a queue's row deterministically

Every instance must choose the *same* row for a given queue: the oldest due
row, with the pollable-type filter applied only afterwards. Choosing the oldest
*pollable* row instead would let two instances that have saturated different
types select different rows of the same queue and both claim it, breaking queue
exclusion. Deciding identically everywhere is what makes a peer's lock read as
"this queue is taken" rather than "try the row below".

### Unqueued rows

They can never be blocked by a sibling, so they come straight off
`idx_job_executions_pending_unqueued` in `execute_at` order, bounded per type.

### Ordered index access is mandatory

`SET LOCAL enable_bitmapscan = off` is not a micro-optimisation. A bitmap scan
returns rows in heap order, destroying the index ordering the queue walk and
both due-scans depend on, and forcing a sort of every candidate: **10.3 ms vs
1.4 ms** on identical data. It is scoped to the poll transaction, alongside the
`plan_cache_mode` override that already lived there.

---

## Contention headroom

`CONTENTION_HEADROOM` (currently 4) is how far past its budget a poll gathers
candidates. It exists so `FOR UPDATE ... SKIP LOCKED` has somewhere to fall
through when a peer instance holds locks on the rows this poll would target.

This looks like the old `* 4` overscan but is a different quantity. The old
multiplier was sized to survive *filtering* — it had to guess how many gathered
rows would turn out unclaimable, and a deep blocked queue could exhaust any
guess. Everything gathered now is already claimable, so the multiplier only has
to cover *contention*: how many candidates a concurrent poller might be
holding.

Removing it entirely regresses a real case: with a type budget of 5 and a peer
holding exactly those 5 rows, a poll gathers 5 candidates, skips all 5, and
claims nothing (`poller_falls_through_locked_head_rows_to_later_due_jobs`).

## Per-type budgets and interleaving

Each type is bounded by its own budget at gather time, so no type's backlog can
consume the scan another type needs. That alone is not sufficient: the final
`LIMIT` is global, so a type with older rows could still take the whole batch.
`ordered_candidates` therefore ranks candidates **round-robin across types** —
every type's oldest ranks ahead of any type's second — and only then oldest
first within a rank. Pinned by
`capped_type_backlog_does_not_starve_another_type`.

---

## Queue fairness: round-robin cursor

The queue walk resumes from a per-poller, in-memory cursor. Simulation over
2000 queues, K=50, 400 rounds (perfectly fair = 10 services each):

| start strategy | never served | min | max | stddev | worst gap |
|---|---|---|---|---|---|
| fixed (lowest first) | 1950 | 0 | 400 | 62.5 | 400 (never) |
| random | 0 | 3 | 19 | 2.84 | 262 rounds |
| **cursor (round-robin)** | 0 | **10** | **10** | **0.00** | **40 rounds** |
| oldest-first (`execute_at`) | 0 | 1 | 18 | 2.55 | 316 rounds |

The cursor is the only option with a *bound*: worst gap 40 rounds is exactly
one sweep (2000 ÷ 50). Random start is unbiased in the mean but has an
exponential tail — 6.5× a sweep, with no upper limit.

Oldest-first optimises job latency rather than per-queue progress, and is not
what this system wants: it leaves some queues waiting 316 rounds.

Trade-offs accepted:

- Queue service order is not `execute_at` order. Among *due* jobs a queue may
  be served out of age order, bounded by one sweep. Future-scheduled jobs are
  still never run early. Queue admission was already documented as not FIFO.
- The cursor is in memory and lost on restart, costing at most one skewed
  sweep — not worth persisting.
- Distinct cursors also decorrelate pollers, which reduces how often instances
  target the same rows.

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

`idx_job_executions_pending_execute_at` is kept: `min_wait` and the
stale-pending reporter need global `execute_at` order, which neither claim
index provides.

---

## Concurrency safety

Verified under 8, 12 and 16 concurrent pollers across all three seeds:

- **0** double-claims
- **0** queues with more than one running job
- **0** deadlocks
- **0** lock waits and **0** ungranted locks, sampled every 1 ms

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
skips the entire queue, instead of claiming the row behind it.

---

## Reproducing

The harness under `.claude/bench/` is intentionally untracked; it needs a local
dev database (`make start-deps`).

- `seed_real.sql` — realistic: 85% queued, ~15k queues, hot entities, ~34.5k pending
- `seed_lowcard.sql` — 50 queues × 600 rows (deep-queue extreme)
- `seed.sql` — uniform, no skew
- `seed_fair.sql` — 2000 flat queues, for the fairness simulation
- `stress.sh` — N concurrent pollers; reports claims, double-claims, deadlocks, lock waits
- `fairness.sql` — selection-strategy simulation
- `idx_sets.sql` / `writecost.sql` — index-set switcher and write-path cost

When changing the claim query, re-run **all three** seeds. The low-cardinality
seed in particular is what caught the per-type `DISTINCT ON` correctness bug;
neither of the other two exposed it.
