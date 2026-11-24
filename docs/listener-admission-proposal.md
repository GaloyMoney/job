# Listener admission: reproduction and design discussion

Status: draft for feedback on the remaining admission problem in #200. No production behavior changes are included.

## Current state

`KeyedJobSpawner::spawn_in_op` already exists and has transaction/rollback tests. The remaining question is how a fleet of long-lived keyed listeners should share process capacity with transient jobs.

At main revision `f4e6060357ad58ef242754281239fc08833b60a4`, running listeners count against the same `JobTracker` budget as transient jobs. Background polling stops at `min_jobs`; the short-circuit spawn path also stops when `max_jobs` is reached. No listener-specific admission class is exposed by `KeyedJobInitializer`.

## Reproduction

The opt-in `listener_budget_repro` integration test creates a fresh PostgreSQL database and:

1. Configures min/max execution slots to two and a database pool with up to 20 connections.
2. Starts two keyed listeners, waiting for both runner bodies to report that they started.
3. Spawns an ordinary canary job and observes it for two seconds.
4. Confirms that the canary remains pending with a database connection idle.
5. Releases one listener and requires the canary to execute within ten seconds.

Observed output:

```text
listeners_running=2, transient_executed=false, transient_state=pending, idle_connections=1
after releasing one listener: transient executed
```

This reproduces the admission mechanism at a small scale, not Lana's production configuration or throughput. Saturation is expected under the existing global-cap contract; the proposed feature would introduce an explicit way to budget parked listeners differently.

```sh
SQLX_OFFLINE=true PG_CON=<local-postgres-url> cargo test --locked --test listener_budget_repro -- --ignored --nocapture
```

The database role needs `CREATEDB` for the isolated database. The test is ignored by default because it records the existing starvation behavior. Once a design is agreed, it should be replaced with a regression asserting transient progress while listeners remain parked.

## Proposed goals

- Explicit, opt-in listener admission; preserve existing ordinary-job defaults.
- Bound the listener fleet and preserve transient capacity.
- Bound active database work when multiple listeners wake simultaneously.
- Preserve keyed singleton semantics, transactional spawning, inherited checkpoints, retries, keep-alive, reclaim, and shutdown behavior.
- Demonstrate transient progress under listener load with an isolated mixed-workload test and benchmark.

## Feedback requested

1. Is listener admission still needed for the current Lana webhook architecture?
2. Should this use separate execution-class budgets, or an API that releases and reacquires capacity around the stream wait?
3. Should resident jobs participate in the same model?

The next implementation should follow the agreed lifecycle and capacity model. This draft does not select a public API or claim to fix #200.
