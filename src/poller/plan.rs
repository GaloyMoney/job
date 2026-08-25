//! Turns the registry's static facts (which types exist, their caps and
//! batch policies) plus the tracker's live in-flight counts into one
//! poll's per-type claim plan, spending `unit_budget` dispatch units -- a
//! unit is roughly one shared-pool connection's worth of dispatch: one
//! row for a plain type, one whole `run_batch` chunk for a batched one.
//! Bounded types (batched / capped plain, real finite demand) and elastic
//! types (uncapped plain, priced at the full window since their true
//! demand is unknowable) first split the budget by tier (`tier_split`,
//! smaller demand first, alternating by tick at budget one).
//!
//! **Policy: every type gets guaranteed forward progress under a scarce
//! budget, via rotation that guarantees a turn within a bounded number of
//! polls -- in both tiers, not just one.** Elastic draws a per-poll floor
//! from a window that ROTATES by tick over the whole (sorted) type list.
//! Bounded spends smallest-demand-first -- that ordering is the tier's
//! whole point, so it is never abandoned -- but ties within it (the common
//! case: most bounded types cost 1 unit) are broken by rotating each
//! same-cost group by tick, so a scarce bounded budget cycles through
//! *which* types in a tie win instead of the same registration-order
//! prefix winning every single poll. Unspent bounded budget still grows
//! the picked elastic types past their floor.
//!
//! Tie-group rotation only rotates types that share the SAME cost, so a
//! type whose demand is larger than every tie ahead of it in cost order is
//! never a tie-group member -- if the cheaper types alone exceed the
//! tier's budget every poll (the scarce-budget case this module exists
//! for), that larger type is starved by cost class, not just by identity,
//! and no amount of tick advancing ever reaches it. `bounded_streak`
//! tracks, per bounded type, consecutive polls where it had real demand
//! but lost the budget; once a type's streak reaches the tier's own size,
//! it is force-included ahead of the normal smallest-first order (ties
//! among several simultaneously-forced types broken by longest-starved,
//! then smallest-demand). This is aging, the standard fix for starvation
//! under a smallest-job-first policy -- it is the fallback that applies
//! when cost differs; tie-group rotation stays the low-latency path for
//! the common same-cost case.
//!
//! The plan also reports what it could NOT cover: `clamped_by_pool` (due
//! work exists that a short budget left unclaimed -- arms the headroom
//! waiter) and `rotation_excluded`/`rotation_lap` (the types EITHER tier's
//! rotation left out this poll -- fed into the sleep computation and the
//! recheck spin bound; see `claim_query` and `recheck`). A bounded type
//! dropped by its tier's budget is pushed into `rotation_excluded` exactly
//! like an excluded elastic type -- it is not a silent third bucket invisible
//! to the honest-sleep/recheck machinery (handoff-0138).

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{entity::JobType, registry::JobRegistry, tracker::JobTracker};

/// One poll's per-type claim plan.
pub(super) struct ClaimPlan {
    pub types: Vec<JobType>,
    pub row_limits: Vec<i32>,
    /// The pool's headroom was the limiting factor; an empty-but-clamped
    /// plan means due work exists that a zero budget kept unclaimed.
    pub clamped_by_pool: bool,
    /// The types EITHER tier's rotation left out this poll -- elastic types
    /// outside the floor window, and bounded types dropped by the tier's
    /// budget; empty whenever nothing was dropped.
    pub rotation_excluded: Vec<JobType>,
    /// Polls needed to guarantee every excluded type -- either tier -- has
    /// had a turn at least once: `max(elastic type count, bounded type
    /// count)` -- the bounded side is sized for the aging fallback's own
    /// worst case, not just tie-group rotation's. Sizes the recheck spin
    /// bound.
    pub rotation_lap: usize,
}

pub(super) struct ClaimPlanner {
    registry: Arc<JobRegistry>,
    tracker: Arc<JobTracker>,
    /// Advanced once per plan; rotates which elastic types (and, at
    /// `unit_budget == 1`, which tier) get a scarce claim slot.
    tick: AtomicUsize,
    /// Consecutive polls each bounded type has had real demand but lost
    /// the tier's budget; reset to absent (0) the poll it is served. Keyed
    /// fresh from scratch every poll, so a type with no current demand is
    /// pruned rather than accumulating a stale streak. See the module doc
    /// -- this is the aging fallback for cross-cost-class starvation.
    bounded_streak: std::sync::Mutex<std::collections::HashMap<JobType, usize>>,
}

impl ClaimPlanner {
    pub(super) fn new(registry: Arc<JobRegistry>, tracker: Arc<JobTracker>) -> Self {
        Self {
            registry,
            tracker,
            tick: AtomicUsize::new(0),
            bounded_streak: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Row limit for each registered type this poll; a type with no free
    /// slot is dropped. Units bound DISPATCHES, not rows: a batched type's
    /// unit is one eventual `dispatch_batches` chunk, and retries (always
    /// dispatched alone) can nudge a claim slightly past budget -- bounded
    /// by `RetrySettings`, the same direction of imprecision as the
    /// clamp's under-claiming elsewhere.
    pub(super) fn plan(&self, n_jobs_to_poll: usize, unit_budget: usize) -> ClaimPlan {
        let natural: Vec<(JobType, usize, usize, bool)> = self
            .registry
            .registered_job_types()
            .into_iter()
            .filter_map(|job_type| {
                let (limit, units, elastic) = match self.registry.batch_policy(&job_type) {
                    Some(policy) => {
                        let limit = policy
                            .max_concurrent_per_process
                            .saturating_sub(self.tracker.units_in_flight(&job_type))
                            .saturating_mul(policy.max_batch_size)
                            .min(n_jobs_to_poll);
                        (limit, limit.div_ceil(policy.max_batch_size.max(1)), false)
                    }
                    None => match self.registry.per_process_cap(&job_type) {
                        Some(cap) => {
                            let limit = cap
                                .saturating_sub(self.tracker.units_in_flight(&job_type))
                                .min(n_jobs_to_poll);
                            (limit, limit, false)
                        }
                        None => (n_jobs_to_poll, n_jobs_to_poll, true),
                    },
                };
                (limit > 0).then_some((job_type, limit, units, elastic))
            })
            .collect();
        let clamped_by_pool =
            natural.iter().map(|(.., units, _)| units).sum::<usize>() > unit_budget;

        let (mut bounded, mut elastic): (Vec<_>, Vec<_>) =
            natural.into_iter().partition(|(.., elastic)| !*elastic);
        elastic.sort_by(|(a, ..), (b, ..)| a.as_str().cmp(b.as_str()));
        let bounded_demand: usize = bounded.iter().map(|(_, _, units, _)| *units).sum();
        let tick = self.tick.fetch_add(1, Ordering::Relaxed);
        let (elastic_tier_budget, bounded_tier_budget) =
            tier_split(elastic.len(), bounded_demand, unit_budget, tick);
        // tier_split caps each tier at its own demand, so unassigned
        // budget is real headroom that must still reach elastic growth.
        let tier_leftover = unit_budget - bounded_tier_budget - elastic_tier_budget;

        // Smallest-demand-first is the tier's whole point and stays fixed;
        // but ties (most bounded types cost 1 unit) are broken by rotating
        // each same-cost run by tick, so a scarce budget cycles through
        // WHICH types in a tie win instead of the same registration-order
        // prefix losing every single poll (handoff-0138).
        bounded.sort_by_key(|(_, _, units, _)| *units);
        let mut start = 0;
        while start < bounded.len() {
            let units = bounded[start].2;
            let mut end = start + 1;
            while end < bounded.len() && bounded[end].2 == units {
                end += 1;
            }
            let group_len = end - start;
            bounded[start..end].rotate_left(tick % group_len);
            start = end;
        }

        // Tie-group rotation cannot help a type whose demand is larger
        // than every tie ahead of it in cost order -- it is never a member
        // of any tie group, so no amount of tick advancing brings it
        // forward. Age it in instead: once a type has lost the budget on
        // `starvation_threshold` consecutive polls where it had real
        // demand, force it to the front of THIS poll's spend, ahead of the
        // normal smallest-first order (handoff-0138 follow-up).
        let n_bounded = bounded.len();
        let starvation_threshold = n_bounded.max(1);
        let old_streaks = std::mem::take(
            &mut *self
                .bounded_streak
                .lock()
                .expect("bounded_streak lock poisoned"),
        );
        let (mut forced, mut rest): (Vec<_>, Vec<_>) = bounded
            .into_iter()
            .map(|(job_type, limit, units, elastic)| {
                let streak = old_streaks.get(&job_type).copied().unwrap_or(0);
                (job_type, limit, units, elastic, streak)
            })
            .partition(|(.., streak)| *streak >= starvation_threshold);
        // Longest-starved wins among several simultaneously-forced types;
        // smallest-demand stays the secondary key.
        forced.sort_by(|a, b| b.4.cmp(&a.4).then(a.2.cmp(&b.2)));
        rest.sort_by_key(|(_, _, units, ..)| *units);

        let mut types = Vec::new();
        let mut row_limits = Vec::new();
        let mut rotation_excluded = Vec::new();
        let mut new_streaks = std::collections::HashMap::new();
        let mut bounded_remaining = bounded_tier_budget;
        for (job_type, limit, units, _, streak) in forced.into_iter().chain(rest) {
            if bounded_remaining == 0 {
                new_streaks.insert(job_type.clone(), streak + 1);
                rotation_excluded.push(job_type);
                continue;
            }
            let (limit, units) = if units <= bounded_remaining {
                (limit, units)
            } else if let Some(policy) = self.registry.batch_policy(&job_type) {
                (
                    bounded_remaining
                        .saturating_mul(policy.max_batch_size)
                        .min(limit),
                    bounded_remaining,
                )
            } else {
                (bounded_remaining, bounded_remaining)
            };
            if limit == 0 {
                new_streaks.insert(job_type.clone(), streak + 1);
                rotation_excluded.push(job_type);
                continue;
            }
            types.push(job_type);
            row_limits.push(limit as i32);
            bounded_remaining -= units;
        }
        *self
            .bounded_streak
            .lock()
            .expect("bounded_streak lock poisoned") = new_streaks;

        let n = elastic.len();
        let take = elastic_tier_budget.min(n);
        let offset = if n == 0 { 0 } else { tick % n };
        let mut growth_budget = bounded_remaining + tier_leftover;
        for i in 0..take {
            let (job_type, limit, ..) = &elastic[(offset + i) % n];
            let mut units = 1;
            let extra = growth_budget.min(limit.saturating_sub(units));
            units += extra;
            growth_budget -= extra;
            types.push(job_type.clone());
            row_limits.push(units as i32);
        }
        rotation_excluded.extend((take..n).map(|i| elastic[(offset + i) % n].0.clone()));

        ClaimPlan {
            types,
            row_limits,
            clamped_by_pool,
            rotation_excluded,
            rotation_lap: n.max(n_bounded),
        }
    }
}

/// Splits `unit_budget` between the elastic tier (demand = one turn per
/// elastic type) and the bounded tier (demand = real summed units),
/// smaller demand first. At `unit_budget == 1` neither tier fits, so
/// priority alternates by `tick` instead of one tier winning every poll.
fn tier_split(
    elastic_demand: usize,
    bounded_demand: usize,
    unit_budget: usize,
    tick: usize,
) -> (usize, usize) {
    if elastic_demand == 0 {
        return (0, bounded_demand.min(unit_budget));
    }
    if bounded_demand == 0 {
        return (elastic_demand.min(unit_budget), 0);
    }
    if unit_budget == 1 {
        return if tick.is_multiple_of(2) {
            (1, 0)
        } else {
            (0, 1)
        };
    }
    let (small, large, small_is_elastic) = if elastic_demand <= bounded_demand {
        (elastic_demand, bounded_demand, true)
    } else {
        (bounded_demand, elastic_demand, false)
    };
    let small_share = small.min(unit_budget / 2);
    let large_share = (unit_budget - small_share).min(large);
    if small_is_elastic {
        (small_share, large_share)
    } else {
        (large_share, small_share)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Job, JobInitializer, JobRunner, JobSpawner};

    /// A capped plain type, keyed by its own job type so several can be
    /// registered side by side in one registry.
    struct FixedCapInitializer {
        job_type: JobType,
        cap: Option<usize>,
    }

    impl JobInitializer for FixedCapInitializer {
        type Config = ();

        fn job_type(&self) -> JobType {
            self.job_type.clone()
        }

        fn max_concurrent_per_process(&self) -> Option<usize> {
            self.cap
        }

        fn init(
            &self,
            _job: &Job,
            _: JobSpawner<Self::Config>,
        ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
            unimplemented!("never invoked by this test")
        }
    }

    /// An uncapped ("elastic") plain type -- `max_concurrent_per_process`
    /// defaults to `None`.
    struct UncappedInitializer(JobType);

    impl JobInitializer for UncappedInitializer {
        type Config = ();

        fn job_type(&self) -> JobType {
            self.0.clone()
        }

        fn init(
            &self,
            _job: &Job,
            _: JobSpawner<Self::Config>,
        ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
            unimplemented!("never invoked by this test")
        }
    }

    fn planner(registry: JobRegistry) -> ClaimPlanner {
        ClaimPlanner::new(Arc::new(registry), Arc::new(JobTracker::new(0, 10)))
    }

    /// Mirrors `capped_type_does_not_starve_others` at the plan level: a
    /// capped type's small demand must be met in full even with an
    /// uncapped sibling also competing for the budget.
    #[test]
    fn capped_type_gets_full_demand_despite_uncapped_sibling() {
        let mut registry = JobRegistry::new();
        let capped = registry.add_initializer(FixedCapInitializer {
            job_type: JobType::new("plan-claim-capped-sibling"),
            cap: Some(1),
        });
        registry.add_initializer(UncappedInitializer(JobType::new(
            "plan-claim-uncapped-sibling",
        )));

        let plan = planner(registry).plan(50, 6);

        let idx = plan
            .types
            .iter()
            .position(|t| t == &capped)
            .expect("the capped type must be in the plan");
        assert_eq!(
            plan.row_limits[idx], 1,
            "the capped type's whole (small) demand must be met"
        );
    }

    /// Bounded demand that sums to the whole budget must not crowd an
    /// uncapped type out of the plan entirely.
    #[test]
    fn elastic_type_is_not_starved_by_many_bounded_types() {
        let mut registry = JobRegistry::new();
        const BOUNDED: [&str; 4] = [
            "plan-claim-bounded-0",
            "plan-claim-bounded-1",
            "plan-claim-bounded-2",
            "plan-claim-bounded-3",
        ];
        for job_type in BOUNDED {
            registry.add_initializer(FixedCapInitializer {
                job_type: JobType::new(job_type),
                cap: Some(1),
            });
        }
        let uncapped = registry.add_initializer(UncappedInitializer(JobType::new(
            "plan-claim-uncapped-starved",
        )));

        // unit_budget equals the four bounded types' combined demand
        // exactly, so a floor-less algorithm excludes the uncapped type
        // outright once bounded spends the whole budget.
        let plan = planner(registry).plan(50, 4);

        let idx = plan.types.iter().position(|t| t == &uncapped);
        assert!(
            idx.is_some(),
            "an uncapped plain type must not be excluded while unit_budget > 0, \
             even when bounded demand consumes the rest of it -- got plan.types = {:?}",
            plan.types
        );
        assert!(
            plan.row_limits[idx.unwrap()] >= 1,
            "the uncapped type must get at least its floor of one claimable row"
        );
    }

    /// The mirror-image boundary: when elastic types outnumber
    /// `unit_budget`, a bounded type competing for the same budget must
    /// still make progress.
    #[test]
    fn bounded_type_is_not_starved_by_many_elastic_types() {
        let mut registry = JobRegistry::new();
        let capped = registry.add_initializer(FixedCapInitializer {
            job_type: JobType::new("plan-claim-bounded-vs-many-elastic"),
            cap: Some(1),
        });
        const ELASTIC: [&str; 5] = [
            "plan-claim-elastic-0",
            "plan-claim-elastic-1",
            "plan-claim-elastic-2",
            "plan-claim-elastic-3",
            "plan-claim-elastic-4",
        ];
        for job_type in ELASTIC {
            registry.add_initializer(UncappedInitializer(JobType::new(job_type)));
        }

        // 5 elastic types, unit_budget == 3: a per-type floor would
        // reserve the whole budget before the capped type is considered.
        let plan = planner(registry).plan(50, 3);

        let idx = plan.types.iter().position(|t| t == &capped);
        assert!(
            idx.is_some(),
            "a bounded type must not be starved by many elastic types \
             sharing the budget -- got plan.types = {:?}",
            plan.types
        );
        assert!(plan.row_limits[idx.unwrap()] >= 1);
    }

    /// Elastic types that lose out on a scarce floor in one poll must win
    /// it in a later one -- the picked subset rotates.
    #[test]
    fn elastic_types_rotate_through_a_scarce_floor_across_polls() {
        let mut registry = JobRegistry::new();
        let elastic: Vec<JobType> = (0..5)
            .map(|i| {
                registry.add_initializer(UncappedInitializer(JobType::new(Box::leak(
                    format!("plan-claim-rotation-{i}").into_boxed_str(),
                ))))
            })
            .collect();
        let planner = planner(registry);

        // unit_budget == 2 across 5 elastic types and no bounded
        // competitor: only 2 can win the floor per poll.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..elastic.len() {
            let plan = planner.plan(50, 2);
            seen.extend(plan.types);
        }

        assert_eq!(
            seen.len(),
            elastic.len(),
            "every elastic type must be picked within enough polls to cycle through them all"
        );
    }

    /// `rotation_excluded` must name exactly the elastic types the floor
    /// window left out; window picks and excluded set partition the
    /// elastic types.
    #[test]
    fn rotation_excluded_names_the_elastic_types_the_window_left_out() {
        let mut registry = JobRegistry::new();
        for i in 0..5 {
            registry.add_initializer(UncappedInitializer(JobType::new(Box::leak(
                format!("plan-claim-rotation-partial-{i}").into_boxed_str(),
            ))));
        }
        let planner = planner(registry);

        // budget 2 < 5 elastic types: the window can't cover them all, and
        // the 3 it left out are exactly the elastic types not in the plan.
        let plan = planner.plan(50, 2);
        assert_eq!(plan.rotation_lap, 5);
        assert_eq!(plan.rotation_excluded.len(), 3);
        for excluded in &plan.rotation_excluded {
            assert!(
                !plan.types.contains(excluded),
                "an excluded type must not also be in the claim plan"
            );
        }

        // budget 5 == 5 elastic types: the window covers every one of them.
        let plan = planner.plan(50, 5);
        assert_eq!(plan.rotation_lap, 5);
        assert!(plan.rotation_excluded.is_empty());
        assert_eq!(plan.types.len(), 5);
    }

    /// At `unit_budget == 1` neither tier's demand fits; the single unit
    /// must alternate by tick instead of one tier winning forever.
    #[test]
    fn tier_split_alternates_at_budget_one() {
        assert_eq!(tier_split(5, 1, 1, 0), (1, 0));
        assert_eq!(tier_split(5, 1, 1, 1), (0, 1));
    }

    /// RED-FIRST regression for handoff-0138: bounded demand that exceeds
    /// the bounded tier's share must still rotate across polls so every
    /// bounded type gets served eventually -- mirrors
    /// `elastic_types_rotate_through_a_scarce_floor_across_polls`, but for
    /// the bounded tier, which currently has NO rotation at all: `bounded`
    /// is `sort_by_key(units)` over a fixed pre-order and a type dropped by
    /// `bounded_remaining == 0` is silently `continue`d, so the SAME losers
    /// lose on every single poll. Realistic type count (30, per staging's
    /// ~65 registered / 36 outbox-resident scale) so a 3-type test can't
    /// hide the bug behind incidental tie-break luck.
    #[test]
    fn bounded_types_rotate_through_a_scarce_floor_across_polls() {
        let mut registry = JobRegistry::new();
        let bounded: Vec<JobType> = (0..30)
            .map(|i| {
                registry.add_initializer(FixedCapInitializer {
                    job_type: JobType::new(Box::leak(
                        format!("plan-claim-bounded-rotation-{i:02}").into_boxed_str(),
                    )),
                    cap: Some(1),
                })
            })
            .collect();
        let planner = planner(registry);

        // unit_budget == 12 against 30 one-unit bounded types and no
        // elastic competitor: only 12 can win the bounded tier's share per
        // poll, so the plan must cycle through all 30 over enough polls.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..bounded.len() {
            let plan = planner.plan(50, 12);
            seen.extend(plan.types);
        }

        assert_eq!(
            seen.len(),
            bounded.len(),
            "every bounded type must be picked within enough polls to cycle \
             through them all, not lose to the same smaller-index types every \
             time -- got {}/{} distinct types served across {} polls",
            seen.len(),
            bounded.len(),
            bounded.len()
        );
    }

    /// RED-FIRST regression, follow-up finding on handoff-0138: tie-group
    /// rotation only rotates types that share the SAME cost. A bounded type
    /// whose demand is strictly larger than every tie ahead of it in
    /// smallest-first order is never a member of any tie group -- if the
    /// smaller-cost types alone consume the whole tier budget every poll
    /// (as they will whenever their combined demand exceeds it, which is
    /// exactly the scarce-budget case this fix targets), the larger type is
    /// reached with `bounded_remaining == 0` on EVERY poll, forever, and
    /// tick never changes that: it is not a tie-group member, so no amount
    /// of rotation ever brings it forward. Smallest-demand-first is
    /// deliberately preserved by this fix (see the module doc), so this
    /// type must still make progress some OTHER way -- across-cost-class
    /// starvation, not just within-tie starvation.
    #[test]
    fn bounded_type_with_demand_exceeding_every_tie_still_makes_progress() {
        let mut registry = JobRegistry::new();
        for i in 0..30 {
            registry.add_initializer(FixedCapInitializer {
                job_type: JobType::new(Box::leak(
                    format!("plan-claim-cross-cost-small-{i:02}").into_boxed_str(),
                )),
                cap: Some(1),
            });
        }
        let heavy = registry.add_initializer(FixedCapInitializer {
            job_type: JobType::new("plan-claim-cross-cost-heavy"),
            cap: Some(5),
        });
        let planner = planner(registry);

        // budget 4 < the 30 one-unit types' combined demand (30): the
        // budget is exhausted by cost-1 items alone every single poll,
        // before smallest-first order ever reaches the cost-5 type.
        let mut served = false;
        for _ in 0..60 {
            let plan = planner.plan(50, 4);
            if let Some(idx) = plan.types.iter().position(|t| t == &heavy) {
                assert!(plan.row_limits[idx] >= 1, "a served type must get >=1 row");
                served = true;
                break;
            }
        }

        assert!(
            served,
            "a bounded type whose demand exceeds every tie ahead of it in \
             smallest-first order must still be planned within a bounded \
             number of polls, not lose to the same smaller-cost types forever"
        );
    }

    /// RED-FIRST regression for handoff-0138: a bounded type dropped by the
    /// tier's budget must be visible in the plan's excluded-types list, the
    /// same way a rotation-excluded elastic type is -- otherwise its due
    /// rows are invisible to the honest-sleep/recheck machinery and the
    /// poller can park past them for `MAX_WAIT`, or forever under a frozen
    /// clock. Currently a budget-dropped bounded type lands in neither
    /// `plan.types` nor `plan.rotation_excluded`: a silent third bucket.
    #[test]
    fn budget_dropped_bounded_type_is_visible_in_the_excluded_list() {
        let mut registry = JobRegistry::new();
        for i in 0..5 {
            registry.add_initializer(FixedCapInitializer {
                job_type: JobType::new(Box::leak(
                    format!("plan-claim-bounded-excluded-{i}").into_boxed_str(),
                )),
                cap: Some(1),
            });
        }
        let planner = planner(registry);

        // budget 2 < 5 one-unit bounded types: 3 must be dropped, and every
        // dropped type must be accounted for somewhere the sleep/recheck
        // machinery can see it.
        let plan = planner.plan(50, 2);
        assert_eq!(plan.types.len(), 2, "only budget's worth should be planned");
        assert_eq!(
            plan.rotation_excluded.len(),
            3,
            "the 3 budget-dropped bounded types must be named in the excluded \
             list, not silently disappear -- got rotation_excluded = {:?} \
             (plan.types = {:?})",
            plan.rotation_excluded,
            plan.types
        );
    }

    /// With no bounded competitor, tier_split caps the elastic tier's
    /// floor at its type count -- the growth phase must recover the rest.
    #[test]
    fn elastic_type_alone_grows_to_its_full_window_not_just_its_floor() {
        let mut registry = JobRegistry::new();
        let uncapped = registry.add_initializer(UncappedInitializer(JobType::new(
            "plan-claim-elastic-alone",
        )));

        let plan = planner(registry).plan(5, 7);

        let idx = plan.types.iter().position(|t| t == &uncapped).unwrap();
        assert_eq!(
            plan.row_limits[idx], 5,
            "the sole elastic type must use the whole window, not just its floor"
        );
    }
}
