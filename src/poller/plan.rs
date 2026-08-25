//! Turns the registry's static facts (which types exist, their caps and batch
//! policies) plus the tracker's live in-flight counts into one poll's
//! per-type claim plan, spending `unit_budget` dispatch units -- a unit is
//! roughly one shared-pool connection's worth of dispatch: one row for a
//! plain type, one whole `run_batch` chunk for a batched one. Bounded types
//! (batched / capped plain, real finite demand) and elastic types (uncapped
//! plain, priced at the full window since their true demand is unknowable)
//! first split the budget by tier (`tier_split`, smaller demand first,
//! alternating by tick at budget one).
//!
//! Policy: every registered type gets guaranteed forward progress under a
//! scarce claim budget, via rotation bounded to a known number of polls, in
//! both tiers. Elastic draws a per-poll floor from a window that rotates by
//! tick over the whole (sorted) type list. Bounded spends smallest-demand-first
//! -- that ordering is the tier's whole point and is never abandoned -- with
//! two layered fairness mechanisms: same-cost ties rotate by tick (the
//! low-latency path for the common case, since most bounded types cost 1
//! unit), and a type whose demand exceeds every tie ahead of it is aged in --
//! force-included once it has lost the budget on as many consecutive polls as
//! the tier has types -- covering the cross-cost-class starvation tie-rotation
//! structurally cannot reach (handoff-0138). A type dropped by either
//! mechanism, or by the elastic floor window, is reported in
//! `rotation_excluded`/`rotation_lap`, feeding `claim_query`'s sleep
//! computation and `recheck`'s spin bound. `rotation_lap` uses the bounded
//! tier's own type count, not the narrower widest-tie-group figure, so the
//! aging threshold above always fires before `recheck`'s spin bound trips
//! (see that module's doc for why the ordering matters).

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{entity::JobType, registry::JobRegistry, tracker::JobTracker};

pub(super) struct ClaimPlan {
    pub types: Vec<JobType>,
    pub row_limits: Vec<i32>,
    pub clamped_by_pool: bool,
    pub rotation_excluded: Vec<JobType>,
    pub rotation_lap: usize,
}

pub(super) struct ClaimPlanner {
    registry: Arc<JobRegistry>,
    tracker: Arc<JobTracker>,
    tick: AtomicUsize,
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
        let tier_leftover = unit_budget - bounded_tier_budget - elastic_tier_budget;

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

    #[test]
    fn rotation_excluded_names_the_elastic_types_the_window_left_out() {
        let mut registry = JobRegistry::new();
        for i in 0..5 {
            registry.add_initializer(UncappedInitializer(JobType::new(Box::leak(
                format!("plan-claim-rotation-partial-{i}").into_boxed_str(),
            ))));
        }
        let planner = planner(registry);

        let plan = planner.plan(50, 2);
        assert_eq!(plan.rotation_lap, 5);
        assert_eq!(plan.rotation_excluded.len(), 3);
        for excluded in &plan.rotation_excluded {
            assert!(
                !plan.types.contains(excluded),
                "an excluded type must not also be in the claim plan"
            );
        }

        let plan = planner.plan(50, 5);
        assert_eq!(plan.rotation_lap, 5);
        assert!(plan.rotation_excluded.is_empty());
        assert_eq!(plan.types.len(), 5);
    }

    #[test]
    fn tier_split_alternates_at_budget_one() {
        assert_eq!(tier_split(5, 1, 1, 0), (1, 0));
        assert_eq!(tier_split(5, 1, 1, 1), (0, 1));
    }

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
