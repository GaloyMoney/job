//! Turns the registry's static facts (which types exist, their caps and
//! batch policies) plus the tracker's live in-flight counts into one
//! poll's per-type claim plan, spending `unit_budget` dispatch units -- a
//! unit is roughly one shared-pool connection's worth of dispatch: one
//! row for a plain type, one whole `run_batch` chunk for a batched one.
//! Bounded types (batched / capped plain, real finite demand) and elastic
//! types (uncapped plain, priced at the full window since their true
//! demand is unknowable) first split the budget by tier (`tier_split`,
//! smaller demand first, alternating by tick at budget one), then bounded
//! spends smallest-demand-first and elastic draws a per-poll floor from a
//! window that ROTATES by tick -- a scarce budget cycles through every
//! elastic type instead of the same ones winning each poll -- with
//! whatever bounded left unspent growing the picked types past their
//! floor.
//!
//! The plan also reports what it could NOT cover: `clamped_by_pool` (due
//! work exists that a short budget left unclaimed -- arms the headroom
//! waiter) and `rotation_excluded`/`n_elastic` (the elastic types outside
//! this poll's window -- fed into the sleep computation and the recheck
//! spin bound; see `claim_query` and `recheck`).

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
    /// The elastic types this poll's rotating floor window did NOT
    /// include; empty whenever the window covers every elastic type.
    pub rotation_excluded: Vec<JobType>,
    /// Total elastic types this poll: one full rotation lap is exactly
    /// this many polls, which sizes the recheck spin bound.
    pub n_elastic: usize,
}

pub(super) struct ClaimPlanner {
    registry: Arc<JobRegistry>,
    tracker: Arc<JobTracker>,
    /// Advanced once per plan; rotates which elastic types (and, at
    /// `unit_budget == 1`, which tier) get a scarce claim slot.
    tick: AtomicUsize,
}

impl ClaimPlanner {
    pub(super) fn new(registry: Arc<JobRegistry>, tracker: Arc<JobTracker>) -> Self {
        Self {
            registry,
            tracker,
            tick: AtomicUsize::new(0),
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

        bounded.sort_by_key(|(_, _, units, _)| *units);
        let mut types = Vec::new();
        let mut row_limits = Vec::new();
        let mut bounded_remaining = bounded_tier_budget;
        for (job_type, limit, units, _) in bounded {
            if bounded_remaining == 0 {
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
                continue;
            }
            types.push(job_type);
            row_limits.push(limit as i32);
            bounded_remaining -= units;
        }

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
        let rotation_excluded = (take..n)
            .map(|i| elastic[(offset + i) % n].0.clone())
            .collect();

        ClaimPlan {
            types,
            row_limits,
            clamped_by_pool,
            rotation_excluded,
            n_elastic: n,
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
        assert_eq!(plan.n_elastic, 5);
        assert_eq!(plan.rotation_excluded.len(), 3);
        for excluded in &plan.rotation_excluded {
            assert!(
                !plan.types.contains(excluded),
                "an excluded type must not also be in the claim plan"
            );
        }

        // budget 5 == 5 elastic types: the window covers every one of them.
        let plan = planner.plan(50, 5);
        assert_eq!(plan.n_elastic, 5);
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
