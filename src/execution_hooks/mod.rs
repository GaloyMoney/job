//! Commit hooks that do database work in `pre_commit`: [`PromoteHeadsHook`]
//! and [`ExecutionInsertHook`]. Each centralizes the SQL for one concern and
//! stages further hooks re-entrantly (`JobEventHook` for notify,
//! [`crate::poller::JobPoller::register_claim_demand`] for the claim) rather
//! than doing dispatch/claim work inline.

mod insert;
mod promote;

pub(crate) use insert::{ExecutionInsertHook, NewExecutionRow};
pub(crate) use promote::{PromoteHeadsHook, PromotedRow};
