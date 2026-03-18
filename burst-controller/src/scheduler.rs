//! Pluggable scheduling abstraction for the controller.
//!
//! A scheduler implementation only needs to implement [`SchedulerStrategy`]
//! and register a factory in [`SchedulerRegistry`].

mod fifo;
mod power2;
mod registry;

use crate::domain::{SchedulingContext, SchedulingDecision};

pub use fifo::FifoFactory;
pub use power2::PowerOfTwoFactory;
pub use registry::SchedulerRegistry;

/// Scheduling algorithm interface.
pub trait SchedulerStrategy: Send {
    fn name(&self) -> &'static str;
    fn next(&mut self, context: &mut SchedulingContext) -> Option<SchedulingDecision>;
}

/// Factory interface used by registry-based scheduler selection.
pub trait SchedulerFactory: Send + Sync {
    fn name(&self) -> &'static str;
    fn build(&self) -> Box<dyn SchedulerStrategy>;
}
