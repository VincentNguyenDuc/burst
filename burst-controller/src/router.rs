//! Pluggable scheduling abstraction for the controller.
//!
//! A scheduler implementation only needs to implement [`RouterStrategy`]
//! and register a factory in [`RouterRegistry`].

mod fifo;
mod power2;
mod registry;

use crate::domain::{RoutingContext, RoutingDecision};

pub use fifo::FifoFactory;
pub use power2::PowerOfTwoFactory;
pub use registry::RouterRegistry;

/// Scheduling algorithm interface.
pub trait RouterStrategy: Send {
    fn name(&self) -> &'static str;
    fn next(&mut self, context: &mut RoutingContext) -> Option<RoutingDecision>;
}

/// Factory interface used by registry-based scheduler selection.
pub trait RouterFactory: Send + Sync {
    fn name(&self) -> &'static str;
    fn build(&self) -> Box<dyn RouterStrategy>;
}
