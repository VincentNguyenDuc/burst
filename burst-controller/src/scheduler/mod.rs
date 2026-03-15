mod fifo;
mod registry;

use crate::domain::{SchedulingContext, SchedulingDecision};

pub use fifo::FifoFactory;
pub use registry::SchedulerRegistry;

pub trait SchedulerStrategy: Send {
    fn name(&self) -> &'static str;
    fn next(&mut self, context: &mut SchedulingContext) -> Option<SchedulingDecision>;
}

pub trait SchedulerFactory: Send + Sync {
    fn name(&self) -> &'static str;
    fn build(&self) -> Box<dyn SchedulerStrategy>;
}
