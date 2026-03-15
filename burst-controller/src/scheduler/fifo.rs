use crate::domain::SchedulingDecision;

use super::SchedulerStrategy;

pub struct FifoScheduler;

impl SchedulerStrategy for FifoScheduler {
    fn name(&self) -> &'static str {
        "fifo"
    }

    fn next(
        &mut self,
        context: &mut crate::domain::SchedulingContext,
    ) -> Option<SchedulingDecision> {
        let worker_id = context
            .workers
            .iter()
            .find(|(_, worker)| worker.available_slots > 0)
            .map(|(worker_id, _)| worker_id.clone())?;

        let job = context.pending_jobs.pop_front()?;
        if let Some(worker) = context.workers.get_mut(&worker_id) {
            worker.available_slots -= 1;
        }

        Some(SchedulingDecision { worker_id, job })
    }
}

pub struct FifoFactory;

impl super::SchedulerFactory for FifoFactory {
    fn name(&self) -> &'static str {
        "fifo"
    }

    fn build(&self) -> Box<dyn super::SchedulerStrategy> {
        Box::new(FifoScheduler)
    }
}
