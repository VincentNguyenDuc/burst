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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use crate::domain::{Job, SchedulingContext, WorkerState};

    use super::{FifoScheduler, SchedulerStrategy};

    #[test]
    fn returns_none_without_workers() {
        let mut scheduler = FifoScheduler;
        let mut context = SchedulingContext {
            pending_jobs: VecDeque::from([Job {
                id: "job-1".to_string(),
                command: "echo".to_string(),
                args: vec!["hello".to_string()],
                output_dir: None,
            }]),
            workers: HashMap::new(),
        };

        let decision = scheduler.next(&mut context);

        assert!(decision.is_none());
        assert_eq!(context.pending_jobs.len(), 1);
    }

    #[test]
    fn returns_none_without_jobs() {
        let mut scheduler = FifoScheduler;
        let mut context = SchedulingContext {
            pending_jobs: VecDeque::new(),
            workers: HashMap::from([(
                "worker-1".to_string(),
                WorkerState {
                    id: "worker-1".to_string(),
                    available_slots: 1,
                },
            )]),
        };

        let decision = scheduler.next(&mut context);

        assert!(decision.is_none());
        assert_eq!(
            context
                .workers
                .get("worker-1")
                .map(|worker| worker.available_slots),
            Some(1)
        );
    }

    #[test]
    fn assigns_first_job_and_decrements_slot() {
        let mut scheduler = FifoScheduler;
        let mut context = SchedulingContext {
            pending_jobs: VecDeque::from([
                Job {
                    id: "job-1".to_string(),
                    command: "echo".to_string(),
                    args: vec!["first".to_string()],
                    output_dir: None,
                },
                Job {
                    id: "job-2".to_string(),
                    command: "echo".to_string(),
                    args: vec!["second".to_string()],
                    output_dir: None,
                },
            ]),
            workers: HashMap::from([(
                "worker-1".to_string(),
                WorkerState {
                    id: "worker-1".to_string(),
                    available_slots: 1,
                },
            )]),
        };

        let decision = scheduler
            .next(&mut context)
            .expect("expected a scheduling decision");

        assert_eq!(decision.worker_id, "worker-1");
        assert_eq!(decision.job.id, "job-1");
        assert_eq!(context.pending_jobs.len(), 1);
        assert_eq!(
            context
                .workers
                .get("worker-1")
                .map(|worker| worker.available_slots),
            Some(0)
        );
    }
}
