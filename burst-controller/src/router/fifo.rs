use crate::domain::RoutingDecision;

use super::RouterStrategy;

pub struct FifoRouter;

impl RouterStrategy for FifoRouter {
    fn name(&self) -> &'static str {
        "fifo"
    }

    fn next(&mut self, context: &mut crate::domain::RoutingContext) -> Option<RoutingDecision> {
        let worker_id = context
            .workers
            .iter()
            .find(|(_, worker)| worker.available_slots > 0)
            .map(|(worker_id, _)| worker_id.clone())?;

        let job = context.pending_jobs.pop_front()?;
        if let Some(worker) = context.workers.get_mut(&worker_id) {
            worker.available_slots -= 1;
        }

        Some(RoutingDecision { worker_id, job })
    }
}

pub struct FifoFactory;

impl super::RouterFactory for FifoFactory {
    fn name(&self) -> &'static str {
        "fifo"
    }

    fn build(&self) -> Box<dyn super::RouterStrategy> {
        Box::new(FifoRouter)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use burst_core::proto::{JobSpec, ProcessSpec, job_spec::Type::Process};

    use crate::domain::{Job, RoutingContext, WorkerState};

    use super::{FifoRouter, RouterStrategy};

    #[test]
    fn returns_none_without_workers() {
        let mut scheduler = FifoRouter;
        let mut context = RoutingContext {
            pending_jobs: VecDeque::from([Job {
                id: "job-1".to_string(),
                spec: JobSpec {
                    r#type: Some(Process(ProcessSpec {
                        command: "echo".to_string(),
                        args: vec!["hello".to_string()],
                    })),
                    ..Default::default()
                },
            }]),
            workers: HashMap::new(),
        };

        let decision = scheduler.next(&mut context);

        assert!(decision.is_none());
        assert_eq!(context.pending_jobs.len(), 1);
    }

    #[test]
    fn returns_none_without_jobs() {
        let mut scheduler = FifoRouter;
        let mut context = RoutingContext {
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
        let mut scheduler = FifoRouter;
        let mut context = RoutingContext {
            pending_jobs: VecDeque::from([
                Job {
                    id: "job-1".to_string(),
                    spec: JobSpec {
                        r#type: Some(Process(ProcessSpec {
                            command: "echo".to_string(),
                            args: vec!["first".to_string()],
                        })),
                        ..Default::default()
                    },
                },
                Job {
                    id: "job-2".to_string(),
                    spec: JobSpec {
                        r#type: Some(Process(ProcessSpec {
                            command: "echo".to_string(),
                            args: vec!["second".to_string()],
                        })),
                        ..Default::default()
                    },
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
