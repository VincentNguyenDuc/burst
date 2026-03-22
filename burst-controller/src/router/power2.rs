use crate::domain::RoutingDecision;
use rand::seq::SliceRandom;
use rand::thread_rng;

use super::RouterStrategy;

pub struct P2CRouter;

impl P2CRouter {
    fn worker_load(worker: &crate::domain::WorkerState) -> f64 {
        if worker.queue_capacity == 0 {
            return f64::INFINITY;
        }
        worker.leased_jobs as f64 / worker.queue_capacity as f64
    }

    fn choose_worker(&mut self, context: &crate::domain::RoutingContext) -> Option<String> {
        let mut available_workers = context
            .workers
            .iter()
            .filter(|(_, worker)| worker.leased_jobs < worker.queue_capacity)
            .map(|(worker_id, worker)| {
                (
                    worker_id.clone(),
                    Self::worker_load(worker),
                    worker.queue_capacity.saturating_sub(worker.leased_jobs),
                )
            })
            .collect::<Vec<_>>();

        if available_workers.is_empty() {
            return None;
        }

        if available_workers.len() == 1 {
            return Some(available_workers[0].0.clone());
        }

        let mut rng = thread_rng();
        available_workers.shuffle(&mut rng);

        let first = &available_workers[0];
        let second = &available_workers[1];

        // Prefer lower normalized load, then more free slots, then stable worker-id tie-break.
        if first.1 < second.1 {
            Some(first.0.clone())
        } else if second.1 < first.1 {
            Some(second.0.clone())
        } else if first.2 > second.2 {
            Some(first.0.clone())
        } else if second.2 > first.2 {
            Some(second.0.clone())
        } else if first.0 <= second.0 {
            Some(first.0.clone())
        } else {
            Some(second.0.clone())
        }
    }
}

impl RouterStrategy for P2CRouter {
    fn name(&self) -> &'static str {
        "power2"
    }

    fn next(&mut self, context: &mut crate::domain::RoutingContext) -> Option<RoutingDecision> {
        if context.pending_jobs.is_empty() {
            return None;
        }

        let worker_id = self.choose_worker(context)?;
        let job = context.pending_jobs.pop_front()?;

        Some(RoutingDecision { worker_id, job })
    }
}

pub struct PowerOfTwoFactory;

impl super::RouterFactory for PowerOfTwoFactory {
    fn name(&self) -> &'static str {
        "power2"
    }

    fn build(&self) -> Box<dyn super::RouterStrategy> {
        Box::new(P2CRouter)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use burst_core::proto::{JobSpec, ProcessSpec, job_spec::Type::Process};

    use crate::domain::{Job, RoutingContext, WorkerState};

    use super::{P2CRouter, RouterStrategy};

    fn job(id: &str) -> Job {
        Job {
            id: id.to_string(),
            spec: JobSpec {
                r#type: Some(Process(ProcessSpec {
                    command: "echo".to_string(),
                    args: vec![id.to_string()],
                })),
                ..Default::default()
            },
        }
    }

    #[test]
    fn returns_none_without_jobs() {
        let mut scheduler = P2CRouter;
        let mut context = RoutingContext {
            pending_jobs: VecDeque::new(),
            workers: HashMap::from([(
                "worker-a".to_string(),
                WorkerState {
                    queue_capacity: 1,
                    leased_jobs: 0,
                },
            )]),
        };

        let decision = scheduler.next(&mut context);

        assert!(decision.is_none());
    }

    #[test]
    fn returns_none_without_available_workers() {
        let mut scheduler = P2CRouter;
        let mut context = RoutingContext {
            pending_jobs: VecDeque::from([job("job-1")]),
            workers: HashMap::from([(
                "worker-a".to_string(),
                WorkerState {
                    queue_capacity: 1,
                    leased_jobs: 1,
                },
            )]),
        };

        let decision = scheduler.next(&mut context);

        assert!(decision.is_none());
        assert_eq!(context.pending_jobs.len(), 1);
    }

    #[test]
    fn chooses_lower_load_between_two_candidates() {
        let mut scheduler = P2CRouter;
        let mut context = RoutingContext {
            pending_jobs: VecDeque::from([job("job-1")]),
            workers: HashMap::from([
                (
                    "worker-a".to_string(),
                    WorkerState {
                        queue_capacity: 4,
                        leased_jobs: 3,
                    },
                ),
                (
                    "worker-b".to_string(),
                    WorkerState {
                        queue_capacity: 4,
                        leased_jobs: 1,
                    },
                ),
            ]),
        };

        let decision = scheduler
            .next(&mut context)
            .expect("expected a scheduling decision");

        assert_eq!(decision.worker_id, "worker-b");
        assert_eq!(decision.job.id, "job-1");
        assert_eq!(context.pending_jobs.len(), 0);
    }

    #[test]
    fn single_available_worker_is_selected() {
        let mut scheduler = P2CRouter;
        let mut context = RoutingContext {
            pending_jobs: VecDeque::from([job("job-1")]),
            workers: HashMap::from([
                (
                    "worker-a".to_string(),
                    WorkerState {
                        queue_capacity: 1,
                        leased_jobs: 1,
                    },
                ),
                (
                    "worker-b".to_string(),
                    WorkerState {
                        queue_capacity: 1,
                        leased_jobs: 0,
                    },
                ),
            ]),
        };

        let decision = scheduler
            .next(&mut context)
            .expect("expected scheduling decision");

        assert_eq!(decision.worker_id, "worker-b");
        assert_eq!(decision.job.id, "job-1");
    }
}
