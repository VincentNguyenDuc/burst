use crate::domain::RoutingDecision;

use super::RouterStrategy;

#[derive(Default)]
pub struct BiasedRouter {
    next_worker_index: usize,
}

impl BiasedRouter {
    fn choose_worker(&mut self, context: &crate::domain::RoutingContext) -> Option<String> {
        let mut workers = context.workers.keys().cloned().collect::<Vec<_>>();
        if workers.is_empty() {
            return None;
        }

        workers.sort();

        let biased_count = (workers.len() / 2).max(1);
        let available_biased_workers = workers
            .iter()
            .take(biased_count)
            .filter_map(|worker_id| {
                let worker = context.workers.get(worker_id)?;
                (worker.leased_jobs < worker.queue_capacity).then(|| worker_id.clone())
            })
            .collect::<Vec<_>>();

        if available_biased_workers.is_empty() {
            return None;
        }

        let selected_index = self.next_worker_index % available_biased_workers.len();
        let worker_id = available_biased_workers[selected_index].clone();
        self.next_worker_index = (selected_index + 1) % available_biased_workers.len();
        Some(worker_id)
    }
}

impl RouterStrategy for BiasedRouter {
    fn name(&self) -> &'static str {
        "biased"
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

pub struct BiasedFactory;

impl super::RouterFactory for BiasedFactory {
    fn name(&self) -> &'static str {
        "biased"
    }

    fn build(&self) -> Box<dyn super::RouterStrategy> {
        Box::new(BiasedRouter::default())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use burst_core::proto::{JobSpec, ProcessSpec, job_spec::Type::Process};

    use crate::domain::{Job, RoutingContext, WorkerState};

    use super::{BiasedRouter, RouterStrategy};

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
    fn routes_only_to_first_half_of_workers() {
        let mut scheduler = BiasedRouter::default();
        let mut context = RoutingContext {
            pending_jobs: VecDeque::from([job("job-1"), job("job-2")]),
            workers: HashMap::from([
                (
                    "worker-1".to_string(),
                    WorkerState {
                        queue_capacity: 2,
                        leased_jobs: 0,
                    },
                ),
                (
                    "worker-2".to_string(),
                    WorkerState {
                        queue_capacity: 2,
                        leased_jobs: 0,
                    },
                ),
                (
                    "worker-3".to_string(),
                    WorkerState {
                        queue_capacity: 2,
                        leased_jobs: 0,
                    },
                ),
                (
                    "worker-4".to_string(),
                    WorkerState {
                        queue_capacity: 2,
                        leased_jobs: 0,
                    },
                ),
            ]),
        };

        let first = scheduler
            .next(&mut context)
            .expect("expected first scheduling decision");
        let second = scheduler
            .next(&mut context)
            .expect("expected second scheduling decision");

        assert!(first.worker_id == "worker-1" || first.worker_id == "worker-2");
        assert!(second.worker_id == "worker-1" || second.worker_id == "worker-2");
    }

    #[test]
    fn does_not_fallback_to_second_half_when_biased_half_full() {
        let mut scheduler = BiasedRouter::default();
        let mut context = RoutingContext {
            pending_jobs: VecDeque::from([job("job-1")]),
            workers: HashMap::from([
                (
                    "worker-1".to_string(),
                    WorkerState {
                        queue_capacity: 1,
                        leased_jobs: 1,
                    },
                ),
                (
                    "worker-2".to_string(),
                    WorkerState {
                        queue_capacity: 1,
                        leased_jobs: 1,
                    },
                ),
                (
                    "worker-3".to_string(),
                    WorkerState {
                        queue_capacity: 1,
                        leased_jobs: 0,
                    },
                ),
                (
                    "worker-4".to_string(),
                    WorkerState {
                        queue_capacity: 1,
                        leased_jobs: 0,
                    },
                ),
            ]),
        };

        let decision = scheduler.next(&mut context);
        assert!(decision.is_none());
        assert_eq!(context.pending_jobs.len(), 1);
    }
}
