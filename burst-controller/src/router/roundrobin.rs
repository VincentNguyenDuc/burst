use crate::domain::RoutingDecision;

use super::RouterStrategy;

#[derive(Default)]
pub struct RoundRobinRouter {
    next_worker_index: usize,
}

impl RoundRobinRouter {
    fn choose_worker(&mut self, context: &crate::domain::RoutingContext) -> Option<String> {
        let mut available_workers = context
            .workers
            .iter()
            .filter(|(_, worker)| worker.available_slots > 0)
            .map(|(worker_id, _)| worker_id.clone())
            .collect::<Vec<_>>();

        if available_workers.is_empty() {
            return None;
        }

        available_workers.sort();

        let selected_index = self.next_worker_index % available_workers.len();
        let worker_id = available_workers[selected_index].clone();

        self.next_worker_index = (selected_index + 1) % available_workers.len();

        Some(worker_id)
    }
}

impl RouterStrategy for RoundRobinRouter {
    fn name(&self) -> &'static str {
        "roundrobin"
    }

    fn next(&mut self, context: &mut crate::domain::RoutingContext) -> Option<RoutingDecision> {
        if context.pending_jobs.is_empty() {
            return None;
        }

        let worker_id = self.choose_worker(context)?;
        let job = context.pending_jobs.pop_front()?;

        if let Some(worker) = context.workers.get_mut(&worker_id) {
            worker.available_slots -= 1;
        }

        Some(RoutingDecision { worker_id, job })
    }
}

pub struct RoundRobinFactory;

impl super::RouterFactory for RoundRobinFactory {
    fn name(&self) -> &'static str {
        "roundrobin"
    }

    fn build(&self) -> Box<dyn super::RouterStrategy> {
        Box::new(RoundRobinRouter::default())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use burst_core::proto::{JobSpec, ProcessSpec, job_spec::Type::Process};

    use crate::domain::{Job, RoutingContext, WorkerState};

    use super::{RoundRobinRouter, RouterStrategy};

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
    fn returns_none_without_workers() {
        let mut scheduler = RoundRobinRouter::default();
        let mut context = RoutingContext {
            pending_jobs: VecDeque::from([job("job-1")]),
            workers: HashMap::new(),
        };

        let decision = scheduler.next(&mut context);

        assert!(decision.is_none());
        assert_eq!(context.pending_jobs.len(), 1);
    }

    #[test]
    fn returns_none_without_jobs() {
        let mut scheduler = RoundRobinRouter::default();
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
    fn alternates_workers_round_robin() {
        let mut scheduler = RoundRobinRouter::default();
        let mut context = RoutingContext {
            pending_jobs: VecDeque::from([job("job-1"), job("job-2")]),
            workers: HashMap::from([
                (
                    "worker-a".to_string(),
                    WorkerState {
                        id: "worker-a".to_string(),
                        available_slots: 1,
                    },
                ),
                (
                    "worker-b".to_string(),
                    WorkerState {
                        id: "worker-b".to_string(),
                        available_slots: 1,
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

        assert_eq!(first.worker_id, "worker-a");
        assert_eq!(first.job.id, "job-1");
        assert_eq!(second.worker_id, "worker-b");
        assert_eq!(second.job.id, "job-2");
    }

    #[test]
    fn skips_unavailable_workers() {
        let mut scheduler = RoundRobinRouter::default();
        let mut context = RoutingContext {
            pending_jobs: VecDeque::from([job("job-1")]),
            workers: HashMap::from([
                (
                    "worker-a".to_string(),
                    WorkerState {
                        id: "worker-a".to_string(),
                        available_slots: 0,
                    },
                ),
                (
                    "worker-b".to_string(),
                    WorkerState {
                        id: "worker-b".to_string(),
                        available_slots: 1,
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
