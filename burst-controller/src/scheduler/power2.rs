use crate::domain::SchedulingDecision;
use rand::seq::SliceRandom;
use rand::thread_rng;

use super::SchedulerStrategy;

pub struct PowerOfTwoScheduler;

impl PowerOfTwoScheduler {
    fn choose_worker(&mut self, context: &crate::domain::SchedulingContext) -> Option<String> {
        let mut available_workers = context
            .workers
            .iter()
            .filter(|(_, worker)| worker.available_slots > 0)
            .map(|(worker_id, worker)| (worker_id.clone(), worker.available_slots))
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

        if first.1 > second.1 {
            Some(first.0.clone())
        } else if second.1 > first.1 {
            Some(second.0.clone())
        } else if first.0 <= second.0 {
            Some(first.0.clone())
        } else {
            Some(second.0.clone())
        }
    }
}

impl SchedulerStrategy for PowerOfTwoScheduler {
    fn name(&self) -> &'static str {
        "power2"
    }

    fn next(
        &mut self,
        context: &mut crate::domain::SchedulingContext,
    ) -> Option<SchedulingDecision> {
        if context.pending_jobs.is_empty() {
            return None;
        }

        let worker_id = self.choose_worker(context)?;
        let job = context.pending_jobs.pop_front()?;

        if let Some(worker) = context.workers.get_mut(&worker_id) {
            worker.available_slots -= 1;
        }

        Some(SchedulingDecision { worker_id, job })
    }
}

pub struct PowerOfTwoFactory;

impl super::SchedulerFactory for PowerOfTwoFactory {
    fn name(&self) -> &'static str {
        "power2"
    }

    fn build(&self) -> Box<dyn super::SchedulerStrategy> {
        Box::new(PowerOfTwoScheduler)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use burst_core::proto::{JobSpec, ProcessSpec, job_spec::Type::Process};

    use crate::domain::{Job, SchedulingContext, WorkerState};

    use super::{PowerOfTwoScheduler, SchedulerStrategy};

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
        let mut scheduler = PowerOfTwoScheduler;
        let mut context = SchedulingContext {
            pending_jobs: VecDeque::new(),
            workers: HashMap::from([(
                "worker-a".to_string(),
                WorkerState {
                    id: "worker-a".to_string(),
                    available_slots: 1,
                },
            )]),
        };

        let decision = scheduler.next(&mut context);

        assert!(decision.is_none());
    }

    #[test]
    fn returns_none_without_available_workers() {
        let mut scheduler = PowerOfTwoScheduler;
        let mut context = SchedulingContext {
            pending_jobs: VecDeque::from([job("job-1")]),
            workers: HashMap::from([(
                "worker-a".to_string(),
                WorkerState {
                    id: "worker-a".to_string(),
                    available_slots: 0,
                },
            )]),
        };

        let decision = scheduler.next(&mut context);

        assert!(decision.is_none());
        assert_eq!(context.pending_jobs.len(), 1);
    }

    #[test]
    fn chooses_higher_capacity_between_two_candidates() {
        let mut scheduler = PowerOfTwoScheduler;
        let mut context = SchedulingContext {
            pending_jobs: VecDeque::from([job("job-1")]),
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
                        available_slots: 3,
                    },
                ),
            ]),
        };

        let decision = scheduler
            .next(&mut context)
            .expect("expected a scheduling decision");

        assert_eq!(decision.worker_id, "worker-b");
        assert_eq!(decision.job.id, "job-1");
        assert_eq!(
            context
                .workers
                .get("worker-b")
                .map(|worker| worker.available_slots),
            Some(2)
        );
    }

    #[test]
    fn single_available_worker_is_selected() {
        let mut scheduler = PowerOfTwoScheduler;
        let mut context = SchedulingContext {
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
