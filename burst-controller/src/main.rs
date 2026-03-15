mod domain;
mod scheduler;

use std::collections::{HashMap, VecDeque};

use domain::{Job, SchedulingContext, WorkerState};
use scheduler::{FifoFactory, SchedulerRegistry};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let mut registry = SchedulerRegistry::new();
    registry.register(FifoFactory);

    let strategy_name = std::env::var("BURST_SCHEDULER").unwrap_or_else(|_| "fifo".to_string());
    let available = registry.available();
    let scheduler = registry.build(&strategy_name);

    match scheduler {
        Some(mut selected) => {
            let mut context = SchedulingContext {
                pending_jobs: VecDeque::from([Job {
                    id: "bootstrap-job".to_string(),
                    command: "echo".to_string(),
                    args: vec!["hello".to_string()],
                }]),
                workers: HashMap::from([(
                    "bootstrap-worker".to_string(),
                    WorkerState {
                        id: "bootstrap-worker".to_string(),
                        available_slots: 1,
                    },
                )]),
            };

            let sample = selected.next(&mut context);
            tracing::info!(
                scheduler = selected.name(),
                sample_decision = ?sample,
                "burst-controller booted with scheduler"
            );
        }
        None => {
            tracing::error!(
                requested = strategy_name,
                available = ?available,
                "requested scheduler is not registered"
            );
            std::process::exit(2);
        }
    }
}
