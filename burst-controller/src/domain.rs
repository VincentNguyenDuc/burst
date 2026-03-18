//! Controller domain model used by scheduling and in-memory state.

use std::collections::{HashMap, VecDeque};

use burst_core::proto::JobSpec;

/// Job assigned by the controller to a worker.
#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub spec: JobSpec,
}

/// Worker runtime capacity tracked by controller.
#[derive(Debug, Clone)]
pub struct WorkerState {
    pub id: String,
    pub available_slots: u32,
}

/// Mutable scheduling snapshot consumed by scheduler strategies.
#[derive(Debug, Default)]
pub struct SchedulingContext {
    pub pending_jobs: VecDeque<Job>,
    pub workers: HashMap<String, WorkerState>,
}

/// Decision returned by scheduler to lease one job to one worker.
#[derive(Debug, Clone)]
pub struct SchedulingDecision {
    pub worker_id: String,
    pub job: Job,
}
