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
    pub max_slots: u32,
    pub processing_slots: u32,
}

/// Mutable routing snapshot consumed by router strategies.
#[derive(Debug, Default)]
pub struct RoutingContext {
    pub pending_jobs: VecDeque<Job>,
    pub workers: HashMap<String, WorkerState>,
}

/// Decision returned by router to lease one job to one worker.
#[derive(Debug, Clone)]
pub struct RoutingDecision {
    pub worker_id: String,
    pub job: Job,
}
