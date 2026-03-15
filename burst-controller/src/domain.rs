use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct WorkerState {
    pub id: String,
    pub available_slots: u32,
}

#[derive(Debug, Default)]
pub struct SchedulingContext {
    pub pending_jobs: VecDeque<Job>,
    pub workers: HashMap<String, WorkerState>,
}

#[derive(Debug, Clone)]
pub struct SchedulingDecision {
    pub worker_id: String,
    pub job: Job,
}
