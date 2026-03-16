//! Controller service for burst.
//!
//! Responsibilities in current POC:
//!
//! - accept job submissions from CLI via gRPC
//! - track in-memory job/worker state
//! - apply pluggable scheduler strategy (default: FIFO)
//! - lease jobs to workers and update terminal job state on report
//!
//! Configuration:
//!
//! - `--config <path>` (default `burst.config.json`)

mod domain;
mod scheduler;

use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
};

use burst_core::config::BurstConfig;
use burst_core::proto::{
    AssignedJob, Empty, GetJobStatusRequest, GetJobStatusResponse, HeartbeatRequest,
    HeartbeatResponse, PollJobRequest, PollJobResponse, RegisterWorkerRequest,
    RegisterWorkerResponse, ReportJobResultRequest, ReportJobResultResponse, SubmitJobRequest,
    SubmitJobResponse,
    controller_rpc_server::{ControllerRpc, ControllerRpcServer},
    poll_job_response,
};
use domain::{Job, SchedulingContext, WorkerState};
use scheduler::{FifoFactory, SchedulerRegistry};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, transport::Server};

fn read_config_path() -> String {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            if let Some(path) = args.next() {
                return path;
            }
        }
    }
    "burst.config.json".to_string()
}

struct ControllerInner {
    next_job_id: u64,
    scheduler: Box<dyn scheduler::SchedulerStrategy>,
    scheduling: SchedulingContext,
    job_states: HashMap<String, String>,
    worker_queues: HashMap<String, VecDeque<Job>>,
}

impl ControllerInner {
    fn schedule_pending(&mut self) {
        while let Some(decision) = self.scheduler.next(&mut self.scheduling) {
            self.job_states
                .insert(decision.job.id.clone(), "leased".to_string());
            self.worker_queues
                .entry(decision.worker_id)
                .or_default()
                .push_back(decision.job);
        }
    }
}

#[derive(Clone)]
struct ControllerService {
    inner: Arc<Mutex<ControllerInner>>,
}

#[tonic::async_trait]
impl ControllerRpc for ControllerService {
    async fn submit_job(
        &self,
        request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        let req = request.into_inner();
        let spec = req
            .spec
            .ok_or_else(|| Status::invalid_argument("missing job spec"))?;

        if spec.command.trim().is_empty() {
            return Err(Status::invalid_argument("command cannot be empty"));
        }

        let mut inner = self.inner.lock().await;
        inner.next_job_id += 1;
        let job_id = format!("job-{:08}", inner.next_job_id);

        inner.scheduling.pending_jobs.push_back(Job {
            id: job_id.clone(),
            command: spec.command,
            args: spec.args,
        });
        inner
            .job_states
            .insert(job_id.clone(), "queued".to_string());
        inner.schedule_pending();

        Ok(Response::new(SubmitJobResponse { job_id }))
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusResponse>, Status> {
        let req = request.into_inner();
        let inner = self.inner.lock().await;
        let state = inner
            .job_states
            .get(&req.job_id)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        Ok(Response::new(GetJobStatusResponse {
            job_id: req.job_id,
            state,
        }))
    }

    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();
        if req.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id cannot be empty"));
        }

        let mut inner = self.inner.lock().await;
        inner.scheduling.workers.insert(
            req.worker_id.clone(),
            WorkerState {
                id: req.worker_id.clone(),
                available_slots: req.slots.max(1),
            },
        );
        inner.worker_queues.entry(req.worker_id).or_default();
        inner.schedule_pending();

        Ok(Response::new(RegisterWorkerResponse { accepted: true }))
    }

    async fn poll_job(
        &self,
        request: Request<PollJobRequest>,
    ) -> Result<Response<PollJobResponse>, Status> {
        let req = request.into_inner();
        let mut inner = self.inner.lock().await;
        inner.schedule_pending();

        let maybe_job = inner
            .worker_queues
            .entry(req.worker_id)
            .or_default()
            .pop_front();

        let response = if let Some(job) = maybe_job {
            PollJobResponse {
                result: Some(poll_job_response::Result::Job(AssignedJob {
                    job_id: job.id,
                    spec: Some(burst_core::proto::JobSpec {
                        command: job.command,
                        args: job.args,
                    }),
                })),
            }
        } else {
            PollJobResponse {
                result: Some(poll_job_response::Result::Empty(Empty {})),
            }
        };

        Ok(Response::new(response))
    }

    async fn report_job_result(
        &self,
        request: Request<ReportJobResultRequest>,
    ) -> Result<Response<ReportJobResultResponse>, Status> {
        let req = request.into_inner();
        let mut inner = self.inner.lock().await;

        let new_state = if req.exit_code == 0 {
            "succeeded"
        } else {
            "failed"
        };
        inner.job_states.insert(req.job_id, new_state.to_string());

        if let Some(worker) = inner.scheduling.workers.get_mut(&req.worker_id) {
            worker.available_slots += 1;
        }
        inner.schedule_pending();

        Ok(Response::new(ReportJobResultResponse { accepted: true }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let inner = self.inner.lock().await;
        let ok = inner.scheduling.workers.contains_key(&req.worker_id);
        Ok(Response::new(HeartbeatResponse { ok }))
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config_path = read_config_path();
    let config = match BurstConfig::load_from_path(&config_path) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(path = config_path, error = %error, "failed to load config");
            std::process::exit(2);
        }
    };

    let bind_addr = config.controller.bind_addr.clone();

    let mut registry = SchedulerRegistry::new();
    registry.register(FifoFactory);

    let strategy_name = config.controller.scheduler.clone();
    let available = registry.available();
    let scheduler = registry.build(&strategy_name);

    match scheduler {
        Some(selected) => {
            let address: SocketAddr = match bind_addr.parse() {
                Ok(address) => address,
                Err(error) => {
                    tracing::error!(bind = bind_addr, error = %error, "invalid bind address");
                    std::process::exit(2);
                }
            };

            let service = ControllerService {
                inner: Arc::new(Mutex::new(ControllerInner {
                    next_job_id: 0,
                    scheduler: selected,
                    scheduling: SchedulingContext {
                        pending_jobs: VecDeque::new(),
                        workers: HashMap::new(),
                    },
                    job_states: HashMap::new(),
                    worker_queues: HashMap::new(),
                })),
            };

            tracing::info!(scheduler = strategy_name, bind = %address, "controller started");

            if let Err(error) = Server::builder()
                .add_service(ControllerRpcServer::new(service))
                .serve(address)
                .await
            {
                tracing::error!(error = %error, "controller server failed");
                std::process::exit(1);
            }
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
