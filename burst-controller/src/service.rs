use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use burst_core::proto::{
    AssignedJob, Empty, GetJobStatusRequest, GetJobStatusResponse, HeartbeatRequest,
    HeartbeatResponse, PollJobRequest, PollJobResponse, RegisterWorkerRequest,
    RegisterWorkerResponse, ReportJobResultRequest, ReportJobResultResponse, SubmitJobRequest,
    SubmitJobResponse, controller_rpc_server::ControllerRpc, poll_job_response,
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::{
    domain::{Job, SchedulingContext, WorkerState},
    scheduler,
};

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
pub(crate) struct ControllerService {
    inner: Arc<Mutex<ControllerInner>>,
}

impl ControllerService {
    pub(crate) fn new(scheduler: Box<dyn scheduler::SchedulerStrategy>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ControllerInner {
                next_job_id: 0,
                scheduler,
                scheduling: SchedulingContext {
                    pending_jobs: VecDeque::new(),
                    workers: HashMap::new(),
                },
                job_states: HashMap::new(),
                worker_queues: HashMap::new(),
            })),
        }
    }
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
            output_dir: spec.output_dir,
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
                        output_dir: job.output_dir,
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

#[cfg(test)]
mod tests {
    use burst_core::proto::{
        GetJobStatusRequest, JobSpec, PollJobRequest, RegisterWorkerRequest,
        ReportJobResultRequest, SubmitJobRequest, controller_rpc_server::ControllerRpc,
        poll_job_response,
    };
    use tonic::Request;

    use crate::scheduler::{FifoFactory, SchedulerFactory};

    use super::ControllerService;

    fn service() -> ControllerService {
        ControllerService::new(FifoFactory.build())
    }

    #[tokio::test]
    async fn submit_rejects_empty_command() {
        let service = service();

        let error = service
            .submit_job(Request::new(SubmitJobRequest {
                spec: Some(JobSpec {
                    command: "   ".to_string(),
                    args: vec![],
                    output_dir: None,
                }),
            }))
            .await
            .expect_err("empty command should be rejected");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn submit_poll_report_updates_status() {
        let service = service();

        service
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: "worker-1".to_string(),
                slots: 1,
            }))
            .await
            .expect("worker registration should succeed");

        let job_id = service
            .submit_job(Request::new(SubmitJobRequest {
                spec: Some(JobSpec {
                    command: "echo".to_string(),
                    args: vec!["hello".to_string()],
                    output_dir: None,
                }),
            }))
            .await
            .expect("submit should succeed")
            .into_inner()
            .job_id;

        let poll = service
            .poll_job(Request::new(PollJobRequest {
                worker_id: "worker-1".to_string(),
            }))
            .await
            .expect("poll should succeed")
            .into_inner();

        let assigned = match poll.result {
            Some(poll_job_response::Result::Job(job)) => job,
            _ => panic!("expected assigned job"),
        };
        assert_eq!(assigned.job_id, job_id);

        service
            .report_job_result(Request::new(ReportJobResultRequest {
                worker_id: "worker-1".to_string(),
                job_id: job_id.clone(),
                exit_code: 0,
                error_message: String::new(),
            }))
            .await
            .expect("report should succeed");

        let status = service
            .get_job_status(Request::new(GetJobStatusRequest {
                job_id: job_id.clone(),
            }))
            .await
            .expect("status should succeed")
            .into_inner();

        assert_eq!(status.job_id, job_id);
        assert_eq!(status.state, "succeeded");
    }

    #[tokio::test]
    async fn poll_returns_empty_without_jobs() {
        let service = service();
        service
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: "worker-1".to_string(),
                slots: 1,
            }))
            .await
            .expect("worker registration should succeed");

        let poll = service
            .poll_job(Request::new(PollJobRequest {
                worker_id: "worker-1".to_string(),
            }))
            .await
            .expect("poll should succeed")
            .into_inner();

        assert!(matches!(
            poll.result,
            Some(poll_job_response::Result::Empty(_))
        ));
    }
}
