use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use burst_core::proto::{
    AssignedJob, Empty, GetJobStatusRequest, GetJobStatusResponse, HeartbeatRequest,
    HeartbeatResponse, PollJobRequest, PollJobResponse, RegisterWorkerRequest,
    RegisterWorkerResponse, ReportJobResultRequest, ReportJobResultResponse, SubmitJobRequest,
    SubmitJobResponse, controller_rpc_server::ControllerRpc, job_spec, poll_job_response,
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::{
    domain::{Job, RoutingContext, WorkerState},
    router,
};

struct ControllerInner {
    next_job_id: u64,
    scheduler: Box<dyn router::RouterStrategy>,
    submission_buffer_capacity: usize,
    scheduling: RoutingContext,
    job_states: HashMap<String, String>,
    worker_queues: HashMap<String, VecDeque<Job>>,
}

impl ControllerInner {
    fn schedule_pending(&mut self) {
        while let Some(decision) = self.scheduler.next(&mut self.scheduling) {
            let job_id = decision.job.id.clone();
            let worker_id = decision.worker_id.clone();
            let job_type = job_type_label(decision.job.spec.r#type.as_ref());

            self.job_states.insert(job_id.clone(), "leased".to_string());
            self.worker_queues
                .entry(worker_id.clone())
                .or_default()
                .push_back(decision.job);

            tracing::info!(
                job_id,
                worker_id,
                job_type,
                scheduler = self.scheduler.name(),
                "job leased to worker"
            );
        }
    }
}

#[derive(Clone)]
pub struct ControllerService {
    inner: Arc<Mutex<ControllerInner>>,
}

impl ControllerService {
    pub fn new(
        scheduler: Box<dyn router::RouterStrategy>,
        submission_buffer_capacity: usize,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ControllerInner {
                next_job_id: 0,
                scheduler,
                submission_buffer_capacity: submission_buffer_capacity.max(1),
                scheduling: RoutingContext {
                    pending_jobs: VecDeque::new(),
                    workers: HashMap::new(),
                },
                job_states: HashMap::new(),
                worker_queues: HashMap::new(),
            })),
        }
    }
}

fn job_type_label(job_type: Option<&job_spec::Type>) -> &'static str {
    match job_type {
        Some(job_spec::Type::Process(_)) => "process",
        Some(job_spec::Type::Python(_)) => "python",
        Some(job_spec::Type::Docker(_)) => "docker",
        None => "unknown",
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

        let job_type = job_type_label(spec.r#type.as_ref());
        tracing::info!(job_type, "received submit request");

        match &spec.r#type {
            Some(job_spec::Type::Process(process)) => {
                if process.command.trim().is_empty() {
                    tracing::warn!(job_type, "submit rejected: empty process command");
                    return Err(Status::invalid_argument("command cannot be empty"));
                }
            }
            Some(job_spec::Type::Python(python)) => {
                if python.entry_point.trim().is_empty() {
                    tracing::warn!(job_type, "submit rejected: empty python entry_point");
                    return Err(Status::invalid_argument(
                        "python entry_point cannot be empty",
                    ));
                }
            }
            Some(job_spec::Type::Docker(_)) => {
                tracing::warn!(job_type, "submit rejected: unsupported job type");
                return Err(Status::invalid_argument("unsupported job type"));
            }
            None => {
                tracing::warn!("submit rejected: missing job type");
                return Err(Status::invalid_argument("missing job type"));
            }
        }

        let mut inner = self.inner.lock().await;
        if inner.scheduling.pending_jobs.len() >= inner.submission_buffer_capacity {
            tracing::warn!(
                pending_jobs = inner.scheduling.pending_jobs.len(),
                submission_buffer_capacity = inner.submission_buffer_capacity,
                "submit rejected: controller submission buffer full"
            );
            return Err(Status::resource_exhausted(
                "controller submission buffer is full",
            ));
        }

        inner.next_job_id += 1;
        let job_id = format!("job-{:08}", inner.next_job_id);

        inner.scheduling.pending_jobs.push_back(Job {
            id: job_id.clone(),
            spec,
        });
        inner
            .job_states
            .insert(job_id.clone(), "queued".to_string());
        tracing::info!(job_id, job_type, "job queued");
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

        tracing::debug!(job_id = %req.job_id, state, "status requested");

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
        let worker_id = req.worker_id.clone();
        let slots = req.slots.max(1);
        if req.worker_id.trim().is_empty() {
            tracing::warn!("worker registration rejected: empty worker_id");
            return Err(Status::invalid_argument("worker_id cannot be empty"));
        }

        let mut inner = self.inner.lock().await;
        inner.scheduling.workers.insert(
            req.worker_id.clone(),
            WorkerState {
                id: req.worker_id.clone(),
                available_slots: slots,
            },
        );
        inner.worker_queues.entry(req.worker_id).or_default();
        inner.schedule_pending();

        tracing::info!(worker_id, slots, "worker registered");

        Ok(Response::new(RegisterWorkerResponse { accepted: true }))
    }

    async fn poll_job(
        &self,
        request: Request<PollJobRequest>,
    ) -> Result<Response<PollJobResponse>, Status> {
        let req = request.into_inner();
        let worker_id = req.worker_id.clone();
        let mut inner = self.inner.lock().await;
        inner.schedule_pending();

        let maybe_job = inner
            .worker_queues
            .entry(req.worker_id)
            .or_default()
            .pop_front();

        let response = if let Some(job) = maybe_job {
            let job_id = job.id.clone();
            let job_type = job_type_label(job.spec.r#type.as_ref());
            tracing::info!(worker_id, job_id, job_type, "poll assigned job");

            PollJobResponse {
                result: Some(poll_job_response::Result::Job(AssignedJob {
                    job_id: job.id,
                    spec: Some(job.spec),
                })),
            }
        } else {
            tracing::debug!(worker_id, "poll returned empty");
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
        let job_id = req.job_id.clone();
        let worker_id = req.worker_id.clone();
        inner.job_states.insert(req.job_id, new_state.to_string());

        if req.error_message.is_empty() {
            tracing::info!(
                job_id,
                worker_id,
                exit_code = req.exit_code,
                state = new_state,
                "job result reported"
            );
        } else {
            tracing::warn!(
                job_id,
                worker_id,
                exit_code = req.exit_code,
                state = new_state,
                error_message = %req.error_message,
                "job result reported with error"
            );
        }

        if let Some(worker) = inner.scheduling.workers.get_mut(&req.worker_id) {
            worker.available_slots += 1;
            tracing::debug!(worker_id = %req.worker_id, available_slots = worker.available_slots, "worker slot released");
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
        tracing::debug!(worker_id = %req.worker_id, ok, "heartbeat received");
        Ok(Response::new(HeartbeatResponse { ok }))
    }
}

#[cfg(test)]
mod tests {
    use burst_core::proto::{
        GetJobStatusRequest, JobSpec, PollJobRequest, ProcessSpec, PythonSpec,
        RegisterWorkerRequest, ReportJobResultRequest, SubmitJobRequest,
        controller_rpc_server::ControllerRpc,
        job_spec::Type::{Process, Python},
        poll_job_response,
    };
    use tonic::Request;

    use crate::router::{RoundRobinFactory, RouterFactory};

    use super::ControllerService;

    const TEST_DEFAULT_SUBMISSION_BUFFER_CAPACITY: usize = 32;

    fn service() -> ControllerService {
        ControllerService::new(
            RoundRobinFactory.build(),
            TEST_DEFAULT_SUBMISSION_BUFFER_CAPACITY,
        )
    }

    #[tokio::test]
    async fn submit_rejects_empty_command() {
        let service = service();

        let error = service
            .submit_job(Request::new(SubmitJobRequest {
                spec: Some(JobSpec {
                    output_dir: None,
                    r#type: Some(Process(ProcessSpec {
                        command: "   ".to_string(),
                        args: vec![],
                    })),
                    ..Default::default()
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
                    output_dir: None,
                    r#type: Some(Process(ProcessSpec {
                        command: "echo".to_string(),
                        args: vec!["hello".to_string()],
                    })),
                    ..Default::default()
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

    #[tokio::test]
    async fn submit_python_poll_returns_python_spec() {
        let service = service();
        service
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: "worker-1".to_string(),
                slots: 1,
            }))
            .await
            .expect("worker registration should succeed");

        service
            .submit_job(Request::new(SubmitJobRequest {
                spec: Some(JobSpec {
                    output_dir: Some("/tmp/out".to_string()),
                    r#type: Some(Python(PythonSpec {
                        entry_point: "script.py".to_string(),
                        args: vec!["--name".to_string(), "burst".to_string()],
                    })),
                    ..Default::default()
                }),
            }))
            .await
            .expect("python submit should succeed");

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

        let spec = assigned.spec.expect("assigned job should include spec");
        assert_eq!(spec.output_dir, Some("/tmp/out".to_string()));

        match spec.r#type {
            Some(Python(python)) => {
                assert_eq!(python.entry_point, "script.py");
                assert_eq!(python.args, vec!["--name".to_string(), "burst".to_string()]);
            }
            _ => panic!("expected python job type"),
        }
    }

    #[tokio::test]
    async fn submit_rejects_when_submission_buffer_is_full() {
        let service = ControllerService::new(RoundRobinFactory.build(), 2);

        for _ in 0..2 {
            service
                .submit_job(Request::new(SubmitJobRequest {
                    spec: Some(JobSpec {
                        output_dir: None,
                        r#type: Some(Process(ProcessSpec {
                            command: "echo".to_string(),
                            args: vec!["hello".to_string()],
                        })),
                        ..Default::default()
                    }),
                }))
                .await
                .expect("submit should succeed while buffer has space");
        }

        let error = service
            .submit_job(Request::new(SubmitJobRequest {
                spec: Some(JobSpec {
                    output_dir: None,
                    r#type: Some(Process(ProcessSpec {
                        command: "echo".to_string(),
                        args: vec!["overflow".to_string()],
                    })),
                    ..Default::default()
                }),
            }))
            .await
            .expect_err("submit should fail when buffer is full");

        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn submit_accepts_again_after_pending_job_is_leased() {
        let service = ControllerService::new(RoundRobinFactory.build(), 1);

        service
            .submit_job(Request::new(SubmitJobRequest {
                spec: Some(JobSpec {
                    output_dir: None,
                    r#type: Some(Process(ProcessSpec {
                        command: "echo".to_string(),
                        args: vec!["first".to_string()],
                    })),
                    ..Default::default()
                }),
            }))
            .await
            .expect("first submit should succeed");

        let full_error = service
            .submit_job(Request::new(SubmitJobRequest {
                spec: Some(JobSpec {
                    output_dir: None,
                    r#type: Some(Process(ProcessSpec {
                        command: "echo".to_string(),
                        args: vec!["overflow".to_string()],
                    })),
                    ..Default::default()
                }),
            }))
            .await
            .expect_err("submit should fail while buffer is full");

        assert_eq!(full_error.code(), tonic::Code::ResourceExhausted);

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
            Some(poll_job_response::Result::Job(_))
        ));

        service
            .submit_job(Request::new(SubmitJobRequest {
                spec: Some(JobSpec {
                    output_dir: None,
                    r#type: Some(Process(ProcessSpec {
                        command: "echo".to_string(),
                        args: vec!["after-drain".to_string()],
                    })),
                    ..Default::default()
                }),
            }))
            .await
            .expect("submit should succeed once buffer drains");
    }
}
