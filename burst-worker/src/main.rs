//! Worker service for burst.
//!
//! Worker loop in current POC:
//!
//! 1. connect to controller
//! 2. register worker id and slot count
//! 3. poll for jobs
//! 4. execute process jobs with `tokio::process::Command`
//! 5. report exit code and error message
//!
//! Configuration:
//!
//! - `--config <path>` (default `burst.config.json`)
//! - `--worker-id <id>` must match a `worker_id` in the `workers` list

mod executor;

use std::collections::VecDeque;
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use burst_core::config::BurstConfig;
use burst_core::proto::{
    AssignedJob, PollJobRequest, RegisterWorkerRequest, ReportJobResultRequest, StealJobsRequest,
    StealJobsResponse,
    controller_rpc_client::ControllerRpcClient,
    poll_job_response,
    worker_peer_rpc_client::WorkerPeerRpcClient,
    worker_peer_rpc_server::{WorkerPeerRpc, WorkerPeerRpcServer},
};
use clap::Parser;
use executor::execute_job;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct WorkerPeerService {
    worker_id: String,
    queue: Arc<Mutex<VecDeque<AssignedJob>>>,
    min_local_jobs: usize,
}

#[tonic::async_trait]
impl WorkerPeerRpc for WorkerPeerService {
    async fn steal_jobs(
        &self,
        request: Request<StealJobsRequest>,
    ) -> Result<Response<StealJobsResponse>, Status> {
        let req = request.into_inner();
        if req.thief_worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("thief_worker_id cannot be empty"));
        }

        if req.thief_worker_id == self.worker_id {
            return Ok(Response::new(StealJobsResponse { jobs: vec![] }));
        }

        let max_jobs = req.max_jobs.clamp(1, 64) as usize;

        let mut queue = self.queue.lock().await;
        let stealable = queue.len().saturating_sub(self.min_local_jobs);
        let count = stealable.min(max_jobs);

        let mut jobs = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(job) = queue.pop_back() {
                jobs.push(job);
            }
        }

        if jobs.is_empty() {
            tracing::debug!(
                victim_worker_id = %self.worker_id,
                thief_worker_id = %req.thief_worker_id,
                queue_len = queue.len(),
                "steal request returned empty"
            );
        } else {
            tracing::info!(
                victim_worker_id = %self.worker_id,
                thief_worker_id = %req.thief_worker_id,
                stolen_jobs = jobs.len(),
                queue_len = queue.len(),
                "jobs stolen by peer"
            );
        }

        Ok(Response::new(StealJobsResponse { jobs }))
    }
}

#[derive(Clone)]
struct PeerEndpoint {
    worker_id: String,
    endpoint: String,
}

fn normalize_endpoint(address: &str) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

async fn try_steal_from_peer(
    worker_id: &str,
    peer: &PeerEndpoint,
    max_jobs: u32,
) -> Result<Vec<AssignedJob>, String> {
    let mut client = WorkerPeerRpcClient::connect(peer.endpoint.clone())
        .await
        .map_err(|error| format!("connect failed: {error}"))?;

    let response = client
        .steal_jobs(StealJobsRequest {
            thief_worker_id: worker_id.to_string(),
            max_jobs: max_jobs.max(1),
        })
        .await
        .map_err(|error| format!("steal request failed: {error}"))?;

    Ok(response.into_inner().jobs)
}

#[derive(Parser)]
#[command(about = "Burst worker service")]
struct Args {
    /// Path to the config file
    #[arg(long, default_value = "burst.config.json")]
    config: String,

    /// Worker ID — must match a `worker_id` entry in the config `workers` list
    #[arg(long)]
    worker_id: String,
}

#[tokio::main]
async fn main() {
    let use_ansi = std::io::stderr().is_terminal();
    tracing_subscriber::fmt()
        .with_ansi(use_ansi)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let config = match BurstConfig::load_from_path(&args.config) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(path = args.config, error = %error, "failed to load config");
            std::process::exit(2);
        }
    };

    let worker_id = args.worker_id;
    let worker_config = match config.workers.iter().find(|w| w.worker_id == worker_id) {
        Some(wc) => wc.clone(),
        None => {
            tracing::error!(worker_id, "no matching worker entry found in config");
            std::process::exit(2);
        }
    };

    let controller_addr = worker_config.controller_addr.clone();
    let slots = worker_config.slots.max(1);
    let poll_interval = Duration::from_millis(worker_config.poll_interval_ms.max(10));
    let retry_interval = Duration::from_millis(worker_config.retry_interval_ms.max(10));
    let steal_interval = Duration::from_millis(worker_config.steal_interval_ms.max(10));
    let steal_batch_size = worker_config.steal_batch_size.max(1);

    let job_queue = Arc::new(Mutex::new(VecDeque::<AssignedJob>::new()));

    if let Some(peer_listen_addr) = worker_config.peer_listen_addr.as_deref() {
        let bind_addr: SocketAddr = match peer_listen_addr.parse() {
            Ok(addr) => addr,
            Err(error) => {
                tracing::error!(
                    worker_id,
                    peer_listen_addr,
                    error = %error,
                    "invalid peer listen address"
                );
                std::process::exit(2);
            }
        };

        let peer_service = WorkerPeerService {
            worker_id: worker_id.clone(),
            queue: Arc::clone(&job_queue),
            min_local_jobs: 1,
        };

        tokio::spawn(async move {
            if let Err(error) = Server::builder()
                .add_service(WorkerPeerRpcServer::new(peer_service))
                .serve(bind_addr)
                .await
            {
                tracing::error!(error = %error, "peer stealing server stopped");
            }
        });

        tracing::info!(worker_id, peer_listen_addr, "peer stealing server started");
    }

    let peers: Vec<PeerEndpoint> = config
        .workers
        .iter()
        .filter(|entry| entry.worker_id != worker_id)
        .filter_map(|entry| {
            let address = entry
                .peer_advertise_addr
                .as_deref()
                .or(entry.peer_listen_addr.as_deref())?;
            Some(PeerEndpoint {
                worker_id: entry.worker_id.clone(),
                endpoint: normalize_endpoint(address),
            })
        })
        .collect();

    if peers.is_empty() {
        tracing::info!(worker_id, "no peer endpoints configured; stealing disabled");
    } else {
        tracing::info!(worker_id, peers = peers.len(), "peer stealing enabled");
    }

    tracing::info!(
        controller = controller_addr,
        worker_id,
        slots,
        "worker starting"
    );

    let mut client = match ControllerRpcClient::connect(controller_addr.clone()).await {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(error = %error, controller = controller_addr, "failed to connect to controller");
            std::process::exit(1);
        }
    };

    if let Err(error) = client
        .register_worker(RegisterWorkerRequest {
            worker_id: worker_id.clone(),
            slots,
        })
        .await
    {
        tracing::error!(error = %error, "worker registration failed");
        std::process::exit(1);
    }

    tracing::info!(worker_id, slots, "worker registered");

    fn assigned_job_type(job: &burst_core::proto::AssignedJob) -> &'static str {
        match job.spec.as_ref().and_then(|spec| spec.r#type.as_ref()) {
            Some(burst_core::proto::job_spec::Type::Process(_)) => "process",
            Some(burst_core::proto::job_spec::Type::Python(_)) => "python",
            Some(burst_core::proto::job_spec::Type::Docker(_)) => "docker",
            None => "unknown",
        }
    }

    let mut victim_cursor = 0usize;

    loop {
        let next_local_job = {
            let mut queue = job_queue.lock().await;
            queue.pop_front()
        };

        if let Some(job) = next_local_job {
            let job_id = job.job_id.clone();
            let job_type = assigned_job_type(&job);
            tracing::info!(worker_id, job_id, job_type, "executing local queued job");

            let (exit_code, error_message) = execute_job(job).await;

            if error_message.is_empty() {
                tracing::info!(worker_id, job_id, exit_code, "job execution finished");
            } else {
                tracing::warn!(
                    worker_id,
                    job_id,
                    exit_code,
                    error_message = %error_message,
                    "job execution finished with error"
                );
            }

            if let Err(error) = client
                .report_job_result(ReportJobResultRequest {
                    worker_id: worker_id.clone(),
                    job_id,
                    exit_code,
                    error_message,
                })
                .await
            {
                tracing::warn!(error = %error, "failed to report job result");
            } else {
                tracing::debug!(worker_id, "job result reported");
            }
            continue;
        }

        if !peers.is_empty() {
            let peer = &peers[victim_cursor % peers.len()];
            victim_cursor = victim_cursor.wrapping_add(1);

            match try_steal_from_peer(&worker_id, peer, steal_batch_size).await {
                Ok(stolen_jobs) if !stolen_jobs.is_empty() => {
                    let stolen_count = stolen_jobs.len();
                    {
                        let mut queue = job_queue.lock().await;
                        queue.extend(stolen_jobs);
                    }
                    tracing::info!(
                        worker_id,
                        victim_worker_id = %peer.worker_id,
                        stolen_count,
                        "stole jobs from peer"
                    );
                    continue;
                }
                Ok(_) => {
                    tracing::debug!(
                        worker_id,
                        victim_worker_id = %peer.worker_id,
                        "peer had no stealable jobs"
                    );
                }
                Err(error) => {
                    tracing::debug!(
                        worker_id,
                        victim_worker_id = %peer.worker_id,
                        error = %error,
                        "peer steal attempt failed"
                    );
                }
            }

            sleep(steal_interval).await;
        }

        let poll_response = match client
            .poll_job(PollJobRequest {
                worker_id: worker_id.clone(),
            })
            .await
        {
            Ok(response) => response.into_inner(),
            Err(error) => {
                tracing::warn!(error = %error, "poll failed; retrying");
                sleep(retry_interval).await;
                continue;
            }
        };

        match poll_response.result {
            Some(poll_job_response::Result::Job(job)) => {
                let job_id = job.job_id.clone();
                let job_type = assigned_job_type(&job);
                {
                    let mut queue = job_queue.lock().await;
                    queue.push_back(job);
                }
                tracing::info!(
                    worker_id,
                    job_id,
                    job_type,
                    "job received from poll and enqueued"
                );
            }
            Some(poll_job_response::Result::Empty(_)) | None => {
                tracing::debug!(worker_id, "poll returned no jobs");
                sleep(poll_interval).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use burst_core::proto::{
        DockerSpec, ProcessSpec, PythonSpec,
        job_spec::Type::{Docker, Process, Python},
    };
    use std::collections::{HashMap, VecDeque};
    use std::{
        fs,
        net::{SocketAddr, TcpListener},
        path::PathBuf,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use burst_core::proto::{AssignedJob, JobSpec};
    use tokio::{sync::Mutex, task::JoinHandle, time::Instant};
    use tonic::Request;

    use super::{
        PeerEndpoint, WorkerPeerRpc, WorkerPeerRpcServer, WorkerPeerService, execute_job,
        try_steal_from_peer,
    };

    fn assigned_job(job_id: &str) -> AssignedJob {
        AssignedJob {
            job_id: job_id.to_string(),
            spec: None,
        }
    }

    fn bind_ephemeral_local_addr() -> SocketAddr {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("failed to bind local ephemeral port");
        let addr = listener
            .local_addr()
            .expect("failed to read local addr from ephemeral listener");
        drop(listener);
        addr
    }

    fn spawn_peer_server(
        worker_id: &str,
        queue: Arc<Mutex<VecDeque<AssignedJob>>>,
        min_local_jobs: usize,
        bind_addr: SocketAddr,
    ) -> JoinHandle<()> {
        let peer_service = WorkerPeerService {
            worker_id: worker_id.to_string(),
            queue,
            min_local_jobs,
        };

        tokio::spawn(async move {
            let result = tonic::transport::Server::builder()
                .add_service(WorkerPeerRpcServer::new(peer_service))
                .serve(bind_addr)
                .await;

            if let Err(error) = result {
                panic!("peer server failed: {error}");
            }
        })
    }

    async fn simulate_worker_drain(
        job_ids: Vec<String>,
        durations_ms: &HashMap<String, u64>,
    ) -> u128 {
        let start = Instant::now();
        for job_id in job_ids {
            let duration_ms = durations_ms
                .get(&job_id)
                .copied()
                .expect("missing duration for job id");
            tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;
        }
        start.elapsed().as_millis()
    }

    fn temp_output_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("burst-worker-{name}-{nanos}"))
    }

    #[tokio::test]
    async fn execute_job_fails_without_spec() {
        let (exit_code, error_message) = execute_job(AssignedJob {
            job_id: "job-1".to_string(),
            spec: None,
        })
        .await;

        assert_eq!(exit_code, -1);
        assert_eq!(error_message, "missing job spec");
    }

    #[tokio::test]
    async fn execute_job_captures_stdout_and_stderr() {
        let output_dir = temp_output_dir("stdout-stderr");
        let output_dir_str = output_dir
            .to_str()
            .expect("temp output path is not valid UTF-8")
            .to_string();

        let (exit_code, error_message) = execute_job(AssignedJob {
            job_id: "job-2".to_string(),
            spec: Some(JobSpec {
                output_dir: Some(output_dir_str),
                r#type: Some(Process(ProcessSpec {
                    command: "/bin/sh".to_string(),
                    args: vec!["-c".to_string(), "echo hello; echo oops 1>&2".to_string()],
                })),
                ..Default::default()
            }),
        })
        .await;

        assert_eq!(exit_code, 0);
        assert_eq!(error_message, "");

        let stdout = fs::read_to_string(output_dir.join("job-2.stdout"))
            .expect("stdout file should be created");
        let stderr = fs::read_to_string(output_dir.join("job-2.stderr"))
            .expect("stderr file should be created");

        assert!(stdout.contains("hello"));
        assert!(stderr.contains("oops"));

        let _ = fs::remove_dir_all(output_dir);
    }

    #[tokio::test]
    async fn execute_job_returns_nonzero_exit_code() {
        let output_dir = temp_output_dir("nonzero");
        let output_dir_str = output_dir
            .to_str()
            .expect("temp output path is not valid UTF-8")
            .to_string();

        let (exit_code, error_message) = execute_job(AssignedJob {
            job_id: "job-3".to_string(),
            spec: Some(JobSpec {
                output_dir: Some(output_dir_str),
                r#type: Some(Process(ProcessSpec {
                    command: "/bin/sh".to_string(),
                    args: vec!["-c".to_string(), "exit 7".to_string()],
                })),
                ..Default::default()
            }),
        })
        .await;

        assert_eq!(exit_code, 7);
        assert_eq!(error_message, "");

        let _ = fs::remove_dir_all(output_dir);
    }

    #[tokio::test]
    async fn execute_job_rejects_python_without_entrypoint() {
        let output_dir = temp_output_dir("python-empty-entrypoint");
        let output_dir_str = output_dir
            .to_str()
            .expect("temp output path is not valid UTF-8")
            .to_string();

        let (exit_code, error_message) = execute_job(AssignedJob {
            job_id: "job-4".to_string(),
            spec: Some(JobSpec {
                output_dir: Some(output_dir_str),
                r#type: Some(Python(PythonSpec {
                    entry_point: "".to_string(),
                    args: vec![],
                })),
                ..Default::default()
            }),
        })
        .await;

        assert_eq!(exit_code, -1);
        assert_eq!(error_message, "python entry_point cannot be empty");

        let _ = fs::remove_dir_all(output_dir);
    }

    #[tokio::test]
    async fn execute_job_rejects_docker_without_image() {
        let output_dir = temp_output_dir("docker-empty-image");
        let output_dir_str = output_dir
            .to_str()
            .expect("temp output path is not valid UTF-8")
            .to_string();

        let (exit_code, error_message) = execute_job(AssignedJob {
            job_id: "job-5".to_string(),
            spec: Some(JobSpec {
                output_dir: Some(output_dir_str),
                r#type: Some(Docker(DockerSpec {
                    image: "".to_string(),
                    command: vec![],
                    args: vec![],
                })),
                ..Default::default()
            }),
        })
        .await;

        assert_eq!(exit_code, -1);
        assert_eq!(error_message, "docker image cannot be empty");

        let _ = fs::remove_dir_all(output_dir);
    }

    #[tokio::test]
    async fn steal_jobs_keeps_minimum_local_jobs_and_steals_from_tail() {
        let queue = Arc::new(Mutex::new(VecDeque::from(vec![
            assigned_job("job-1"),
            assigned_job("job-2"),
            assigned_job("job-3"),
            assigned_job("job-4"),
        ])));

        let service = WorkerPeerService {
            worker_id: "worker-a".to_string(),
            queue: Arc::clone(&queue),
            min_local_jobs: 1,
        };

        let response = service
            .steal_jobs(Request::new(burst_core::proto::StealJobsRequest {
                thief_worker_id: "worker-b".to_string(),
                max_jobs: 2,
            }))
            .await
            .expect("steal request should succeed")
            .into_inner();

        let stolen_ids: Vec<String> = response.jobs.into_iter().map(|job| job.job_id).collect();
        assert_eq!(stolen_ids, vec!["job-4".to_string(), "job-3".to_string()]);

        let remaining_ids: Vec<String> = {
            let queue = queue.lock().await;
            queue.iter().map(|job| job.job_id.clone()).collect()
        };
        assert_eq!(
            remaining_ids,
            vec!["job-1".to_string(), "job-2".to_string()]
        );
    }

    #[tokio::test]
    async fn steal_jobs_rejects_empty_thief_id() {
        let queue = Arc::new(Mutex::new(VecDeque::from(vec![assigned_job("job-1")])));

        let service = WorkerPeerService {
            worker_id: "worker-a".to_string(),
            queue,
            min_local_jobs: 1,
        };

        let error = service
            .steal_jobs(Request::new(burst_core::proto::StealJobsRequest {
                thief_worker_id: " ".to_string(),
                max_jobs: 1,
            }))
            .await
            .expect_err("empty thief id should fail");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn integration_idle_worker_steals_from_busy_peer_with_long_job() {
        let queue = Arc::new(Mutex::new(VecDeque::from(vec![
            assigned_job("long"),
            assigned_job("short-a"),
            assigned_job("short-b"),
            assigned_job("short-c"),
        ])));

        let bind_addr = bind_ephemeral_local_addr();
        let server = spawn_peer_server("worker-busy", Arc::clone(&queue), 1, bind_addr);

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        let peer = PeerEndpoint {
            worker_id: "worker-busy".to_string(),
            endpoint: format!("http://{bind_addr}"),
        };

        let stolen_jobs = try_steal_from_peer("worker-idle", &peer, 2)
            .await
            .expect("steal attempt should succeed");
        assert_eq!(stolen_jobs.len(), 2);

        let busy_job_ids: Vec<String> = {
            let queue = queue.lock().await;
            queue.iter().map(|job| job.job_id.clone()).collect()
        };
        let idle_job_ids: Vec<String> = stolen_jobs.into_iter().map(|job| job.job_id).collect();

        assert!(busy_job_ids.iter().any(|job_id| job_id == "long"));
        assert!(
            idle_job_ids
                .iter()
                .all(|job_id| job_id.starts_with("short-"))
        );

        let durations_ms = HashMap::from([
            ("long".to_string(), 120_u64),
            ("short-a".to_string(), 25_u64),
            ("short-b".to_string(), 25_u64),
            ("short-c".to_string(), 25_u64),
        ]);

        let (busy_elapsed_ms, idle_elapsed_ms) = tokio::join!(
            simulate_worker_drain(busy_job_ids, &durations_ms),
            simulate_worker_drain(idle_job_ids, &durations_ms)
        );
        let makespan_ms = busy_elapsed_ms.max(idle_elapsed_ms);

        assert!(
            makespan_ms < 170,
            "expected stealing to lower completion skew, got makespan {makespan_ms}ms"
        );

        server.abort();
    }
}
