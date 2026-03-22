use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;

use burst_core::config::WorkerConfig;
use burst_core::proto::{
    AssignedJob, StealJobsRequest, StealJobsResponse,
    worker_peer_rpc_client::WorkerPeerRpcClient,
    worker_peer_rpc_server::{WorkerPeerRpc, WorkerPeerRpcServer},
};
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub type JobQueue = Arc<Mutex<VecDeque<AssignedJob>>>;

#[derive(Clone)]
pub struct WorkerPeerService {
    pub worker_id: String,
    pub queue: JobQueue,
    pub min_local_jobs: usize,
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
            tracing::info!(
                victim_worker_id = %self.worker_id,
                thief_worker_id = %req.thief_worker_id,
                "ignoring self-steal request"
            );
            return Ok(Response::new(StealJobsResponse { jobs: vec![] }));
        }

        let max_jobs = req.max_jobs.clamp(1, 64) as usize;

        let mut queue = self.queue.lock().await;
        let queue_len_before = queue.len();
        let stealable = queue.len().saturating_sub(self.min_local_jobs);
        let count = stealable.min(max_jobs);

        let mut jobs = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(job) = queue.pop_back() {
                jobs.push(job);
            }
        }

        if jobs.is_empty() {
            tracing::info!(
                victim_worker_id = %self.worker_id,
                thief_worker_id = %req.thief_worker_id,
                requested_max_jobs = max_jobs,
                queue_len_before,
                stealable,
                queue_len = queue.len(),
                "steal request returned empty"
            );
        } else {
            tracing::info!(
                victim_worker_id = %self.worker_id,
                thief_worker_id = %req.thief_worker_id,
                requested_max_jobs = max_jobs,
                queue_len_before,
                stealable,
                stolen_jobs = jobs.len(),
                queue_len = queue.len(),
                "jobs stolen by peer"
            );
        }

        Ok(Response::new(StealJobsResponse { jobs }))
    }
}

#[derive(Clone)]
pub struct PeerEndpoint {
    pub worker_id: String,
    pub endpoint: String,
}

pub fn build_peer_endpoints(workers: &[WorkerConfig], worker_id: &str) -> Vec<PeerEndpoint> {
    workers
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
        .collect()
}

pub fn start_peer_server(
    worker_id: &str,
    queue: JobQueue,
    peer_listen_addr: &str,
    min_local_jobs: usize,
) -> Result<(), String> {
    let bind_addr: SocketAddr = peer_listen_addr
        .parse()
        .map_err(|error| format!("invalid peer listen address '{peer_listen_addr}': {error}"))?;

    let peer_service = WorkerPeerService {
        worker_id: worker_id.to_string(),
        queue,
        min_local_jobs,
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

    Ok(())
}

pub async fn try_steal_from_peer(
    worker_id: &str,
    peer: &PeerEndpoint,
    max_jobs: u32,
) -> Result<Vec<AssignedJob>, String> {
    tracing::info!(
        worker_id,
        victim_worker_id = %peer.worker_id,
        endpoint = %peer.endpoint,
        requested_max_jobs = max_jobs.max(1),
        "attempting peer steal request"
    );

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

    let jobs = response.into_inner().jobs;

    tracing::info!(
        worker_id,
        victim_worker_id = %peer.worker_id,
        endpoint = %peer.endpoint,
        stolen_jobs = jobs.len(),
        "peer steal request completed"
    );

    Ok(jobs)
}

fn normalize_endpoint(address: &str) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Arc;

    use burst_core::proto::worker_peer_rpc_server::{WorkerPeerRpc, WorkerPeerRpcServer};
    use burst_core::proto::{AssignedJob, StealJobsRequest};
    use tokio::{sync::Mutex, task::JoinHandle, time::Instant};
    use tonic::Request;

    use super::{PeerEndpoint, WorkerPeerService, try_steal_from_peer};

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
            .steal_jobs(Request::new(StealJobsRequest {
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
            .steal_jobs(Request::new(StealJobsRequest {
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
