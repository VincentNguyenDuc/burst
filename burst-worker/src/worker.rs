use std::time::Duration;

use burst_core::proto::{
    PollJobRequest, ReportJobResultRequest, controller_rpc_client::ControllerRpcClient,
    poll_job_response,
};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tonic::transport::Channel;

use crate::executor::execute_job;
use crate::peer::{JobQueue, PeerEndpoint, try_steal_from_peer};

pub async fn run(
    client: &mut ControllerRpcClient<Channel>,
    worker_id: &str,
    job_queue: JobQueue,
    peers: Vec<PeerEndpoint>,
    max_parallel_jobs: usize,
    max_local_queue_size: usize,
    steal_batch_size: u32,
    steal_interval: Duration,
    retry_interval: Duration,
    poll_interval: Duration,
) {
    let max_parallel_jobs = max_parallel_jobs.max(1);
    let max_local_queue_size = max_local_queue_size.max(1);
    let mut victim_cursor = 0usize;
    let mut in_flight = JoinSet::<JobOutcome>::new();

    loop {
        let mut did_work = false;

        while let Some(join_result) = in_flight.try_join_next() {
            did_work = true;
            handle_job_completion(worker_id, client, join_result).await;
        }

        while in_flight.len() < max_parallel_jobs {
            let next_local_job = {
                let mut queue = job_queue.lock().await;
                queue.pop_front()
            };

            let Some(job) = next_local_job else {
                break;
            };

            did_work = true;
            spawn_job(worker_id, &mut in_flight, job);
        }

        let local_queue_len = {
            let queue = job_queue.lock().await;
            queue.len()
        };

        tracing::debug!(
            worker_id,
            in_flight = in_flight.len(),
            queue_len = local_queue_len,
            queue_capacity = max_local_queue_size,
            max_parallel_jobs,
            "worker loop state"
        );

        if local_queue_len == 0 && in_flight.len() < max_parallel_jobs && !peers.is_empty() {
            let peer = &peers[victim_cursor % peers.len()];
            victim_cursor = victim_cursor.wrapping_add(1);

            tracing::debug!(
                worker_id,
                victim_worker_id = %peer.worker_id,
                local_queue_len,
                steal_batch_size,
                "local queue empty; attempting to steal from peer"
            );

            match try_steal_from_peer(worker_id, peer, steal_batch_size).await {
                Ok(stolen_jobs) if !stolen_jobs.is_empty() => {
                    let stolen_count = stolen_jobs.len();
                    let local_queue_len_after;
                    {
                        let mut queue = job_queue.lock().await;
                        queue.extend(stolen_jobs);
                        local_queue_len_after = queue.len();
                    }
                    did_work = true;
                    tracing::info!(
                        worker_id,
                        victim_worker_id = %peer.worker_id,
                        stolen_count,
                        local_queue_len_after,
                        "stole jobs from peer"
                    );
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
                        local_queue_len,
                        error = %error,
                        "peer steal attempt failed"
                    );
                }
            }
        }

        let local_queue_len = {
            let queue = job_queue.lock().await;
            queue.len()
        };

        if should_poll_controller(local_queue_len, max_local_queue_size) {
            let remaining_queue_capacity = max_local_queue_size.saturating_sub(local_queue_len);
            let poll_response = match client
                .poll_job(PollJobRequest {
                    worker_id: worker_id.to_string(),
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
                    let local_queue_len_after;
                    {
                        let mut queue = job_queue.lock().await;
                        queue.push_back(job);
                        local_queue_len_after = queue.len();
                    }
                    did_work = true;
                    tracing::info!(
                        worker_id,
                        job_id,
                        job_type,
                        local_queue_len_after,
                        max_local_queue_size,
                        remaining_queue_capacity_before_enqueue = remaining_queue_capacity,
                        max_parallel_jobs,
                        in_flight = in_flight.len(),
                        "job received from poll and enqueued"
                    );
                }
                Some(poll_job_response::Result::Empty(_)) | None => {
                    tracing::debug!(
                        worker_id,
                        in_flight = in_flight.len(),
                        local_queue_len,
                        max_local_queue_size,
                        "poll returned no jobs"
                    );
                }
            }
        } else {
            tracing::debug!(
                worker_id,
                local_queue_len,
                max_local_queue_size,
                "local queue is full; skipping poll"
            );
        }

        if !did_work {
            if !peers.is_empty() {
                sleep(steal_interval).await;
            } else {
                sleep(poll_interval).await;
            }
        }
    }
}

fn should_poll_controller(local_queue_len: usize, max_local_queue_size: usize) -> bool {
    local_queue_len < max_local_queue_size
}

struct JobOutcome {
    job_id: String,
    exit_code: i32,
    error_message: String,
}

fn spawn_job(
    worker_id: &str,
    in_flight: &mut JoinSet<JobOutcome>,
    job: burst_core::proto::AssignedJob,
) {
    let job_id = job.job_id.clone();
    let job_type = assigned_job_type(&job);

    tracing::info!(
        worker_id,
        job_id,
        job_type,
        in_flight = in_flight.len(),
        "spawning job execution task"
    );

    in_flight.spawn(async move {
        let (exit_code, error_message) = execute_job(job).await;
        JobOutcome {
            job_id,
            exit_code,
            error_message,
        }
    });
}

async fn handle_job_completion(
    worker_id: &str,
    client: &mut ControllerRpcClient<Channel>,
    join_result: Result<JobOutcome, tokio::task::JoinError>,
) {
    let outcome = match join_result {
        Ok(outcome) => outcome,
        Err(error) => {
            tracing::error!(worker_id, error = %error, "job execution task failed to join");
            return;
        }
    };

    if outcome.error_message.is_empty() {
        tracing::info!(
            worker_id,
            job_id = outcome.job_id,
            exit_code = outcome.exit_code,
            "job execution finished"
        );
    } else {
        tracing::warn!(
            worker_id,
            job_id = outcome.job_id,
            exit_code = outcome.exit_code,
            error_message = %outcome.error_message,
            "job execution finished with error"
        );
    }

    if let Err(error) = client
        .report_job_result(ReportJobResultRequest {
            worker_id: worker_id.to_string(),
            job_id: outcome.job_id,
            exit_code: outcome.exit_code,
            error_message: outcome.error_message,
        })
        .await
    {
        tracing::warn!(worker_id, error = %error, "failed to report job result");
    } else {
        tracing::debug!(worker_id, "job result reported");
    }
}

fn assigned_job_type(job: &burst_core::proto::AssignedJob) -> &'static str {
    match job.spec.as_ref().and_then(|spec| spec.r#type.as_ref()) {
        Some(burst_core::proto::job_spec::Type::Process(_)) => "process",
        Some(burst_core::proto::job_spec::Type::Python(_)) => "python",
        Some(burst_core::proto::job_spec::Type::Docker(_)) => "docker",
        None => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::should_poll_controller;

    #[test]
    fn polls_when_queue_has_room() {
        assert!(should_poll_controller(0, 64));
        assert!(should_poll_controller(63, 64));
    }

    #[test]
    fn does_not_poll_when_queue_full() {
        assert!(!should_poll_controller(64, 64));
        assert!(!should_poll_controller(80, 64));
    }
}
