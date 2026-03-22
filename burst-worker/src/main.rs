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
mod peer;
mod worker;

use std::collections::VecDeque;
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::Duration;

use burst_core::config::BurstConfig;
use burst_core::proto::{
    AssignedJob, RegisterWorkerRequest, controller_rpc_client::ControllerRpcClient,
};
use clap::Parser;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;

use peer::{build_peer_endpoints, start_peer_server};
use worker::run;

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
    let local_queue_capacity = worker_config.local_queue_capacity.max(slots as usize);
    let poll_interval = Duration::from_millis(worker_config.poll_interval_ms.max(10));
    let retry_interval = Duration::from_millis(worker_config.retry_interval_ms.max(10));
    let steal_interval = Duration::from_millis(worker_config.steal_interval_ms.max(10));
    let steal_batch_size = worker_config.steal_batch_size.max(1);

    let job_queue = Arc::new(Mutex::new(VecDeque::<AssignedJob>::new()));

    if let Some(peer_listen_addr) = worker_config.peer_listen_addr.as_deref() {
        if let Err(error) =
            start_peer_server(&worker_id, Arc::clone(&job_queue), peer_listen_addr, 1)
        {
            tracing::error!(worker_id, error = %error, "failed to start peer stealing server");
            std::process::exit(2);
        }

        tracing::info!(worker_id, peer_listen_addr, "peer stealing server started");
    }

    let peers = build_peer_endpoints(&config.workers, &worker_id);

    if peers.is_empty() {
        tracing::info!(worker_id, "no peer endpoints configured; stealing disabled");
    } else {
        tracing::info!(worker_id, peers = peers.len(), "peer stealing enabled");
    }

    tracing::info!(
        controller = controller_addr,
        worker_id,
        slots,
        local_queue_capacity,
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
            queue_capacity: local_queue_capacity.min(u32::MAX as usize) as u32,
        })
        .await
    {
        tracing::error!(error = %error, "worker registration failed");
        std::process::exit(1);
    }

    tracing::info!(worker_id, slots, "worker registered");

    run(
        &mut client,
        &worker_id,
        job_queue,
        peers,
        slots as usize,
        local_queue_capacity,
        steal_batch_size,
        steal_interval,
        retry_interval,
        poll_interval,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use burst_core::proto::{
        DockerSpec, ProcessSpec, PythonSpec,
        job_spec::Type::{Docker, Process, Python},
    };
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use burst_core::proto::{AssignedJob, JobSpec};

    use crate::executor::execute_job;

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
}
