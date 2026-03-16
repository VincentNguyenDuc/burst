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

use std::time::Duration;

use burst_core::config::BurstConfig;
use burst_core::proto::{
    PollJobRequest, RegisterWorkerRequest, ReportJobResultRequest,
    controller_rpc_client::ControllerRpcClient, poll_job_response,
};
use clap::Parser;
use tokio::time::sleep;

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

async fn execute_job(job: burst_core::proto::AssignedJob) -> (i32, String) {
    let Some(spec) = job.spec else {
        return (-1, "missing job spec".to_string());
    };

    let mut command = tokio::process::Command::new(spec.command);
    command.args(spec.args);

    match command.status().await {
        Ok(status) => {
            let code = status.code().unwrap_or(1);
            (code, String::new())
        }
        Err(error) => (-1, error.to_string()),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

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

    loop {
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
                tracing::info!(job_id, "executing job");
                let (exit_code, error_message) = execute_job(job).await;

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
                }
            }
            Some(poll_job_response::Result::Empty(_)) | None => {
                sleep(poll_interval).await;
            }
        }
    }
}
