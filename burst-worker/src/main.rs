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
//! - `--worker-id <id>` optional override
//! - `--slots <n>` optional override

use std::time::Duration;

use burst_core::proto::{
    PollJobRequest, RegisterWorkerRequest, ReportJobResultRequest,
    controller_rpc_client::ControllerRpcClient, poll_job_response,
};
use burst_core::config::BurstConfig;
use tokio::time::sleep;

fn read_option(args: &[String], name: &str) -> Option<String> {
    for pair in args.windows(2) {
        if pair[0] == name {
            return Some(pair[1].clone());
        }
    }
    None
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

    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let config_path = read_option(&args, "--config").unwrap_or_else(|| "burst.config.json".to_string());

    let config = match BurstConfig::load_from_path(&config_path) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(path = config_path, error = %error, "failed to load config");
            std::process::exit(2);
        }
    };

    let controller_addr = config.worker.controller_addr.clone();
    let worker_id = read_option(&args, "--worker-id")
        .unwrap_or_else(|| format!("worker-{}", std::process::id()));
    let slots = read_option(&args, "--slots")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(config.worker.default_slots)
        .max(1);
    let poll_interval = Duration::from_millis(config.worker.poll_interval_ms.max(10));
    let retry_interval = Duration::from_millis(config.worker.retry_interval_ms.max(10));

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
