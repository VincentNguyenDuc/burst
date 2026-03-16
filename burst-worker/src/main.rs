use std::time::Duration;

use burst_core::proto::{
    PollJobRequest, RegisterWorkerRequest, ReportJobResultRequest,
    controller_rpc_client::ControllerRpcClient, poll_job_response,
};
use tokio::time::sleep;

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

    let controller_addr = std::env::var("BURST_CONTROLLER_ADDR")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let worker_id = std::env::var("BURST_WORKER_ID")
        .unwrap_or_else(|_| format!("worker-{}", std::process::id()));
    let slots = std::env::var("BURST_WORKER_SLOTS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1)
        .max(1);

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
                sleep(Duration::from_millis(500)).await;
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
                sleep(Duration::from_millis(250)).await;
            }
        }
    }
}
