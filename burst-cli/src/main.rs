use burst_core::proto::{
    GetJobStatusRequest, JobSpec, SubmitJobRequest, controller_rpc_client::ControllerRpcClient,
};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "burst-cli")]
struct Cli {
    #[arg(long, global = true, default_value = "http://127.0.0.1:50051")]
    controller: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Submit {
        #[arg(required = true, trailing_var_arg = true)]
        argv: Vec<String>,
    },
    Status {
        #[arg(long)]
        job_id: String,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let mut client = match ControllerRpcClient::connect(cli.controller.clone()).await {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(error = %error, controller = cli.controller, "failed to connect");
            std::process::exit(1);
        }
    };

    match cli.command {
        Commands::Submit { argv } => {
            let command = argv[0].clone();
            let args = argv[1..].to_vec();

            match client
                .submit_job(SubmitJobRequest {
                    spec: Some(JobSpec { command, args }),
                })
                .await
            {
                Ok(response) => {
                    println!("{}", response.into_inner().job_id);
                }
                Err(error) => {
                    tracing::error!(error = %error, "submit failed");
                    std::process::exit(1);
                }
            }
        }
        Commands::Status { job_id } => {
            match client.get_job_status(GetJobStatusRequest { job_id }).await {
                Ok(response) => {
                    let body = response.into_inner();
                    println!("{} {}", body.job_id, body.state);
                }
                Err(error) => {
                    tracing::error!(error = %error, "status failed");
                    std::process::exit(1);
                }
            }
        }
    }
}
