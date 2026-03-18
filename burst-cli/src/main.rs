//! CLI client for burst.
//!
//! Commands in current POC:
//!
//! - `submit <command> [args...]`
//! - `status --job-id <job-id>`
//!
//! Global option:
//!
//! - `--config <path>` (default `burst.config.json`)

use burst_core::config::BurstConfig;
use burst_core::proto::{
    GetJobStatusRequest, JobSpec, ProcessSpec, SubmitJobRequest,
    controller_rpc_client::ControllerRpcClient, job_spec::Type::Process,
};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "burst-cli")]
struct Cli {
    #[arg(long, global = true, default_value = "burst.config.json")]
    config: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Submit {
        #[arg(long)]
        output_dir: Option<String>,

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
    let config = match BurstConfig::load_from_path(&cli.config) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(path = cli.config, error = %error, "failed to load config");
            std::process::exit(2);
        }
    };

    let controller_addr = config.cli.controller_addr.clone();
    let mut client = match ControllerRpcClient::connect(controller_addr.clone()).await {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(error = %error, controller = controller_addr, "failed to connect");
            std::process::exit(1);
        }
    };

    match cli.command {
        Commands::Submit { output_dir, argv } => {
            let command = argv[0].clone();
            let args = argv[1..].to_vec();

            match client
                .submit_job(SubmitJobRequest {
                    spec: Some(JobSpec {
                        output_dir,
                        r#type: Some(Process(ProcessSpec { command, args })),
                        ..Default::default()
                    }),
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

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Commands};

    #[test]
    fn parses_submit_command() {
        let cli = Cli::parse_from([
            "burst-cli",
            "--config",
            "my-config.json",
            "submit",
            "--output-dir",
            "/tmp/out",
            "echo",
            "hello",
        ]);

        assert_eq!(cli.config, "my-config.json");
        match cli.command {
            Commands::Submit { output_dir, argv } => {
                assert_eq!(output_dir, Some("/tmp/out".to_string()));
                assert_eq!(argv, vec!["echo".to_string(), "hello".to_string()]);
            }
            _ => panic!("expected submit command"),
        }
    }

    #[test]
    fn parses_status_command() {
        let cli = Cli::parse_from(["burst-cli", "status", "--job-id", "job-00000001"]);

        match cli.command {
            Commands::Status { job_id } => {
                assert_eq!(job_id, "job-00000001");
            }
            _ => panic!("expected status command"),
        }
    }
}
