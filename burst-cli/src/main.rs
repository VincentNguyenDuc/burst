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
    DockerSpec, GetJobStatusRequest, JobSpec, ProcessSpec, PythonSpec, SubmitJobRequest,
    controller_rpc_client::ControllerRpcClient, job_spec,
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
    Submit(SubmitArgs),
    Status {
        #[arg(long)]
        job_id: String,
    },
}

#[derive(Parser)]
struct SubmitArgs {
    #[arg(long)]
    output_dir: Option<String>,

    #[command(subcommand)]
    job: SubmitJobCommand,
}

#[derive(Subcommand)]
enum SubmitJobCommand {
    Process {
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        argv: Vec<String>,
    },
    Python {
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        argv: Vec<String>,
    },
    Docker {
        image: String,

        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
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
        Commands::Submit(submit) => {
            let spec = build_job_spec(submit);

            match client
                .submit_job(SubmitJobRequest { spec: Some(spec) })
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

fn build_job_spec(submit: SubmitArgs) -> JobSpec {
    let output_dir = submit.output_dir;

    let r#type = match submit.job {
        SubmitJobCommand::Process { argv } => {
            let command = argv[0].clone();
            let args = argv[1..].to_vec();
            job_spec::Type::Process(ProcessSpec { command, args })
        }
        SubmitJobCommand::Python { argv } => {
            let entry_point = argv[0].clone();
            let args = argv[1..].to_vec();
            job_spec::Type::Python(PythonSpec { entry_point, args })
        }
        SubmitJobCommand::Docker { image, args } => job_spec::Type::Docker(DockerSpec {
            image,
            command: vec![],
            args,
        }),
    };

    JobSpec {
        output_dir,
        r#type: Some(r#type),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use burst_core::proto::job_spec;
    use clap::Parser;

    use super::{Cli, Commands, SubmitArgs, SubmitJobCommand, build_job_spec};

    #[test]
    fn parses_submit_process_command() {
        let cli = Cli::parse_from([
            "burst-cli",
            "--config",
            "my-config.json",
            "submit",
            "--output-dir",
            "/tmp/out",
            "process",
            "echo",
            "hello",
        ]);

        assert_eq!(cli.config, "my-config.json");
        match cli.command {
            Commands::Submit(SubmitArgs {
                output_dir,
                job: SubmitJobCommand::Process { argv },
            }) => {
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

    #[test]
    fn parses_submit_python_command() {
        let cli = Cli::parse_from(["burst-cli", "submit", "python", "-c", "print('hi')"]);

        match cli.command {
            Commands::Submit(SubmitArgs {
                output_dir,
                job: SubmitJobCommand::Python { argv },
            }) => {
                assert_eq!(output_dir, None);
                assert_eq!(argv, vec!["-c".to_string(), "print('hi')".to_string()]);
            }
            _ => panic!("expected python submit command"),
        }
    }

    #[test]
    fn parses_submit_docker_command() {
        let cli = Cli::parse_from([
            "burst-cli",
            "submit",
            "docker",
            "alpine:3.20",
            "echo",
            "hello",
        ]);

        match cli.command {
            Commands::Submit(SubmitArgs {
                output_dir,
                job: SubmitJobCommand::Docker { image, args },
            }) => {
                assert_eq!(output_dir, None);
                assert_eq!(image, "alpine:3.20");
                assert_eq!(args, vec!["echo".to_string(), "hello".to_string()]);
            }
            _ => panic!("expected docker submit command"),
        }
    }

    #[test]
    fn build_job_spec_process_type() {
        let spec = build_job_spec(SubmitArgs {
            output_dir: Some("/tmp/out".to_string()),
            job: SubmitJobCommand::Process {
                argv: vec!["echo".to_string(), "hello".to_string()],
            },
        });

        assert_eq!(spec.output_dir, Some("/tmp/out".to_string()));
        match spec.r#type {
            Some(job_spec::Type::Process(process)) => {
                assert_eq!(process.command, "echo");
                assert_eq!(process.args, vec!["hello".to_string()]);
            }
            _ => panic!("expected process type"),
        }
    }

    #[test]
    fn build_job_spec_python_type() {
        let spec = build_job_spec(SubmitArgs {
            output_dir: None,
            job: SubmitJobCommand::Python {
                argv: vec!["-c".to_string(), "print('ok')".to_string()],
            },
        });

        match spec.r#type {
            Some(job_spec::Type::Python(python)) => {
                assert_eq!(python.entry_point, "-c");
                assert_eq!(python.args, vec!["print('ok')".to_string()]);
            }
            _ => panic!("expected python type"),
        }
    }

    #[test]
    fn build_job_spec_docker_type() {
        let spec = build_job_spec(SubmitArgs {
            output_dir: None,
            job: SubmitJobCommand::Docker {
                image: "alpine:3.20".to_string(),
                args: vec!["echo".to_string(), "hello".to_string()],
            },
        });

        match spec.r#type {
            Some(job_spec::Type::Docker(docker)) => {
                assert_eq!(docker.image, "alpine:3.20");
                assert!(docker.command.is_empty());
                assert_eq!(docker.args, vec!["echo".to_string(), "hello".to_string()]);
            }
            _ => panic!("expected docker type"),
        }
    }
}
