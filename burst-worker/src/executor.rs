mod docker;
mod process;
mod python;

pub use docker::DockerExecutor;
pub use process::ProcessExecutor;
pub use python::PythonExecutor;

use burst_core::proto::job_spec;
use std::{future::Future, path::PathBuf, pin::Pin, process::Stdio};
use tokio::io;

pub async fn execute_job(job: burst_core::proto::AssignedJob) -> (i32, String) {
    let Some(spec) = job.spec else {
        return (-1, "missing job spec".to_string());
    };

    let output_dir: PathBuf = match spec.output_dir.as_deref() {
        Some(dir) if !dir.trim().is_empty() => PathBuf::from(dir),
        _ => match std::env::current_dir() {
            Ok(dir) => dir,
            Err(error) => return (-1, format!("failed to resolve current dir: {error}")),
        },
    };

    if let Err(error) = tokio::fs::create_dir_all(&output_dir).await {
        return (
            -1,
            format!(
                "failed to create output_dir '{}': {error}",
                output_dir.display()
            ),
        );
    }

    let stdout_path = output_dir.join(format!("{}.stdout", job.job_id));
    let stderr_path = output_dir.join(format!("{}.stderr", job.job_id));

    let stdout_file = match tokio::fs::File::create(&stdout_path).await {
        Ok(f) => f,
        Err(error) => {
            return (
                -1,
                format!(
                    "failed to create stdout file '{}': {error}",
                    stdout_path.display()
                ),
            );
        }
    };
    let stderr_file = match tokio::fs::File::create(&stderr_path).await {
        Ok(f) => f,
        Err(error) => {
            return (
                -1,
                format!(
                    "failed to create stderr file '{}': {error}",
                    stderr_path.display()
                ),
            );
        }
    };

    tracing::info!(
        job_id = job.job_id,
        stdout = %stdout_path.display(),
        stderr = %stderr_path.display(),
        "capturing job output"
    );

    let context = ExecutionContext {
        stdout_file,
        stderr_file,
    };

    let executor = match resolve_executor(spec) {
        Ok(executor) => executor,
        Err(error) => return (-1, error),
    };

    executor.execute(context).await
}

struct ExecutionContext {
    stdout_file: tokio::fs::File,
    stderr_file: tokio::fs::File,
}

trait JobExecutor {
    fn execute(
        self: Box<Self>,
        context: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = (i32, String)> + Send>>;
}

fn resolve_executor(
    spec: burst_core::proto::JobSpec,
) -> Result<Box<dyn JobExecutor + Send>, String> {
    match spec.r#type {
        Some(job_spec::Type::Process(process)) => Ok(Box::new(ProcessExecutor { spec: process })),
        Some(job_spec::Type::Python(python)) => Ok(Box::new(PythonExecutor { spec: python })),
        Some(job_spec::Type::Docker(docker)) => Ok(Box::new(DockerExecutor { spec: docker })),
        None => Err("missing job type".to_string()),
    }
}

async fn run_command_with_capture(
    mut command: tokio::process::Command,
    context: ExecutionContext,
) -> (i32, String) {
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return (-1, error.to_string()),
    };

    let mut child_stdout = match child.stdout.take() {
        Some(s) => s,
        None => return (-1, "failed to capture child stdout".to_string()),
    };
    let mut child_stderr = match child.stderr.take() {
        Some(s) => s,
        None => return (-1, "failed to capture child stderr".to_string()),
    };

    let stdout_task = tokio::spawn(async move {
        let mut out = context.stdout_file;
        io::copy(&mut child_stdout, &mut out).await
    });
    let stderr_task = tokio::spawn(async move {
        let mut err = context.stderr_file;
        io::copy(&mut child_stderr, &mut err).await
    });

    let status = match child.wait().await {
        Ok(status) => status,
        Err(error) => return (-1, error.to_string()),
    };

    let stdout_copied = stdout_task
        .await
        .map_err(|e| e.to_string())
        .and_then(|r| r.map_err(|e| e.to_string()));
    if let Err(error) = stdout_copied {
        return (-1, format!("failed to write stdout: {error}"));
    }

    let stderr_copied = stderr_task
        .await
        .map_err(|e| e.to_string())
        .and_then(|r| r.map_err(|e| e.to_string()));
    if let Err(error) = stderr_copied {
        return (-1, format!("failed to write stderr: {error}"));
    }

    let code = status.code().unwrap_or(1);
    (code, String::new())
}
