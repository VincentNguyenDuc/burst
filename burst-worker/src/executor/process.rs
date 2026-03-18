use burst_core::proto::ProcessSpec;
use std::pin::Pin;

use crate::executor::{ExecutionContext, JobExecutor, run_command_with_capture};

pub struct ProcessExecutor {
    pub spec: ProcessSpec,
}

impl JobExecutor for ProcessExecutor {
    fn execute(
        self: Box<Self>,
        context: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = (i32, String)> + Send>> {
        Box::pin(async move {
            if self.spec.command.trim().is_empty() {
                return (-1, "process command cannot be empty".to_string());
            }

            let mut command = tokio::process::Command::new(self.spec.command);
            command.args(self.spec.args);
            run_command_with_capture(command, context).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ProcessExecutor;
    use crate::executor::{ExecutionContext, JobExecutor};
    use burst_core::proto::ProcessSpec;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_output_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("burst-worker-process-executor-{name}-{nanos}"))
    }

    async fn temp_context(name: &str) -> (PathBuf, ExecutionContext) {
        let output_dir = temp_output_dir(name);
        tokio::fs::create_dir_all(&output_dir)
            .await
            .expect("failed to create test output dir");

        let stdout_file = tokio::fs::File::create(output_dir.join("stdout.log"))
            .await
            .expect("failed to create stdout file");
        let stderr_file = tokio::fs::File::create(output_dir.join("stderr.log"))
            .await
            .expect("failed to create stderr file");

        (
            output_dir,
            ExecutionContext {
                stdout_file,
                stderr_file,
            },
        )
    }

    #[tokio::test]
    async fn process_executor_rejects_empty_command() {
        let (output_dir, context) = temp_context("empty-command").await;

        let result = Box::new(ProcessExecutor {
            spec: ProcessSpec {
                command: "  ".to_string(),
                args: vec![],
            },
        })
        .execute(context)
        .await;

        assert_eq!(result.0, -1);
        assert_eq!(result.1, "process command cannot be empty");

        let _ = fs::remove_dir_all(output_dir);
    }

    #[tokio::test]
    async fn process_executor_captures_stdout_and_stderr() {
        let (output_dir, context) = temp_context("capture").await;

        let result = Box::new(ProcessExecutor {
            spec: ProcessSpec {
                command: "/bin/sh".to_string(),
                args: vec!["-c".to_string(), "echo out; echo err 1>&2".to_string()],
            },
        })
        .execute(context)
        .await;

        assert_eq!(result.0, 0);
        assert_eq!(result.1, "");

        let stdout = fs::read_to_string(output_dir.join("stdout.log"))
            .expect("stdout should be captured to file");
        let stderr = fs::read_to_string(output_dir.join("stderr.log"))
            .expect("stderr should be captured to file");

        assert!(stdout.contains("out"));
        assert!(stderr.contains("err"));

        let _ = fs::remove_dir_all(output_dir);
    }

    #[tokio::test]
    async fn process_executor_returns_nonzero_exit_code() {
        let (output_dir, context) = temp_context("nonzero").await;

        let result = Box::new(ProcessExecutor {
            spec: ProcessSpec {
                command: "/bin/sh".to_string(),
                args: vec!["-c".to_string(), "exit 9".to_string()],
            },
        })
        .execute(context)
        .await;

        assert_eq!(result.0, 9);
        assert_eq!(result.1, "");

        let _ = fs::remove_dir_all(output_dir);
    }

    #[tokio::test]
    async fn process_executor_reports_spawn_error_for_missing_binary() {
        let (output_dir, context) = temp_context("spawn-error").await;

        let result = Box::new(ProcessExecutor {
            spec: ProcessSpec {
                command: "/definitely/missing/binary".to_string(),
                args: vec![],
            },
        })
        .execute(context)
        .await;

        assert_eq!(result.0, -1);
        assert!(!result.1.is_empty());

        let _ = fs::remove_dir_all(output_dir);
    }
}
