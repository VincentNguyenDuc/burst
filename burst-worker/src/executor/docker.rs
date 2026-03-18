use std::pin::Pin;

use crate::executor::{ExecutionContext, JobExecutor, run_command_with_capture};

use burst_core::proto::DockerSpec;

pub struct DockerExecutor {
    pub spec: DockerSpec,
}

impl JobExecutor for DockerExecutor {
    fn execute(
        self: Box<Self>,
        context: ExecutionContext,
    ) -> Pin<Box<dyn Future<Output = (i32, String)> + Send>> {
        Box::pin(async move {
            if self.spec.image.trim().is_empty() {
                return (-1, "docker image cannot be empty".to_string());
            }

            let mut command = tokio::process::Command::new("docker");
            command.arg("run");
            command.arg("--rm");
            command.arg(self.spec.image);
            if !self.spec.command.is_empty() {
                command.args(self.spec.command);
            }
            command.args(self.spec.args);
            run_command_with_capture(command, context).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::DockerExecutor;
    use crate::executor::{ExecutionContext, JobExecutor};
    use burst_core::proto::DockerSpec;
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
        std::env::temp_dir().join(format!("burst-worker-docker-executor-{name}-{nanos}"))
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
    async fn docker_executor_rejects_empty_image() {
        let (output_dir, context) = temp_context("empty-image").await;

        let result = Box::new(DockerExecutor {
            spec: DockerSpec {
                image: "".to_string(),
                command: vec![],
                args: vec![],
            },
        })
        .execute(context)
        .await;

        assert_eq!(result.0, -1);
        assert_eq!(result.1, "docker image cannot be empty");

        let _ = fs::remove_dir_all(output_dir);
    }
}
