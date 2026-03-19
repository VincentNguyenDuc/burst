use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BurstConfig {
    pub controller: ControllerConfig,
    pub workers: Vec<WorkerConfig>,
    pub cli: CliConfig,
}

impl Default for BurstConfig {
    fn default() -> Self {
        Self {
            controller: ControllerConfig::default(),
            workers: vec![WorkerConfig::default()],
            cli: CliConfig::default(),
        }
    }
}

impl BurstConfig {
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, String> {
        let path_ref = path.as_ref();
        let raw = fs::read_to_string(path_ref)
            .map_err(|error| format!("failed to read config '{}': {error}", path_ref.display()))?;

        serde_json::from_str::<Self>(&raw)
            .map_err(|error| format!("failed to parse config '{}': {error}", path_ref.display()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ControllerConfig {
    pub bind_addr: String,
    pub router: String,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:50051".to_string(),
            router: "fifo".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub controller_addr: String,
    pub slots: u32,
    pub poll_interval_ms: u64,
    pub retry_interval_ms: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: "worker-1".to_string(),
            controller_addr: "http://127.0.0.1:50051".to_string(),
            slots: 1,
            poll_interval_ms: 250,
            retry_interval_ms: 500,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CliConfig {
    pub controller_addr: String,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            controller_addr: "http://127.0.0.1:50051".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::BurstConfig;

    fn temp_config_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("burst-{name}-{nanos}.json"))
    }

    #[test]
    fn load_from_path_uses_defaults_for_missing_fields() {
        let path = temp_config_path("defaults");
        fs::write(&path, "{}").expect("failed to write temp config file");

        let config = BurstConfig::load_from_path(&path).expect("config should parse");
        let _ = fs::remove_file(&path);

        assert_eq!(config.controller.router, "fifo");
        assert_eq!(config.workers.len(), 1);
        assert_eq!(config.workers[0].worker_id, "worker-1");
        assert_eq!(config.cli.controller_addr, "http://127.0.0.1:50051");
    }

    #[test]
    fn load_from_path_reads_custom_values() {
        let path = temp_config_path("custom");
        fs::write(
            &path,
            r#"{
  "controller": { "bind_addr": "127.0.0.1:7000", "router": "fifo" },
  "workers": [
    {
      "worker_id": "worker-x",
      "controller_addr": "http://127.0.0.1:7000",
      "slots": 3,
      "poll_interval_ms": 25,
      "retry_interval_ms": 50
    }
  ],
  "cli": { "controller_addr": "http://127.0.0.1:7000" }
}"#,
        )
        .expect("failed to write temp config file");

        let config = BurstConfig::load_from_path(&path).expect("config should parse");
        let _ = fs::remove_file(&path);

        assert_eq!(config.controller.bind_addr, "127.0.0.1:7000");
        assert_eq!(config.workers[0].slots, 3);
        assert_eq!(config.cli.controller_addr, "http://127.0.0.1:7000");
    }

    #[test]
    fn load_from_path_reports_read_error() {
        let path = temp_config_path("missing");
        let error = BurstConfig::load_from_path(&path).expect_err("missing file should fail");

        assert!(error.contains("failed to read config"));
    }
}
