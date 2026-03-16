use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BurstConfig {
    pub controller: ControllerConfig,
    pub worker: WorkerConfig,
    pub cli: CliConfig,
    pub cluster: ClusterConfig,
}

impl Default for BurstConfig {
    fn default() -> Self {
        Self {
            controller: ControllerConfig::default(),
            worker: WorkerConfig::default(),
            cli: CliConfig::default(),
            cluster: ClusterConfig::default(),
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
    pub scheduler: String,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:50051".to_string(),
            scheduler: "fifo".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkerConfig {
    pub controller_addr: String,
    pub default_slots: u32,
    pub poll_interval_ms: u64,
    pub retry_interval_ms: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            controller_addr: "http://127.0.0.1:50051".to_string(),
            default_slots: 1,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub num_workers: u32,
    pub worker_slots: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            num_workers: 2,
            worker_slots: 1,
        }
    }
}
