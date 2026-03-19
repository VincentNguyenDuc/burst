//! Controller service for burst.
//!
//! Responsibilities in current POC:
//!
//! - accept job submissions from CLI via gRPC
//! - track in-memory job/worker state
//! - apply pluggable scheduler strategy (default: FIFO)
//! - lease jobs to workers and update terminal job state on report
//!
//! Configuration:
//!
//! - `--config <path>` (default `burst.config.json`)

mod domain;
mod router;
mod service;

use std::io::IsTerminal;
use std::net::SocketAddr;

use burst_core::config::BurstConfig;
use burst_core::proto::controller_rpc_server::ControllerRpcServer;
use router::{FifoFactory, PowerOfTwoFactory, RouterRegistry};
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

fn read_config_path() -> String {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            if let Some(path) = args.next() {
                return path;
            }
        }
    }
    "burst.config.json".to_string()
}

#[tokio::main]
async fn main() {
    let use_ansi = std::io::stderr().is_terminal();
    tracing_subscriber::fmt()
        .with_ansi(use_ansi)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config_path = read_config_path();
    let config = match BurstConfig::load_from_path(&config_path) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(path = config_path, error = %error, "failed to load config");
            std::process::exit(2);
        }
    };

    let bind_addr = config.controller.bind_addr.clone();

    let mut registry = RouterRegistry::new();
    registry.register(FifoFactory);
    registry.register(PowerOfTwoFactory);

    let strategy_name = config.controller.router.clone();
    let available = registry.available();
    let scheduler = registry.build(&strategy_name);

    match scheduler {
        Some(selected) => {
            let address: SocketAddr = match bind_addr.parse() {
                Ok(address) => address,
                Err(error) => {
                    tracing::error!(bind = bind_addr, error = %error, "invalid bind address");
                    std::process::exit(2);
                }
            };

            let service = service::ControllerService::new(selected);

            tracing::info!(scheduler = strategy_name, bind = %address, "controller started");

            if let Err(error) = Server::builder()
                .add_service(ControllerRpcServer::new(service))
                .serve(address)
                .await
            {
                tracing::error!(error = %error, "controller server failed");
                std::process::exit(1);
            }
        }
        None => {
            tracing::error!(
                requested = strategy_name,
                available = ?available,
                "requested scheduler is not registered"
            );
            std::process::exit(2);
        }
    }
}
