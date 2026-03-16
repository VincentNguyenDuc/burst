//! Shared API crate for burst control-plane communication.
//!
//! This crate exposes protobuf-generated message types and gRPC client/server
//! stubs used by:
//!
//! - `burst-controller`
//! - `burst-worker`
//! - `burst-cli`

pub mod config;

/// Protobuf-generated API for `burst.v1`.
pub mod proto {
    tonic::include_proto!("burst.v1");
}
