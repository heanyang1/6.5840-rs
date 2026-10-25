// Skeleton code: some placeholders are unused until you fill them in.
#![allow(unused_variables, unused_imports, dead_code)]

//! 6.5840 Lab 2: Key/Value server.
//!
//! You will modify [`server`] (the server) and [`client`] (the Clerk);
//! [`lock`] implements a distributed lock on top of your Clerk.  The RPC
//! types are in the [`rpc`] module (re-exported from the `kvsrv1-rpc`
//! crate, the analog of Go's `kvsrv1/rpc` package).

pub mod client;
pub mod lock;
pub mod server;

/// The RPC types, shared with the later labs (Go: `6.5840/kvsrv1/rpc`).
pub mod rpc {
    pub use kvsrv1_rpc::*;
}
