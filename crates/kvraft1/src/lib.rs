// Skeleton code: some placeholders are unused until you fill them in.
#![allow(unused_variables, unused_imports, dead_code)]

//! 6.5840 Lab 4: Fault-tolerant Key/Value service built on Raft.
//!
//! Part A: implement the replicated-state-machine package [`rsm`]
//! (agnostic of the requests it replicates).  Part B: implement the
//! replicated key/value service in [`server`] and [`client`].  Part C:
//! snapshots (via Raft's snapshot support in your Lab 3 code).

pub mod client;
pub mod rsm;
pub mod server;

/// The RPC types shared with Lab 2 (Go: `6.5840/kvsrv1/rpc`).
pub mod rpc {
    pub use kvsrv1_rpc::*;
}

/// Group id used by the tests (Go: kvraft.Gid).
pub const GID: i64 = tester::GRP0;
