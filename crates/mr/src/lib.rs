// Skeleton code: some placeholders are unused until you fill them in.
#![allow(unused_variables, unused_imports, dead_code)]

//! 6.5840 Lab 1: MapReduce.
//!
//! You implement the distributed MapReduce in [`coordinator`] and
//! [`worker`]; the RPC argument/reply types live in [`rpc`], and [`net`]
//! provides the Unix-socket RPC plumbing (the Rust analog of Go's
//! `net/rpc` over a unix socket).
//!
//! Applications ("plugins" in Go) are ordinary functions registered in
//! [`apps`]; the `mr-worker` binary selects one by name.

pub mod apps;
pub mod coordinator;
pub mod net;
pub mod rpc;
pub mod worker;

/// Map functions return a slice of key/value pairs.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}
