// Skeleton code: some placeholders are unused until you fill them in.
#![allow(unused_variables, unused_imports, dead_code)]

//! 6.5840 Lab 5: Sharded Key/Value service.
//!
//! [`shardcfg`] describes configurations (provided); [`shardctrler`] is
//! the controller you implement; [`shardgrp`] contains the shardgrp
//! server and clerk; [`client`] is the sharded-service Clerk.

pub mod client;
pub mod shardcfg;
pub mod shardctrler;
pub mod shardgrp;

/// The RPC types shared with Lab 2 (Go: `6.5840/kvsrv1/rpc`).
pub mod rpc {
    pub use kvsrv1_rpc::*;
}
