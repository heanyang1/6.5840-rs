//! The shard controller: stores configurations in a Lab 2 kvsrv, and
//! moves shards between shardgrps (port of Go's
//! shardkv1/shardctrler/shardctrler.go).

use crate::shardcfg::{self, ShardConfig};
use std::sync::Arc;
use tester::kvtest::IKVClerk;
use tester::Clnt;

/// ShardCtrler for the controller and the kv clerk.
pub struct ShardCtrler {
    #[allow(dead_code)]
    clnt: Arc<Clnt>,
    /// The Lab 2 clerk used to store configurations (Go embeds an
    /// IKVClerk; call `self.kv.get(...)`/`self.kv.put(...)`).
    #[allow(dead_code)]
    kv: Arc<dyn IKVClerk>,

    // Your data here.
}

/// Make a ShardCtrler, which stores its state in a kvsrv.
pub fn make_shard_ctrler(clnt: Arc<Clnt>) -> Arc<ShardCtrler> {
    let srv = tester::server_name(tester::GRP0, 0);
    let kv = kvsrv1::client::make_clerk(Arc::clone(&clnt), &srv);
    let sck = Arc::new(ShardCtrler { clnt, kv });
    // Your code here.
    sck
}

impl ShardCtrler {
    /// The tester calls init_controller() before starting a new
    /// controller.  In part A, this method doesn't need to do anything.
    /// In B and C, this method implements recovery.
    pub fn init_controller(&self) {
        // Your code here (Parts B and C).
    }

    /// Called once by the tester to supply the first configuration.  You
    /// can serialize a ShardConfig into a string using
    /// `cfg.to_string_json()`, and then Put it in the kvsrv for the
    /// controller at version 0.  You can pick the key naming the
    /// configuration.  The initial configuration lists shardgrp
    /// shardcfg::GID1 for all shards.
    pub fn init_config(&self, _cfg: &ShardConfig) {
        // Your code here.
    }

    /// Called by the tester to ask the controller to change the
    /// configuration from the current one to `new`.  While the controller
    /// changes the configuration it may be superseded by another
    /// controller.
    pub fn change_config_to(&self, _new: &ShardConfig) {
        // Your code here.
    }

    /// Return the current configuration.
    pub fn query(&self) -> ShardConfig {
        // Your code here.
        shardcfg::make_shard_config()
    }
}
