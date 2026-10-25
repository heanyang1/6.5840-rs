//! The shardgrp clerk, used by the controller to move shards and by the
//! sharded-service Clerk to talk to one group (port of Go's
//! shardkv1/shardgrp/client.go).

use crate::rpc;
use crate::shardcfg;
use crate::shardgrp::shardrpc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tester::Clnt;

pub struct Clerk {
    clnt: Arc<Clnt>,
    servers: Vec<String>,
    leader: AtomicUsize, // last successful leader (index into servers[])
    // You can add to this struct.
}

/// Make a Clerk for the shardgrp at `servers`.
pub fn make_clerk(clnt: Arc<Clnt>, servers: Vec<String>) -> Arc<Clerk> {
    Arc::new(Clerk {
        clnt,
        servers,
        leader: AtomicUsize::new(0),
    })
}

impl Clerk {
    pub fn leader(&self) -> usize {
        self.leader.load(Ordering::SeqCst)
    }

    pub fn get(&self, _key: &str) -> (String, rpc::Tversion, rpc::Err) {
        // Your code here
        (String::new(), 0, String::new())
    }

    pub fn put(&self, _key: &str, _value: &str, _version: rpc::Tversion) -> rpc::Err {
        // Your code here
        String::new()
    }

    /// Freeze shard `s` as of config number `num`; returns the shard's
    /// state.
    pub fn freeze_shard(&self, _s: shardcfg::Tshid, _num: shardcfg::Tnum) -> (Vec<u8>, rpc::Err) {
        // Your code here
        (Vec::new(), String::new())
    }

    pub fn install_shard(
        &self,
        _s: shardcfg::Tshid,
        _state: &[u8],
        _num: shardcfg::Tnum,
    ) -> rpc::Err {
        // Your code here
        String::new()
    }

    pub fn delete_shard(&self, _s: shardcfg::Tshid, _num: shardcfg::Tnum) -> rpc::Err {
        // Your code here
        String::new()
    }
}
