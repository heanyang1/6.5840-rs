//! Client code to talk to the sharded key/value service (port of Go's
//! shardkv1/client.go).
//!
//! The client uses the shardctrler to query for the current configuration
//! and find the assignment of shards (keys) to groups, and then talks to
//! the group that holds the key's shard.

use crate::rpc;
use crate::shardcfg;
use crate::shardctrler;
use crate::shardgrp;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tester::kvtest::IKVClerk;
use tester::Clnt;

pub struct Clerk {
    #[allow(dead_code)]
    clnt: Arc<Clnt>,
    #[allow(dead_code)]
    sck: Arc<shardctrler::ShardCtrler>,
    rcks: Mutex<HashMap<i64, Arc<shardgrp::client::Clerk>>>,
    // You will have to modify this struct.
}

/// The tester calls make_clerk and passes in a shardctrler so that the
/// client can call its query() method.
pub fn make_clerk(clnt: Arc<Clnt>, sck: Arc<shardctrler::ShardCtrler>) -> Arc<Clerk> {
    Arc::new(Clerk {
        clnt,
        sck,
        rcks: Mutex::new(HashMap::new()),
    })
}

impl Clerk {
    /// The cached shardgrp clerk for `gid`, if we've made one (Go:
    /// GetClerk; used by the tester's shutdownLeader).
    pub fn get_clerk(&self, gid: i64) -> Option<Arc<shardgrp::client::Clerk>> {
        self.rcks.lock().unwrap().get(&gid).cloned()
    }

    /// Get a key from a shardgrp.  You can use shardcfg::key2shard(key) to
    /// find the shard responsible for the key and self.sck.query() to read
    /// the current configuration and lookup the servers in the group
    /// responsible for the key.  You can make a clerk for that group by
    /// calling shardgrp::client::make_clerk(self.clnt.clone(), servers).
    pub fn get(&self, _key: &str) -> (String, rpc::Tversion, rpc::Err) {
        // You will have to modify this function.
        (String::new(), 0, String::new())
    }

    /// Put a key to a shard group.  Must return ErrMaybe when the reply
    /// was maybe lost; the inner shardgrp put signals this with an error.
    pub fn put(&self, _key: &str, _value: &str, _version: rpc::Tversion) -> rpc::Err {
        // You will have to modify this function.
        String::new()
    }

    /// Helper (recommended): get-or-create the cached clerk for the group
    /// serving `sh` in `cfg`.
    #[allow(dead_code)]
    fn clerk_for(
        &self,
        _cfg: &shardcfg::ShardConfig,
        _sh: shardcfg::Tshid,
    ) -> Option<Arc<shardgrp::client::Clerk>> {
        // Your code here.
        None
    }
}

impl IKVClerk for Clerk {
    fn get(&self, key: &str) -> (String, rpc::Tversion, rpc::Err) {
        Clerk::get(self, key)
    }
    fn put(&self, key: &str, value: &str, version: rpc::Tversion) -> rpc::Err {
        Clerk::put(self, key, value, version)
    }
}
