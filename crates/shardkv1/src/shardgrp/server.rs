//! A single shardgrp server, replicated with the rsm package from Lab 4
//! (port of Go's shardkv1/shardgrp/server.go).
//!
//! Like kvraft1::server, the state machine (`ShardKvSm`) and the RPC
//! handle (`KVServer`) are split into two objects sharing the rsm.

use crate::rpc;
use crate::shardcfg;
use crate::shardgrp::shardrpc;
use kvraft1::rsm::rsm::{make_rsm, Rsm, StateMachine};
use raft1::api::Cmd;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tester::Persister;

/// The ops that travel through the rsm/Raft (Go passes `any`; here an
/// enum).  Add fields as needed (client/sequence ids for duplicate
/// detection, config numbers, ...).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Op {
    Get(rpc::GetArgs),
    Put(rpc::PutArgs),
    FreezeShard(shardrpc::FreezeShardArgs),
    InstallShard(shardrpc::InstallShardArgs),
    DeleteShard(shardrpc::DeleteShardArgs),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OpReply {
    Get(rpc::GetReply),
    Put(rpc::PutReply),
    FreezeShard(shardrpc::FreezeShardReply),
    InstallShard(shardrpc::InstallShardReply),
    DeleteShard(shardrpc::DeleteShardReply),
}


/// The replicated state machine of this shardgrp: the key/value maps of
/// the shards it holds, plus whatever state you need for frozen shards and
/// configuration tracking (Go: the KVServer's fields).
pub struct ShardKvSm {
    #[allow(dead_code)]
    gid: i64,
    #[allow(dead_code)]
    me: usize,

    // Your definitions here.
    #[allow(dead_code)]
    db: Mutex<HashMap<String, (String, rpc::Tversion)>>,
}

impl StateMachine<Op, OpReply> for ShardKvSm {
    fn do_op(&self, _req: Op) -> OpReply {
        // Your code here.
        OpReply::Get(rpc::GetReply::new("", 0, rpc::ERR_NO_KEY))
    }

    fn snapshot(&self) -> Vec<u8> {
        // Your code here.
        Vec::new()
    }

    fn restore(&self, _data: &[u8]) {
        // Your code here.
    }
}

/// The RPC-facing handle for one shardgrp server.
pub struct KVServer {
    #[allow(dead_code)]
    me: usize,
    #[allow(dead_code)]
    gid: i64,
    rsm: Arc<Rsm<Op, OpReply>>,
}

impl KVServer {
    pub fn get(&self, _args: rpc::GetArgs) -> rpc::GetReply {
        // Your code here.
        rpc::GetReply::new("", 0, rpc::ERR_NO_KEY)
    }

    pub fn put(&self, _args: rpc::PutArgs) -> rpc::PutReply {
        // Your code here.
        rpc::PutReply::new(rpc::ERR_NO_KEY)
    }

    /// Freeze the specified shard (i.e., reject future Get/Puts for this
    /// shard) and return the key/values stored in that shard.
    pub fn freeze_shard(&self, _args: shardrpc::FreezeShardArgs) -> shardrpc::FreezeShardReply {
        // Your code here.
        shardrpc::FreezeShardReply { state: Vec::new(), num: 0, err: rpc::ERR_NO_KEY.to_string() }
    }

    /// Install the supplied state for the specified shard.
    pub fn install_shard(&self, _args: shardrpc::InstallShardArgs) -> shardrpc::InstallShardReply {
        // Your code here.
        shardrpc::InstallShardReply { err: rpc::ERR_NO_KEY.to_string() }
    }

    /// Delete the specified shard.
    pub fn delete_shard(&self, _args: shardrpc::DeleteShardArgs) -> shardrpc::DeleteShardReply {
        // Your code here.
        shardrpc::DeleteShardReply { err: rpc::ERR_NO_KEY.to_string() }
    }
}

/// Provided: build the tester's server box for one shardgrp server (Go:
/// StartServerShardGrp + NewServer).
///
/// StartServerShardGrp() and make_rsm() must return quickly, so they
/// should start threads for any long-running work.
pub fn start_server_shard_grp(
    args: tester::ServerArgs,
    maxraftstate: i64,
) -> (Arc<KVServer>, tester::ServerBox) {
    let me = args.srv;
    let gid = args.gid;

    let sm = Arc::new(ShardKvSm {
        gid,
        me,
        db: Mutex::new(HashMap::new()),
    });
    let rsm = make_rsm(args.ends.clone(), me, Arc::clone(&args.persister), maxraftstate, sm);
    let kv = Arc::new(KVServer { me, gid, rsm: Arc::clone(&rsm) });

    // Your code here (e.g., if gid == shardcfg::GID1, this group starts
    // out owning all shards).

    let kv2 = Arc::clone(&kv);
    let box_ = tester::ServerBox::new()
        .with_service(kv_service(&kv))
        .with_service(raft1::raft::raft_service(&rsm.raft()))
        .with_kill(move || kv2.rsm.raft().kill());
    (kv, box_)
}

/// Provided: build the "KVServer" RPC service for this shardgrp server.
pub fn kv_service(kv: &Arc<KVServer>) -> labrpc::Service {
    let svc = labrpc::Service::new("KVServer");
    {
        let kv = Arc::clone(kv);
        svc.handle("Get", move |args: rpc::GetArgs| kv.get(args));
    }
    {
        let kv = Arc::clone(kv);
        svc.handle("Put", move |args: rpc::PutArgs| kv.put(args));
    }
    {
        let kv = Arc::clone(kv);
        svc.handle("FreezeShard", move |args: shardrpc::FreezeShardArgs| {
            kv.freeze_shard(args)
        });
    }
    {
        let kv = Arc::clone(kv);
        svc.handle("InstallShard", move |args: shardrpc::InstallShardArgs| {
            kv.install_shard(args)
        });
    }
    {
        let kv = Arc::clone(kv);
        svc.handle("DeleteShard", move |args: shardrpc::DeleteShardArgs| {
            kv.delete_shard(args)
        });
    }
    svc
}

/// Keep the shardcfg import used by student code suggestions.
#[allow(dead_code)]
fn _s() -> i64 {
    shardcfg::GID1
}
