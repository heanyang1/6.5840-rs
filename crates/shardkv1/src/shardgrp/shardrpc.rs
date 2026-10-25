//! RPCs used by the controller to move shards between shardgrps (port of
//! Go's shardkv1/shardgrp/shardrpc; provided code, please don't change).

use crate::rpc;
use crate::shardcfg::{Tnum, Tshid};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FreezeShardArgs {
    pub shard: Tshid,
    pub num: Tnum,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FreezeShardReply {
    pub state: Vec<u8>,
    pub num: Tnum,
    pub err: rpc::Err,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InstallShardArgs {
    pub shard: Tshid,
    pub state: Vec<u8>,
    pub num: Tnum,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InstallShardReply {
    pub err: rpc::Err,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeleteShardArgs {
    pub shard: Tshid,
    pub num: Tnum,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeleteShardReply {
    pub err: rpc::Err,
}
