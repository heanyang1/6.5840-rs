//! The Raft service interface (port of Go's raftapi package).

use serde::de::DeserializeOwned;
use serde::Serialize;

/// A command that can travel through the Raft log: the lab's tests use
/// plain integers; the key/value labs use their own operation enums.
pub trait Cmd:
    Clone + Send + Sync + Serialize + DeserializeOwned + PartialEq + std::fmt::Debug + 'static
{
}
impl<T> Cmd for T where
    T: Clone + Send + Sync + Serialize + DeserializeOwned + PartialEq + std::fmt::Debug + 'static
{
}

/// As each Raft peer becomes aware that successive log entries are
/// committed, the peer should send an ApplyMsg to the server (or tester),
/// via the apply channel passed to make().  Set `command_valid` to true to
/// indicate that the ApplyMsg contains a newly committed log entry.
///
/// You'll find the Snapshot fields useful later in the lab.  Exactly one
/// of `command_valid` and `snapshot_valid` should be true.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ApplyMsg<C> {
    pub command_valid: bool,
    pub command: Option<C>,
    pub command_index: usize,

    pub snapshot_valid: bool,
    pub snapshot: Vec<u8>,
    pub snapshot_term: i64,
    pub snapshot_index: usize,
}

impl<C: Cmd> ApplyMsg<C> {
    /// A message carrying a newly committed command at `command_index`.
    pub fn command(cmd: C, command_index: usize) -> ApplyMsg<C> {
        ApplyMsg {
            command_valid: true,
            command: Some(cmd),
            command_index,
            snapshot_valid: false,
            snapshot: Vec::new(),
            snapshot_term: 0,
            snapshot_index: 0,
        }
    }

    /// A message carrying a snapshot that replaces the service's state up
    /// to (and including) `snapshot_index`.
    pub fn snapshot_msg(snapshot: Vec<u8>, snapshot_term: i64, snapshot_index: usize) -> ApplyMsg<C> {
        ApplyMsg {
            command_valid: false,
            command: None,
            command_index: 0,
            snapshot_valid: true,
            snapshot,
            snapshot_term,
            snapshot_index,
        }
    }
}
