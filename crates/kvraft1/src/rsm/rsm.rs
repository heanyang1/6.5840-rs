//! The replicated state machine layer (port of Go's kvraft1/rsm/rsm.go).
//!
//! In the common situation of a client/server service using Raft for
//! replication, the service interacts with Raft in two ways: the service
//! leader submits client operations by calling raft.start(), and all
//! service replicas receive committed operations via Raft's apply
//! channel, which they execute.  This package encapsulates that
//! interaction.

use crate::rpc;
use raft1::api::{ApplyMsg, Cmd};
use raft1::raft::Raft;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tester::Persister;

/// A server (i.e., kvraft1::server) that wants to replicate itself calls
/// make_rsm and must implement the StateMachine trait.  This trait allows
/// the rsm package to interact with the server for server-specific
/// operations: the server must implement do_op to execute an operation
/// (e.g., a Get or Put request), and snapshot/restore to snapshot and
/// restore the server's state.
pub trait StateMachine<Req, Rep>: Send + Sync {
    fn do_op(&self, req: Req) -> Rep;
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&self, data: &[u8]);
}

/// The Op structure wraps each client operation with a unique identifier,
/// so that the reader thread can match apply-channel messages with waiting
/// calls to submit().  Field names/contents are up to you (in Go:
/// `Op{Me, Id, Req}`); it must be serializable because it travels through
/// the Raft log and snapshots.
//
// NOTE: keep the struct itself generic without the Cmd bound (the derive
// needs the plain `Req: Deserialize` bound it generates); the Cmd bound is
// enforced where Op is used (Rsm, Raft).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Op<Req> {
    // Your definitions here.
    #[allow(dead_code)]
    pub req: Req,
}

/// A server that wants to be replicated.  `Req` is the request type (what
/// the service passes to submit()) and `Rep` the reply type (what do_op
/// returns).
pub struct Rsm<Req: Cmd, Rep: Cmd> {
    mu: Mutex<RsmState<Req, Rep>>,
    me: usize,
    rf: Arc<Raft<Op<Req>>>,
    maxraftstate: i64, // snapshot if log grows this big
    sm: Arc<dyn StateMachine<Req, Rep>>,
}

struct RsmState<Req: Cmd, Rep: Cmd> {
    // Your definitions here.
    //
    // Suggestions: a unique-id counter for ops, a map from (client, seq)
    // to a place to deliver the result of executing an op, and the last
    // applied index.
    #[allow(dead_code)]
    marker: std::marker::PhantomData<(Req, Rep)>,
}

/// servers[] contains the ends of the set of servers that will cooperate
/// via Raft to form the fault-tolerant key/value service.  `me` is the
/// index of the current server in servers[].  The Rsm should snapshot when
/// Raft's saved state exceeds maxraftstate bytes, in order to allow Raft
/// to garbage-collect its log.  If maxraftstate is -1, you don't need to
/// snapshot.  make_rsm() must return quickly, so it should start threads
/// for any long-running work.
pub fn make_rsm<Req, Rep>(
    servers: Vec<Arc<labrpc::ClientEnd>>,
    me: usize,
    persister: Arc<Persister>,
    maxraftstate: i64,
    sm: Arc<dyn StateMachine<Req, Rep>>,
) -> Arc<Rsm<Req, Rep>>
where
    Req: Cmd,
    Rep: Cmd,
{
    let (tx, rx) = crossbeam_channel::unbounded::<ApplyMsg<Op<Req>>>();
    let rf = raft1::raft::make(servers, me, persister.clone(), tx);
    let rsm = Arc::new(Rsm {
        mu: Mutex::new(RsmState {
            marker: std::marker::PhantomData,
        }),
        me,
        rf,
        maxraftstate,
        sm,
    });

    // Your code here: start the reader thread that consumes `rx`, executes
    // committed ops via sm.do_op(), and hands results back to waiting
    // submit() calls.  Also (4C) check rf.persist_bytes() against
    // maxraftstate and call rf.snapshot() when the log grows too large.

    // On restart, restore the server state from the persisted snapshot:
    let snap = persister.read_snapshot();
    if !snap.is_empty() {
        // Your code here (4C): decode the snapshot; it contains the
        // service state and the index it corresponds to.
    }

    // Keep the receiver alive (the reader thread should own it).
    std::mem::forget(rx);

    rsm
}

impl<Req, Rep> Rsm<Req, Rep>
where
    Req: Cmd,
    Rep: Cmd,
{
    /// The underlying Raft instance (used by the tester to start/stop it).
    pub fn raft(&self) -> Arc<Raft<Op<Req>>> {
        Arc::clone(&self.rf)
    }

    /// Submit a command to Raft, and wait for it to be committed.  It
    /// should return ErrWrongLeader if the client should find a new leader
    /// and try again, or (rpc::OK, Some(rep)) with the result of executing
    /// the operation.
    ///
    /// Submit creates an Op structure to run a command through Raft; for
    /// example `Op { me, id, req }`, where req is the argument to submit()
    /// and id is a unique id for the op.
    pub fn submit(&self, _req: Req) -> (rpc::Err, Option<Rep>) {
        // Your code here (4A).
        //
        // 1. wrap req in an Op with a unique id,
        // 2. call rf.start(op); if this peer isn't the leader, return
        //    (rpc::ERR_WRONG_LEADER, None),
        // 3. wait for the reader thread to deliver the result of executing
        //    this op (or detect lost leadership, e.g. because the term
        //    changed or a different op was applied at your index).

        (rpc::ERR_WRONG_LEADER.to_string(), None) // i'm dead, try another server.
    }
}

// The map type you'll likely want for matching applied ops to waiters:
#[allow(dead_code)]
type Waiters<Rep> = HashMap<u64, crossbeam_channel::Sender<Rep>>;
