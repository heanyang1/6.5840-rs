//! The replicated key/value server (port of Go's kvraft1/server.go).
//!
//! Go can build cyclic object graphs (the server implements the
//! StateMachine interface *and* holds its Rsm); in Rust we split it into
//! two objects sharing ownership:
//!
//! * `KvSm` — the state machine (the database + any duplicate-detection
//!   state).  An `Arc<KvSm>` is handed to make_rsm(), which calls do_op(),
//!   snapshot() and restore() on it.
//! * `KVServer` — the RPC-facing handle, holding the Rsm.  Its get()/put()
//!   methods submit operations through the Rsm.

use crate::rsm::rsm::{make_rsm, Rsm, StateMachine};
use crate::rpc;
use raft1::api::Cmd;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tester::Persister;

/// The request type submitted through the rsm (Go uses `any`; in Rust an
/// enum plays that role).  You will add fields (e.g. client and sequence
/// ids) as needed for duplicate detection in Part B.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Op {
    Get(rpc::GetArgs),
    Put(rpc::PutArgs),
}

/// The reply type returned by do_op.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OpReply {
    Get(rpc::GetReply),
    Put(rpc::PutReply),
}


/// The replicated state: the key/value database.  (Go: the KVServer's
/// fields; here they live in the state-machine object.)
pub struct KvSm {
    #[allow(dead_code)]
    me: usize,

    // Your definitions here.
    #[allow(dead_code)]
    db: Mutex<HashMap<String, (String, rpc::Tversion)>>,
}

/// To pattern-match req to the right type, use a `match` on the enum
/// variants (the analog of Go's type switches):
///
/// ```ignore
/// match req {
///     Op::Get(args) => ...,
///     Op::Put(args) => ...,
/// }
/// ```
impl StateMachine<Op, OpReply> for KvSm {
    fn do_op(&self, _req: Op) -> OpReply {
        // Your code here.
        OpReply::Get(rpc::GetReply::new("", 0, rpc::ERR_NO_KEY))
    }

    fn snapshot(&self) -> Vec<u8> {
        // Your code here (4C).
        Vec::new()
    }

    fn restore(&self, _data: &[u8]) {
        // Your code here (4C).
    }
}

/// The RPC-facing server handle (Go: the KVServer's Get/Put methods).
pub struct KVServer {
    #[allow(dead_code)]
    me: usize,
    rsm: Arc<Rsm<Op, OpReply>>,
}

impl KVServer {
    pub fn get(&self, _args: rpc::GetArgs) -> rpc::GetReply {
        // Your code here.  Use self.rsm.submit() to submit args; match on
        // the returned OpReply to get a GetReply back.
        rpc::GetReply::new("", 0, rpc::ERR_NO_KEY)
    }

    pub fn put(&self, _args: rpc::PutArgs) -> rpc::PutReply {
        // Your code here.  Use self.rsm.submit() to submit args.
        rpc::PutReply::new(rpc::ERR_NO_KEY)
    }
}

/// Provided: build the tester's server box for one kvraft server (Go:
/// StartKVServer + NewServer).  start_kvserver() and make_rsm() must
/// return quickly, so they should start threads for any long-running work.
///
/// Returns the KVServer handle (used by the tests) and the ServerBox.
pub fn start_kvserver(
    args: tester::ServerArgs,
    maxraftstate: i64,
) -> (Arc<KVServer>, tester::ServerBox) {
    let me = args.srv;
    // call labgob on structures you want serialized: in Rust, just make
    // sure all Op/OpReply variants derive Serialize/Deserialize (they do).

    let sm = Arc::new(KvSm {
        me,
        db: Mutex::new(HashMap::new()),
    });
    let rsm = make_rsm(
        args.ends.clone(),
        me,
        Arc::clone(&args.persister),
        maxraftstate,
        sm,
    );
    let kv = Arc::new(KVServer { me, rsm: Arc::clone(&rsm) });

    let kv2 = Arc::clone(&kv);
    let box_ = tester::ServerBox::new()
        .with_service(kv_service(&kv))
        .with_service(raft1::raft::raft_service(&rsm.raft()))
        .with_kill(move || kv2.rsm.raft().kill());
    (kv, box_)
}

/// Provided: build the "KVServer" RPC service for this server (registers
/// Get and Put handlers).  The tester publishes it under the server's
/// network name.
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
    svc
}

/// Keep the persister import used (students need it in make_rsm calls).
#[allow(dead_code)]
fn _p(_x: &Persister) {}
