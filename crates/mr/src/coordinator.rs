//! The MapReduce coordinator, which hands out tasks to workers and copes
//! with failed workers.
//!
//! main/mr-coordinator.rs calls [`make_coordinator`] and then polls
//! [`Coordinator::done`] once per second until it returns true.

use crate::net::RpcServer;
use std::sync::Arc;
use std::sync::Mutex;

pub struct Coordinator {
    // Your definitions here.
    //
    // The coordinator is shared by concurrent RPC handlers, so protect
    // your state with a Mutex (the analog of Go's sync.Mutex).

    pub(crate) state: Mutex<()>, // placeholder: replace with your state
}

/// An example RPC handler.
///
/// The RPC argument and reply types are defined in rpc.rs.
fn example(_c: &Coordinator, args: crate::rpc::ExampleArgs) -> crate::rpc::ExampleReply {
    crate::rpc::ExampleReply { y: args.x + 1 }
}

impl Coordinator {
    // Your code here -- RPC handlers for the worker to call.

    /// Start a thread that listens for RPCs from worker.rs
    /// (Go: Coordinator.server).  Register all your handlers here.
    fn server(self: &Arc<Self>, sockname: &str) {
        let mut srv = RpcServer::new();
        // An example registration.  The handler runs concurrently with
        // other handlers, hence the Arc<Coordinator>.
        let this = Arc::clone(self);
        srv.register("Coordinator.Example", move |args: crate::rpc::ExampleArgs| {
            example(&this, args)
        });
        srv.serve(sockname);
    }

    /// main/mr-coordinator.rs calls done() periodically to find out if the
    /// entire job has finished.
    pub fn done(&self) -> bool {
        let ret = false;

        // Your code here.

        ret
    }
}

/// Create a Coordinator (Go: MakeCoordinator).
///
/// main/mr-coordinator.rs calls this function.  `files` are the input
/// files; each file corresponds to one "split", and is the input to one
/// Map task.  `n_reduce` is the number of reduce tasks to use.
pub fn make_coordinator(
    sockname: &str,
    files: Vec<String>,
    n_reduce: usize,
) -> Arc<Coordinator> {
    let c = Arc::new(Coordinator {
        state: Mutex::new(()), // replace with your state
    });

    let _ = (&files, n_reduce);

    // Your code here.

    c.server(sockname);
    c
}
