//! The Clerk (client) for the replicated key/value service (port of Go's
//! kvraft1/client.go).

use crate::rpc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tester::kvtest::IKVClerk;
use tester::Clnt;

pub struct Clerk {
    clnt: Arc<Clnt>,
    servers: Vec<String>,
    leader: AtomicUsize, // last successful leader (index into servers[])
    // You can add to this struct.
}

/// Make a Clerk (Go: MakeClerk).  `servers` are the network names of all
/// the kv servers.
pub fn make_clerk(clnt: Arc<Clnt>, servers: Vec<String>) -> Arc<Clerk> {
    let ck = Arc::new(Clerk {
        clnt,
        servers,
        leader: AtomicUsize::new(0),
    });
    // You'll have to add code here.
    ck
}

impl Clerk {
    /// The index of the server that answered our last RPC (used by the
    /// tester).
    pub fn leader(&self) -> usize {
        self.leader.load(Ordering::SeqCst)
    }

    /// Get fetches the current value and version for a key.  It returns
    /// ErrNoKey if the key does not exist.  It keeps trying forever in the
    /// face of all other errors.
    ///
    /// You can send an RPC to server i with code like this:
    ///
    /// ```ignore
    /// let reply: Option<rpc::GetReply> =
    ///     self.clnt.call(&self.servers[i], "KVServer.Get", &args);
    /// ```
    ///
    /// `None` means the RPC failed (wrong server, dropped message, ...):
    /// try the next server.
    pub fn get(&self, _key: &str) -> (String, rpc::Tversion, rpc::Err) {
        // You will have to modify this function.
        (String::new(), 0, String::new())
    }

    /// Put updates key with value only if the version in the request
    /// matches the version of the key at the server.  If Put receives
    /// ErrVersion on its first RPC, Put should return ErrVersion, since
    /// the Put was definitely not performed at the server.  If the server
    /// returns ErrVersion on a resend RPC, then Put must return ErrMaybe
    /// to the application, since its earlier RPC might have been processed
    /// by the server successfully but the response was lost, and the Clerk
    /// doesn't know if the Put was performed or not.
    pub fn put(&self, _key: &str, _value: &str, _version: rpc::Tversion) -> rpc::Err {
        // You will have to modify this function.
        String::new()
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
