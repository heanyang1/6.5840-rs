//! The key/value server (port of Go's kvsrv1/server.go).

use crate::rpc;
use std::sync::Mutex;

pub const DEBUG: bool = false;

#[macro_export]
macro_rules! dprintf {
    ($($arg:tt)*) => {
        if $crate::server::DEBUG {
            println!($($arg)*);
        }
    };
}

pub struct KVServer {
    // Your definitions here.

    #[allow(dead_code)]
    mu: Mutex<KVState>, // placeholder: replace with your state
}

/// Placeholder for the server's state; replace with your own.
#[derive(Default)]
pub struct KVState {}

/// Create a KV server (Go: MakeKVServer).
pub fn make_kvserver() -> KVServer {
    let kv = KVServer {
        mu: Mutex::new(KVState::default()),
    };
    // Your code here.
    kv
}

impl KVServer {
    /// Get returns the value and version for args.key, if args.key
    /// exists.  Otherwise, Get returns ErrNoKey.
    pub fn get(&self, _args: &rpc::GetArgs) -> rpc::GetReply {
        // Your code here.
        rpc::GetReply::new("", 0, rpc::ERR_NO_KEY)
    }

    /// Update the value for a key if args.version matches the version of
    /// the key on the server.  If versions don't match, return
    /// ErrVersion.  If the key doesn't exist, Put installs the value if
    /// the args.version is 0, and returns ErrNoKey otherwise.
    pub fn put(&self, _args: &rpc::PutArgs) -> rpc::PutReply {
        // Your code here.
        rpc::PutReply::new(rpc::ERR_NO_KEY)
    }
}

/// Provided: construct the tester's server box for a kvsrv server: wire up
/// the "KVServer" RPC service.  (In Go the tester uses reflection over the
/// object's methods; in Rust the harness registers handlers explicitly.)
pub fn start_server(_args: tester::ServerArgs) -> tester::ServerBox {
    // the single-server lab ignores ends/gid/persister
    let kv = std::sync::Arc::new(make_kvserver());
    let svc = labrpc::Service::new("KVServer");
    svc.handle("Get", { let kv = std::sync::Arc::clone(&kv); move |args: rpc::GetArgs| kv.get(&args) });
    svc.handle("Put", { let kv = std::sync::Arc::clone(&kv); move |args: rpc::PutArgs| kv.put(&args) });
    tester::ServerBox::new().with_service(svc)
}

