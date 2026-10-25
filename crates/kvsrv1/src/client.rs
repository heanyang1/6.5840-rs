//! The Clerk (client) used by applications to talk to the key/value
//! server (port of Go's kvsrv1/client.go).

use crate::rpc;
use std::sync::Arc;
use tester::kvtest::IKVClerk;
use tester::Clnt;

pub struct Clerk {
    clnt: Arc<Clnt>,
    server: String,
}

/// Make a Clerk (Go: MakeClerk).
pub fn make_clerk(clnt: Arc<Clnt>, server: &str) -> Arc<Clerk> {
    let ck = Arc::new(Clerk {
        clnt,
        server: server.to_string(),
    });
    // You may add code here.
    ck
}

impl Clerk {
    /// Fetch the current value and version for a key.  It returns
    /// ErrNoKey if the key does not exist.  It keeps trying forever in
    /// the face of all other errors.
    ///
    /// You can send an RPC with code like this:
    ///
    /// ```ignore
    /// let reply: Option<rpc::GetReply> =
    ///     self.clnt.call(&self.server, "KVServer.Get", &args);
    /// ```
    ///
    /// `None` means no reply was received (e.g. the network dropped the
    /// request or the reply); `Some(reply)` carries the server's answer.
    pub fn get(&self, _key: &str) -> (String, rpc::Tversion, rpc::Err) {
        // You will have to modify this function.
        (String::new(), 0, rpc::ERR_NO_KEY.to_string())
    }

    /// Update key with value only if the version in the request matches
    /// the version of the key at the server.  If the version numbers
    /// don't match, the server returns ErrVersion.  If Put receives
    /// ErrVersion on its *first* RPC, Put should return ErrVersion, since
    /// the Put was definitely not performed at the server.  If the server
    /// returns ErrVersion on a *resend* RPC, then Put must return
    /// ErrMaybe to the application, since its earlier RPC might have been
    /// processed by the server successfully but the response was lost,
    /// and the Clerk doesn't know if the Put was performed or not.
    pub fn put(&self, _key: &str, _value: &str, _version: rpc::Tversion) -> rpc::Err {
        // You will have to modify this function.
        rpc::ERR_NO_KEY.to_string()
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
