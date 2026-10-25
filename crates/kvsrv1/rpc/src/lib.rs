//! RPC definitions shared by the key/value labs (the Rust port of Go's
//! `kvsrv1/rpc` package).  These types travel over the simulated network,
//! so they derive Serialize/Deserialize (the analog of Go's
//! "only exported fields are sent" rule).

/// Errors returned by the server and the Clerk.
pub mod err {
    /// Errors returned by server and Clerk:
    pub const OK: &str = "OK";
    pub const ERR_NO_KEY: &str = "ErrNoKey";
    pub const ERR_VERSION: &str = "ErrVersion";

    /// Error returned by Clerk only:
    pub const ERR_MAYBE: &str = "ErrMaybe";

    /// For the later labs:
    pub const ERR_WRONG_LEADER: &str = "ErrWrongLeader";
    pub const ERR_WRONG_GROUP: &str = "ErrWrongGroup";
}

// Convenience re-exports so lab code can write `rpc::OK`, like Go.
pub use err::{ERR_MAYBE, ERR_NO_KEY, ERR_VERSION, ERR_WRONG_GROUP, ERR_WRONG_LEADER, OK};

/// RPC errors are plain strings (like Go's `rpc.Err` string type), so they
/// serialize trivially and compare with `==`.
pub type Err = String;

/// Version number of a key: records the number of times it's been written.
pub type Tversion = u64;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PutArgs {
    pub key: String,
    pub value: String,
    pub version: Tversion,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PutReply {
    pub err: Err,
}

impl PutReply {
    pub fn new(err: &str) -> PutReply {
        PutReply { err: err.to_string() }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GetArgs {
    pub key: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GetReply {
    pub value: String,
    pub version: Tversion,
    pub err: Err,
}

impl GetReply {
    pub fn new(value: &str, version: Tversion, err: &str) -> GetReply {
        GetReply { value: value.to_string(), version, err: err.to_string() }
    }
}
