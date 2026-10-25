//! labgob: serialization support for the 6.5840 Rust labs.
//!
//! The Go labs use a restricted version of Go's `gob` encoder so that
//! persisted Raft state and RPC arguments/replies contain no references to
//! live program objects.  In Rust, the same job is done by `serde` +
//! `bincode`: values are serialized to bytes and deserialized back into
//! fresh values, so nothing is ever shared by reference across the network
//! or through the Persister.
//!
//! Use [`encode`] to serialize a value and [`decode`] to deserialize it.
//! Every struct sent over RPC or passed to `Persister::save()` must
//! derive `serde::Serialize` and `serde::Deserialize` (this is the Rust
//! analog of "only capitalized fields are sent by Go RPC" -- in Rust the
//! compiler enforces it for you).

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Serialize a value to bytes (like `labgob.NewEncoder(w).Encode(v)`).
pub fn encode<T: Serialize>(val: &T) -> Vec<u8> {
    bincode::serialize(val).expect("labgob: encode failed")
}

/// Deserialize a value from bytes.  Returns `None` on a decode error
/// (like a non-nil error from `labgob.NewDecoder(r).Decode(&v)`).
pub fn decode<T: DeserializeOwned>(data: &[u8]) -> Option<T> {
    match bincode::deserialize(data) {
        Ok(v) => Some(v),
        Err(e) => {
            eprintln!("labgob: decode error: {} (len={}, head={:?})", e, data.len(), &data[..data.len().min(12)]);
            None
        }
    }
}
