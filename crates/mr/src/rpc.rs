//! RPC argument/reply definitions for the coordinator/worker protocol.
//!
//! The example below shows the shape; add your own task RPC types here.

/// Example request type (sent by the worker).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExampleArgs {
    pub x: i64,
}

/// Example reply type (filled in by the coordinator).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExampleReply {
    pub y: i64,
}

// Add your RPC definitions here.
//
// Remember: any struct you send over RPC needs
// #[derive(serde::Serialize, serde::Deserialize)].
