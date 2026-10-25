// Skeleton code: some placeholders are unused until you fill them in.
#![allow(unused_variables, unused_imports, dead_code)]

//! 6.5840 Lab 3: Raft.
//!
//! You implement Raft in [`raft`].  [`api`] defines the `ApplyMsg` type
//! that Raft sends to the service (or tester) as entries become committed,
//! plus the trait bound that a Raft command must satisfy.

pub mod api;
pub mod raft;
pub mod util;
