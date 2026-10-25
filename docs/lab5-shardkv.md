# 6.5840 Lab 5: Sharded Key/Value Service (Rust)

Course write-up: <https://pdos.csail.mit.edu/6.5840/labs/lab-shard1.html>
Read it first; this document is the Rust-specific companion.

## Introduction

You shard a key/value store over several Raft-replicated **shardgrps**
(12 shards, `shardcfg::key2shard` maps a key to its shard).  A
**controller** stores configurations in a Lab 2 kvsrv and moves shards
between shardgrps using `FreezeShard` / `InstallShard` / `DeleteShard`
RPCs.  Clients read the configuration to find the group serving each
key.  This lab uses your Lab 2 kvsrv, and your Lab 3 Raft + Lab 4 rsm.

* **Part A**: `InitConfig`, `Query`, `ChangeConfigTo` without failures;
  shardgrp + shardkv clerks; freeze/install/delete.
* **Part B**: recovering from a failed controller (current + next
  configuration in the kvsrv).
* **Part C**: concurrent controllers fencing each other with `Num` and
  versioned Puts.
* **Part D**: an extension of your choice (write `extension.md`).

## Where your code goes

| Go file | Rust file | Role |
|---------|-----------|------|
| `shardkv1/shardcfg/shardcfg.go` | `crates/shardkv1/src/shardcfg.rs` | provided (config math, `key2shard`) |
| `shardkv1/shardctrler/shardctrler.go` | `crates/shardkv1/src/shardctrler.rs` | **you implement** |
| `shardkv1/shardgrp/server.go` | `crates/shardkv1/src/shardgrp/server.rs` | **you implement** |
| `shardkv1/shardgrp/client.go` | `crates/shardkv1/src/shardgrp/client.rs` | **you implement** |
| `shardkv1/shardgrp/shardrpc/shardrpc.go` | `crates/shardkv1/src/shardgrp/shardrpc.rs` | provided |
| `shardkv1/client.go` | `crates/shardkv1/src/client.rs` | **you implement** |
| `shardkv1/test.go`, `shardkv_test.go` | `crates/shardkv1/tests/` | tests (don't modify) |

Types: `shardcfg::Tshid`/`Tnum` are `i64`; group ids are `i64`
(`shardcfg::GID1 == 1`); `ShardConfig` serializes to/from a string with
`cfg.to_string_json()` / `ShardConfig::from_string(...)` (Go's
`String()`/`FromString()`); store it in the kvsrv under a key of your
choosing via the controller's embedded Lab 2 clerk (`self.kv.get/put`).

The shardgrp server is split Rust-style, exactly like kvraft (see Lab 4
doc): `ShardKvSm` (replicated state: per-shard key/value maps, frozen
shards, highest `Num` seen per shard) + `KVServer` (the RPC handle whose
get/put/freeze_shard/install_shard/delete_shard submit through the rsm).
All five operations must go **through the rsm/Raft**.

## Suggested porting plan

1. **InitConfig/Query** (no shardgrps needed): store the initial config
   in the kvsrv with a versioned Put at version 0; `query()` reads it
   with `Get` and `ShardConfig::from_string`s it.  Test:
   `TestInitQuery5A`.
2. **Shardgrp + shardkv clerk**: copy from your Lab 4 kvraft
   server/client.  The first group (`shardcfg::GID1`) starts out owning
   all shards.  The shardkv clerk queries the config, caches a
   `shardgrp::client::Clerk` per gid (the `rcks` map in the skeleton),
   and retries on `ErrWrongGroup` after re-reading the config.  Test:
   `TestStaticOneShardGroup5A`.
3. **ChangeConfigTo + freeze/install/delete**: for each shard that moves:
   freeze at the source (reject Puts/Gets, hand back the shard's state as
   bytes — a whole `HashMap` is fine), install at the destination, delete
   at the source, then post the new configuration.  Fence **all** these
   RPCs with the config `Num`: shardgrps remember the largest `Num` seen
   per shard and reject older ones.  Tests: `TestJoinBasic5A`,
   `TestDeleteBasic5A`, `TestJoinLeaveBasic5A`,
   `TestManyJoinLeave*5A`, `TestShutdown5A`, `TestProgress*5A`,
   `Test*ConcurrentClerk*5A`.
4. **Part B**: keep *current* and *next* configurations in the kvsrv;
   `init_controller()` completes an interrupted change (a higher-numbered
   next config exists).  Repeated freeze/install/delete RPCs are harmless
   thanks to the `Num` fencing.  Tests: `TestJoinLeave5B`,
   `TestRecoverCtrler5B`.
5. **Part C**: several controllers run concurrently; only one may post
   the next configuration for a given `Num` — use the kvsrv's versioned
   Put (compare-and-swap) on the next-config key; losers return without
   doing anything.  Tests: `TestConcurrentReliable5C`,
   `TestAcquireLockConcurrentUnreliable5C`, `TestPartitionControllerJoin5C`,
   `TestPartitionRecovery*5C`.

## Running and testing

```sh
cd rust
make RUN=5A shardkv    # ~4-5 min when passing
make RUN=5B shardkv
make RUN=5C shardkv
make shardkv           # everything (~15 min)
```

Test names match the Go suite: `TestInitQuery5A`,
`TestStaticOneShardGroup5A`, `TestJoinBasic5A`, `TestDeleteBasic5A`,
`TestJoinLeaveBasic5A`, `TestManyJoinLeaveReliable/Unreliable5A`,
`TestShutdown5A`, `TestProgressShutdown5A`, `TestProgressJoin5A`,
`TestOne/ManyConcurrentClerkReliable/Unreliable5A`, `TestJoinLeave5B`,
`TestRecoverCtrler5B`, `TestConcurrentReliable5C`,
`TestAcquireLockConcurrentUnreliable5C`, `TestPartitionControllerJoin5C`,
`TestPartitionRecovery*5C`.

The harness (`crates/shardkv1/tests/common/mod.rs`) provides
`setup_kv_service` (starts GID1 and stores the initial config),
`join_groups`/`leave_groups`, `check_shutdown_sharding` (verifies data is
actually sharded by shutting a group down), `partition_ctrler` (kills a
controller mid-change and requires recovery), and `concur_ctrler`
(runs several controllers concurrently) — ports of the Go helpers.

Skeleton failure modes: `TestInitQuery5A` fails on `cfg.num != 1`;
everything downstream fails until the controller works.

## Hints

* Send the shard's whole state map in the InstallShard RPC (like Go's
  hint: "send an entire map"); serialize with `labgob::encode` /
  `serde_json`.  Always **copy** maps out of the state under the lock
  before putting them into a reply (the Go race warning applies: the RPC
  layer serializes the reply on another thread).
* A shardgrp must reject client ops for shards it doesn't own with
  `rpc::ERR_WRONG_GROUP`; the shardkv clerk then re-queries the
  configuration.
* Serve unaffected shards **while** a config change is in flight
  (`TestProgressJoin5A` measures this).
* Dedup state in the shardgrp (client/seq ids in ops) matters now that
  the controller retries move RPCs.
* Gradescope re-runs Labs 3 and 4 too — keep them passing.

## Part D (extension)

Pick an idea from the course page (kvraft-backed controller, exactly-once
kvsrv, Range queries, leader-served Gets with leases, transactions in
kvraft or shardkv) or your own.  Implement it and write a paragraph in
`rust/extension.md`; upload that file to Gradescope.  You must write your
own tests for the extension (add them to the relevant crate's `tests/`).

## Handin

`make shardkv` build-checks and tars `rust/` for Gradescope, which also
re-runs the Lab 3 and 4 tests, exactly like the Go labs.
