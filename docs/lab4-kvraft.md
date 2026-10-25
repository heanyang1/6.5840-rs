# 6.5840 Lab 4: Fault-tolerant Key/Value Service (Rust)

Course write-up: <https://pdos.csail.mit.edu/6.5840/labs/lab-kvraft1.html>
Read it first; this document is the Rust-specific companion.

## Introduction

You build a fault-tolerant key/value service on your Lab 3 Raft.  Clients
see Lab 2 semantics (at-most-once Puts, linearizable history) replicated
across a majority-tolerant server group.

* **Part A (rsm)**: a replicated-state-machine package, agnostic of the
  replicated operations.
* **Part B**: the key/value service using rsm, without snapshots.
* **Part C**: snapshots (using your Lab 3D `Snapshot()`).

## Where your code goes

| Go file | Rust file | Role |
|---------|-----------|------|
| `kvraft1/rsm/rsm.go` | `crates/kvraft1/src/rsm/rsm.rs` | **Part A: you implement** |
| `kvraft1/server.go`  | `crates/kvraft1/src/server.rs` | **Part B/C: you implement** |
| `kvraft1/client.go`  | `crates/kvraft1/src/client.rs` | **Part B: you implement** |
| `kvraft1/rsm/server.go` | `crates/kvraft1/src/rsm/server.rs` | provided test support (counter service) |
| `kvtest1/*` | `crates/tester/src/kvtest.rs` | provided test helpers |

The RPC types come from the shared `kvsrv1-rpc` crate, re-exported as
`kvraft1::rpc` (Go: `6.5840/kvsrv1/rpc`).

### One structural difference: server = Sm + handle

Go builds a cyclic object graph (KVServer implements StateMachine *and*
holds its Rsm).  Rust splits it into two objects:

* `KvSm` — the state machine (database + duplicate-detection state); an
  `Arc<KvSm>` is handed to `make_rsm`, which calls `do_op`, `snapshot`,
  `restore` on it.  `do_op`'s argument is an enum:

  ```rust
  pub enum Op { Get(rpc::GetArgs), Put(rpc::PutArgs) }   // Go: any
  ```

* `KVServer` — the RPC-facing handle (its `get`/`put` submit through the
  rsm and match the returned `OpReply`).

The same split exists in `rsm/server.rs` (provided) and in Lab 5's
shardgrp server.

## Part A: the rsm package

Implement `Rsm::submit` and the apply-reader thread in
`crates/kvraft1/src/rsm/rsm.rs`:

* `submit(req)` wraps the request in an `Op` with a **unique id** (e.g.
  `(server_id, counter)`), calls `raft.start(op)`; if not leader, return
  `(rpc::ERR_WRONG_LEADER, None)`.  Otherwise wait for the reader to
  deliver the result of executing that op (a `crossbeam_channel` per
  waiting op works well), watching for lost leadership (term changed, or
  a different op was applied at your index) → `ErrWrongLeader`.
* The reader thread loops on the apply channel (`rf.apply receiver`),
  executes each committed op via `sm.do_op(...)`, hands the result to the
  waiting `submit` (match by id — ops from other leaders' terms are
  ignored), and advances the applied index.
* **Shutdown**: when the apply channel closes (Raft `kill()`), the reader
  must unblock all waiting `submit`s so they return an error — the
  `TestShutdown4A` test depends on it.
* **Part C**: after executing each op, check `rf.persist_bytes()` against
  `maxraftstate`; when it's too big, call `sm.snapshot()`, wrap it with
  the applied index (encode `(index, state)` with `labgob::encode`), pass
  it to `rf.snapshot(index, &bytes)` and remember it.  On restart, decode
  the persister's snapshot and call `sm.restore(...)`; skip applied
  entries at or below the snapshot index.  The reader must also handle
  `ApplyMsg`s that carry snapshots from the leader (pass them on by
  re-emitting, or feed `Restore` directly — mirror what your Raft sends).

`Op` travels through the Raft log: keep it `Serialize + Deserialize`
(already derived).

## Part B: the key/value service

Copy the structure of your Lab 2 code:

* `client.rs`: like Lab 2's Clerk, but try **each server** until one
  replies; remember the leader (`self.leader`) and try it first next
  time.  `get`/`put` retry forever; `put` maps `ErrVersion`-on-retry to
  `ErrMaybe` exactly as in Lab 2.  Give requests unique client+sequence
  ids so the server can filter duplicates (this is what makes Puts
  at-most-once when you retry after losing a reply).
* `server.rs`: `KvSm::do_op` executes Get/Put against the database map
  (plus your duplicate table); `KVServer::get`/`put` submit through the
  rsm and return the inner reply.  **Every** operation (including Get) is
  submitted through the log — no read-at-leader optimization is required
  (or allowed by the tests without leases).

## Part C: snapshots

Implemented cooperatively between rsm (above) and your Raft's 3D code.
`TestSnapshot4C` (in the rsm suite) and the 4C tests in `kvraft1` check
that the persisted Raft state stays under `8 * maxraftstate` bytes and
that snapshots stay small (`TestSnapshotSize4C`: 500 bytes for the
workload; don't put the whole history in the snapshot — just the DB and
the duplicate table).

## Running and testing

```sh
cd rust
make RUN=4A rsm1       # rsm tests: TestBasic4A ... TestRestartSubmit4A
make RUN=4B kvraft1    # service tests: TestBasic4B ... Linearizable4B
make RUN=4C kvraft1    # snapshot tests
make rsm1
make kvraft1
```

Test names match the Go suite: `TestBasic4A`, `TestConcurrent4A`,
`TestLeaderFailure4A`, `TestLeaderPartition4A`, `TestRestartReplay4A`,
`TestShutdown4A`, `TestRestartSubmit4A`, `TestSnapshot4C`;
`TestBasic4B`, `TestSpeed4B`, `TestConcurrent4B`, `TestUnreliable4B`,
`TestOnePartition4B`, `TestManyPartitions*4B`, `TestPersist*4B`,
`TestPersistPartitionUnreliableLinearizable4B`, `TestSnapshotRPC4C`,
`TestSnapshotSize4C`, `TestSpeed4C`, `TestSnapshotRecover*4C`, ...

The harness provides the same helpers as the Go tester
(`crates/tester/src/kvtest.rs`): `spawn_clients_with` +
`one_client_put`/`one_put`, `put_at_least_once`, `check_get`,
`check_porcupine` (linearizability checking with the ported porcupine),
partitions, and crash/restart of whole groups.

Skeleton failure modes: `TestBasic4A` fails with
*checkCounter: only 0 srvs have 1 instead of 3*; 4B tests fail with
`Wrong error` on the first Get/Put.

## Hints

* Start locking early: `Arc<Mutex<...>>` around the dedup table and DB.
* On the leader, a submitted op may never commit (leadership lost): the
  waiting `submit` must notice (term change / other op at its index) and
  return `ErrWrongLeader`, not wait forever.
* When a server restarts, it replays the log (and/or restores the
  snapshot): `do_op` must therefore tolerate re-execution of ops that
  were already acknowledged via a previous incarnation — the dedup table
  in the *replicated state* is what makes this safe.  Dedup on
  `(client_id, seq)`; `submit` must include them in `Op`.
* `TestLeaderPartition4A` submits `Dec` ops from the minority — your rsm
  must not apply them; `TestShutdown4A` kills Raft while ops are in
  flight — `submit` must return.

## Handin

`make rsm1` / `make kvraft1` build-check and tar `rust/` for Gradescope.
Gradescope will also re-run your Lab 3 tests, exactly like the Go labs.
