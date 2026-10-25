# 6.5840 Lab 2: Key/Value Server (Rust)

Course write-up: <https://pdos.csail.mit.edu/6.5840/labs/lab-kvsrv1.html>
Read it first; this document is the Rust-specific companion.

## Introduction

You build a key/value server for a single machine that ensures that each
Put is executed **at-most-once** despite dropped messages, and that
operations are **linearizable**.  You then use it to implement a
distributed lock.  Later labs replicate a server like this with Raft.

The server stores a `(value, version)` per key:

* `Put(key, value, version)` installs `value` only if `version` matches
  the stored version (and increments it); mismatch → `rpc::ERR_VERSION`;
  unknown key with version > 0 → `rpc::ERR_NO_KEY`.  A new key is created
  with version 0.
* `Get(key)` returns `(value, version)`, or `rpc::ERR_NO_KEY`.

With dropped messages, a Clerk that retries cannot distinguish "reply to
my first Put was lost" from "another client beat me to it".  If the
server answers `ErrVersion` to a **retransmitted** Put, the Clerk must
return `rpc::ERR_MAYBE`.

## Where your code goes

| Go file | Rust file |
|---------|-----------|
| `kvsrv1/server.go`   | `crates/kvsrv1/src/server.rs` |
| `kvsrv1/client.go`   | `crates/kvsrv1/src/client.rs` |
| `kvsrv1/rpc/rpc.go`  | `crates/kvsrv1/rpc/src/lib.rs` (provided; shared with labs 4-5) |
| `kvsrv1/lock/lock.go`| `crates/kvsrv1/src/lock.rs` |
| `kvtest1/*`          | `crates/tester/src/kvtest.rs` (provided) |

The RPC types already derive `serde::Serialize`/`Deserialize` — the
analog of Go's "only exported fields are marshalled" rule.  Error
constants have Rust names: `rpc::OK`, `rpc::ERR_NO_KEY`,
`rpc::ERR_VERSION`, `rpc::ERR_MAYBE` (and, for later labs,
`rpc::ERR_WRONG_LEADER`, `rpc::ERR_WRONG_GROUP`).

### The server

Fill in `KVServer::get` and `KVServer::put` in `server.rs`.  Guard the
map with the provided `Mutex` (put the map *inside* a struct guarded by
the mutex, or use `Mutex<HashMap<...>>`):

```rust
let mut db = self.db.lock().unwrap();   // Go: kv.mu.Lock()
```

`start_server()` (provided) wires `get`/`put` up as the `"KVServer"` RPC
service that the test harness publishes on the simulated network.

### The Clerk

Fill in `Clerk::get` and `Clerk::put` in `client.rs`.  Send an RPC like
this:

```rust
let reply: Option<rpc::GetReply> =
    self.clnt.call(&self.server, "KVServer.Get", &args);
```

`None` means no reply (dropped request/reply, down server): retry.
Before retrying, sleep a little (`std::thread::sleep(Duration::from_millis(100))`).

For `put`, remember the tricky case: `ErrVersion` on a **retry** must
become `ErrMaybe` to the application; `ErrVersion` on the **first** attempt
is a real `ErrVersion`.

The Clerk implements the tester's `IKVClerk` trait (Go's `kvtest.IKVClerk`)
so the shared test helpers can drive it.

### The lock

Fill in `Lock::acquire`/`Lock::release` in `lock.rs`, storing lock state
in the key/value server under `lockname`.  You'll need a unique id per
lock client: use `tester::rand_value(8)`.  Think carefully about what
`acquire` should do when `put` returns `ErrMaybe` or `ErrVersion`
(looping on the version read by `get`).

## Running and testing

The tester runs one server in-process behind the simulated network; the
tests exercise it with reliable and unreliable networks, many concurrent
clerks, and check linearizability of the recorded history.

```sh
cd rust
make kvsrv1              # all KV server tests
make RUN=Reliable kvsrv1 # just the reliable-network tests
make lock1               # all lock tests
```

Tests (same names as the Go tests):

* `TestReliablePut` — basic Put/Get/version semantics.
* `TestPutConcurrentReliable` — 10 clients racing on one key; counts
  OK/Maybe and checks linearizability with the (ported) porcupine
  checker.
* `TestMemPutManyClientsReliable` — 20,000 clients; the server must not
  keep per-client state.  (The Go test measures the server process's
  memory with `runtime.ReadMemStats`; in the Rust port everything is one
  process, so the check is a loose bound on total heap growth via a
  counting allocator — the property enforced is the same.)
* `TestUnreliableNet` — one client, dropped messages; Put must not
  happen more than once, and `ErrMaybe` must occur.
* `lock1` tests — `TestReliableBasic`, `TestReliableNested`,
  `TestOne/ManyClientsReliable`, `TestOne/ManyClientsUnreliable`: only
  one client at a time may hold a lock, checked by writing its id under
  `l0` while holding it.

With the skeleton, `TestReliablePut` fails immediately with
`Put err ErrNoKey` — that's your starting point.

## Hints

* Linearizability is easy here: execute each RPC handler's body while
  holding the mutex.
* The server code does not need to change when you add retries in the
  Clerk; conditional Put on the version is what makes retries safe.
* The lock: think about which (key, version) operations make `acquire`
  wait, and what to do about `ErrMaybe` in `acquire`/`release` (see the
  course FAQ).  If `release` sees `ErrMaybe`, the safest port of the
  intended solution is to crash the process (the test treats an
  unreleased lock as a bug; in the Go labs most solutions `panic!` here).

## Handin

`make kvsrv1` / `make lock1` build-check and tar up `rust/` for
Gradescope like the Go Makefile does.
