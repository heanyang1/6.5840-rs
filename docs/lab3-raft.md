# 6.5840 Lab 3: Raft (Rust)

Course write-up: <https://pdos.csail.mit.edu/6.5840/labs/lab-raft1.html>
Read it first (especially the extended Raft paper's Figure 2); this
document is the Rust-specific companion.

## Introduction

You implement Raft as a Rust type usable as a module in a larger service.
A set of Raft instances talk over the simulated `labrpc` network to
maintain replicated logs.  Committed entries are delivered to the service
on an apply channel.

The lab has four parts: **3A** leader election + heartbeats, **3B** log,
**3C** persistence, **3D** log compaction (snapshots).

## Where your code goes

| Go file | Rust file | Role |
|---------|-----------|------|
| `raft1/raft.go` | `crates/raft1/src/raft.rs` | **all your code** |
| `raftapi/raftapi.go` | `crates/raft1/src/api.rs` | `ApplyMsg` + `Cmd` trait (provided) |
| `raft1/util.go` | `crates/raft1/src/util.rs` | `dprintf!` debug macro |
| `raft1/test.go`, `raft_test.go`, `server.go` | `crates/raft1/tests/` | tests (don't modify) |

### The shape of the skeleton

```rust
pub struct Raft<C: Cmd> { ... }       // one peer; C is the command type

pub fn make<C: Cmd>(
    peers: Vec<Arc<ClientEnd>>,       // RPC ends to all peers, index = peer
    me: usize,
    persister: Arc<Persister>,
    apply_ch: crossbeam_channel::Sender<ApplyMsg<C>>,
) -> Arc<Raft<C>>;
```

`C: Cmd` is the Rust stand-in for Go's `interface{}` commands: the tests
use `Raft<i64>` (plain integers); Lab 4 uses `Raft<rsm::Op<Req>>`.

Key provided pieces (in `raft.rs`):

* `get_state() -> (term, is_leader)` — implement in 3A.
* `send_request_vote(server, &args) -> Option<Reply>` — example of an
  outgoing RPC; `None` means lost/dead.  Write your own
  `send_append_entries` (3A/3B) and `send_install_snapshot` (3D)
  analogously: `self.peers[server].call("Raft.AppendEntries", &args)`.
* `request_vote(&args) -> Reply` — the RPC handler; register **all** your
  handlers in the provided `raft_service()` function at the bottom of
  `raft.rs` (this is the explicit-Rust analog of Go's reflection-based
  service registration):

  ```rust
  svc.handle("AppendEntries", { let rf = Arc::clone(rf); move |args: AppendEntriesArgs| rf.append_entries(&args) });
  ```
* `start(command) -> (index, term, is_leader)` — implement in 3B.
* `persist()` / `read_persist()` — implement in 3C using
  `labgob::encode`/`labgob::decode` (e.g. encode a tuple
  `(current_term, voted_for, log)`), and
  `self.persister.save(&data, None /* or Some(snapshot) */)`.
* `snapshot(index, snapshot)` — implement in 3D.
* `ticker()` — the election-timer loop (provided structure: sleep between
  checks; do **not** busy-loop).
* `send_apply(msg) -> bool` — use this to deliver `ApplyMsg`s; it returns
  false once the peer was killed.
* `kill()` — provided: sets the dead flag and closes the apply channel.
  Ensure your threads check `killed()` so they exit; Lab 4's shutdown
  test depends on the channel closing.

### Concurrency in Rust terms

Go's `rf.mu` becomes `self.mu: Mutex<RaftState<C>>`.  Two porting rules
will save you days:

1. **Don't hold the lock across an RPC.**  Lock, snapshot what you need
   (`let (term, log) = { let g = self.mu.lock().unwrap(); (...) };`),
   drop the guard, then call.  `labrpc` handlers run on their own
   threads, so a handler that can't get the lock just delays.
2. **A `MutexGuard` is dropped at the end of the statement** unless bound
   with `let`.  `self.mu.lock().unwrap().field += 1` works; leaving a
   `let g` alive across a `call()` deadlocks.

Channels: use `crossbeam_channel` (Sender/Receiver are cloneable, and
`recv_timeout` exists).  A `Condvar` also works but is rarely needed.

### State, log indices, persistence

The Go advice applies verbatim: 0-index your log with a dummy entry at
index 0 (term 0), implement the election restriction, back up
`next_index` by more than one using the XTerm/XIndex/XLen optimization in
3C, and persist on every change of persistent state.  All of Figure 2's
"Rules for Servers" applies unchanged.

For 3D, the service (the test harness) calls `rf.snapshot(index, bytes)`;
discard the log before `index` and track `last_included_index` (Go's
`snapshots start at index X` hint).  Send the **entire** snapshot in one
InstallSnapshot RPC (no offset chunking).  Persist the snapshot with
`persister.save(&raftstate, Some(&snapshot))`.  On restart the harness
restores itself from the persisted snapshot before your Raft starts, and
expects your Raft to have loaded `last_included_index` from the
persister (read it back in `read_persist`).

## Running and testing

```sh
cd rust
make RUN=3A raft1    # ~20s when passing
make RUN=3B raft1    # ~60-90s
make RUN=3C raft1    # ~3min
make RUN=3D raft1    # ~5min
make raft1           # everything (~10min)
```

The tests are direct ports of the Go tests: `TestInitialElection3A`,
`TestReElection3A`, `TestManyElections3A`, `TestBasicAgree3B`,
`TestRPCBytes3B`, `TestFollowerFailure3B`, `TestLeaderFailure3B`,
`TestFailAgree3B`, `TestFailNoAgree3B`, `TestConcurrentStarts3B`,
`TestRejoin3B`, `TestBackup3B`, `TestCount3B`, `TestPersist1/2/33C`,
`TestFigure83C`, `TestUnreliableAgree3C`, `TestFigure8Unreliable3C`,
`TestReliableChurn3C`, `TestUnreliableChurn3C`, `TestSnapshotBasic3D`,
`TestSnapshotInstall*3D`, `TestSnapshotAllCrash3D`, `TestSnapshotInit3D`.

The harness (in `tests/common/mod.rs`) plays the role of Go's tester +
rfsrv: it counts RPCs and bytes, disconnects/partitions peers, kills and
restarts peers (fresh `Raft` from the same `Persister`), checks that all
servers apply the same command at each index, and requires heartbeats ≤
10/s and re-election within 5s — same bounds as the Go tester.

With the skeleton, `TestInitialElection3A` fails after ~5s with
*expected one leader, got none*.  If a test *hangs* instead, a watchdog
aborts it after 10 minutes with a message (that's a bug in your code —
often a deadlock or an unimplemented `kill()`).

## Hints specific to the Rust port

* `labrpc`'s `call()` never blocks forever (unlike Go's), but behave as
  if it might: your structure should be timeout-driven anyway.
* The tester counts RPC **bytes**; `TestRPCBytes3B`'s bound is the same.
  Don't send whole logs when a tail would do.
* `rand::thread_rng().gen_range(a..b)` is Go's `rand.Int63n`.
* Sleeping: `std::thread::sleep(Duration::from_millis(ms))`.
* A common Rust pitfall: `Mutex::lock().unwrap()` on a mutex **poisoned**
  by a panic in another thread will itself panic.  If you see cascading
  panics, fix the first panic in the log, not the last.
* When you fail a test, run it alone and read the harness code in
  `tests/common/mod.rs`; it mirrors `raft1/test.go` line for line.

## Handin

`make raft1` (or per-part targets) build-checks and tars `rust/` for
Gradescope.
