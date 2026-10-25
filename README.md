# 6.5840 Labs Skeleton Code in Rust

> Disclaimer:
> 1. This repo only contain the skeleton code. I'll NEVER publish any solution online.
> 2. This is a vibe-coded project, so there may be mistakes or leaked solutions. I'll try to finish the labs and fix them along the way.

Rust versions of the MIT 6.5840 (Distributed Systems) lab skeleton code.
The labs and their tests mirror the Go labs at
<https://pdos.csail.mit.edu/6.5840/schedule.html> (Spring 2026), so you
can follow the course write-ups while writing Rust.

## Labs

| Lab | Go package | Rust crate | What you build |
|-----|-----------|------------|----------------|
| 1   | `src/mr`            | `crates/mr`         | Distributed MapReduce (coordinator + workers) |
| 2   | `src/kvsrv1`        | `crates/kvsrv1`     | Key/value server with at-most-once Puts + a distributed lock |
| 3   | `src/raft1`         | `crates/raft1`      | Raft (3A election, 3B log, 3C persistence, 3D snapshots) |
| 4   | `src/kvraft1`       | `crates/kvraft1`    | Fault-tolerant KV service on Raft (4A rsm, 4B service, 4C snapshots) |
| 5   | `src/shardkv1`      | `crates/shardkv1`   | Sharded KV service with reconfiguration (5A-5D) |

Each lab has an instructions file under [`docs/`](docs/):

* [Lab 1: MapReduce](docs/lab1-mapreduce.md)
* [Lab 2: Key/Value Server](docs/lab2-kvserver.md)
* [Lab 3: Raft](docs/lab3-raft.md)
* [Lab 4: Fault-tolerant KV Service](docs/lab4-kvraft.md)
* [Lab 5: Sharded KV Service](docs/lab5-shardkv.md)

Read the course page for your lab *and* the Rust doc: the course page
explains what to build; the Rust doc tells you where the code lives, the
few places where Rust differs from Go, and the exact test commands.

## Setup

Install Rust (any recent stable toolchain; developed on 1.97):

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Build everything and check the tests compile (the skeleton tests fail --
that's expected, just like `go test` on the Go skeleton):

```sh
cd rust
cargo build
make mr          # runs Lab 1's tests (will fail/hang until you implement it)
```

## Building and testing

The `Makefile` mirrors the Go labs' Makefile:

```sh
make mr           # Lab 1
make kvsrv1       # Lab 2 (KV server)
make lock1        # Lab 2 (lock)
make raft1        # Lab 3
make rsm1         # Lab 4A
make kvraft1      # Lab 4B/4C
make shardkv      # Lab 5
```

`make RUN=<filter> <lab>` runs only the tests whose names contain
`<filter>` (the analog of `make RUN="-run 3A" raft1`):

```sh
make RUN=3A raft1      # Part 3A tests only
make RUN=4B kvraft1    # Part 4B tests only
make RUN=Reliable kvsrv1
```

You can also invoke cargo directly:

```sh
cargo test -p raft1 --test raft_test 3A -- --nocapture
```

Tests run **sequentially** (configured in `.cargo/config.toml`), matching
`go test`'s behavior; the tests make global timing assumptions (election
timeouts, etc.) and must not run in parallel.  Every lab test installs a
watchdog (the analog of `go test`'s 10-minute timeout): a hung test is
aborted after 10 minutes with a message.

Output visualization: where the Go tester writes a porcupine/timeline
visualization, the Rust tests print the failure directly.  `--nocapture`
(the Makefile passes it) shows per-test progress lines like
`Test (3A): initial election`.

## How the Go skeleton maps to Rust

The Rust skeleton is a faithful port of the Go skeleton, module by
module.  You fill in the same functions with the same semantics.  The
differences:

| Go | Rust |
|----|------|
| goroutines | `std::thread::spawn` |
| channels | `crossbeam_channel` (`unbounded()`, `send`, `recv`, `recv_timeout`, cloneable `Sender`/`Receiver`) |
| `sync.Mutex` + fields | `Mutex<T>` guarding a state struct |
| `sync.Cond` | condvars exist (`std::sync::Condvar`), but channels/`recv_timeout` are usually simpler |
| Go RPC (`net/rpc`) + `labrpc` | `labrpc` crate: simulated unreliable network with typed `end.call("Svc.Method", &args) -> Option<Rep>` |
| `labgob` | `labgob` crate: `encode`/`decode` via serde+bincode; any struct sent over RPC or persisted needs `#[derive(Serialize, Deserialize)]` |
| Go plugin `.so` for MapReduce apps | apps are functions in `crates/mr/src/apps.rs`, selected by name (`mr-worker wc sock123`) |
| `interface{}` commands/ops | generics (`Raft<C: Cmd>`) or enums (`enum Op { Get(..), Put(..) }`) |
| raftapi.Raft interface | concrete `raft1::Raft<C>` type, generic over the command type `C` |
| tester runs servers in daemon processes | one process; servers are threads behind a simulated network (same disconnect/partition/kill/restart powers) |

### Concurrency rules of thumb (read this!)

* **Share state with `Arc<Mutex<...>>`**, not raw pointers.  If the
  borrow checker complains about a field being borrowed in two closures,
  that's the compiler telling you to protect it with a `Mutex` and clone
  the `Arc` into each thread.
* **Never hold a mutex across a blocking call** (an RPC `call()`, a
  channel `recv()`, or `send_apply()` to a *bounded* buffer).  Deadlocks
  will fail the tests with timeouts.
* Beware `lock().unwrap()` **re-entrancy**: a function that locks `mu`
  must not call another function that also locks `mu`.  Split your state
  or drop the guard first (this is the classic Go bug too).
* `Kill()` semantics: your Raft's `kill()` must make all background
  threads exit **and close the apply channel** (the provided skeleton
  does the closing; make sure your threads observe `killed()`).

### Where things live

```
rust/
├── Cargo.toml            # workspace
├── Makefile              # make [mr|kvsrv1|lock1|raft1|rsm1|kvraft1|shardkv], RUN= filter
├── .cargo/config.toml    # sequential test execution
├── docs/                 # per-lab instructions (start here!)
└── crates/
    ├── labgob/           # provided: serde-based encode/decode (Go's labgob)
    ├── labrpc/           # provided: simulated unreliable RPC network
    ├── tester/           # provided: Persister, test harness (Config/ServerGrp/Clnt),
    │                     #   kvtest helpers, linearizability checker
    ├── mr/               # Lab 1: you edit coordinator.rs, worker.rs, rpc.rs
    ├── kvsrv1/           # Lab 2: you edit server.rs, client.rs, lock.rs
    │   └── rpc/          #   shared RPC types (provided)
    ├── raft1/            # Lab 3: you edit raft.rs
    ├── kvraft1/          # Lab 4: you edit rsm/rsm.rs, server.rs, client.rs
    └── shardkv1/         # Lab 5: you edit shardctrler.rs, client.rs,
                          #   shardgrp/{server,client}.rs
```

Tests are in each crate's `tests/` directory; they are ports of the Go
test suites (same scenarios, same names: `TestInitialElection3A`,
`TestBackup3B`, `TestSnapshotRPC4C`, `TestRecoverCtrler5B`, ...).  Don't
modify the tests or the provided infrastructure; the grading will use
fresh copies, exactly like the Go labs.

### Feedback and grading equivalence

The Rust harness simulates the same failures as the Go tester: lost
requests/replies, long delays, long reordering, partitions, and
crash/restart (fresh server instance, same `Persister`).  The
linearizability checker is a port of `porcupine` with the same KV model;
checks that exceed their time budget are assumed OK, exactly like the Go
tests.

## Debugging tips

* `println!`/`eprintln!` freely in your code; run tests with `--nocapture`
  (the Makefile does) to see the output.
* Add debug prints to `crates/raft1/src/util.rs`'s `dprintf!` and flip
  `raft1::util::set_debug(true)`.
* When a test fails, run it alone: `make RUN=<TestName> <lab>`.
* Rust panics print the file/line that failed — the tests are line-by-line
  ports of the Go tests, so you can compare against `raft_test.go` &c in
  the Go tree when confused about what's being checked.
* Deadlock?  Run with `RUST_BACKTRACE=1` after the watchdog fires, or
  attach `gdb -p <pid>` and `thread apply all bt`.

## Licence

MIT
