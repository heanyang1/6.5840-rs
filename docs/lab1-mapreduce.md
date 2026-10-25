# 6.5840 Lab 1: MapReduce (Rust)

Course write-up: <https://pdos.csail.mit.edu/6.5840/labs/lab-mr.html>
Read it first; this document is the Rust-specific companion.

## Introduction

In this lab you'll build a MapReduce system: a **worker** process that
calls application Map and Reduce functions and handles reading and
writing files, and a **coordinator** process that hands out tasks to
workers and copes with failed workers.  (This lab uses "coordinator"
instead of the paper's "master".)

Workers talk to the coordinator via RPC over a unix domain socket.  Each
worker asks the coordinator for a task, executes it, writes output, and
asks for more.  The coordinator re-issues tasks whose worker hasn't
completed them within **ten seconds**.

## Where your code goes

| Go file | Rust file | Role |
|---------|-----------|------|
| `src/mr/coordinator.go` | `crates/mr/src/coordinator.rs` | coordinator (you implement) |
| `src/mr/worker.go`      | `crates/mr/src/worker.rs`      | worker (you implement) |
| `src/mr/rpc.go`         | `crates/mr/src/rpc.rs`         | RPC arg/reply types (you define) |
| `src/mr/util.go`        | `crates/mr/tests/mr_test.rs`   | test helpers (don't modify) |
| `src/main/mrsequential.go` | `crates/mr/src/bin/mr-sequential.rs` | provided |
| `src/main/mrcoordinator.go` | `crates/mr/src/bin/mr-coordinator.rs` | provided |
| `src/main/mrworker.go`  | `crates/mr/src/bin/mr-worker.rs` | provided |
| `src/mrapps/*.go`       | `crates/mr/src/apps.rs`         | provided applications |

Provided plumbing you do **not** have to write: `crates/mr/src/net.rs`
implements what Go's `net/rpc` over a unix socket does in the Go labs:

```rust
// coordinator side: register handlers, then serve (background thread):
let mut srv = net::RpcServer::new();
srv.register("Coordinator.GetTask", |args: GetTaskArgs| -> GetTaskReply { ... });
srv.serve(sockname);

// worker side: returns None if the call failed (e.g. coordinator exited):
let reply: Option<GetTaskReply> = net::call(sockname, "Coordinator.GetTask", &args);
```

The `Coordinator` is shared by concurrent RPC handlers, so keep its state
behind a `Mutex` (the analog of Go's `sync.Mutex`).

One structural difference from Go: application Map/Reduce functions are
**not** loaded from `.so` plugins (Rust has no stable plugin ABI).  They
live in `crates/mr/src/apps.rs` and are selected by name: `mr-worker wc
sock123` instead of `mrworker wc.so sock123`.  All the apps the tests use
(`wc`, `indexer`, `mtiming`, `rtiming`, `jobcount`, `early_exit`,
`crash`, `nocrash`) are provided, and adding your own app is one `match`
arm in `apps::lookup`.  (If you feel ambitious you can implement
`dylib`-based plugin loading as a no-credit challenge.)

## Running

Build and run the word count on your own (once partially implemented):

```sh
cd rust
cargo build
cd target/debug
rm -f mr-out*
./mr-coordinator /tmp/5840-mr-sock ../../crates/mr/testdata/pg*.txt &
./mr-worker wc /tmp/5840-mr-sock &
./mr-worker wc /tmp/5840-mr-sock &
./mr-worker wc /tmp/5840-mr-sock &
cat mr-out-* | sort | more
```

`mr-sequential` runs the same job single-threaded, for producing the
expected output:

```sh
./mr-sequential wc ../../crates/mr/testdata/pg*.txt   # writes mr-out-0
```

Input files are in `crates/mr/testdata/pg-*.txt` (the Go tree's
`src/main/pg-*.txt`).

## Testing

```sh
cd rust
make mr                  # all Lab 1 tests (~90s when passing)
make RUN=Wc mr           # just TestWc
```

The tests spawn real coordinator/worker processes in a temp directory and
compare the merged `mr-out-*` output against output produced by
`mr-sequential`, exactly like the Go tests:

* `TestWc`, `TestIndexer` — correct output.
* `TestMapParallel`, `TestReduceParallel` — workers genuinely run tasks
  in parallel.
* `TestJobCount` — no task is executed more than once when there are no
  failures.
* `TestEarlyExit` — no worker/coordinator exits before the output is
  final.
* `TestCrashWorker` — recovers from workers that crash mid-task (the
  `crash` app randomly exits).

With the skeleton, `TestWc` **hangs**: the coordinator's `done()`
always returns `false`, so the test waits forever (the Go skeleton does
the same).  Kill it with Ctrl-C or wait for the 10-minute watchdog.  You
can temporarily `return true` in `done()` to see the failure mode.

## Rules and hints (translated)

* The map phase divides intermediate keys into `n_reduce` buckets
  (`n_reduce` is 10, passed by `mr-coordinator.rs`).  Each mapper creates
  `n_reduce` intermediate files.
* The output of the X'th reduce task goes in `mr-out-X`; one line per
  output key, formatted `format!("{} {}", key, value)` — this is the
  correct format, tests compare byte-for-byte.
* A reasonable intermediate file name is `mr-X-Y` (map task X, reduce Y).
* Workers share a filesystem; use a temp file + atomic
  `std::fs::rename` so nobody observes a partially written file (the
  MapReduce paper's trick; the `crash` app tests this).
* Use JSON (`serde_json`) for intermediate key/value files, like the Go
  hint suggests (`KeyValue` already derives the needed traits).
* Use `ihash(key) % n_reduce` (provided in `worker.rs`) to pick a key's
  reduce task.
* The coordinator's handlers run concurrently: **lock your state**.
* Workers may need to wait (reduces can't start until maps are done);
  polling with `std::thread::sleep` between RPCs is fine, and so is
  making the coordinator's handler wait — each connection is served on
  its own thread.
* The coordinator should wait **ten seconds** before re-assigning a task
  to another worker, and must not schedule extra (backup) tasks when
  workers don't crash.
* When the job is done, workers should exit: a `None` return from
  `call()` (coordinator gone) is the standard signal — or use a "please
  exit" pseudo-task.

## Handin

`make mr` also tars up `rust/` (without `target/`) into
`mr-handin.tar.gz` for Gradescope, mirroring the Go Makefile.
