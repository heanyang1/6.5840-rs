//! tester: the test harness for the 6.5840 Rust labs.
//!
//! Rust port of the Go labs' `tester1` + `kvtest1` + `models1` packages.
//! Everything runs in one process: servers are background threads talking
//! over the simulated `labrpc` network, which lets the harness disconnect,
//! partition, and kill/restart them just like the Go tester does.
//!
//! You will normally not modify this crate: your code lives in the per-lab
//! crates (`kvsrv1`, `raft1`, `kvraft1`, `shardkv1`), and the tests live in
//! each lab's `tests/` directory.

pub mod config;
pub mod kvtest;
pub mod mem;
pub mod porcupine;

pub use config::{server_name, Clnt, Config, MakeServer, ServerArgs, ServerBox, ServerGrp, Tgid, GRP0};
pub use mem::{allocated, counting_alloc_enabled, enable_counting_alloc};

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// The labrpc types re-exported for convenience in lab code.
pub use labrpc::{ClientEnd, Network};

//
// support for Raft and kvraft to save persistent Raft state (log &c) and
// k/v server snapshots.
//
// Port of Go's tester1/persister.go.
//

#[derive(Default)]
pub struct Persister {
    state: Mutex<PersistState>,
}

#[derive(Default)]
struct PersistState {
    raftstate: Vec<u8>,
    snapshot: Vec<u8>,
}

impl Persister {
    pub fn new() -> Arc<Persister> {
        Arc::new(Persister::default())
    }

    /// A copy of this persister's current contents (used when the tester
    /// snapshots a server group's state).
    pub fn checkpoint(&self) -> Arc<Persister> {
        let g = self.state.lock().unwrap();
        Arc::new(Persister {
            state: Mutex::new(PersistState {
                raftstate: g.raftstate.clone(),
                snapshot: g.snapshot.clone(),
            }),
        })
    }

    pub fn read_raft_state(&self) -> Vec<u8> {
        self.state.lock().unwrap().raftstate.clone()
    }

    pub fn raft_state_size(&self) -> usize {
        self.state.lock().unwrap().raftstate.len()
    }

    /// Save both Raft state and the K/V snapshot as a single atomic action,
    /// to help avoid them getting out of sync.  Pass `None` (or an empty
    /// vec) as the snapshot if there is no snapshot yet.
    pub fn save(&self, raftstate: &[u8], snapshot: Option<&[u8]>) {
        let mut g = self.state.lock().unwrap();
        g.raftstate = raftstate.to_vec();
        if let Some(s) = snapshot {
            g.snapshot = s.to_vec();
        }
    }

    pub fn read_snapshot(&self) -> Vec<u8> {
        self.state.lock().unwrap().snapshot.clone()
    }

    pub fn snapshot_size(&self) -> usize {
        self.state.lock().unwrap().snapshot.len()
    }
}

/// n specifies the length of the random string to generate.
pub fn randstring(n: usize) -> String {
    const LETTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..n).map(|_| LETTERS[rng.gen_range(0..LETTERS.len())] as char).collect()
}

/// Like Go's kvtest.RandValue(n).
pub fn rand_value(n: usize) -> String {
    randstring(n)
}

/// Monotonic milliseconds since an arbitrary fixed point.  Used to
/// timestamp operations for the linearizability checker.
pub fn now_us(t0: std::time::Instant) -> u64 {
    t0.elapsed().as_micros() as u64
}

/// Global op counter, the analog of Config.OpInc() in the Go tester.
static OPS: AtomicU64 = AtomicU64::new(0);
pub fn op_inc() -> u64 {
    OPS.fetch_add(1, Ordering::SeqCst) + 1
}
pub fn op_count() -> u64 {
    OPS.load(Ordering::SeqCst)
}

/// Kill the test process if no test has re-armed the watchdog within
/// `secs` (the analog of `go test`'s default timeout, which cargo lacks).
/// Every call extends a process-global deadline, so a long test suite
/// keeps pushing it forward; a single hung test stops extending it and
/// gets aborted.  With tests running one-at-a-time
/// (RUST_TEST_THREADS=1) this kills just the hung test.
pub fn watchdog(secs: u64) {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    static DEADLINE_MS: AtomicU64 = AtomicU64::new(0);
    static STARTED: AtomicBool = AtomicBool::new(false);

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    let new_deadline = now_ms() + secs * 1000;
    let mut cur = DEADLINE_MS.load(Ordering::SeqCst);
    while cur < new_deadline {
        match DEADLINE_MS.compare_exchange(cur, new_deadline, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(x) => cur = x,
        }
    }

    if STARTED.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if now_ms() >= DEADLINE_MS.load(Ordering::SeqCst) {
                let what = std::thread::current().name().unwrap_or("<test>").to_string();
                eprintln!("***TEST TIMEOUT: process aborted after deadline; hung test: {}", what);
                eprintln!("***(a test that hangs usually means the lab isn't\n implemented yet, or your code is deadlocked)\n");
                std::process::abort();
            }
        });
    }
}

/// Helper for building deterministic i64 commands in tests.
pub fn serialize_cmd<C: Serialize>(c: &C) -> Vec<u8> {
    labgob::encode(c)
}
