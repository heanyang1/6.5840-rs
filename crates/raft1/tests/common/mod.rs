//! Test harness for the Raft tests (port of Go's raft1/test.go plus the
//! tester-side parts of raft1/server.go).  In the Go 2026 labs the servers
//! run in daemon processes; here they run as threads in the test process,
//! wired through the simulated labrpc network.  Everything else -- leader
//! checks, agreement checks, kills/restarts, partitions -- behaves like
//! the Go tester.

use labrpc::ClientEnd;
use raft1::api::ApplyMsg;
use raft1::raft::Raft;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use tester::config::Config;
use tester::Persister;

/// The tester generously allows solutions to complete elections in one
/// second (much more than the paper's range of timeouts).
pub const RAFT_ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);

/// The interval at which the test service snapshots (Go: SnapShotInterval).
pub const SNAPSHOT_INTERVAL: usize = 10;

struct SrvInfo {
    raft: Option<Weak<Raft<i64>>>,
    apply_err: String,
    logs: HashMap<i64, i64>, // copy of server's committed entries
}

impl Default for SrvInfo {
    fn default() -> Self {
        SrvInfo {
            raft: None,
            apply_err: String::new(),
            logs: HashMap::new(),
        }
    }
}

struct TestState {
    srvs: Vec<SrvInfo>,
    max_index: i64,
}

pub struct RaftTest {
    pub cfg: OnceLock<Arc<Config>>,
    pub n: usize,
    #[allow(dead_code)]
    pub snapshot: bool,
    finished: AtomicBool,
    mu: Mutex<TestState>,
}

impl RaftTest {
    pub fn make_test(n: usize, reliable: bool, snapshot: bool) -> Arc<RaftTest> {
    tester::watchdog(600);
        let ts = Arc::new(RaftTest {
            cfg: OnceLock::new(),
            n,
            snapshot,
            finished: AtomicBool::new(false),
            mu: Mutex::new(TestState {
                srvs: (0..n).map(|_| SrvInfo::default()).collect(),
                max_index: 0,
            }),
        });
        let make = make_raft_server(Arc::downgrade(&ts), snapshot);
        let cfg = Config::new("raft1", n, reliable, make);
        cfg.set_long_delays(true);
        ts.cfg.set(cfg).ok().unwrap();
        ts
    }

    pub fn cleanup(&self) {
        self.finished.store(true, Ordering::SeqCst);
        if let Some(cfg) = self.cfg.get() {
            cfg.end();
            cfg.cleanup();
        }
    }

    fn grp(&self) -> Arc<tester::ServerGrp> {
        self.cfg.get().unwrap().group(tester::GRP0)
    }

    /// The raft instance currently running on server i, if any.
    pub fn raft(&self, i: usize) -> Option<Arc<Raft<i64>>> {
        self.mu.lock().unwrap().srvs[i].raft.as_ref().and_then(|w| w.upgrade())
    }

    fn set_raft(&self, i: usize, rf: Weak<Raft<i64>>) {
        self.mu.lock().unwrap().srvs[i].raft = Some(rf);
    }

    /// Kill (crash) the listed servers.
    pub fn kill(&self, srvs: &[usize]) {
        self.grp().kill(srvs);
        let mut g = self.mu.lock().unwrap();
        for &s in srvs {
            g.srvs[s].raft = None;
        }
    }

    /// Restart the listed servers (fresh Raft instances from the same
    /// persisters).
    pub fn restart(&self, srvs: &[usize]) {
        let grp = self.grp();
        grp.start_servers(srvs);
    }

    pub fn check_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    // ------------------------- checker helpers (Go: test.go) ------------

    /// Is there a single leader among the connected servers?  Returns the
    /// leader's index.
    pub fn check_one_leader(&self) -> usize {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        for _ in 0..10 {
            let ms = 450 + rng.gen_range(0..100);
            std::thread::sleep(Duration::from_millis(ms));

            let mut leaders: HashMap<i64, Vec<usize>> = HashMap::new();
            for i in 0..self.n {
                if self.grp().is_connected(i) {
                    if let Some(rf) = self.raft(i) {
                        let (term, leader) = rf.get_state();
                        if leader {
                            leaders.entry(term).or_default().push(i);
                        }
                    }
                }
            }

            let mut last_term_with_leader = -1i64;
            for (term, ls) in leaders.iter() {
                assert!(ls.len() <= 1, "term {} has {} (>1) leaders", term, ls.len());
                if *term > last_term_with_leader {
                    last_term_with_leader = *term;
                }
            }
            if let Some(ls) = leaders.get(&last_term_with_leader) {
                if !ls.is_empty() {
                    return ls[0];
                }
            }
        }
        panic!("expected one leader, got none");
    }

    /// Check that all connected servers agree on the term.  Returns the
    /// term.
    pub fn check_terms(&self) -> i64 {
        let mut term = -1i64;
        for i in 0..self.n {
            if self.grp().is_connected(i) {
                if let Some(rf) = self.raft(i) {
                    let (xterm, _) = rf.get_state();
                    if term == -1 {
                        term = xterm;
                    } else {
                        assert_eq!(term, xterm, "servers disagree on term");
                    }
                }
            }
        }
        term
    }

    /// Check that none of the connected servers thinks it is the leader.
    pub fn check_no_leader(&self) {
        for i in 0..self.n {
            if self.grp().is_connected(i) {
                if let Some(rf) = self.raft(i) {
                    let (_, is_leader) = rf.get_state();
                    assert!(!is_leader, "unexpected leader = {}", i);
                }
            }
        }
    }

    pub fn check_no_agreement(&self, index: i64) {
        let (n, _) = self.n_committed(index);
        assert!(n == 0, "{} committed but no majority", n);
    }

    /// How many servers think a log entry is committed?  Returns the count
    /// and the command (if any).
    pub fn n_committed(&self, index: i64) -> (usize, Option<i64>) {
        let g = self.mu.lock().unwrap();
        let mut count = 0;
        let mut cmd: Option<i64> = None;
        for rs in &g.srvs {
            if !rs.apply_err.is_empty() {
                panic!("{}", rs.apply_err);
            }
            if let Some(cmd1) = rs.logs.get(&index) {
                if count > 0 && cmd != Some(*cmd1) {
                    panic!(
                        "committed values at index {} do not match ({:?} != {:?})",
                        index, cmd, cmd1
                    );
                }
                count += 1;
                cmd = Some(*cmd1);
            }
        }
        (count, cmd)
    }

    /// Called by the server's applier thread on each committed entry (Go:
    /// Test.CheckLogs).  Returns (err_msg, prevok).
    pub fn check_logs(&self, i: usize, m: &ApplyMsg<i64>) -> (String, bool) {
        let mut g = self.mu.lock().unwrap();
        let mut err_msg = String::new();
        let v = m.command;
        for (j, rs) in g.srvs.iter().enumerate() {
            if let Some(old) = rs.logs.get(&(m.command_index as i64)) {
                if Some(old) != v.as_ref() {
                    // Some server has already committed a different value
                    // for this entry!
                    err_msg = format!(
                        "commit index={} server={} {:?} != server={} {:?}",
                        m.command_index, i, v, j, old
                    );
                }
            }
        }
        let prevok = g.srvs[i].logs.contains_key(&(m.command_index as i64 - 1));
        g.srvs[i].logs.insert(m.command_index as i64, *v.as_ref().unwrap());
        if (m.command_index as i64) > g.max_index {
            g.max_index = m.command_index as i64;
        }
        (err_msg, prevok)
    }

    /// Called when a restarted server ingests its initial snapshot (Go:
    /// Test.IngestLog).
    pub fn ingest_log(&self, i: usize, xlog: HashMap<i64, i64>) {
        let mut g = self.mu.lock().unwrap();
        g.srvs[i].logs = xlog;
    }

    pub fn apply_err(&self, i: usize, err: String) {
        self.mu.lock().unwrap().srvs[i].apply_err = err.clone();
        eprintln!("apply error on server {}: {}", i, err);
    }

    /// Do a complete agreement (Go: Test.one).  It might choose the wrong
    /// leader initially and have to re-submit after giving up.  Entirely
    /// gives up after about 10 seconds.  If `retry` is false, calls
    /// start() only once, to simplify the early 3B tests.
    pub fn one(&self, cmd: i64, expected_servers: usize, retry: bool) -> i64 {
        let t0 = Instant::now();
        let mut starts = 0usize;
        while t0.elapsed() < Duration::from_secs(10) && !self.check_finished() {
            // Try all the servers, maybe one is the leader.
            let mut index: i64 = -1;
            for _ in 0..self.n {
                starts = (starts + 1) % self.n;
                if self.grp().is_connected(starts) {
                    if let Some(rf) = self.raft(starts) {
                        let (index1, _, ok) = rf.start(cmd);
                        if ok {
                            index = index1 as i64;
                            break;
                        }
                    }
                }
            }

            if index != -1 {
                // Somebody claimed to be the leader and to have submitted
                // our command; wait a while for agreement.
                let t1 = Instant::now();
                while t1.elapsed() < Duration::from_secs(2) {
                    let (nd, cmd1) = self.n_committed(index);
                    if nd > 0 && nd >= expected_servers && cmd1 == Some(cmd) {
                        // Committed, and it was the command we submitted.
                        return index;
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                if !retry {
                    panic!("one({}) failed to reach agreement", cmd);
                }
            } else {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        if !self.check_finished() {
            panic!("one({}) failed to reach agreement", cmd);
        }
        -1
    }

    /// Wait for at least n servers to commit, but don't wait forever.
    /// Returns the committed command, or -1 if the term moved on.
    pub fn wait(&self, index: i64, n: usize, start_term: i64) -> i64 {
        let mut to = Duration::from_millis(10);
        for _ in 0..30 {
            let (nd, _) = self.n_committed(index);
            if nd >= n {
                break;
            }
            std::thread::sleep(to);
            if to < Duration::from_secs(1) {
                to *= 2;
            }
            if start_term > -1 {
                for i in 0..self.n {
                    if let Some(rf) = self.raft(i) {
                        let (t, _) = rf.get_state();
                        if t > start_term {
                            // someone has moved on; can no longer
                            // guarantee that we'll "win".
                            return -1;
                        }
                    }
                }
            }
        }
        let (nd, cmd) = self.n_committed(index);
        assert!(
            nd >= n,
            "only {} decided for index {}; wanted {}",
            nd,
            index,
            n
        );
        cmd.unwrap_or(-1)
    }

    /// Total bytes of persisted Raft state in the group (Go:
    /// ts.g.RaftSize()).
    pub fn raft_size(&self) -> usize {
        self.grp().raft_size()
    }

    pub fn rpc_count(&self, i: usize) -> u64 {
        self.grp().rpc_count(i)
    }

    pub fn group(&self) -> Arc<tester::ServerGrp> {
        self.grp()
    }
}

// ---------------------------------------------------------------------------
// The server-side wrapper (Go: raft1/server.go's rfsrv + proxies), adapted
// to run in-process.
// ---------------------------------------------------------------------------

/// Build the tester's `make_server` closure for a group of Raft servers.
/// Each instance: creates a Raft, spawns an applier thread that feeds
/// committed entries to the harness (and takes snapshots in 3D mode), and
/// registers the "Raft" RPC service.
pub fn make_raft_server(harness: Weak<RaftTest>, snapshot: bool) -> tester::MakeServer {
    Arc::new(move |args: tester::ServerArgs| {
        let me = args.srv;
        let (tx, rx) = crossbeam_channel::unbounded::<ApplyMsg<i64>>();

        // Grab a copy of the initial snapshot, to avoid a possible race
        // with make() and the threads it starts (Go: newRfsrv).
        let mut log: HashMap<i64, i64> = HashMap::new();
        let mut last_applied: i64 = 0;
        let sn = args.persister.read_snapshot();
        if snapshot && !sn.is_empty() {
            // mimic the KV server and process the snapshot now.
            if let Some((last_included_index, xlog)) =
                labgob::decode::<(i64, Vec<Option<i64>>)>(&sn)
            {
                log.clear();
                for (j, v) in xlog.iter().enumerate() {
                    if let Some(v) = v {
                        log.insert(j as i64, *v);
                    }
                }
                last_applied = last_included_index;
                if let Some(h) = harness.upgrade() {
                    h.ingest_log(me, log.clone());
                }
            } else if let Some(h) = harness.upgrade() {
                h.apply_err(me, "failed to decode snapshot".to_string());
            }
        }

        let rf = raft1::raft::make(args.ends.clone(), me, Arc::clone(&args.persister), tx);
        if let Some(h) = harness.upgrade() {
            h.set_raft(me, Arc::downgrade(&rf));
        }

        // The applier thread: reads committed entries and reports them to
        // the harness (Go: rfsrv.applier / applierSnap).
        let harness2 = harness.clone();
        let rf2 = Arc::clone(&rf);
        let do_snapshot = snapshot;
        std::thread::spawn(move || {
            for m in rx.iter() {
                let mut err_msg = String::new();
                let h = match harness2.upgrade() {
                    Some(h) => h,
                    None => return, // test over
                };
                if m.snapshot_valid {
                    // ingest snapshot (Go: ingestSnap + IngestLog)
                    match labgob::decode::<(i64, Vec<Option<i64>>)>(&m.snapshot) {
                        Some((last_included_index, xlog)) => {
                            if m.snapshot_index != 0
                                && m.snapshot_index as i64 != last_included_index
                            {
                                err_msg = format!(
                                    "server {} snapshot doesn't match m.SnapshotIndex",
                                    me
                                );
                            } else {
                                let mut map = HashMap::new();
                                for (j, v) in xlog.iter().enumerate() {
                                    if let Some(v) = v {
                                        map.insert(j as i64, *v);
                                    }
                                }
                                last_applied = last_included_index;
                                log = map;
                                h.ingest_log(me, log.clone());
                            }
                        }
                        None => {
                            err_msg = "failed to decode snapshot".to_string();
                        }
                    }
                } else if m.command_valid {
                    if do_snapshot {
                        if m.command_index as i64 != last_applied + 1 {
                            err_msg = format!(
                                "server {} apply out of order, expected index {}, got {}",
                                me,
                                last_applied + 1,
                                m.command_index
                            );
                        }
                        if err_msg.is_empty() {
                            let (e, prevok) = h.check_logs(me, &m);
                            if e != "ErrRPC" && m.command_index > 1 && !prevok {
                                err_msg = format!(
                                    "server {} apply out of order {}",
                                    me, m.command_index
                                );
                            } else if !e.is_empty() {
                                err_msg = e;
                            }
                        }
                    } else {
                        let (e, prevok) = h.check_logs(me, &m);
                        if m.command_index > 1 && !prevok {
                            err_msg =
                                format!("server {} apply out of order {}", me, m.command_index);
                        } else if !e.is_empty() {
                            err_msg = e;
                        }
                    }

                    log.insert(m.command_index as i64, *m.command.as_ref().unwrap());
                    last_applied = m.command_index as i64;

                    if do_snapshot
                        && (m.command_index + 1) % SNAPSHOT_INTERVAL == 0
                        && err_msg.is_empty()
                    {
                        // Take a snapshot of the "service state" (the whole
                        // log so far) and hand it to Raft.
                        let xlog: Vec<Option<i64>> =
                            (0..=m.command_index).map(|j| log.get(&(j as i64)).copied()).collect();
                        let snap = labgob::encode(&(m.command_index as i64, &xlog));
                        rf2.snapshot(m.command_index, &snap);
                    }
                } else {
                    // Ignore other types of ApplyMsg.
                }
                if !err_msg.is_empty() {
                    h.apply_err(me, err_msg);
                    // Keep reading after error so that Raft doesn't block
                    // holding locks...
                }
            }
        });

        tester::ServerBox::new()
            .with_service(raft1::raft::raft_service(&rf))
            .with_kill(move || rf.kill())
    })
}


