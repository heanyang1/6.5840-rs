//! Harness for the rsm (4A) tests (port of Go's kvraft1/rsm/test.go).

use kvraft1::rsm::server::{new_rsm_srv, Rep, Req, RsmSrv};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use kvraft1::rsm::server::{Inc, Null};
use tester::config::Config;

pub const NSRV: usize = 3;
pub const NSEC: u64 = 10;
pub const GID: i64 = tester::GRP0;

pub struct RsmTest {
    pub cfg: Arc<Config>,
    leader: AtomicUsize,
}

impl RsmTest {
    pub fn make_test(maxraftstate: i64) -> Arc<RsmTest> {
    tester::watchdog(600);
        let maxrs = maxraftstate;
        let cfg = Config::new(
            "rsm1",
            NSRV,
            true,
            Arc::new(move |args| {
                let name = format!("server-{}-{}", args.gid, args.srv);
                let (s, box_) = new_rsm_srv(args, maxrs);
                rsm_registry::put(&name, s);
                box_
            }),
        );
        Arc::new(RsmTest {
            cfg,
            leader: AtomicUsize::new(0),
        })
    }

    pub fn cleanup(&self) {
        self.cfg.end();
        self.cfg.cleanup();
    }

    pub fn group(&self) -> Arc<tester::ServerGrp> {
        self.cfg.group(GID)
    }

    /// The rsm server handle for server i, if it is running.  Since the
    /// harness creates instances inside make_server, we track them via a
    /// global registry keyed by server name.
    pub fn srv(&self, i: usize) -> Option<Arc<kvraft1::rsm::server::RsmSrv>> {
        rsm_registry::get(&self.group().srv_name(i))
    }

    pub fn kill(&self, srvs: &[usize]) {
        self.group().kill(srvs);
        for &s in srvs {
            rsm_registry::remove(&self.group().srv_name(s));
        }
    }

    pub fn restart(&self, srvs: &[usize]) {
        self.group().start_servers(srvs);
        self.group().connect_all();
    }

    /// Try all servers (optionally restricted to partition `p`) for NSEC
    /// seconds, until one accepts the request as leader.  Returns None on
    /// timeout, like Go's onePartition returning nil.
    pub fn one_partition_opt(&self, p: Option<&[usize]>, req: Req) -> Option<Rep> {
        let t0 = Instant::now();
        let mut index = self.get_leader();
        while t0.elapsed() < Duration::from_secs(NSEC) {
            for _ in 0..NSRV {
                if index >= NSRV {
                    panic!("index {}", index);
                }
                let in_p = match p {
                    None => true,
                    Some(p) => p.contains(&index),
                };
                if in_p && self.group().is_connected(index) {
                    if let Some(s) = self.srv(index) {
                        let (err, rep) = s.submit(req.clone());
                        if err == kvraft1::rpc::OK {
                            self.leader.store(index, Ordering::SeqCst);
                            return rep;
                        }
                    }
                }
                index = (index + 1) % NSRV;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        None
    }

    pub fn one_inc(&self) -> i64 {
        match self.one_partition_opt(None, Req::Inc(Inc)) {
            Some(Rep::Inc(r)) => r.n,
            // Go returns an empty IncRep on timeout; the callers' value
            // checks then fail naturally.
            _ => kvraft1::rsm::server::IncRep { n: 0 }.n,
        }
    }

    pub fn one_null(&self) {
        // Go's oneNull tolerates a timeout (returns an empty NullRep).
        let _ = self.one_partition_opt(None, Req::Null(Null));
    }

    pub fn check_counter(&self, v: i64, nsrv: usize) {
        let mut to = Duration::from_millis(10);
        let mut n = 0;
        for _ in 0..30 {
            n = self.count_value(v);
            if n >= nsrv {
                return;
            }
            std::thread::sleep(to);
            if to < Duration::from_secs(1) {
                to *= 2;
            }
        }
        panic!("checkCounter: only {} srvs have {} instead of {}", n, v, nsrv);
    }

    pub fn count_value(&self, v: i64) -> usize {
        let mut i = 0;
        for s in 0..NSRV {
            if let Some(srv) = self.srv(s) {
                if srv.get_counter() == v {
                    i += 1;
                }
            }
        }
        i
    }

    pub fn disconnect_leader(&self) -> usize {
        let l = self.get_leader();
        self.group().disconnect_all(l);
        l
    }

    pub fn connect(&self, i: usize) {
        self.group().connect_one(i);
    }

    pub fn get_leader(&self) -> usize {
        self.leader.load(Ordering::SeqCst)
    }
}

/// A process-global registry mapping server names to their running rsm
/// test-server handles, so the harness can reach them.
pub mod rsm_registry {
    use kvraft1::rsm::server::RsmSrv;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};

    fn reg() -> &'static Mutex<HashMap<String, Arc<RsmSrv>>> {
        static REG: OnceLock<Mutex<HashMap<String, Arc<RsmSrv>>>> = OnceLock::new();
        REG.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub fn put(name: &str, s: Arc<RsmSrv>) {
        reg().lock().unwrap().insert(name.to_string(), s);
    }
    pub fn get(name: &str) -> Option<Arc<RsmSrv>> {
        reg().lock().unwrap().get(name).cloned()
    }
    pub fn remove(name: &str) {
        reg().lock().unwrap().remove(name);
    }
}

