//! kvtest: shared test helpers for the key/value labs (ports of Go's
//! `kvtest1` package: IKVClerk, Test, OpLog + linearizability checking).

use crate::config::{Clnt, Config, ServerGrp, Tgid};
use crate::porcupine::{self, KvInput, KvOutput, Operation};
use crate::op_inc;
use crate::randstring;
use kvsrv1_rpc as rpc;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The clerk interface implemented by the lab Clerks (Go: kvtest.IKVClerk).
pub trait IKVClerk: Send + Sync {
    fn get(&self, key: &str) -> (String, rpc::Tversion, rpc::Err);
    fn put(&self, key: &str, value: &str, version: rpc::Tversion) -> rpc::Err;
}

impl<T: IKVClerk + ?Sized> IKVClerk for Arc<T> {
    fn get(&self, key: &str) -> (String, rpc::Tversion, rpc::Err) {
        (**self).get(key)
    }
    fn put(&self, key: &str, value: &str, version: rpc::Tversion) -> rpc::Err {
        (**self).put(key, value, version)
    }
}

/// n specifies the length of the random value to generate.
pub fn rand_value(n: usize) -> String {
    crate::rand_value(n)
}

/// Make n keys "k0".."kn-1" (Go: kvtest.MakeKeys).
pub fn make_keys(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("k{}", i)).collect()
}

#[derive(Clone, Debug, Default)]
pub struct ClntRes {
    pub nok: i64,
    pub nmaybe: i64,
}

/// The recorded history of Get/Put operations (Go: kvtest.OpLog).
#[derive(Default)]
pub struct OpLog {
    ops: Mutex<Vec<Operation>>,
}

impl OpLog {
    pub fn append(&self, op: Operation) {
        self.ops.lock().unwrap().push(op);
    }
    pub fn read(&self) -> Vec<Operation> {
        self.ops.lock().unwrap().clone()
    }
}

/// Test helper state shared by the kvsrv/kvraft/shardkv tests
/// (Go: kvtest.Test).
pub struct KvTest {
    pub cfg: Arc<Config>,
    pub oplog: Arc<OpLog>,
    pub randomkeys: bool,
    t0: Instant,
}

/// The tester generously allows solutions to complete elections in one
/// second (much more than the paper's range of timeouts).
pub const ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);

impl KvTest {
    pub fn new(cfg: Arc<Config>, randomkeys: bool) -> Arc<KvTest> {
        Arc::new(KvTest {
            cfg,
            oplog: Arc::new(OpLog::default()),
            randomkeys,
            t0: Instant::now(),
        })
    }

    pub fn cleanup(&self) {
        self.cfg.end();
        self.cfg.cleanup();
    }

    // ----- operation wrappers that record history for linearizability ----

    pub fn get(&self, ck: &dyn IKVClerk, key: &str, cli: i64) -> (String, rpc::Tversion, rpc::Err) {
        let start = self.t0.elapsed().as_micros() as u64;
        let (val, ver, err) = ck.get(key);
        let end = self.t0.elapsed().as_micros() as u64;
        op_inc();
        self.oplog.append(Operation {
            input: KvInput {
                op: 0,
                key: key.to_string(),
                value: String::new(),
                version: 0,
            },
            output: KvOutput {
                value: val.clone(),
                version: ver,
                err: err.clone(),
            },
            call: start,
            ret: end,
            client: cli,
        });
        (val, ver, err)
    }

    pub fn put(
        &self,
        ck: &dyn IKVClerk,
        key: &str,
        value: &str,
        version: rpc::Tversion,
        cli: i64,
    ) -> rpc::Err {
        let start = self.t0.elapsed().as_micros() as u64;
        let err = ck.put(key, value, version);
        let end = self.t0.elapsed().as_micros() as u64;
        op_inc();
        self.oplog.append(Operation {
            input: KvInput {
                op: 1,
                key: key.to_string(),
                value: value.to_string(),
                version,
            },
            output: KvOutput {
                value: String::new(),
                version: 0,
                err: err.clone(),
            },
            call: start,
            ret: end,
            client: cli,
        });
        err
    }

    // ----- JSON helpers (Go: GetJson/PutJson) -----

    pub fn get_json<T: DeserializeOwned>(
        &self,
        ck: &dyn IKVClerk,
        key: &str,
        me: i64,
    ) -> (T, rpc::Tversion) {
        let (val, ver, err) = self.get(ck, key, me);
        if err != rpc::OK {
            panic!("{}: Get {:?} err {}", me, key, err);
        }
        (serde_json::from_str(&val).expect("unmarshal"), ver)
    }

    pub fn put_json<T: Serialize>(
        &self,
        ck: &dyn IKVClerk,
        key: &str,
        v: &T,
        ver: rpc::Tversion,
        me: i64,
    ) -> rpc::Err {
        let s = serde_json::to_string(v).expect("marshal");
        self.put(ck, key, &s, ver, me)
    }

    // ----- common checks -----

    pub fn check_get(&self, ck: &dyn IKVClerk, key: &str, value: &str, version: rpc::Tversion) {
        let (val, ver, err) = self.get(ck, key, 0);
        if err != rpc::OK {
            panic!("Get({:?}) returns error = {}", key, err);
        }
        if val != value || ver != version {
            panic!(
                "Get({:?}) returns ({:?}, {}) != ({:?}, {})",
                key, val, ver, value, version
            );
        }
    }

    /// Assumes different cks put to different keys.  Returns final version.
    pub fn put_at_least_once(
        &self,
        ck: &dyn IKVClerk,
        key: &str,
        value: &str,
        mut ver: rpc::Tversion,
        me: i64,
    ) -> rpc::Tversion {
        let ver_prev = ver;
        loop {
            let err = self.put(ck, key, value, ver, me);
            match err {
                e if e == rpc::OK => {
                    ver += 1;
                    break;
                }
                e if e == rpc::ERR_MAYBE || e == rpc::ERR_VERSION => {
                    ver += 1;
                }
                _ => {
                    if ver != 0 {
                        panic!("Put {:?} ver {} err {}", key, ver, err);
                    }
                }
            }
        }
        println!("Put({:?}, {:?}) completes; version {} -> {}", key, value, ver_prev, ver);
        ver
    }

    /// Check that the number of successful puts matches the server's
    /// version.  `prev` carries the accumulated ClntRes from earlier
    /// rounds of this test (Go: CheckPutConcurrent's `res *ClntRes`
    /// accumulator), since the server's version is cumulative.
    pub fn check_put_concurrent(
        &self,
        ck: &dyn IKVClerk,
        key: &str,
        rs: &[ClntRes],
        prev: &ClntRes,
        reliable: bool,
    ) {
        let (_e, ver0): (EntryV, rpc::Tversion) = self.get_json(ck, key, -1);
        let mut nok = prev.nok;
        let mut nmaybe = prev.nmaybe;
        for r in rs {
            nok += r.nok;
            nmaybe += r.nmaybe;
        }
        if reliable {
            if ver0 != nok as rpc::Tversion {
                panic!(
                    "Reliable: Wrong number of puts: server {} clnts ok={} maybe={}",
                    ver0, nok, nmaybe
                );
            }
        } else if ver0 > (nok + nmaybe) as rpc::Tversion {
            panic!(
                "Unreliable: Wrong number of puts: server {} clnts ok={} maybe={}",
                ver0, nok, nmaybe
            );
        }
    }

    /// Spawn nclnt clients running `f`, each with a clerk created by
    /// `mkc`; let them run for `secs`, then set the done flag and collect
    /// results (Go: SpawnClientsAndWait; the clerk-maker plays the role of
    /// Go's IClerkMaker interface).
    pub fn spawn_clients_with<M, F>(&self, nclnt: usize, secs: u64, mkc: M, f: F) -> Vec<ClntRes>
    where
        M: Fn() -> Arc<dyn IKVClerk> + Send + Sync + 'static,
        F: Fn(usize, Arc<dyn IKVClerk>, Arc<AtomicBool>) -> ClntRes + Send + Sync + 'static,
    {
        let done = Arc::new(AtomicBool::new(false));
        let f = Arc::new(f);
        let mkc = Arc::new(mkc);
        let mut handles = Vec::new();
        for cli in 0..nclnt {
            let f = Arc::clone(&f);
            let done = Arc::clone(&done);
            let mkc = Arc::clone(&mkc);
            handles.push(std::thread::spawn(move || {
                let ck = mkc();
                let r = f(cli, ck, done);
                r
            }));
        }
        std::thread::sleep(Duration::from_secs(secs));
        done.store(true, Ordering::SeqCst);
        handles
            .into_iter()
            .map(|h| h.join().expect("client thread panicked"))
            .collect()
    }

    /// One put attempt with version tracking; returns (new version, ok)
    /// (Go: kvtest.OnePut).
    pub fn one_put(
        &self,
        me: usize,
        ck: &dyn IKVClerk,
        key: &str,
        mut ver: rpc::Tversion,
    ) -> (rpc::Tversion, bool) {
        loop {
            let err = self.put_json(ck, key, &EntryV { id: me as i64, v: ver }, ver, me as i64);
            if !(err == rpc::OK || err == rpc::ERR_VERSION || err == rpc::ERR_MAYBE) {
                panic!("Wrong error {}", err);
            }
            let (e, ver0): (EntryV, rpc::Tversion) = self.get_json(ck, key, me as i64);
            if err == rpc::OK && ver0 == ver + 1 {
                // my put?
                if e.id != me as i64 && e.v != ver {
                    panic!("Wrong value {:?}", e);
                }
            }
            ver = ver0;
            if err == rpc::OK || err == rpc::ERR_MAYBE {
                return (ver, err == rpc::OK);
            }
        }
    }

    /// One of perhaps many clients doing OnePuts until `done` (Go:
    /// kvtest.OneClientPut).
    pub fn one_client_put(
        &self,
        cli: usize,
        ck: &Arc<dyn IKVClerk>,
        keys: &[String],
        done: Arc<AtomicBool>,
    ) -> ClntRes {
        let mut res = ClntRes::default();
        let mut verm: std::collections::HashMap<String, rpc::Tversion> =
            keys.iter().map(|k| (k.clone(), 0)).collect();
        while !done.load(Ordering::SeqCst) {
            let k = if self.randomkeys {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                keys[rng.gen_range(0..keys.len())].clone()
            } else {
                keys[0].clone()
            };
            let (ver, ok) = self.one_put(cli, ck, &k, verm[&k]);
            verm.insert(k, ver);
            if ok {
                res.nok += 1;
            } else {
                res.nmaybe += 1;
            }
        }
        res
    }

    /// Periodically repartition the group until `stop` (Go:
    /// kvtest.Partitioner).
    pub fn partitioner(self: &Arc<Self>, gid: Tgid, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
        use rand::Rng;
        let ts = Arc::clone(self);
        std::thread::spawn(move || {
            let grp: Arc<ServerGrp> = ts.cfg.group(gid);
            let mut rng = rand::thread_rng();
            while !stop.load(Ordering::SeqCst) {
                let n = grp.n;
                let mut p: Vec<Vec<usize>> = vec![Vec::new(), Vec::new()];
                for j in 0..n {
                    let part = rng.gen_range(0..2);
                    p[part].push(j);
                }
                grp.partition(&p[0], &p[1]);
                let pause = ELECTION_TIMEOUT + Duration::from_millis(rng.gen_range(0..200));
                std::thread::sleep(pause);
            }
        })
    }

    /// At each iteration the client appends (me, i) to key "k" using
    /// Get + versioned Put (Go: kvtest.OneClientAppend).
    pub fn one_client_append(
        &self,
        me: usize,
        ck: &Arc<dyn IKVClerk>,
        done: Arc<AtomicBool>,
    ) -> ClntRes {
        let mut nmay = 0i64;
        let mut nok = 0i64;
        let mut i = 0u64;
        while !done.load(Ordering::SeqCst) {
            loop {
                let (mut es, ver): (Vec<EntryN>, rpc::Tversion) =
                    self.get_json(ck, "k", me as i64);
                es.push(EntryN { id: me as i64, n: i });
                let err = self.put_json(ck, "k", &es, ver, me as i64);
                if err == rpc::OK {
                    nok += 1;
                    break;
                } else if err == rpc::ERR_MAYBE {
                    nmay += 1;
                    break;
                }
            }
            i += 1;
        }
        ClntRes { nok, nmaybe: nmay }
    }

    /// Check the value accumulated by OneClientAppend (Go:
    /// kvtest.CheckAppends).
    pub fn check_appends(&self, es: &[EntryN], nclnt: usize, rs: &[ClntRes], ver: rpc::Tversion) {
        let mut expect = vec![0u64; nclnt];
        let mut skipped = vec![0u64; nclnt];
        for e in es {
            if expect[e.id as usize] > e.n {
                panic!("{}: wrong, expecting {} but got {}", e.id, expect[e.id as usize], e.n);
            } else if expect[e.id as usize] == e.n {
                expect[e.id as usize] += 1;
            } else {
                let s = e.n - expect[e.id as usize];
                expect[e.id as usize] = e.n + 1;
                skipped[e.id as usize] += s;
            }
        }
        if es.len() as u64 + 1 != ver {
            panic!("{} appends in val != puts on server {}", es.len(), ver);
        }
        for c in 0..nclnt {
            if skipped[c] > rs[c].nmaybe as u64 {
                panic!(
                    "{}: skipped puts {} on server > {} maybe",
                    c, skipped[c], rs[c].nmaybe
                );
            }
            if expect[c] > (rs[c].nok + rs[c].nmaybe) as u64 {
                panic!(
                    "{}: {} puts on server > ok+maybe {}",
                    c,
                    expect[c],
                    rs[c].nok + rs[c].nmaybe
                );
            }
        }
    }

    /// Put n keys with random values, then check them (Go: SpreadPuts).
    pub fn spread_puts_size(
        &self,
        ck: &dyn IKVClerk,
        n: usize,
        valsz: usize,
    ) -> (Vec<String>, Vec<String>) {
        let ka = make_keys(n);
        let mut va = Vec::with_capacity(n);
        for i in 0..n {
            let v = randstring(valsz);
            ck.put(&ka[i], &v, 0);
            va.push(v);
        }
        for i in 0..n {
            self.check_get(ck, &ka[i], &va[i], 1);
        }
        (ka, va)
    }

    pub fn spread_puts(&self, ck: &dyn IKVClerk, n: usize) -> (Vec<String>, Vec<String>) {
        self.spread_puts_size(ck, n, 20)
    }

    /// Check the recorded history for linearizability (Go:
    /// kvtest.CheckPorcupine).  Assumes OK if the check times out.
    pub fn check_porcupine_t(&self, nsec: Duration) {
        let ops = self.oplog.read();
        let res = porcupine::check(&ops, nsec);
        match res {
            Some(true) => {}
            Some(false) => panic!("history is not linearizable!"),
            None => {
                println!("info: linearizability check timed out, assuming history is ok")
            }
        }
    }

    pub fn check_porcupine(&self) {
        self.check_porcupine_t(Duration::from_secs(1));
    }
}

/// Used by the JSON-based tests (Go: kvtest.EntryV).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EntryV {
    pub id: i64,
    pub v: rpc::Tversion,
}

/// Used by the append tests (Go: kvtest.entry/EntryN).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EntryN {
    pub id: i64,
    pub n: u64,
}

/// Helper for tests that need a Clnt (kept for parity with Go's
/// kvtest.TestClerk.Clnt).
pub fn clnt_call<Req, Rep>(
    clnt: &Clnt,
    server: &str,
    method: &str,
    args: &Req,
) -> Option<Rep>
where
    Req: Serialize,
    Rep: DeserializeOwned,
{
    clnt.call(server, method, args)
}
