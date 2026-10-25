//! Shared test setup for the kvraft1 tests (port of Go's kvraft1/test.go
//! plus the tester plumbing).

use kvraft1::client;
use std::sync::Arc;
use tester::config::{Clnt, Config, GRP0};
use tester::kvtest::{IKVClerk, KvTest};

pub struct KvRaftTest {
    pub kvtest: Arc<KvTest>,
    pub gid: i64,
    pub nclients: usize,
    #[allow(dead_code)]
    pub nservers: usize,
    pub crash: bool,
    pub partitions: bool,
    pub maxraftstate: i64,
    pub randomkeys: bool,
}

pub fn make_test(
    part: &str,
    nclients: usize,
    nservers: usize,
    reliable: bool,
    crash: bool,
    partitions: bool,
    maxraftstate: i64,
    randomkeys: bool,
) -> Arc<KvRaftTest> {
    tester::watchdog(600);
    let maxrs = maxraftstate;
    let cfg = Config::new(
        "kvraft1",
        nservers,
        reliable,
        Arc::new(move |args| {
            let (_kv, box_) = kvraft1::server::start_kvserver(args, maxrs);
            box_
        }),
    );
    cfg.set_long_delays(true);
    let kvtest = KvTest::new(cfg, randomkeys);
    let ts = Arc::new(KvRaftTest {
        kvtest,
        gid: GRP0,
        nclients,
        nservers,
        crash,
        partitions,
        maxraftstate,
        randomkeys,
    });
    let title = make_title(ts.crash, ts.partitions, ts.maxraftstate, ts.randomkeys, ts.nclients, part);
    ts.kvtest.cfg.begin(&title);
    ts
}

fn make_title(
    crash: bool,
    partitions: bool,
    maxraftstate: i64,
    randomkeys: bool,
    nclients: usize,
    part: &str,
) -> String {
    let mut title = String::from("Test: ");
    if crash {
        title.push_str("restarts, ");
    }
    if partitions {
        title.push_str("partitions, ");
    }
    if maxraftstate != -1 {
        title.push_str("snapshots, ");
    }
    if randomkeys {
        title.push_str("random keys, ");
    }
    if nclients > 1 {
        title.push_str("many clients");
    } else {
        title.push_str("one client");
    }
    title.push_str(&format!(" ({})", part));
    title
}

impl KvRaftTest {
    pub fn group(&self) -> Arc<tester::ServerGrp> {
        self.kvtest.cfg.group(self.gid)
    }

    pub fn killall(&self) {
        self.group().shutdown();
    }

    pub fn restartall(&self) {
        let all = self.group().all();
        self.group().start_servers(&all);
    }

    /// A clerk connected to all servers.
    pub fn make_clerk(&self) -> Arc<dyn IKVClerk> {
        let clnt = self.kvtest.cfg.make_client();
        let servers = self.group().srv_names();
        client::make_clerk(clnt, servers)
    }

    /// The concrete clerk (tests that need the leader index).
    pub fn make_clerk_raw(&self) -> Arc<client::Clerk> {
        let clnt = self.kvtest.cfg.make_client();
        let servers = self.group().srv_names();
        client::make_clerk(clnt, servers)
    }

    /// A clerk connected only to the servers in `to`.  Also returns the
    /// underlying Clnt so tests can re-connect it (Go's
    /// TestClerk.Clnt.ConnectAll).
    pub fn make_clerk_to(&self, to: &[usize]) -> (Arc<dyn IKVClerk>, Arc<Clnt>) {
        let names = self.group().srv_names_to(to);
        let clnt = self.kvtest.cfg.make_client_to(&names);
        let servers = self.group().srv_names();
        (client::make_clerk(clnt.clone(), servers), clnt)
    }

    pub fn cleanup(&self) {
        self.kvtest.cleanup();
    }

    /// Clerk-maker for spawn_clients_with.
    pub fn clerk_maker(self: &Arc<Self>) -> impl Fn() -> Arc<dyn IKVClerk> + Send + Sync + 'static {
        let ts = Arc::clone(self);
        move || ts.make_clerk()
    }

    /// The common 4B/4C scenario: one or more clients submitting
    /// Puts/Gets for some period of time, then checks (Go:
    /// kvraft GenericTest).
    pub fn generic_test(self: &Arc<Self>) {
        const NITER: usize = 3;
        const NSEC: u64 = 1;
        const NKEYS: usize = 100;

        let ck = self.make_clerk();
        // Cumulative results across iterations (Go's `res *ClntRes`).
        let res_clnt: Arc<std::sync::Mutex<tester::kvtest::ClntRes>> =
            Arc::new(std::sync::Mutex::new(tester::kvtest::ClntRes::default()));
        let default_key: Vec<String> = if self.randomkeys {
            tester::kvtest::make_keys(NKEYS)
        } else {
            vec!["k".to_string()]
        };
        for _i in 0..NITER {
            let ts = Arc::clone(self);
            let keys = default_key.clone();
            let h = {
                let ts = Arc::clone(&ts);
                let mkc = ts.clerk_maker();
                let res_clnt = Arc::clone(&res_clnt);
                std::thread::spawn(move || {
                    let kvtest = Arc::clone(&ts.kvtest);
                    let kvtest2 = Arc::clone(&kvtest);
                    let keys2 = keys.clone();
                    let rs = kvtest.spawn_clients_with(
                        ts.nclients,
                        NSEC,
                        mkc,
                        move |cli, ck, done| kvtest2.one_client_put(cli, &ck, &keys2, done),
                    );
                    if !ts.randomkeys {
                        let reliable = ts.kvtest.cfg.is_reliable() && !ts.crash && !ts.partitions;
                        let ck = ts.make_clerk();
                        let acc = res_clnt.lock().unwrap().clone();
                        kvtest.check_put_concurrent(&*ck, &keys[0], &rs, &acc, reliable);
                    }
                    rs
                })
            };

            let mut partitioner = None;
            if self.partitions {
                // Allow the clients to perform some operations without
                // interruption
                std::thread::sleep(std::time::Duration::from_secs(1));
                let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let kvtest = Arc::clone(&self.kvtest);
                partitioner = Some((
                    kvtest.partitioner(self.gid, Arc::clone(&stop)),
                    stop,
                ));
            }

            let rs = h.join().expect("clients panicked");
            // Accumulate results across iterations, like the Go test's
            // `res` accumulator.
            {
                let mut acc = res_clnt.lock().unwrap();
                for r in &rs {
                    acc.nok += r.nok;
                    acc.nmaybe += r.nmaybe;
                }
            }

            self.kvtest.check_porcupine();

            if let Some((ph, stop)) = partitioner {
                stop.store(true, std::sync::atomic::Ordering::SeqCst);
                ph.join().unwrap();
                // reconnect network and submit a request.  A client may
                // have submitted a request in a minority.  That request
                // won't return until that server discovers a new term has
                // started.
                self.group().connect_all();
                // wait for a while so that we have a new term
                std::thread::sleep(tester::kvtest::ELECTION_TIMEOUT);
            }

            if self.crash {
                self.killall();
                // Wait for a while for servers to shutdown, since shutdown
                // isn't a real crash and isn't instantaneous
                std::thread::sleep(tester::kvtest::ELECTION_TIMEOUT);
                // crash and re-start all
                self.restartall();
            }

            if self.maxraftstate > 0 {
                // Check maximum after the servers have processed all client
                // requests and had time to checkpoint.
                let sz = self.group().raft_size();
                assert!(
                    sz <= (8 * self.maxraftstate) as usize,
                    "logs were not trimmed ({} > 8*{})",
                    sz,
                    self.maxraftstate
                );
            }
            if self.maxraftstate < 0 {
                // Check that snapshots are not used.
                let ssz = self.group().snapshot_size();
                assert!(
                    ssz == 0,
                    "snapshot too large ({}), should not be used when maxraftstate = {}",
                    ssz,
                    self.maxraftstate
                );
            }
        }
        let _ = ck;
    }

    /// Check that ops are committed fast enough, better than 1 per
    /// heartbeat interval (Go: GenericTestSpeed).
    pub fn generic_test_speed(self: &Arc<Self>) {
        const NUM_OPS: usize = 1000;

        let ck = self.make_clerk();

        // wait until first op completes, so we know a leader is elected
        // and KV servers are ready to process client requests
        ck.get("x");

        let start = std::time::Instant::now();
        for i in 0..NUM_OPS {
            let err = ck.put("k", &i.to_string(), i as u64);
            assert!(err == kvraft1::rpc::OK, "Put err {}", err);
        }
        let dur = start.elapsed();

        let (_, ver, err) = ck.get("k");
        assert!(err == kvraft1::rpc::OK, "Get err {}", err);
        assert_eq!(ver, NUM_OPS as u64, "Get too few ops {}", ver);

        // heartbeat interval should be ~ 100 ms; require at least 3 ops per
        // interval
        const TIME_PER_OP: std::time::Duration = std::time::Duration::from_millis(100 / 3);
        println!("dur {:?} {:?}", dur / NUM_OPS as u32, TIME_PER_OP);
        assert!(
            dur <= TIME_PER_OP * NUM_OPS as u32,
            "Operations completed too slowly {:?}/op > {:?}/op",
            dur / NUM_OPS as u32,
            TIME_PER_OP
        );
    }
}

