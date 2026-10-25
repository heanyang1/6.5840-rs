//! Shared test setup for the shardkv1 tests (port of Go's
//! shardkv1/test.go).

use shardkv1::client;
use shardkv1::shardcfg;
use shardkv1::shardctrler;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tester::config::{Config, GRP0};
use tester::kvtest::{IKVClerk, KvTest};

pub const NSRV: usize = 3; // servers per group
pub const INTERGRPDELAY_MS: u64 = 200; // time between group changes
pub const NGRP: usize = 8;
pub const NKEYS: usize = 5 * shardcfg::NSHARDS;

pub struct ShardTest {
    pub kvtest: Arc<KvTest>,
    sck: OnceLock<Arc<shardctrler::ShardCtrler>>,
    pub partition: bool,
    pub maxraftstate: i64,
    ngid: Arc<AtomicI64>,
}

pub fn make_test(part: &str, reliable: bool) -> Arc<ShardTest> {
    make_test_max_raft(part, reliable, false, -1)
}

pub fn make_test_leases(part: &str, reliable: bool) -> Arc<ShardTest> {
    make_test_max_raft(part, reliable, true, -1)
}

pub fn make_test_max_raft(part: &str, reliable: bool, partition: bool, maxraftstate: i64) -> Arc<ShardTest> {
    tester::watchdog(600);
    let maxrs = maxraftstate;
    let cfg = Config::new(
        "kvsrv1",
        1,
        reliable,
        Arc::new(kvsrv1::server::start_server),
    );
    let kvtest = KvTest::new(cfg, false);
    let ts = Arc::new(ShardTest {
        kvtest,
        sck: OnceLock::new(),
        partition,
        maxraftstate: maxrs,
        ngid: Arc::new(AtomicI64::new(shardcfg::GID1 + 1)),
    });
    ts.kvtest.cfg.begin(part);
    ts
}

impl ShardTest {
    pub fn group(&self, gid: i64) -> Arc<tester::ServerGrp> {
        self.kvtest.cfg.group(gid)
    }

    fn shardgrp_make_server(&self) -> tester::MakeServer {
        ShardTest::shardgrp_make_server_max(self.maxraftstate)
    }

    /// The concrete shardkv clerk (tests need get_clerk()).
    pub fn make_clerk_raw(&self) -> Arc<client::Clerk> {
        let clnt = self.kvtest.cfg.make_client();
        let sck = self.make_shard_ctrler();
        client::make_clerk(clnt, sck)
    }

    pub fn make_clerk(&self) -> Arc<dyn IKVClerk> {
        self.make_clerk_raw()
    }

    pub fn make_shard_ctrler(&self) -> Arc<shardctrler::ShardCtrler> {
        let clnt = self.kvtest.cfg.make_client();
        shardkv1::shardctrler::make_shard_ctrler(clnt)
    }

    /// (controller, its client) so tests can partition the controller.
    pub fn make_shard_ctrler_clnt(&self) -> (Arc<shardctrler::ShardCtrler>, Arc<tester::Clnt>) {
        let clnt = self.kvtest.cfg.make_client();
        (shardkv1::shardctrler::make_shard_ctrler(Arc::clone(&clnt)), clnt)
    }

    pub fn new_gid(&self) -> i64 {
        self.ngid.fetch_add(1, Ordering::SeqCst)
    }

    /// The pieces needed by controller threads ('static clones).
    pub fn shared(&self) -> (Arc<Config>, i64, Arc<AtomicI64>) {
        (
            Arc::clone(&self.kvtest.cfg),
            self.maxraftstate,
            Arc::clone(&self.ngid),
        )
    }

    pub fn shardgrp_make_server_max(maxraftstate: i64) -> tester::MakeServer {
        Arc::new(move |args| {
            let (_s, box_) = shardkv1::shardgrp::server::start_server_shard_grp(args, maxraftstate);
            box_
        })
    }

    pub fn groups(&self, n: usize) -> Vec<i64> {
        (0..n).map(|_| self.new_gid()).collect()
    }

    /// Set up the KV service with one group GID1, which should initialize
    /// itself to own all shards (Go: setupKVService).
    pub fn setup_kv_service(&self) -> i64 {
        let sck = self.make_shard_ctrler();
        let mut scfg = shardcfg::make_shard_config();
        let maxrs = self.maxraftstate;
        let ms = self.shardgrp_make_server();
        self.kvtest.cfg.make_group_with(shardcfg::GID1, NSRV, maxrs, ms);
        let mut servers: HashMap<i64, Vec<String>> = HashMap::new();
        servers.insert(
            shardcfg::GID1,
            self.group(shardcfg::GID1).srv_names(),
        );
        assert!(scfg.join_balance(&servers), "join_balance failed");
        sck.init_config(&scfg);
        self.sck.set(sck).ok().unwrap();
        shardcfg::GID1
    }

    pub fn shard_ctrler(&self) -> Arc<shardctrler::ShardCtrler> {
        self.sck.get().cloned().expect("setup_kv_service not called")
    }

    pub fn check_member(&self, sck: &shardctrler::ShardCtrler, gid: i64) -> bool {
        sck.query().is_member(gid)
    }

    /// Add group gid (Go: join).
    pub fn join(&self, sck: &shardctrler::ShardCtrler, gid: i64, srvs: Vec<String>) {
        let cfg = sck.query();
        let mut newcfg = cfg.copy();
        let mut servers: HashMap<i64, Vec<String>> = HashMap::new();
        servers.insert(gid, srvs);
        assert!(newcfg.join_balance(&servers), "join: group {} is already present", gid);
        sck.change_config_to(&newcfg);
    }

    /// Join groups, starting each group's servers first; returns false if
    /// the group isn't a member afterwards (Go: joinGroups).
    pub fn join_groups(self: &Arc<Self>, sck: &Arc<shardctrler::ShardCtrler>, gids: &[i64]) -> bool {
        for &gid in gids {
            let ms = self.shardgrp_make_server();
            self.kvtest
                .cfg
                .make_group_with(gid, NSRV, self.maxraftstate, ms);
            self.join(sck, gid, self.group(gid).srv_names());
            if !self.check_member(sck, gid) {
                return false;
            }
            std::thread::sleep(Duration::from_millis(INTERGRPDELAY_MS));
        }
        true
    }

    /// Group gid leaves (Go: leave).
    pub fn leave(&self, sck: &shardctrler::ShardCtrler, gid: i64) {
        let cfg = sck.query();
        let mut newcfg = cfg.copy();
        assert!(newcfg.leave_balance(&[gid]), "leave: group {} is already not present", gid);
        sck.change_config_to(&newcfg);
    }

    pub fn leave_groups(self: &Arc<Self>, sck: &Arc<shardctrler::ShardCtrler>, gids: &[i64]) -> bool {
        for &gid in gids {
            self.leave(sck, gid);
            if self.check_member(sck, gid) {
                return false;
            }
            self.kvtest.cfg.exit_group(gid);
            std::thread::sleep(Duration::from_millis(INTERGRPDELAY_MS));
        }
        true
    }

    /// Shut down the leader of a shardgrp (Go: shutdownLeader).
    pub fn shutdown_leader(&self, ck: &client::Clerk, gid: i64) -> usize {
        let c = ck
            .get_clerk(gid)
            .unwrap_or_else(|| panic!("shutdownLeader: no shardgrp clerk for {}", gid));
        let l = c.leader();
        self.group(gid).shutdown_server(l);
        l
    }

    /// Make sure that the data really is sharded by shutting down one
    /// group and checking that some Get()s don't succeed (Go:
    /// checkShutdownSharding).
    pub fn check_shutdown_sharding(&self, down: i64, ka: &[String], va: &[String]) {
        const NSEC: u64 = 2;

        self.group(down).shutdown();

        let n = ka.len();
        let (tx, rx) = std::sync::mpsc::channel::<Result<(), String>>();
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut handles = Vec::new();
        for (xi, key) in ka.iter().enumerate() {
            let done = Arc::clone(&done);
            let tx = tx.clone();
            let ck1 = self.make_clerk();
            let key = key.clone();
            let want = va[xi].to_string();
            handles.push(std::thread::spawn(move || {
                let (v, _, _) = ck1.get(&key);
                if done.load(Ordering::SeqCst) {
                    return;
                }
                if v != want {
                    let _ = tx.send(Err(format!(
                        "Get({:?}): expected:\n{}\nreceived:\n{}",
                        key, want, v
                    )));
                } else {
                    let _ = tx.send(Ok(()));
                }
            }));
        }
        drop(tx);

        let mut ndone = 0;
        let deadline = std::time::Instant::now() + Duration::from_secs(NSEC);
        while !done.load(Ordering::SeqCst) {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(Ok(())) => ndone += 1,
                Ok(Err(e)) => panic!("{}", e),
                Err(_) => {}
            }
            if std::time::Instant::now() > deadline {
                done.store(true, Ordering::SeqCst);
            }
        }
        done.store(true, Ordering::SeqCst);
        // Detach the (possibly still-retrying) client goroutines, exactly
        // like the Go test abandons them.
        drop(handles);

        assert!(
            ndone < n,
            "expected less than {} completions with shard {} down",
            n,
            down
        );

        // bring the crashed shard/group back to life.
        let all = self.group(down).all();
        self.group(down).start_servers(&all);
    }

    pub fn cleanup(&self) {
        self.kvtest.cleanup();
    }
}




// --------------------------------------------------------------------------
// Controller helpers (port of Go's partitionCtrler / concurCtrler).
//
// The Go test runs the controller in a goroutine; the Rust port runs it on
// a spawned thread given 'static clones (config, controller, gid
// allocator).  The thread publishes its progress (current gid + whether it
// is in the join or the leave) through shared atomics.
// --------------------------------------------------------------------------

pub const JOIN: i64 = 1;
pub const LEAVE: i64 = 2;

pub struct CtrlerProgress {
    pub gid: AtomicI64,
    pub state: AtomicI64,
}

/// One controller's work loop: repeatedly join a fresh group and (if the
/// join stuck) leave it again, until deposed or `stop` is set.  Progress
/// is published to `prog`.
pub fn controller_loop(
    cfg: Arc<Config>,
    sck: Arc<shardctrler::ShardCtrler>,
    ngid: Arc<AtomicI64>,
    maxraftstate: i64,
    stop: Arc<AtomicBool>,
    prog: Arc<CtrlerProgress>,
) {
    loop {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let g = ngid.fetch_add(1, Ordering::SeqCst);
        prog.gid.store(g, Ordering::SeqCst);
        prog.state.store(JOIN, Ordering::SeqCst);

        let ms = ShardTest::shardgrp_make_server_max(maxraftstate);
        cfg.make_group_with(g, NSRV, maxraftstate, ms);
        let mut servers = HashMap::new();
        servers.insert(g, cfg.group(g).srv_names());
        let mut scfg = sck.query();
        if !scfg.join_balance(&servers) {
            return;
        }
        sck.change_config_to(&scfg);
        if !sck.query().is_member(g) {
            // deposed during join
            return;
        }

        prog.state.store(LEAVE, Ordering::SeqCst);
        let mut scfg = sck.query();
        if !scfg.leave_balance(&[g]) {
            return;
        }
        sck.change_config_to(&scfg);
        if sck.query().is_member(g) {
            // deposed during leave
            return;
        }
    }
}

impl ShardTest {
    /// Run one controller, then partition it after some time.  Run another
    /// controller that must finish the first controller's unfinished shard
    /// moves (Go: partitionCtrler).
    pub fn partition_ctrler(self: &Arc<Self>, ck: &dyn IKVClerk, gid: i64, ka: &[String], va: &[String]) {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let (sck, clnt) = self.make_shard_ctrler_clnt();
        sck.init_controller();

        let num = self.shard_ctrler().query().num;

        // Run the first controller in a thread, publishing its progress.
        let (cfg, maxraftstate, ngid) = self.shared();
        let stop = Arc::new(AtomicBool::new(false));
        let prog = Arc::new(CtrlerProgress {
            gid: AtomicI64::new(0),
            state: AtomicI64::new(0),
        });
        let h = {
            let cfg = Arc::clone(&cfg);
            let sck1 = Arc::clone(&sck);
            let ngid = Arc::clone(&ngid);
            let stop1 = Arc::clone(&stop);
            let prog1 = Arc::clone(&prog);
            std::thread::spawn(move || {
                controller_loop(cfg, sck1, ngid, maxraftstate, stop1, prog1)
            })
        };

        // let sck run for a little while
        std::thread::sleep(Duration::from_millis(1000));

        let r: u64 = rng.gen_range(0..400);
        std::thread::sleep(Duration::from_millis(r));

        // where is the controller now?  (read before we block it)
        let moving_gid = prog.gid.load(Ordering::SeqCst);
        let moving_state = prog.state.load(Ordering::SeqCst);

        self.group(gid).shutdown();

        // sleep for a while to get sck stuck in join or leave, because gid
        // is down
        std::thread::sleep(Duration::from_millis(1000));

        // partition controller
        clnt.disconnect_all();

        if self.partition {
            // wait a while before restarting shardgrp `gid`
            std::thread::sleep(Duration::from_secs(1));
        }

        self.group(gid).start_all();

        // start new controller to pick up where sck left off
        let (sck0, clnt0) = self.make_shard_ctrler_clnt();
        sck0.init_controller();
        let cfg = sck0.query();

        assert!(
            cfg.num > num,
            "didn't recover; expected {} > {}",
            num,
            cfg.num
        );

        let s = if moving_state == LEAVE { "leave" } else { "join" };
        if moving_gid != 0 {
            let present = cfg.is_member(moving_gid);
            assert!(
                !((moving_state == JOIN && !present) || (moving_state == LEAVE && present)),
                "didn't recover {} correctly after {}",
                moving_gid,
                s
            );

            if moving_state == JOIN && present {
                // cleanup if disconnected after join but before leave
                self.leave_groups(&sck0, &[moving_gid]);
            }
        }

        if self.partition {
            // reconnect old controller, which should bail out, because it
            // has been superseded.
            clnt.connect_all();
            std::thread::sleep(Duration::from_millis(100));
        }

        // Release the first controller (it should already be done or
        // deposed; the stop flag makes sure the thread terminates).
        stop.store(true, Ordering::SeqCst);
        let _ = h;

        for (i, k) in ka.iter().enumerate() {
            self.kvtest.check_get(ck, k, &va[i], 1);
        }

        self.kvtest.cfg.delete_client(&clnt);
        self.kvtest.cfg.delete_client(&clnt0);
    }

    /// Run several controllers concurrently joining/leaving groups (Go:
    /// concurCtrler).
    pub fn concur_ctrler(self: &Arc<Self>, ck: &dyn IKVClerk, ka: &[String], va: &[String]) {
        const NSEC: u64 = 2;
        const N: usize = 4;

        let (cfg, maxraftstate, ngid) = self.shared();
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::new();
        for _ in 0..N {
            let cfg = Arc::clone(&cfg);
            let ngid = Arc::clone(&ngid);
            let stop = Arc::clone(&stop);
            let prog = Arc::new(CtrlerProgress {
                gid: AtomicI64::new(0),
                state: AtomicI64::new(0),
            });
            handles.push(std::thread::spawn(move || {
                let sck = shardkv1::shardctrler::make_shard_ctrler(cfg.make_client());
                sck.init_controller();
                controller_loop(cfg, sck, ngid, maxraftstate, stop, prog);
            }));
        }

        // let the controllers run for a while
        std::thread::sleep(Duration::from_secs(NSEC));

        stop.store(true, Ordering::SeqCst);
        for h in handles {
            h.join().unwrap();
        }

        for (i, k) in ka.iter().enumerate() {
            self.kvtest.check_get(ck, k, &va[i], 1);
        }
    }
}
