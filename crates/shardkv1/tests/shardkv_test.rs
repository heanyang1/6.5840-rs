#![allow(non_snake_case, unused_variables, unused_imports)]
//! Tests for Lab 5 (port of Go's shardkv1/shardkv_test.go).

#![allow(non_snake_case)]

mod common;

use common::{make_test, make_test_leases, make_test_max_raft, ShardTest, NGRP, NKEYS};
use shardkv1::shardcfg;
use std::sync::Arc;
use tester::kvtest::{make_keys, ClntRes, IKVClerk};

#[test]
fn TestInitQuery5A() {
    // MakeTest starts your lab2 key/value server.
    let ts = make_test("Test (5A): Init and Query ...", true);
    let _ = &ts;

    // Make a shard controller.
    let sck = ts.make_shard_ctrler();

    // Make an empty shard configuration.
    let mut scfg = shardcfg::make_shard_config();

    // Compute a new shard configuration as if `shardcfg::GID1` joins the
    // cluster, assigning all shards to `shardcfg::GID1`.
    let mut servers = std::collections::HashMap::new();
    servers.insert(shardcfg::GID1, vec!["xxx".to_string()]);
    assert!(scfg.join_balance(&servers));

    // Invoke the controller to initialize to store the first configuration.
    sck.init_config(&scfg);

    // Read the initial configuration and check it.
    let cfg = sck.query();
    assert!(
        cfg.num == 1 && cfg.shards[0] == shardcfg::GID1,
        "Static wrong {:?}",
        cfg
    );
    cfg.check_config(&[shardcfg::GID1]);
    ts.cleanup();
}

/// Test shardkv clerk's Get/Put with 1 shardgrp (without reconfiguration).
#[test]
fn TestStaticOneShardGroup5A() {
    let ts = make_test("Test (5A): one shard group ...", true);

    ts.setup_kv_service();

    let ck = ts.make_clerk_raw(); // make a shardkv clerk
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS); // do some puts
    let n = ka.len();
    for i in 0..n {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1); // check the puts
    }

    // Shutdown raft leader of shardgrp and check that keys are still
    // available.
    ts.shutdown_leader(&ck, shardcfg::GID1);

    for i in 0..n {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1); // check the puts
    }
    ts.cleanup();
}

/// Test shardctrler's join, which adds a new group and must move shards to
/// the new group; the old group should reject Get/Puts on shards that
/// moved.
#[test]
fn TestJoinBasic5A() {
    let ts = make_test("Test (5A): a group joins...", true);

    let gid1 = ts.setup_kv_service();
    let ck = ts.make_clerk();
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS);

    let sck = ts.shard_ctrler();
    let cfg = sck.query();

    let gid2 = ts.new_gid();
    assert!(
        ts.join_groups(&Arc::clone(&sck), &[gid2]),
        "TestJoinBasic5A: joinGroups failed"
    );

    let cfg1 = sck.query();
    assert_eq!(
        cfg.num + 1,
        cfg1.num,
        "TestJoinBasic5A: wrong num {} expected {}",
        cfg1.num,
        cfg.num + 1
    );

    assert!(cfg1.is_member(gid2), "TestJoinBasic5A: {} isn't a member of {:?}", gid2, cfg1);

    ts.check_shutdown_sharding(gid1, &ka, &va);

    for i in 0..ka.len() {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
    }

    ts.check_shutdown_sharding(gid2, &ka, &va);

    for i in 0..ka.len() {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
    }
    ts.cleanup();
}

/// Test shardgrps delete moved shards.
#[test]
fn TestDeleteBasic5A() {
    const MAXRAFTSTATE: usize = 1000;
    const VALUESIZE: i64 = 10000;

    let ts = make_test_max_raft("Test (5A): delete ...", true, false, VALUESIZE);

    let gid1 = ts.setup_kv_service();
    let ck = ts.make_clerk();

    let (ka, va) = ts.kvtest.spread_puts_size(&*ck, NKEYS, MAXRAFTSTATE);
    if std::env::var("KV_DEBUG").is_ok() {
        eprintln!("DBG: spread done");
    }

    let sz = ts.group(gid1).snapshot_size();

    let sck = ts.shard_ctrler();
    let gid2 = ts.new_gid();
    assert!(
        ts.join_groups(&Arc::clone(&sck), &[gid2]),
        "TestDeleteBasic5A: joinGroups failed"
    );
    if std::env::var("KV_DEBUG").is_ok() {
        eprintln!("DBG: joined gid2={}", gid2);
    }

    // push more Get's through so that all peers snapshot.
    for _ in 0..5 {
        for i in 0..ka.len() {
            ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
        }
    }
    if std::env::var("KV_DEBUG").is_ok() {
        eprintln!("DBG: extra gets done");
    }

    let sz1 = ts.group(gid1).snapshot_size();
    let sz2 = ts.group(gid2).snapshot_size();
    assert!(
        sz1 + sz2 <= sz + 10000,
        "TestDeleteBasic5A: gid1 {} + gid2 {} = {} use too much space {}",
        sz1,
        sz2,
        sz1 + sz2,
        sz
    );
    ts.cleanup();
}

/// Test shardctrler's leave.
#[test]
fn TestJoinLeaveBasic5A() {
    let ts = make_test("Test (5A): basic groups join/leave ...", true);

    let gid1 = ts.setup_kv_service();
    let ck = ts.make_clerk();
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS);

    let sck = ts.shard_ctrler();
    let gid2 = ts.new_gid();
    assert!(
        ts.join_groups(&Arc::clone(&sck), &[gid2]),
        "TestJoinLeaveBasic5A: joinGroups failed"
    );

    ts.check_shutdown_sharding(gid1, &ka, &va);

    for i in 0..ka.len() {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
    }

    ts.leave(&sck, shardcfg::GID1);
    assert!(
        !ts.check_member(&sck, shardcfg::GID1),
        "{} is a member after leave",
        shardcfg::GID1
    );

    ts.group(shardcfg::GID1).shutdown();

    for i in 0..ka.len() {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
    }

    // bring the crashed shard/group back to life.
    let all = ts.group(shardcfg::GID1).all();
    ts.group(shardcfg::GID1).start_servers(&all);

    // Rejoin
    ts.join(&sck, shardcfg::GID1, ts.group(shardcfg::GID1).srv_names());

    for i in 0..ka.len() {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
    }

    ts.check_shutdown_sharding(gid2, &ka, &va);
    ts.cleanup();
}

/// Test many groups joining and leaving, reliable or unreliable.
fn join_leave_5a(reliable: bool) {
    let ts = make_test("Test (5A): many groups join/leave ...", reliable);

    ts.setup_kv_service();
    let ck = ts.make_clerk();
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS);

    let sck = ts.shard_ctrler();
    let grps = ts.groups(NGRP);

    assert!(ts.join_groups(&Arc::clone(&sck), &grps), "joinGroups failed");

    ts.check_shutdown_sharding(grps[0], &ka, &va);

    for i in 0..ka.len() {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
    }

    assert!(ts.leave_groups(&Arc::clone(&sck), &grps), "leaveGroups failed");

    for i in 0..ka.len() {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
    }
    ts.cleanup();
}

#[test]
fn TestManyJoinLeaveReliable5A() {
    join_leave_5a(true);
}

#[test]
fn TestManyJoinLeaveUnreliable5A() {
    join_leave_5a(false);
}

/// Test recovery from complete shutdown.
#[test]
fn TestShutdown5A() {
    const NJOIN: usize = 2;

    let ts = make_test("Test (5A): shutdown ...", true);

    ts.setup_kv_service();

    let ck = ts.make_clerk();
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS);

    let sck = ts.shard_ctrler();
    let grps = ts.groups(NJOIN);
    assert!(ts.join_groups(&Arc::clone(&sck), &grps), "joinGroups failed");

    ts.check_shutdown_sharding(grps[0], &ka, &va);

    for i in 0..ka.len() {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
    }

    // All groups: GID1 plus the NJOIN groups that joined.
    let mut all_groups = vec![shardcfg::GID1];
    all_groups.extend_from_slice(&grps);

    for &g in &all_groups {
        ts.group(g).shutdown();
    }

    for &g in &all_groups {
        let all = ts.group(g).all();
        ts.group(g).start_servers(&all);
    }

    for i in 0..ka.len() {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
    }
    ts.cleanup();
}

/// Test that Gets for keys at groups that are alive return.
#[test]
fn TestProgressShutdown5A() {
    const NJOIN: usize = 4;
    const NSEC: u64 = 2;

    let ts = make_test("Test (5A): progress ...", true);

    ts.setup_kv_service();

    let ck = ts.make_clerk();
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS);

    let sck = ts.shard_ctrler();
    let grps = ts.groups(NJOIN);
    assert!(ts.join_groups(&Arc::clone(&sck), &grps), "joinGroups failed");

    let end = 2;
    for g in &grps[0..end] {
        ts.group(*g).shutdown();
    }

    let alive: std::collections::HashSet<i64> = grps[end..].iter().cloned().collect();

    let cfg = sck.query();

    let (tx, rx) = std::sync::mpsc::channel();
    let ts2 = Arc::clone(&ts.kvtest);
    let keys: Vec<String> = ka.clone();
    std::thread::spawn(move || {
        for (i, key) in keys.iter().enumerate() {
            let s = shardcfg::key2shard(key);
            let g = cfg.shards[s as usize];
            if alive.contains(&g) {
                ts2.check_get(&*ck, key, &va[i], 1);
            }
        }
        tx.send(()).unwrap();
    });

    let started = std::time::Instant::now();
    loop {
        if rx.try_recv().is_ok() {
            break;
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(NSEC),
            "Gets didn't finish"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    ts.cleanup();
}

/// Test that Gets from a non-moving shard return quickly.
#[test]
fn TestProgressJoin5A() {
    const NJOIN: usize = 4;
    const NSEC: u64 = 4;
    const NCNT: usize = 100;

    let ts = make_test("Test (5A): progress ...", true);

    ts.setup_kv_service();

    let ck = ts.make_clerk();
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS);

    let sck = ts.shard_ctrler();
    let grps = ts.groups(NJOIN);
    assert!(ts.join_groups(&Arc::clone(&sck), &grps), "joinGroups failed");

    let cfg = sck.query();
    let mut newcfg = cfg.copy();
    let newgid = (NJOIN + 3) as i64;
    let mut servers = std::collections::HashMap::new();
    servers.insert(newgid, vec!["xxx".to_string()]);
    assert!(newcfg.join_balance(&servers), "JoinBalance failed");
    let mut newcfg1 = newcfg.copy();
    assert!(newcfg1.leave_balance(&[newgid]), "JoinBalance failed");

    // compute which shards don't move and which groups are involved in
    // moving shards.
    let mut stable = std::collections::HashSet::new();
    let mut participating: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for (i, &g) in newcfg1.shards.iter().enumerate() {
        if newcfg.shards[i] == g {
            stable.insert(i as shardcfg::Tshid);
        } else {
            participating.insert(g);
        }
    }

    // Gets on keys that are on stable shards of participating groups must
    // keep working while a join/leave of a new group is in flight.
    let ts2 = Arc::clone(&ts.kvtest);
    let keys: Vec<String> = ka.clone();
    let values: Vec<String> = va.clone();
    let ck2 = ts.make_clerk();
    let (tx, rx) = std::sync::mpsc::channel::<usize>();
    std::thread::spawn(move || {
        let t = std::time::Instant::now() + std::time::Duration::from_secs(NSEC);
        let mut nget = 0;
        while std::time::Instant::now() < t {
            for (i, key) in keys.iter().enumerate() {
                let s = shardcfg::key2shard(key);
                if stable.contains(&s) {
                    let g = newcfg1.shards[s as usize];
                    if participating.contains(&g) {
                        ts2.check_get(&*ck2, key, &values[i], 1);
                        nget += 1;
                    }
                }
            }
        }
        tx.send(nget).unwrap();
    });

    // Meanwhile, join and leave a new group repeatedly.
    loop {
        if rx.try_recv().is_ok() {
            break;
        }
        let ngid = ts.new_gid();
        let sck2 = Arc::clone(&sck);
        assert!(ts.join_groups(&sck2, &[ngid]), "TestProgressJoin: join failed");
        if rx.try_recv().is_ok() {
            break;
        }
        assert!(ts.leave_groups(&sck2, &[ngid]), "TestProgressJoin: leave failed");
    }
    let cnt = rx.recv().unwrap_or(NCNT);
    assert!(cnt >= NCNT, "Too few gets finished {}; expected more than {}", cnt, NCNT);
    ts.cleanup();
}

/// Test linearizability with groups joining/leaving and `nclnt` concurrent
/// clerks doing put/get's.
fn concurrent_clerk(nclnt: usize, reliable: bool, part: &'static str) {
    const NSEC: u64 = 20;

    let ts = make_test(part, reliable);

    ts.setup_kv_service();

    let ka = make_keys(NKEYS);

    let ts2 = Arc::clone(&ts.kvtest);
    let keys = ka.clone();
    let (tx, rx) = std::sync::mpsc::channel::<Vec<ClntRes>>();
    std::thread::spawn(move || {
        let mkc = {
            let ts = Arc::clone(&ts2);
            move || -> Arc<dyn IKVClerk> {
                // Like ts.make_clerk, but callable from this thread.
                let clnt = ts.cfg.make_client();
                let sck = shardkv1::shardctrler::make_shard_ctrler(clnt.clone());
                shardkv1::client::make_clerk(clnt, sck)
            }
        };
        let ts3 = Arc::clone(&ts2);
        let rs = ts2.spawn_clients_with(
            nclnt,
            NSEC,
            mkc,
            move |me, ck, done| ts3.one_client_put(me, &ck, &keys, done),
        );
        tx.send(rs).unwrap();
    });

    let sck = ts.shard_ctrler();
    let grps = ts.groups(NGRP);
    assert!(ts.join_groups(&Arc::clone(&sck), &grps), "concurrentClerk: joinGroups failed");
    assert!(ts.leave_groups(&Arc::clone(&sck), &grps), "concurrentClerk: leaveGroups failed");

    rx.recv_timeout(std::time::Duration::from_secs(NSEC + 30))
        .expect("clients did not finish");

    ts.kvtest.check_porcupine();
    ts.cleanup();
}

#[test]
fn TestOneConcurrentClerkReliable5A() {
    concurrent_clerk(1, true, "Test (5A): one concurrent clerk reliable...");
}

#[test]
fn TestManyConcurrentClerkReliable5A() {
    const NCLNT: usize = 10;
    concurrent_clerk(NCLNT, true, "Test (5A): many concurrent clerks reliable...");
}

#[test]
fn TestOneConcurrentClerkUnreliable5A() {
    concurrent_clerk(1, false, "Test (5A): one concurrent clerk unreliable...");
}

#[test]
fn TestManyConcurrentClerkUnreliable5A() {
    const NCLNT: usize = 10;
    concurrent_clerk(NCLNT, false, "Test (5A): many concurrent clerks unreliable...");
}

// ------------------------------- 5B --------------------------------------

#[test]
fn TestJoinLeave5B() {
    let ts = make_test("Test (5B): Join/leave while a shardgrp is down...", true);

    let gid1 = ts.setup_kv_service();
    let ck = ts.make_clerk();
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS);

    let sck = ts.shard_ctrler();
    let cfg = sck.query();

    ts.group(gid1).shutdown();

    let gid2 = ts.new_gid();
    // Start the join on a thread: it must block while gid1 (which holds
    // the shards to move) is down.
    let ts2 = Arc::clone(&ts);
    let sck2 = Arc::clone(&sck);
    let (tx, rx) = std::sync::mpsc::channel::<bool>();
    let h = std::thread::spawn(move || {
        let ok = ts2.join_groups(&sck2, &[gid2]);
        tx.send(ok).unwrap();
    });

    // Give the join some time to try; it must not finish.
    let started = std::time::Instant::now();
    loop {
        if h.is_finished() {
            panic!("Join finished early");
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "join is still blocked, as expected"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
        break;
    }
    // (After one second, the join must still be running.)
    assert!(!h.is_finished(), "Join finished while gid1 was down");

    // Now the join should be able to finish.
    ts.group(gid1).start_all();

    let ok = rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("Join didn't complete");
    assert!(ok, "TestJoinLeave5B: Join returned {}", ok);
    h.join().unwrap();

    let cfg1 = sck.query();
    assert_eq!(cfg.num + 1, cfg1.num, "wrong num {} expected {}", cfg1.num, cfg.num + 1);

    ts.group(gid2).shutdown();

    // Same for the leave of gid1 while gid2 (which must receive the
    // shards) is down.
    let ts3 = Arc::clone(&ts);
    let sck3 = Arc::clone(&sck);
    let (tx2, rx2) = std::sync::mpsc::channel::<bool>();
    let h2 = std::thread::spawn(move || {
        let ok = ts3.leave_groups(&sck3, &[shardcfg::GID1]);
        tx2.send(ok).unwrap();
    });

    let started = std::time::Instant::now();
    loop {
        if h2.is_finished() {
            panic!("Leave finished early");
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "leave is still blocked, as expected"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
        break;
    }
    assert!(!h2.is_finished(), "Leave finished while gid2 was down");

    // Now leave should be able to finish.
    ts.group(gid2).start_all();

    let ok = rx2
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("Leave didn't complete");
    assert!(ok, "TestJoinLeave5B: Leave failed {}", ok);
    h2.join().unwrap();

    for i in 0..ka.len() {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
    }
    ts.cleanup();
}

/// Test recovery of partitioned controllers.
#[test]
fn TestRecoverCtrler5B() {
    const NPARTITION: usize = 5;

    let ts = make_test("Test (5B): recover controller ...", true);

    let gid = ts.setup_kv_service();
    let ck = ts.make_clerk();
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS);

    for _ in 0..NPARTITION {
        ts.partition_ctrler(&*ck, gid, &ka, &va);
    }
    ts.cleanup();
}

// ------------------------------- 5C --------------------------------------

/// Test concurrent controllers fighting for leadership.
#[test]
fn TestConcurrentReliable5C() {
    let ts = make_test_leases("Test (5C): Concurrent ctrlers ...", true);
    ts.setup_kv_service();
    let ck = ts.make_clerk();
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS);
    ts.concur_ctrler(&*ck, &ka, &va);
    ts.cleanup();
}

#[test]
fn TestAcquireLockConcurrentUnreliable5C() {
    let ts = make_test_leases("Test (5C): Concurrent ctrlers ...", false);
    ts.setup_kv_service();
    let ck = ts.make_clerk();
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS);
    ts.concur_ctrler(&*ck, &ka, &va);
    ts.cleanup();
}

/// Test if an old controller is fenced off when reconnecting while it is
/// in the middle of a join.
#[test]
fn TestPartitionControllerJoin5C() {
    const NSEC: u64 = 1;

    let ts = make_test_leases("Test (5C): partition controller in join...", true);
    ts.setup_kv_service();
    let ck = ts.make_clerk();
    let (ka, va) = ts.kvtest.spread_puts(&*ck, NKEYS);

    let (sck, clnt) = ts.make_shard_ctrler_clnt();
    sck.init_controller();

    let ngid = ts.new_gid();
    let ms = ShardTest::shardgrp_make_server_max(ts.maxraftstate);
    ts.kvtest.cfg.make_group_with(ngid, common::NSRV, ts.maxraftstate, ms);
    ts.group(ngid).shutdown();

    // The controller will try to join ngid, whose group is down, so it
    // blocks inside ChangeConfigTo until the partition heals.
    let ts2 = Arc::clone(&ts);
    let sck1 = Arc::clone(&sck);
    let servers = ts.group(ngid).srv_names();
    let h = std::thread::spawn(move || {
        ts2.join(&sck1, ngid, servers);
    });

    // sleep for a while to give the controller the chance to get stuck in
    // join, because gid is down.
    std::thread::sleep(std::time::Duration::from_secs(1));

    // partition sck
    clnt.disconnect_all();

    // wait a while before restarting shardgrp `ngid`
    std::thread::sleep(std::time::Duration::from_secs(NSEC));

    ts.group(ngid).start_all();

    // start new controller to supersede the partitioned one,
    let sck0 = ts.make_shard_ctrler();
    sck0.init_controller();

    let scfg = sck0.query();
    assert!(scfg.is_member(ngid), "Didn't recover gid {}", ngid);

    // reconnect old controller, which shouldn't finish change_config_to
    // (the join was superseded), but must terminate.
    clnt.connect_all();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if h.is_finished() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "old controller didn't exit"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    h.join().unwrap();

    std::thread::sleep(std::time::Duration::from_secs(1));

    for i in 0..ka.len() {
        ts.kvtest.check_get(&*ck, &ka[i], &va[i], 1);
    }
    ts.cleanup();
}

/// Make a leader controller lose its leadership during join/leave and test
/// if the next controller recovers correctly (Go: partitionRecovery5C).
fn partition_recovery_5c(reliable: bool, npart: usize, nclnt: usize) {
    const NSEC: u64 = 60;

    let ts = make_test_leases("Test (5C): controllers with leased leadership ...", reliable);
    let gid = ts.setup_kv_service();
    let ck = ts.make_clerk();

    let mut ka: Vec<String> = Vec::new();
    let mut va: Vec<String> = Vec::new();
    if nclnt <= 0 {
        let (a, b) = ts.kvtest.spread_puts(&*ck, NKEYS);
        ka = a;
        va = b;
    }

    let mut client_handle = None;
    if nclnt > 0 {
        let keys = make_keys(1);
        let ts2 = Arc::clone(&ts.kvtest);
        let mkc = {
            let ts3 = Arc::clone(&ts2);
            move || -> Arc<dyn IKVClerk> {
                let clnt = ts3.cfg.make_client();
                let sck = shardkv1::shardctrler::make_shard_ctrler(clnt.clone());
                shardkv1::client::make_clerk(clnt, sck)
            }
        };
        let (tx, rx) = std::sync::mpsc::channel::<Vec<ClntRes>>();
        let ts3 = Arc::clone(&ts2);
        std::thread::spawn(move || {
            let rs = ts2.spawn_clients_with(
                nclnt,
                NSEC,
                mkc,
                move |me, ck, done| ts3.one_client_put(me, &ck, &keys, done),
            );
            tx.send(rs).unwrap();
        });
        client_handle = Some(rx);
    }

    for _ in 0..npart {
        ts.partition_ctrler(&*ck, gid, &ka, &va);
    }

    if let Some(rx) = client_handle {
        rx.recv_timeout(std::time::Duration::from_secs(NSEC + 30))
            .expect("clients did not finish");
        ts.kvtest.check_porcupine();
    }
    ts.cleanup();
}

#[test]
fn TestPartitionRecoveryReliableNoClerk5C() {
    const NPARTITION: usize = 5;
    partition_recovery_5c(true, NPARTITION, 0);
}

#[test]
fn TestPartitionRecoveryUnreliableNoClerk5C() {
    const NPARTITION: usize = 3;
    partition_recovery_5c(false, NPARTITION, 0);
}

#[test]
fn TestPartitionRecoveryReliableClerks5C() {
    const NPARTITION: usize = 5;
    partition_recovery_5c(true, NPARTITION, 5);
}

#[test]
fn TestPartitionRecoveryUnreliableClerks5C() {
    const NPARTITION: usize = 3;
    partition_recovery_5c(false, NPARTITION, 5);
}
