#![allow(non_snake_case, unused_variables, unused_imports)]
//! Tests for Parts B and C of Lab 4 (port of Go's kvraft1/kvraft_test.go).

#![allow(non_snake_case)]

mod common;

use common::{make_test, KvRaftTest};
use kvraft1::rpc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tester::kvtest::IKVClerk;

/// The common 4B scenario with various flags.
fn generic(
    part: &'static str,
    nclients: usize,
    nservers: usize,
    reliable: bool,
    crash: bool,
    partitions: bool,
    maxraftstate: i64,
    randomkeys: bool,
) {
    let ts = make_test(part, nclients, nservers, reliable, crash, partitions, maxraftstate, randomkeys);
    ts.generic_test();
    ts.cleanup();
}

#[test]
fn TestBasic4B() {
    generic("4B basic", 1, 5, true, false, false, -1, false);
}

#[test]
fn TestSpeed4B() {
    let ts = make_test("4B speed", 1, 3, true, false, false, -1, false);
    ts.generic_test_speed();
    ts.cleanup();
}

#[test]
fn TestConcurrent4B() {
    generic("4B many clients", 5, 5, true, false, false, -1, false);
}

#[test]
fn TestUnreliable4B() {
    generic("4B many clients", 5, 5, false, false, false, -1, false);
}

/// Submit a request in the minority partition and check that the request
/// doesn't go through until the partition heals.  The leader in the
/// original network ends up in the minority partition.
#[test]
fn TestOnePartition4B() {
    let ts = make_test("4B progress in majority", 0, 5, false, false, false, -1, false);
    let grp = ts.group();

    let ck = ts.make_clerk_raw();

    let ver0 = ts.kvtest.put_at_least_once(&*ck, "1", "13", 0, -1);
    let l = ck.leader();
    println!("leader found = {}", l);
    let (p1, p2) = grp.make_partition(l);
    grp.partition(&p1, &p2);

    let (ckp1, _c1) = ts.make_clerk_to(&p1); // connect ckp1 to p1
    let (ckp2a, c2a) = ts.make_clerk_to(&p2); // connect ckp2a to p2
    let (ckp2b, c2b) = ts.make_clerk_to(&p2); // connect ckp2b to p2

    let ver1 = ts.kvtest.put_at_least_once(&*ckp1, "1", "14", ver0, -1);
    ts.kvtest.check_get(&*ckp1, "1", "14", ver1);

    ts.kvtest.cfg.end();

    ts.kvtest.cfg.begin("Test: no progress in minority (4B)");

    // Submit Put(1, 15) and Get(1) in the minority: neither should
    // complete while the partition holds.
    let (tx, rx) = std::sync::mpsc::channel::<&'static str>();
    let ts3 = Arc::clone(&ts.kvtest);
    let ts4 = Arc::clone(&ts.kvtest);
    let tx2 = tx.clone();
    let (tx_ver, rx_ver) = std::sync::mpsc::channel::<rpc::Tversion>();
    let h0 = std::thread::spawn(move || {
        let r = ts3.put_at_least_once(&*ckp2a, "1", "15", ver1, -1);
        let _ = tx_ver.send(r);
        tx2.send("put").unwrap();
    });
    let h1 = std::thread::spawn(move || {
        ts4.get(&*ckp2b, "1", -1); // different clerk in p2
        tx.send("get").unwrap();
    });

    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(which) => panic!("{} in minority completed", which),
        Err(_) => {}
    }

    ts.kvtest.check_get(&*ckp1, "1", "14", ver1);
    let ver2 = ts.kvtest.put_at_least_once(&*ckp1, "1", "16", ver1, -1);
    ts.kvtest.check_get(&*ckp1, "1", "16", ver2);

    ts.kvtest.cfg.end();

    ts.kvtest.cfg.begin("Test: completion after heal (4B)");

    grp.connect_all();
    // Re-connect the minority clerks, like Go's ckp2a.Clnt.ConnectAll().
    c2a.connect_all();
    c2b.connect_all();

    std::thread::sleep(tester::kvtest::ELECTION_TIMEOUT);
    eprintln!("healed + election timeout");

    ts.kvtest.cfg.begin("status of Put(1, 15)");
    // The blocked Put must complete once the partition resolves.
    {
        let started = Instant::now();
        loop {
            if h0.is_finished() {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_millis(3000),
                "Put(1, 15) did not complete after partition resolved"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        h0.join().unwrap();
    }
    ts.kvtest.cfg.begin("status of Get(1)");
    {
        let started = Instant::now();
        loop {
            if h1.is_finished() {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_millis(3000),
                "Get(1) did not complete after partition resolved"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        h1.join().unwrap();
    }

    // The healed Put(1, 15) went through (at a newer version, since "16"
    // forced PutAtLeastOnce to retry): check value and version.
    let ver15 = rx_ver.try_recv().expect("Put(1,15) version");
    ts.kvtest.check_get(&*ck, "1", "15", ver15);
    ts.cleanup();
}

#[test]
fn TestManyPartitionsOneClient4B() {
    generic("4B partitions, one client", 1, 5, true, false, true, -1, false);
}

#[test]
fn TestManyPartitionsManyClients4B() {
    generic("4B partitions, many clients (4B)", 5, 5, true, false, true, -1, false);
}

#[test]
fn TestPersistOneClient4B() {
    generic("4B restarts, one client 4B", 1, 5, true, true, false, -1, false);
}

#[test]
fn TestPersistConcurrent4B() {
    generic("4B restarts, many clients", 5, 5, true, true, false, -1, false);
}

#[test]
fn TestPersistConcurrentUnreliable4B() {
    generic("4B restarts, many clients", 5, 5, false, true, false, -1, false);
}

#[test]
fn TestPersistPartition4B() {
    generic("4B restarts, partitions, many clients", 5, 5, true, true, true, -1, false);
}

#[test]
fn TestPersistPartitionUnreliable4B() {
    generic("4B restarts, partitions, many clients", 5, 5, false, true, true, -1, false);
}

#[test]
fn TestPersistPartitionUnreliableLinearizable4B() {
    generic(
        "4B restarts, partitions, random keys, many clients",
        15,
        7,
        false,
        true,
        true,
        -1,
        true,
    );
}

/// If one server falls behind, then rejoins, does it recover by using the
/// InstallSnapshot RPC?  Also checks that the majority discards committed
/// log entries even if the minority doesn't respond.
#[test]
fn TestSnapshotRPC4C() {
    const NSRV: usize = 3;
    const MAXRAFTSTATE: i64 = 1000;

    let ts = make_test("4C SnapshotsRPC", 0, NSRV, true, false, false, MAXRAFTSTATE, false);
    let grp = ts.group();

    ts.kvtest.cfg.begin("Test: InstallSnapshot RPC (4C)");

    let ck = ts.make_clerk_raw();

    let vera = ts.kvtest.put_at_least_once(&*ck, "a", "A", 0, -1);
    ts.kvtest.check_get(&*ck, "a", "A", vera);

    // a bunch of puts into the majority partition.
    grp.partition(&[0, 1], &[2]);
    {
        let (ck1, _c) = ts.make_clerk_to(&[0, 1]);
        for i in 0..50 {
            ts.kvtest.put_at_least_once(&*ck1, &i.to_string(), &i.to_string(), 0, -1);
        }
        std::thread::sleep(tester::kvtest::ELECTION_TIMEOUT);
        ts.kvtest.put_at_least_once(&*ck1, "b", "B", 0, -1);
    }
    let verb = {
        let (ck1b, _cb) = ts.make_clerk_to(&[0, 1]);
        let (e, v, err) = ts.kvtest.get(&*ck1b, "b", -1);
        assert!(err == rpc::OK, "{}", err);
        let _ = (e, err);
        v
    };

    // check that the majority partition has thrown away most of its log
    // entries.
    let sz = grp.raft_size();
    assert!(
        sz <= (8 * MAXRAFTSTATE) as usize,
        "logs were not trimmed ({} > 8*{})",
        sz,
        MAXRAFTSTATE
    );

    // now make a group that requires participation of the lagging server,
    // so that it has to catch up.
    grp.partition(&[0, 2], &[1]);
    let verc;
    {
        let (ck1, _c) = ts.make_clerk_to(&[0, 2]);
        verc = ts.kvtest.put_at_least_once(&*ck1, "c", "C", 0, -1);
        ts.kvtest.put_at_least_once(&*ck1, "d", "D", 0, -1);
        ts.kvtest.check_get(&*ck1, "a", "A", vera);
        ts.kvtest.check_get(&*ck1, "b", "B", verb);
        ts.kvtest.check_get(&*ck1, "1", "1", 1);
        ts.kvtest.check_get(&*ck1, "49", "49", 1);
    }

    // now everybody
    grp.partition(&[0, 1, 2], &[]);

    let vere = ts.kvtest.put_at_least_once(&*ck, "e", "E", 0, -1);
    ts.kvtest.check_get(&*ck, "c", "C", verc);
    ts.kvtest.check_get(&*ck, "e", "E", vere);
    ts.kvtest.check_get(&*ck, "1", "1", 1);
    ts.cleanup();
}

/// Are the snapshots not too huge?  500 bytes is a generous bound for the
/// operations we're doing here.
#[test]
fn TestSnapshotSize4C() {
    const MAXRAFTSTATE: i64 = 1000;

    let ts = make_test("4C snapshot size is reasonable", 0, 3, true, false, false, MAXRAFTSTATE, false);

    let maxsnapshotstate = 500;

    let ck = ts.make_clerk_raw();

    let mut ver = 0;
    for _ in 0..200 {
        ver = ts.kvtest.put_at_least_once(&*ck, "x", "0", ver, -1);
        ts.kvtest.check_get(&*ck, "x", "0", ver);
        ver = ts.kvtest.put_at_least_once(&*ck, "x", "1", ver, -1);
        ts.kvtest.check_get(&*ck, "x", "1", ver);
    }

    // check that servers have thrown away most of their log entries
    let sz = ts.group().raft_size();
    assert!(
        sz <= (8 * MAXRAFTSTATE) as usize,
        "logs were not trimmed ({} > 8*{})",
        sz,
        MAXRAFTSTATE
    );

    // check that the snapshots are not unreasonably large
    let ssz = ts.group().snapshot_size();
    assert!(
        ssz <= maxsnapshotstate,
        "snapshot too large ({} > {})",
        ssz,
        maxsnapshotstate
    );
    ts.cleanup();
}

#[test]
fn TestSpeed4C() {
    let ts = make_test("4C speed", 1, 3, true, false, false, 1000, false);
    ts.generic_test_speed();
    ts.cleanup();
}

#[test]
fn TestSnapshotRecover4C() {
    generic("4C restarts, snapshots, one client", 1, 5, true, true, false, 1000, false);
}

#[test]
fn TestSnapshotRecoverManyClients4C() {
    generic("4C restarts, snapshots, many clients", 20, 5, true, true, false, 1000, false);
}

#[test]
fn TestSnapshotUnreliable4C() {
    generic("4C unreliable net, snapshots, many clients", 5, 5, false, false, false, 1000, false);
}

#[test]
fn TestSnapshotUnreliableRecover4C() {
    generic("4C unreliable net, restarts, snapshots, many clients", 5, 5, false, true, false, 1000, false);
}

#[test]
fn TestSnapshotUnreliableRecoverConcurrentPartition4C() {
    generic(
        "4C unreliable net, restarts, partitions, snapshots, many clients",
        5,
        5,
        false,
        true,
        true,
        1000,
        false,
    );
}

#[test]
fn TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable4C() {
    generic(
        "4C unreliable net, restarts, partitions, snapshots, random keys, many clients",
        15,
        7,
        false,
        true,
        true,
        1000,
        true,
    );
}

