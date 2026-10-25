#![allow(non_snake_case, unused_variables, unused_imports)]
//! Tests for Part A of Lab 4: the replicated state machine (port of Go's
//! kvraft1/rsm/rsm_test.go), plus the 4C snapshot test.

#![allow(non_snake_case)]

mod rsm_common;

use kvraft1::rsm::server::{Dec, Rep, Req, Inc, Null};
use rsm_common::RsmTest;
use std::sync::Arc;

/// Test that each server executes increments and updates its counter.
#[test]
fn TestBasic4A() {
    const NINC: usize = 10;
    let ts = RsmTest::make_test(-1);

    ts.cfg.begin("Test RSM basic");

    for i in 0..NINC {
        let r = ts.one_inc();
        assert_eq!(r, (i + 1) as i64, "expected {} instead of {}", i + 1, r);
        ts.check_counter(r, rsm_common::NSRV);
    }
    ts.cleanup();
}

/// Test submitting concurrently.
#[test]
fn TestConcurrent4A() {
    const NINC: usize = 50;
    let ts = RsmTest::make_test(-1);

    ts.cfg.begin("Test concurrent submit");

    let mut handles = Vec::new();
    for _ in 0..NINC {
        let ts2 = Arc::clone(&ts);
        handles.push(std::thread::spawn(move || {
            ts2.one_inc();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    ts.check_counter(NINC as i64, rsm_common::NSRV);
    ts.cleanup();
}

/// Test that each server executes increments after disconnecting and
/// reconnecting leader.
#[test]
fn TestLeaderFailure4A() {
    let ts = RsmTest::make_test(-1);

    ts.cfg.begin("Test Leader Failure");

    let r = ts.one_inc();
    ts.check_counter(r, rsm_common::NSRV);

    let l = ts.disconnect_leader();
    let r = ts.one_inc();
    ts.check_counter(r, rsm_common::NSRV - 1);

    ts.connect(l);

    ts.check_counter(r, rsm_common::NSRV);
    ts.cleanup();
}

/// Test that partitioned leader doesn't commit an operation.
#[test]
fn TestLeaderPartition4A() {
    const NSUBMIT: usize = 100;

    let ts = RsmTest::make_test(-1);

    ts.cfg.begin("Test Leader Partition");

    // submit an Inc
    let r = ts.one_inc();
    ts.check_counter(r, rsm_common::NSRV);

    // partition leader
    let l = ts.get_leader();
    let grp = ts.cfg.group(rsm_common::GID);
    let (p1, p2) = grp.make_partition(l);
    grp.partition(&p1, &p2);

    println!("partition leader {}", l);

    // Submit many Dec's concurrently, which will result in many
    // raft.start()'s, but none should commit and be executed.
    let ts2 = Arc::clone(&ts);
    let dec = std::thread::spawn(move || {
        for i in 0..NSUBMIT {
            if let Some(s) = ts2.srv(l) {
                let (err, rep) = s.submit(Req::Dec(Dec));
                if err == kvraft1::rpc::OK {
                    panic!("Submit {} in minority completed {:?}", i, rep);
                }
            }
        }
    });

    // give old leader some time to submit
    std::thread::sleep(std::time::Duration::from_millis(10));

    // submit an Inc in the majority
    let rep = match ts.one_partition_opt(Some(&p1), Req::Inc(Inc)) {
        Some(Rep::Inc(r)) => r,
        _ => panic!("no Inc reply from majority partition"),
    };

    assert!(
        !dec.is_finished(),
        "Dec's in minority completed before partition healed"
    );
    // (Give the minority a second to incorrectly complete.)
    std::thread::sleep(std::time::Duration::from_secs(1));
    assert!(
        !dec.is_finished(),
        "Dec's in minority completed; Submit returned OK"
    );

    // reconnect leader
    ts.connect(l);

    // wait for the dec submissions to finish (they must terminate)
    let started = std::time::Instant::now();
    loop {
        if dec.is_finished() {
            break;
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(11),
            "Submit after healing didn't return"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    dec.join().unwrap();

    // check that all replicas have the same value for counter
    ts.check_counter(rep.n, rsm_common::NSRV);
    ts.cleanup();
}

/// Test that restart replays Incs.
#[test]
fn TestRestartReplay4A() {
    const NINC: usize = 100;

    let ts = RsmTest::make_test(-1);

    ts.cfg.begin("Test Restart");

    for i in 0..NINC {
        let r = ts.one_inc();
        assert_eq!(r, (i + 1) as i64, "expected {} instead of {}", i + 1, r);
        ts.check_counter(r, rsm_common::NSRV);
    }

    ts.kill(&ts.group().all());

    std::thread::sleep(std::time::Duration::from_secs(1));

    ts.restart(&ts.group().all());

    // submit an Inc
    let r = ts.one_inc();
    assert_eq!(r, (NINC + 1) as i64, "expected {} got {}", NINC + 1, r);

    std::thread::sleep(std::time::Duration::from_secs(1));

    ts.check_counter(r, rsm_common::NSRV);
    ts.cleanup();
}

/// Test if submit() terminates after the tester's shutdown has called
/// raft's kill().  kill() should cause your raft to close the apply
/// channel passed to it in make(), which in turn allows rsm to know that
/// it is done.
#[test]
fn TestShutdown4A() {
    const NSUBMIT: usize = 100;

    let ts = RsmTest::make_test(-1);

    ts.cfg.begin("Test Shutdown");

    // Submit many Null's concurrently
    let ts2 = Arc::clone(&ts);
    let done = std::thread::spawn(move || {
        let mut handles = Vec::new();
        for _ in 0..NSUBMIT {
            let ts3 = Arc::clone(&ts2);
            handles.push(std::thread::spawn(move || {
                ts3.one_null();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    // give some time to submit
    std::thread::sleep(std::time::Duration::from_millis(20));

    ts.kill(&ts.group().all());

    let started = std::time::Instant::now();
    loop {
        if done.is_finished() {
            break;
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(rsm_common::NSEC + 1),
            "Submit didn't stop after shutdown"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    done.join().unwrap();
    ts.cleanup();
}

/// Test if commands after restart don't get confused with ones submitted
/// before shutdown.
#[test]
fn TestRestartSubmit4A() {
    const NINC: usize = 100;
    const NSUBMIT: usize = 100;

    let ts = RsmTest::make_test(-1);

    ts.cfg.begin("Test Restart and submit");

    for i in 0..NINC {
        let r = ts.one_inc();
        assert_eq!(r, (i + 1) as i64, "expected {} instead of {}", i + 1, r);
        ts.check_counter(r, rsm_common::NSRV);
    }

    ts.kill(&ts.group().all());

    std::thread::sleep(std::time::Duration::from_secs(1));

    ts.restart(&ts.group().all());

    // submit an Inc
    let r = ts.one_inc();
    assert_eq!(r, (NINC + 1) as i64, "Expected {} got {}", NINC + 1, r);

    std::thread::sleep(std::time::Duration::from_secs(1));

    // Submit many Null's concurrently
    let ts2 = Arc::clone(&ts);
    let done = std::thread::spawn(move || {
        let mut handles = Vec::new();
        for _ in 0..NSUBMIT {
            let ts3 = Arc::clone(&ts2);
            handles.push(std::thread::spawn(move || {
                ts3.one_null();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    // give some time to submit
    std::thread::sleep(std::time::Duration::from_millis(20));

    ts.kill(&ts.group().all());

    let started = std::time::Instant::now();
    loop {
        if done.is_finished() {
            break;
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(rsm_common::NSEC + 1),
            "Submit didn't stop after shutdown"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    done.join().unwrap();

    ts.restart(&ts.group().all());

    let r = ts.one_inc();
    ts.check_counter(r, rsm_common::NSRV);
    ts.cleanup();
}

/// Test snapshot and restore (Part C of Lab 4, exercised through rsm).
#[test]
fn TestSnapshot4C() {
    const N: i64 = 100;
    const MAXRAFTSTATE: i64 = 1000;

    let ts = RsmTest::make_test(MAXRAFTSTATE);

    ts.cfg.begin("Test creating and restoring snapshot");

    for _ in 0..N {
        ts.one_inc();
    }
    ts.check_counter(N, rsm_common::NSRV);

    let sz = ts.group().raft_size();
    assert!(
        sz <= (2 * MAXRAFTSTATE) as usize,
        "logs were not trimmed ({} > 2 * {})",
        sz,
        MAXRAFTSTATE
    );

    // rsm must have made snapshots by now; shutdown all servers and
    // restart them from a snapshot
    ts.kill(&ts.group().all());
    ts.restart(&ts.group().all());

    // make restarted servers do one increment
    ts.one_inc();

    ts.check_counter(N + 1, rsm_common::NSRV);
    ts.cleanup();
}
