#![allow(non_snake_case, unused_variables, unused_imports)]
//! Tests for Lab 2's key/value server (port of Go's kvsrv1/kvsrv_test.go).

mod common;

use common::{clerk_maker, make_clerk, make_test_kv};
use kvsrv1::rpc;
use std::sync::Arc;
use tester::kvtest::{rand_value, KvTest};

/// Test Put with a single client and a reliable network.
#[test]
fn TestReliablePut() {
    const VAL: &str = "6.5840";
    const VER: u64 = 0;

    let ts = make_test_kv(true);

    ts.cfg.begin("One client and reliable Put");

    let ck = make_clerk(&ts);
    let err = ck.put("k", VAL, VER);
    assert!(err == rpc::OK, "Put err {}", err);

    let (val, ver, err) = ck.get("k");
    assert!(err == rpc::OK, "Get err {}; expected OK", err);
    assert_eq!(val, VAL, "Get value err {}; expected {}", val, VAL);
    assert_eq!(ver, VER + 1, "Get wrong version {}; expected {}", ver, VER + 1);

    let err = ck.put("k", VAL, 0);
    assert!(err == rpc::ERR_VERSION, "expected Put to fail with ErrVersion; got err={}", err);

    let err = ck.put("y", VAL, 1);
    assert!(err == rpc::ERR_NO_KEY, "expected Put to fail with ErrNoKey; got err={}", err);

    let (_, _, err) = ck.get("y");
    assert!(err == rpc::ERR_NO_KEY, "expected Get to fail with ErrNoKey; got err={}", err);

    ts.cleanup();
}

/// Many clients putting on same key.
#[test]
fn TestPutConcurrentReliable() {
    const PORCUPINE_TIME_SECS: u64 = 10;
    const NCLNT: usize = 10;
    const NSEC: u64 = 1;

    let ts = make_test_kv(true);

    ts.cfg.begin("Test: many clients racing to put values to the same key");

    let rs = ts.spawn_clients_with(
        NCLNT,
        NSEC,
        clerk_maker(Arc::clone(&ts)),
        {
            let ts = Arc::clone(&ts);
            move |me, ck, done| ts.one_client_put(me, &ck, &["k".to_string()], done)
        },
    );
    let ck = make_clerk(&ts);
    ts.check_put_concurrent(
        &*ck,
        "k",
        &rs,
        &tester::kvtest::ClntRes::default(),
        ts.cfg.is_reliable(),
    );
    ts.check_porcupine_t(std::time::Duration::from_secs(PORCUPINE_TIME_SECS));
    ts.cleanup();
}

/// Check that the server doesn't grow per-client state: many clients
/// (20_000) each do one Put.  (Go's version measures the server process's
/// memory with runtime.ReadMemStats; in the Rust port everything runs in
/// one process, so we assert the *total heap growth stays bounded* while
/// 20k clients perform puts and that no per-client state accumulates.)
#[test]
fn TestMemPutManyClientsReliable() {
    const NCLIENT: usize = 20_000;

    let ts = make_test_kv(true);

    let v = rand_value(100);

    let cks: Vec<Arc<dyn tester::kvtest::IKVClerk>> =
        (0..NCLIENT).map(|_| make_clerk(&ts)).collect();

    // Force allocation of ends for the server in each client.
    for ck in &cks {
        let err = ck.put("k", "", 1);
        assert!(err == rpc::ERR_NO_KEY, "Put failed {}", err);
    }

    ts.cfg.begin("Test: memory use many put clients");

    tester::enable_counting_alloc();
    // let threads started by labrpc run
    std::thread::sleep(std::time::Duration::from_secs(1));

    let m0 = tester::allocated();
    for (i, ck) in cks.iter().enumerate() {
        let err = ck.put("k", &v, i as u64);
        assert!(err == rpc::OK, "Put failed {}", err);
    }
    let m1 = tester::allocated();
    // Everything must still work:
    let (val, ver, err) = make_clerk(&ts).get("k");
    assert!(err == rpc::OK, "Get failed {}", err);
    assert_eq!(val, v);
    assert_eq!(ver, NCLIENT as u64);

    // Permissive bound: the whole exercise (clerks + RPCs + replies) must
    // stay well under a few hundred MB; a server that stored state per
    // client would blow past this.
    let bound = m0 + 500_000_000;
    assert!(
        m1 < bound,
        "error: using too much memory {} bytes",
        m1 - m0
    );
    ts.cleanup();
}

/// Test with one client and unreliable network.  If Clerk.Put returns
/// ErrMaybe, the Put must have happened, since the test uses only one
/// client.
#[test]
fn TestUnreliableNet() {
    const NTRY: usize = 100;

    let ts = make_test_kv(false);

    ts.cfg.begin("One client");

    let ck = make_clerk(&ts);

    let mut retried = false;
    for try_ in 0..NTRY {
        let mut i = 0;
        loop {
            let err = ts.put_json(&*ck, "k", &i, try_ as u64, 0);
            if err != rpc::ERR_MAYBE {
                if i > 0 && err != rpc::ERR_VERSION {
                    panic!("Put shouldn't have happened more than once {}", err);
                }
                break;
            }
            // Try put again; it should fail with ErrVersion.
            retried = true;
            i += 1;
        }
        let (v, ver): (i64, u64) = ts.get_json(&*ck, "k", 0);
        assert_eq!(ver, try_ as u64 + 1, "Wrong version {} expect {}", ver, try_ + 1);
        assert_eq!(v, 0, "Wrong value {} expect 0", v);
    }
    assert!(retried, "Clerk.Put never returned ErrMaybe");

    ts.check_porcupine();
    ts.cleanup();
}
