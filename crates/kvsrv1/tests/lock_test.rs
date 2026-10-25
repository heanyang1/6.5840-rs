#![allow(non_snake_case, unused_variables, unused_imports)]
//! Tests for Lab 2's distributed lock (port of Go's
//! kvsrv1/lock/lock_test.go).

mod common;

use common::{clerk_maker, make_clerk, make_test_kv};
use kvsrv1::lock::make_lock;
use kvsrv1::rpc;
use std::sync::Arc;
use tester::kvtest::{rand_value, ClntRes, KvTest};

const NCLNT: usize = 10;
const NSEC: u64 = 2;

#[test]
fn TestReliableBasic() {
    let ts = make_test_kv(true);

    ts.cfg.begin("Test: a single Acquire and Release");

    let name1 = rand_value(12);
    let ck = make_clerk(&ts);
    let lk1 = make_lock(Arc::clone(&ck), &name1);
    lk1.acquire();
    lk1.release();
    ts.cleanup();
}

/// Check that locks with different names can be held at the same time.
#[test]
fn TestReliableNested() {
    let ts = make_test_kv(true);

    ts.cfg.begin("Test: one client, two locks");

    let name1 = rand_value(12);
    let name2 = rand_value(12);
    let ck = make_clerk(&ts);
    let lk1 = make_lock(Arc::clone(&ck), &name1);
    let lk2 = make_lock(Arc::clone(&ck), &name2);

    lk1.acquire();
    lk2.acquire();
    lk2.release();
    lk1.release();

    lk2.acquire();
    lk1.acquire();
    lk1.release();
    lk1.acquire();
    lk2.release();
    lk1.release();
    ts.cleanup();
}

fn one_client(
    me: usize,
    ck: Arc<dyn tester::kvtest::IKVClerk>,
    done: Arc<std::sync::atomic::AtomicBool>,
    name1: &str,
    name2: &str,
) -> ClntRes {
    use std::sync::atomic::Ordering;
    let lk1 = make_lock(Arc::clone(&ck), name1);
    let lk2 = make_lock(Arc::clone(&ck), name2);

    lk1.acquire();
    lk2.acquire();
    lk1.release();
    lk2.release();

    ck.put("l0", "", 0);
    let mut i = 1;
    while !done.load(Ordering::SeqCst) {
        lk1.acquire();

        let b = me.to_string();
        let (val, ver, err) = ck.get("l0");
        if err == rpc::OK {
            assert!(val.is_empty(), "{}: two clients acquired lock {}", me, val);
        } else {
            panic!("{}: get failed {}", me, err);
        }

        let err = ck.put("l0", &b, ver);
        assert!(err == rpc::OK || err == rpc::ERR_MAYBE, "{}: put failed {}", me, err);

        std::thread::sleep(std::time::Duration::from_millis(10));

        let err = ck.put("l0", "", ver + 1);
        assert!(err == rpc::OK || err == rpc::ERR_MAYBE, "{}: put failed {}", me, err);

        lk1.release();
        i += 1;
    }
    ClntRes { nok: i, nmaybe: 0 }
}

/// Run test clients.
fn run_clients(nclnt: usize, reliable: bool) {
    let ts = make_test_kv(reliable);

    ts.cfg.begin(&format!("Test: {} lock clients", nclnt));

    let name1 = rand_value(12);
    let name2 = rand_value(12);
    let ts2 = Arc::clone(&ts);
    ts.spawn_clients_with(
        nclnt,
        NSEC,
        clerk_maker(ts2),
        move |me, myck, done| one_client(me, myck, done, &name1, &name2),
    );
    ts.cleanup();
}

#[test]
fn TestOneClientReliable() {
    run_clients(1, true);
}

#[test]
fn TestManyClientsReliable() {
    run_clients(NCLNT, true);
}

#[test]
fn TestOneClientUnreliable() {
    run_clients(1, false);
}

#[test]
fn TestManyClientsUnreliable() {
    run_clients(NCLNT, false);
}
