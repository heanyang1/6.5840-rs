//! Self-tests for the test infrastructure (labrpc, config harness,
//! linearizability checker).  These validate the harness itself, not lab
//! code; they should always pass.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tester::config::Config;
use tester::porcupine::{check, KvInput, KvOutput, Operation};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct EchoArgs {
    x: u64,
    sleep_ms: u64,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct EchoReply {
    x: u64,
}

#[test]
fn labrpc_reliable_echo() {
    let cfg = Config::new(
        "infra",
        1,
        true,
        Arc::new(|args| {
            let svc = labrpc::Service::new("Echo");
            svc.handle("Ping", |a: EchoArgs| {
                if a.sleep_ms > 0 {
                    std::thread::sleep(Duration::from_millis(a.sleep_ms));
                }
                EchoReply { x: a.x }
            });
            let _ = args;
            tester::ServerBox::new().with_service(svc)
        }),
    );
    let clnt = cfg.make_client();
    let srv = tester::server_name(tester::GRP0, 0);
    for i in 0..100 {
        let rep: Option<EchoReply> = clnt.call(&srv, "Echo.Ping", &EchoArgs { x: i, sleep_ms: 0 });
        assert_eq!(rep.unwrap().x, i);
    }
    // Unknown method: dispatch fails loudly (reply dropped).
    let rep: Option<EchoReply> = clnt.call(&srv, "Echo.Nope", &EchoArgs { x: 1, sleep_ms: 0 });
    assert!(rep.is_none());
    cfg.cleanup();
}

#[test]
fn labrpc_unreliable_and_concurrent() {
    let cfg = Config::new(
        "infra",
        1,
        false,
        Arc::new(|args| {
            let svc = labrpc::Service::new("Echo");
            svc.handle("Ping", |a: EchoArgs| {
                if a.sleep_ms > 0 {
                    std::thread::sleep(Duration::from_millis(a.sleep_ms));
                }
                EchoReply { x: a.x }
            });
            let _ = args;
            tester::ServerBox::new().with_service(svc)
        }),
    );
    cfg.set_long_reordering(true);
    let clnt = cfg.make_client();
    let srv = tester::server_name(tester::GRP0, 0);

    // Hammer with concurrent calls; count successes.  Calls must never
    // hang (they have a bounded timeout in labrpc).
    let ok = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for t in 0..8 {
        let clnt = Arc::clone(&clnt);
        let ok = Arc::clone(&ok);
        let srv = srv.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..200u64 {
                let rep: Option<EchoReply> =
                    clnt.call(&srv, "Echo.Ping", &EchoArgs { x: t * 1000 + i, sleep_ms: 1 });
                if rep.is_some() {
                    ok.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // With ~12% loss per direction, most calls must succeed.
    let got = ok.load(Ordering::SeqCst);
    assert!(
        got > 8 * 200 / 2,
        "too few successful RPCs on unreliable net: {}",
        got
    );
    assert!(cfg.rpc_total() > 0);
    cfg.cleanup();
}

#[test]
fn config_disconnect_partition_kill_restart() {
    let counter = Arc::new(AtomicU64::new(0));
    let c2 = Arc::clone(&counter);
    let cfg = Config::new(
        "infra",
        3,
        true,
        Arc::new(move |args| {
            let svc = labrpc::Service::new("Echo");
            let c3 = Arc::clone(&c2);
            svc.handle("Ping", move |a: EchoArgs| EchoReply { x: a.x + c3.load(Ordering::SeqCst) });
            let _ = args;
            let c = Arc::clone(&c2);
            tester::ServerBox::new().with_service(svc).with_kill(move || {
                c.fetch_add(1, Ordering::SeqCst);
            })
        }),
    );
    let grp = cfg.group(tester::GRP0);
    let clnt = cfg.make_client();
    let srv0 = grp.srv_name(0);

    let rep: Option<EchoReply> = clnt.call(&srv0, "Echo.Ping", &EchoArgs { x: 0, sleep_ms: 0 });
    assert!(rep.is_some());

    // Disconnect all servers from each other.
    grp.disconnect_all(0);
    let rep: Option<EchoReply> = clnt.call(&srv0, "Echo.Ping", &EchoArgs { x: 0, sleep_ms: 0 });
    assert!(rep.is_some(), "client end is independent of group ends");
    assert!(!grp.is_connected(0));
    grp.connect_one(0);
    assert!(grp.is_connected(0));

    // Partition {0} from {1,2}: every server stays "connected" (to its
    // own partition -- the Go tester's IsConnected means the same), but
    // the cross-partition ends are disabled.
    grp.partition(&[0], &[1, 2]);
    assert!(grp.is_connected(0));
    assert!(grp.is_connected(1));
    assert!(
        !cfg.net
            .is_enabled(&format!("{}-to-{}", grp.srv_name(1), grp.srv_name(0))),
        "server 1 must not reach server 0 across the partition"
    );

    // Kill and restart server 0.
    grp.kill(&[0]);
    let rep: Option<EchoReply> = clnt.call(&srv0, "Echo.Ping", &EchoArgs { x: 0, sleep_ms: 0 });
    assert!(rep.is_none(), "server 0 is down");
    grp.start_servers(&[0]);
    let rep: Option<EchoReply> = clnt.call(&srv0, "Echo.Ping", &EchoArgs { x: 0, sleep_ms: 0 });
    assert!(rep.is_some(), "server 0 restarted");
    assert_eq!(counter.load(Ordering::SeqCst), 1, "kill closure ran once");
    cfg.cleanup();
}

fn op(op: u8, key: &str, value: &str, version: u64, call: u64, ret: u64, err: &str) -> Operation {
    Operation {
        input: KvInput {
            op,
            key: key.to_string(),
            value: value.to_string(),
            version,
        },
        output: KvOutput {
            value: String::new(),
            version: 0,
            err: err.to_string(),
        },
        call,
        ret,
        client: 0,
    }
}

#[test]
fn porcupine_linearizable_history() {
    // put(k,a,0) completes, then get(k) sees "a": linearizable.
    let mut hist = vec![
        op(1, "k", "a", 0, 10, 20, "OK"),
        op(0, "k", "", 0, 30, 40, "OK"),
    ];
    hist[1].output.value = "a".to_string();
    hist[1].output.version = 1;
    assert_eq!(check(&hist, Duration::from_secs(5)), Some(true));

    // put completes after get starts but before get returns; get still
    // sees the new value: linearizable.
    let mut hist = vec![op(1, "k", "a", 0, 10, 20, "OK"), op(0, "k", "", 0, 15, 25, "OK")];
    hist[1].output.value = "a".to_string();
    assert_eq!(check(&hist, Duration::from_secs(5)), Some(true));

    // get starts after put returns but doesn't see the value: NOT
    // linearizable.
    let mut hist = vec![op(1, "k", "a", 0, 10, 20, "OK"), op(0, "k", "", 0, 30, 40, "OK")];
    hist[1].output.value = String::new();
    hist[1].output.err = "OK".to_string();
    assert_eq!(check(&hist, Duration::from_secs(5)), Some(false));
}

#[test]
fn porcupine_errmaybe_counts_as_success() {
    // The KV model treats a version-matching Put that returned ErrMaybe
    // as executed (same as Go's models1): a later Get must observe it.
    let mut hist = vec![
        op(1, "k", "a", 0, 10, 20, "ErrMaybe"),
        op(0, "k", "", 0, 30, 40, "OK"),
    ];
    hist[1].output.value = "a".to_string();
    hist[1].output.version = 1;
    assert_eq!(check(&hist, Duration::from_secs(5)), Some(true));

    // If the Get does not observe it, the history is not linearizable.
    let mut hist = vec![
        op(1, "k", "a", 0, 10, 20, "ErrMaybe"),
        op(0, "k", "", 0, 30, 40, "OK"),
    ];
    hist[1].output.value = String::new();
    assert_eq!(check(&hist, Duration::from_secs(5)), Some(false));
}

#[test]
fn persister_save_read() {
    let p = tester::Persister::new();
    assert_eq!(p.raft_state_size(), 0);
    p.save(b"state", None);
    assert_eq!(p.read_raft_state(), b"state".to_vec());
    assert_eq!(p.raft_state_size(), 5);
    assert_eq!(p.read_snapshot(), Vec::<u8>::new());
    p.save(b"state2", Some(b"snap"));
    assert_eq!(p.read_snapshot(), b"snap".to_vec());
    let q = p.checkpoint();
    q.save(b"x", None);
    assert_eq!(p.read_raft_state(), b"state2".to_vec());
}

#[test]
fn labgob_roundtrip() {
    let v = (7i64, vec![1i64, 2, 3], "hello".to_string());
    let b = labgob::encode(&v);
    let w: (i64, Vec<i64>, String) = labgob::decode(&b).unwrap();
    assert_eq!(v, w);
    // garbage input decodes to None, not a panic
    let bad: Option<(i64,)> = labgob::decode(&[0u8; 3]);
    assert!(bad.is_none());
}
