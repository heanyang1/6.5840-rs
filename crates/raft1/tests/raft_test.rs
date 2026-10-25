#![allow(non_snake_case, unused_variables, unused_imports)]
//! Tests for Lab 3 (port of Go's raft1/raft_test.go).  Run all tests with
//! `make raft1` or a part with `make RUN=3A raft1` (etc.).

#![allow(non_snake_case)]

mod common;

use common::{RaftTest, RAFT_ELECTION_TIMEOUT, SNAPSHOT_INTERVAL};
use raft1::raft::Raft;
use std::sync::Arc;

fn TestInitialElection3A_impl(ts: &Arc<RaftTest>) {
    ts.cfg.get().unwrap().begin("Test (3A): initial election");

    // is a leader elected?
    ts.check_one_leader();

    // sleep a bit to avoid racing with followers learning of the election,
    // then check that all peers agree on the term.
    std::thread::sleep(std::time::Duration::from_millis(50));
    let term1 = ts.check_terms();
    assert!(term1 >= 1, "term is {}, but should be at least 1", term1);

    // does the leader+term stay the same if there is no network failure?
    std::thread::sleep(2 * RAFT_ELECTION_TIMEOUT);
    let term2 = ts.check_terms();
    if term1 != term2 {
        println!("warning: term changed even though there were no failures");
    }

    // there should still be a leader.
    ts.check_one_leader();
}

#[test]
fn TestInitialElection3A() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);
    TestInitialElection3A_impl(&ts);
    ts.cleanup();
}

#[test]
fn TestReElection3A() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);
    let g = ts.group();

    ts.cfg.get().unwrap().begin("Test (3A): election after network failure");

    let leader1 = ts.check_one_leader();

    // if the leader disconnects, a new one should be elected.
    g.disconnect_all(leader1);
    ts.check_one_leader();

    // if the old leader rejoins, that shouldn't disturb the new leader.
    // and the old leader should switch to follower.
    g.connect_one(leader1);
    let leader2 = ts.check_one_leader();

    // if there's no quorum, no new leader should be elected.
    g.disconnect_all(leader2);
    g.disconnect_all((leader2 + 1) % servers);
    std::thread::sleep(2 * RAFT_ELECTION_TIMEOUT);

    // check that the one connected server does not think it is the leader.
    ts.check_no_leader();

    // if a quorum arises, it should elect a leader.
    g.connect_one((leader2 + 1) % servers);
    ts.check_one_leader();

    // re-join of last node shouldn't prevent leader from existing.
    g.connect_one(leader2);
    ts.check_one_leader();
    ts.cleanup();
}

#[test]
fn TestManyElections3A() {
    let servers = 7;
    let ts = RaftTest::make_test(servers, true, false);
    let g = ts.group();
    use rand::Rng;
    let mut rng = rand::thread_rng();

    ts.cfg.get().unwrap().begin("Test (3A): multiple elections");

    ts.check_one_leader();

    let iters = 10;
    for _ in 1..iters {
        // disconnect three nodes
        let i1 = rng.gen_range(0..servers);
        let i2 = rng.gen_range(0..servers);
        let i3 = rng.gen_range(0..servers);
        g.disconnect_all(i1);
        g.disconnect_all(i2);
        g.disconnect_all(i3);

        // either the current leader should still be alive, or the
        // remaining four should elect a new one.
        ts.check_one_leader();

        g.connect_one(i1);
        g.connect_one(i2);
        g.connect_one(i3);
    }
    ts.check_one_leader();
    ts.cleanup();
}

#[test]
fn TestBasicAgree3B() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);

    ts.cfg.get().unwrap().begin("Test (3B): basic agreement");

    let iters = 3;
    for index in 1..=iters {
        let (nd, _) = ts.n_committed(index as i64);
        assert!(nd == 0, "some have committed before start()");

        let xindex = ts.one((index * 100) as i64, servers, false);
        assert_eq!(xindex, index as i64, "got index {} but expected {}", xindex, index);
    }
    ts.cleanup();
}

/// Check, based on counting bytes of RPCs, that each command is sent to
/// each peer just once.
#[test]
fn TestRPCBytes3B() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);

    ts.cfg.get().unwrap().begin("Test (3B): RPC byte count");

    ts.one(99, servers, false);
    let bytes0 = ts.cfg.get().unwrap().bytes_total();

    let iters = 10;
    let mut sent: i64 = 0;
    for index in 2..iters + 2 {
        let cmd_str = tester::randstring(5000);
        // the tests use strings as commands; we hash them to an i64.
        let cmd = hash_cmd(&cmd_str);
        let xindex = ts.one(cmd, servers, false);
        assert_eq!(xindex, index as i64, "got index {} but expected {}", xindex, index);
        sent += cmd_str.len() as i64;
    }

    let bytes1 = ts.cfg.get().unwrap().bytes_total();
    let got = (bytes1 - bytes0) as i64;
    let expected = servers as i64 * sent;
    assert!(got <= expected + 50000, "too many RPC bytes; got {}, expected {}", got, expected);
    ts.cleanup();
}

fn hash_cmd(s: &str) -> i64 {
    // FNV-1a, deterministic
    let mut h: u64 = 1469598103934665603;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    (h & 0x7fff_ffff_ffff_ffff) as i64
}

/// Test just failure of followers.
#[test]
fn TestFollowerFailure3B() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);
    let g = ts.group();

    ts.cfg.get().unwrap().begin("Test (3B): test progressive failure of followers");

    ts.one(101, servers, false);

    // disconnect one follower from the network.
    let leader1 = ts.check_one_leader();
    g.disconnect_all((leader1 + 1) % servers);

    // the leader and remaining follower should be able to agree despite
    // the disconnected follower.
    ts.one(102, servers - 1, false);
    std::thread::sleep(RAFT_ELECTION_TIMEOUT);
    ts.one(103, servers - 1, false);

    // disconnect the remaining follower
    let leader2 = ts.check_one_leader();
    g.disconnect_all((leader2 + 1) % servers);
    g.disconnect_all((leader2 + 2) % servers);

    // submit a command.
    let rf = ts.raft(leader2).unwrap();
    let (index, _, ok) = rf.start(104);
    assert!(ok, "leader rejected start()");
    assert_eq!(index, 4, "expected index 4, got {}", index);

    std::thread::sleep(2 * RAFT_ELECTION_TIMEOUT);

    // check that command 104 did not commit.
    ts.check_no_agreement(index as i64);
    ts.cleanup();
}

/// Test just failure of leaders.
#[test]
fn TestLeaderFailure3B() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);
    let g = ts.group();

    ts.cfg.get().unwrap().begin("Test (3B): test failure of leaders");

    ts.one(101, servers, false);

    // disconnect the first leader.
    let leader1 = ts.check_one_leader();
    g.disconnect_all(leader1);

    // the remaining followers should elect a new leader.
    ts.one(102, servers - 1, false);
    std::thread::sleep(RAFT_ELECTION_TIMEOUT);
    ts.one(103, servers - 1, false);

    // disconnect the new leader.
    let leader2 = ts.check_one_leader();
    g.disconnect_all(leader2);

    // submit a command to each server.
    for i in 0..servers {
        if let Some(rf) = ts.raft(i) {
            rf.start(104);
        }
    }

    std::thread::sleep(2 * RAFT_ELECTION_TIMEOUT);

    // check that command 104 did not commit.
    ts.check_no_agreement(4);
    ts.cleanup();
}

/// Test that a follower participates after disconnect and re-connect.
#[test]
fn TestFailAgree3B() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);
    let g = ts.group();

    ts.cfg.get().unwrap().begin("Test (3B): agreement after follower reconnects");

    ts.one(101, servers, false);

    // disconnect one follower from the network.
    let leader = ts.check_one_leader();
    g.disconnect_all((leader + 1) % servers);

    // the leader and remaining follower should be able to agree despite
    // the disconnected follower.
    ts.one(102, servers - 1, false);
    ts.one(103, servers - 1, false);
    std::thread::sleep(RAFT_ELECTION_TIMEOUT);
    ts.one(104, servers - 1, false);
    ts.one(105, servers - 1, false);

    // re-connect
    g.connect_one((leader + 1) % servers);

    // the full set of servers should preserve previous agreements, and be
    // able to agree on new commands.
    ts.one(106, servers, true);
    std::thread::sleep(RAFT_ELECTION_TIMEOUT);
    ts.one(107, servers, true);
    ts.cleanup();
}

#[test]
fn TestFailNoAgree3B() {
    let servers = 5;
    let ts = RaftTest::make_test(servers, true, false);
    let g = ts.group();

    ts.cfg.get().unwrap().begin("Test (3B): no agreement if too many followers disconnect");

    ts.one(10, servers, false);

    // 3 of 5 followers disconnect
    let leader = ts.check_one_leader();
    g.disconnect_all((leader + 1) % servers);
    g.disconnect_all((leader + 2) % servers);
    g.disconnect_all((leader + 3) % servers);

    let rf = ts.raft(leader).unwrap();
    let (index, _, ok) = rf.start(20);
    assert!(ok, "leader rejected start()");
    assert_eq!(index, 2, "expected index 2, got {}", index);

    std::thread::sleep(2 * RAFT_ELECTION_TIMEOUT);

    let (n, _) = ts.n_committed(index as i64);
    assert!(n == 0, "{} committed but no majority", n);

    // repair
    g.connect_one((leader + 1) % servers);
    g.connect_one((leader + 2) % servers);
    g.connect_one((leader + 3) % servers);

    // the disconnected majority may have chosen a leader from among their
    // own ranks, forgetting index 2.
    let leader2 = ts.check_one_leader();
    let rf2 = ts.raft(leader2).unwrap();
    let (index2, _, ok2) = rf2.start(30);
    assert!(ok2, "leader2 rejected start()");
    assert!((2..=3).contains(&index2), "unexpected index {}", index2);

    ts.one(1000, servers, true);
    ts.cleanup();
}

#[test]
fn TestConcurrentStarts3B() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);

    ts.cfg.get().unwrap().begin("Test (3B): concurrent start()s");

    let mut success = false;
    'loop_: for try_ in 0..5 {
        if try_ > 0 {
            // give solution some time to settle
            std::thread::sleep(std::time::Duration::from_secs(3));
        }

        let leader = ts.check_one_leader();
        let rf = ts.raft(leader).unwrap();
        let (_, term, ok) = rf.start(1);
        if !ok {
            // leader moved on really quickly
            continue;
        }

        let iters = 5;
        let mut handles = Vec::new();
        for ii in 0..iters {
            let rf = ts.raft(leader).unwrap();
            handles.push(std::thread::spawn(move || {
                let (i, term1, ok) = rf.start(100 + ii as i64);
                if term1 != term || !ok {
                    return None;
                }
                Some(i)
            }));
        }
        let mut is: Vec<usize> = Vec::new();
        for h in handles {
            if let Some(i) = h.join().unwrap() {
                is.push(i);
            }
        }

        for j in 0..servers {
            if let Some(rf) = ts.raft(j) {
                let (t, _) = rf.get_state();
                if t != term {
                    // term changed -- can't expect low RPC counts
                    continue 'loop_;
                }
            }
        }

        let mut failed = false;
        let mut cmds: Vec<i64> = Vec::new();
        for index in &is {
            let cmd = ts.wait(*index as i64, servers, term);
            if cmd == -1 {
                // peers have moved on to later terms so we can't expect
                // all start()s to have succeeded
                failed = true;
                break;
            }
            cmds.push(cmd);
        }

        if failed {
            continue;
        }

        for ii in 0..iters {
            let x = 100 + ii as i64;
            assert!(cmds.contains(&x), "cmd {} missing in {:?}", x, cmds);
        }

        success = true;
        break;
    }

    assert!(success, "term changed too often");
    ts.cleanup();
}

#[test]
fn TestRejoin3B() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);
    let g = ts.group();

    ts.cfg.get().unwrap().begin("Test (3B): rejoin of partitioned leader");

    ts.one(101, servers, true);

    // leader network failure
    let leader1 = ts.check_one_leader();
    g.disconnect_all(leader1);

    // make old leader try to agree on some entries
    if let Some(rf) = ts.raft(leader1) {
        rf.start(102);
        rf.start(103);
        rf.start(104);
    }

    // new leader commits, also for index=2
    ts.one(103, 2, true);

    // new leader network failure
    let leader2 = ts.check_one_leader();
    g.disconnect_all(leader2);

    // old leader connected again
    g.connect_one(leader1);

    ts.one(104, 2, true);

    // all together now
    g.connect_one(leader2);

    ts.one(105, servers, true);
    ts.cleanup();
}

#[test]
fn TestBackup3B() {
    let servers = 5;
    let ts = RaftTest::make_test(servers, true, false);
    let g = ts.group();
    use rand::Rng;
    let mut rng = rand::thread_rng();

    ts.cfg.get().unwrap().begin("Test (3B): leader backs up quickly over incorrect follower logs");

    ts.one(rng.gen(), servers, true);

    // put leader and one follower in a partition
    let leader1 = ts.check_one_leader();
    g.disconnect_all((leader1 + 2) % servers);
    g.disconnect_all((leader1 + 3) % servers);
    g.disconnect_all((leader1 + 4) % servers);

    // submit lots of commands that won't commit
    for _ in 0..50 {
        if let Some(rf) = ts.raft(leader1) {
            rf.start(rng.gen());
        }
    }

    std::thread::sleep(RAFT_ELECTION_TIMEOUT / 2);

    g.disconnect_all((leader1 + 0) % servers);
    g.disconnect_all((leader1 + 1) % servers);

    // allow other partition to recover
    g.connect_one((leader1 + 2) % servers);
    g.connect_one((leader1 + 3) % servers);
    g.connect_one((leader1 + 4) % servers);

    // lots of successful commands to new group.
    for _ in 0..50 {
        ts.one(rng.gen(), 3, true);
    }

    // now another partitioned leader and one follower
    let leader2 = ts.check_one_leader();
    let mut other = (leader1 + 2) % servers;
    if leader2 == other {
        other = (leader2 + 1) % servers;
    }
    g.disconnect_all(other);

    // lots more commands that won't commit
    for _ in 0..50 {
        if let Some(rf) = ts.raft(leader2) {
            rf.start(rng.gen());
        }
    }

    std::thread::sleep(RAFT_ELECTION_TIMEOUT / 2);

    // bring original leader back to life,
    for i in 0..servers {
        g.disconnect_all(i);
    }
    g.connect_one((leader1 + 0) % servers);
    g.connect_one((leader1 + 1) % servers);
    g.connect_one(other);

    // lots of successful commands to new group.
    for _ in 0..50 {
        ts.one(rng.gen(), 3, true);
    }

    // now everyone
    for i in 0..servers {
        g.connect_one(i);
    }
    ts.one(rng.gen(), servers, true);
    ts.cleanup();
}

#[test]
fn TestCount3B() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);
    use rand::Rng;
    let mut rng = rand::thread_rng();

    ts.cfg.get().unwrap().begin("Test (3B): RPC counts aren't too high");

    let rpcs = || -> u64 {
        let mut n = 0;
        for j in 0..servers {
            n += ts.rpc_count(j);
        }
        n
    };

    let leader = ts.check_one_leader();

    let total1 = rpcs();
    assert!((1..=30).contains(&total1), "too many or few RPCs ({}) to elect initial leader", total1);

    let mut total2: u64 = 0;
    let mut success = false;
    'loop_: for try_ in 0..5 {
        if try_ > 0 {
            // give solution some time to settle
            std::thread::sleep(std::time::Duration::from_secs(3));
        }

        let leader = ts.check_one_leader();
        let total1 = rpcs();

        let rf = ts.raft(leader).unwrap();
        let (starti, term, ok) = rf.start(1);
        if !ok {
            // leader moved on really quickly
            continue;
        }

        let iters = 10;
        let mut cmds: Vec<i64> = Vec::new();
        for i in 1..iters + 2 {
            let x: i64 = rng.gen();
            cmds.push(x);
            let (index1, term1, ok) = rf.start(x);
            if term1 != term {
                // Term changed while starting
                continue 'loop_;
            }
            if !ok {
                // No longer the leader, so term has changed
                continue 'loop_;
            }
            assert_eq!(
                starti + i as usize,
                index1,
                "start() failed: the command should be at index {}, but the leader puts it at {}",
                starti + i,
                index1
            );
        }

        for i in 1..iters + 1 {
            let cmd = ts.wait(starti as i64 + i as i64, servers, term);
            assert!(
                cmd == cmds[i - 1],
                "wrong value {:?} committed for index {}; expected {:?}",
                cmd,
                starti as i64 + i as i64,
                cmds
            );
        }

        let mut failed = false;
        total2 = 0;
        for j in 0..servers {
            if let Some(rf) = ts.raft(j) {
                let (t, _) = rf.get_state();
                if t != term {
                    // term changed -- can't expect low RPC counts
                    failed = true;
                }
            }
            total2 += ts.rpc_count(j);
        }

        if failed {
            continue 'loop_;
        }

        assert!(
            total2 - total1 <= ((iters + 1 + 3) * 3) as u64,
            "too many RPCs ({}) for {} entries",
            total2 - total1,
            iters
        );

        success = true;
        break;
    }

    assert!(success, "term changed too often");

    ts.cfg.get().unwrap().begin("checking reasonable RPC counts in idle");

    std::thread::sleep(RAFT_ELECTION_TIMEOUT);

    let mut total3: u64 = 0;
    for j in 0..servers {
        total3 += ts.rpc_count(j);
    }

    assert!(
        total3 - total2 <= 3 * 20,
        "too many RPCs ({}) for 1 second of idleness",
        total3 - total2
    );
    ts.cleanup();
}

// ------------------------------ 3C --------------------------------------

#[test]
fn TestPersist13C() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);
    let g = ts.group();

    ts.cfg.get().unwrap().begin("Test (3C): basic persistence");

    ts.one(11, servers, true);

    ts.kill(&g.all());
    ts.restart(&g.all());

    ts.one(12, servers, true);

    let leader1 = ts.check_one_leader();
    ts.kill(&[leader1]);
    ts.restart(&[leader1]);

    ts.one(13, servers, true);

    let leader2 = ts.check_one_leader();
    ts.kill(&[leader2]);

    ts.one(14, servers - 1, true);

    ts.restart(&[leader2]);

    ts.wait(4, servers, -1); // wait for leader2 to join before killing i3

    let i3 = (ts.check_one_leader() + 1) % servers;
    ts.kill(&[i3]);

    ts.one(15, servers - 1, true);

    ts.restart(&[i3]);

    ts.one(16, servers, true);
    ts.cleanup();
}

#[test]
fn TestPersist23C() {
    let servers = 5;
    let ts = RaftTest::make_test(servers, true, false);
    let g = ts.group();

    ts.cfg.get().unwrap().begin("Test (3C): more persistence");

    let mut index = 1;
    for _ in 0..5 {
        ts.one(10 + index, servers, true);
        index += 1;

        let leader1 = ts.check_one_leader();

        ts.kill(&[(leader1 + 1) % servers, (leader1 + 2) % servers]);

        ts.one(10 + index, servers - 2, true);
        index += 1;

        ts.kill(&[
            (leader1 + 0) % servers,
            (leader1 + 3) % servers,
            (leader1 + 4) % servers,
        ]);

        ts.restart(&[(leader1 + 1) % servers, (leader1 + 2) % servers]);

        std::thread::sleep(RAFT_ELECTION_TIMEOUT);

        ts.restart(&[(leader1 + 3) % servers]);

        ts.one(10 + index, servers - 2, true);
        index += 1;

        ts.restart(&[(leader1 + 4) % servers]);
        ts.restart(&[(leader1 + 0) % servers]);
    }

    ts.one(1000, servers, true);
    ts.cleanup();
}

#[test]
fn TestPersist33C() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, true, false);
    let g = ts.group();

    ts.cfg.get().unwrap().begin("Test (3C): partitioned leader and one follower crash, leader restarts");

    ts.one(101, 3, true);

    let leader = ts.check_one_leader();
    g.disconnect_all((leader + 2) % servers);

    ts.one(102, 2, true);

    ts.kill(&[(leader + 0) % servers, (leader + 1) % servers]);
    g.connect_one((leader + 2) % servers);
    ts.restart(&[(leader + 0) % servers]);

    ts.one(103, 2, true);

    ts.restart(&[(leader + 1) % servers]);

    ts.one(104, servers, true);
    ts.cleanup();
}

/// Test the scenarios described in Figure 8 of the extended Raft paper.
#[test]
fn TestFigure83C() {
    let servers = 5;
    let ts = RaftTest::make_test(servers, true, false);
    use rand::Rng;
    let mut rng = rand::thread_rng();

    ts.cfg.get().unwrap().begin("Test (3C): Figure 8");

    ts.one(rng.gen(), 1, true);

    let mut nup = servers as i32;
    for _ in 0..1000 {
        let mut leader = -1i32;
        for i in 0..servers {
            if let Some(rf) = ts.raft(i) {
                let cmd: i64 = rng.gen();
                let (_, _, ok) = rf.start(cmd);
                if ok {
                    leader = i as i32;
                }
            }
        }

        if (rng.gen::<u32>() % 1000) < 100 {
            let ms = rng.gen_range(0..(RAFT_ELECTION_TIMEOUT.as_millis() as u64) / 2);
            std::thread::sleep(std::time::Duration::from_millis(ms));
        } else {
            let ms = rng.gen_range(0..13);
            std::thread::sleep(std::time::Duration::from_millis(ms));
        }

        if leader != -1 {
            ts.kill(&[leader as usize]);
            nup -= 1;
        }

        if nup < 3 {
            let s = rng.gen_range(0..servers);
            if ts.raft(s).is_none() {
                ts.restart(&[s]);
                nup += 1;
            }
        }
    }

    for i in 0..servers {
        if ts.raft(i).is_none() {
            ts.restart(&[i]);
        }
    }

    ts.one(rng.gen(), servers, true);
    ts.cleanup();
}

#[test]
fn TestUnreliableAgree3C() {
    let servers = 5;
    let ts = RaftTest::make_test(servers, false, false);

    ts.cfg.get().unwrap().begin("Test (3C): unreliable agreement");

    let mut handles = Vec::new();
    for iters in 1..50 {
        for j in 0..4 {
            let ts2 = Arc::clone(&ts);
            handles.push(std::thread::spawn(move || {
                ts2.one((100 * iters + j) as i64, 1, true);
            }));
        }
        ts.one(iters as i64, 1, true);
    }

    ts.cfg.get().unwrap().set_reliable(true);

    for h in handles {
        h.join().unwrap();
    }

    ts.one(100, servers, true);
    ts.cleanup();
}

#[test]
fn TestFigure8Unreliable3C() {
    let servers = 5;
    let ts = RaftTest::make_test(servers, false, false);
    let g = ts.group();
    use rand::Rng;
    let mut rng = rand::thread_rng();

    ts.cfg.get().unwrap().begin("Test (3C): Figure 8 (unreliable)");

    ts.one(rng.gen_range(0..10000) as i64, 1, true);

    let mut nup = servers as i32;
    for iters in 0..1000 {
        if iters == 200 {
            ts.cfg.get().unwrap().set_long_reordering(true);
        }
        let mut leader = -1i32;
        for i in 0..servers {
            let cmd: i64 = rng.gen_range(0..10000);
            let mut ok = false;
            if let Some(rf) = ts.raft(i) {
                let (_, _, ok1) = rf.start(cmd);
                ok = ok1;
            }
            if ok && g.is_connected(i) {
                leader = i as i32;
            }
        }

        if (rng.gen::<u32>() % 1000) < 100 {
            let ms = rng.gen_range(0..(RAFT_ELECTION_TIMEOUT.as_millis() as u64) / 2);
            std::thread::sleep(std::time::Duration::from_millis(ms));
        } else {
            let ms = rng.gen_range(0..13);
            std::thread::sleep(std::time::Duration::from_millis(ms));
        }

        if leader != -1 && (rng.gen::<u32>() % 1000) < RAFT_ELECTION_TIMEOUT.as_millis() as u32 / 2 {
            g.disconnect_all(leader as usize);
            nup -= 1;
        }

        if nup < 3 {
            let s = rng.gen_range(0..servers);
            if !g.is_connected(s) {
                g.connect_one(s);
                nup += 1;
            }
        }
    }

    for i in 0..servers {
        if !g.is_connected(i) {
            g.connect_one(i);
        }
    }

    ts.one(rng.gen_range(0..10000) as i64, servers, true);
    ts.cleanup();
}

fn internal_churn(reliable: bool) {
    let servers = 5;
    let ts = RaftTest::make_test(servers, reliable, false);
    let g = ts.group();
    use rand::Rng;
    use std::sync::atomic::Ordering;

    if reliable {
        ts.cfg.get().unwrap().begin("Test (3C): churn");
    } else {
        ts.cfg.get().unwrap().begin("Test (3C): unreliable churn");
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // create concurrent clients
    let n = servers;
    let cfn = move |me: usize, ch: std::sync::mpsc::Sender<Vec<i64>>, ts: Arc<RaftTest>, stop: Arc<std::sync::atomic::AtomicBool>| {
        let mut rng = rand::thread_rng();
        let mut values: Vec<i64> = Vec::new();
        while !stop.load(Ordering::SeqCst) {
            let x: i64 = rng.gen();
            let mut index: i64 = -1;
            let mut ok = false;
            for i in 0..n {
                // try them all, maybe one of them is a leader
                if let Some(rf) = ts.raft(i) {
                    let (index1, _, ok1) = rf.start(x);
                    if ok1 {
                        ok = true;
                        index = index1 as i64;
                    }
                }
            }
            if ok {
                // maybe leader will commit our value, maybe not.  but don't
                // wait forever.
                for to in [10u64, 20, 50, 100, 200] {
                    let (nd, cmd) = ts.n_committed(index);
                    if nd > 0 {
                        if let Some(xx) = cmd {
                            if xx == x {
                                values.push(x);
                            }
                        }
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(to));
                }
            } else {
                std::thread::sleep(std::time::Duration::from_millis(79 + (me * 17) as u64));
            }
        }
        ch.send(values).unwrap();
    };

    let ncli = 3;
    let (tx, rx) = std::sync::mpsc::channel();
    let mut handles = Vec::new();
    for i in 0..ncli {
        let ts2 = Arc::clone(&ts);
        let stop2 = Arc::clone(&stop);
        let tx2 = tx.clone();
        handles.push(std::thread::spawn(move || cfn(i, tx2, ts2, stop2)));
    }

    let mut rng = rand::thread_rng();
    for _ in 0..20 {
        if (rng.gen::<u32>() % 1000) < 200 {
            let i = rng.gen_range(0..servers);
            g.disconnect_all(i);
        }

        if (rng.gen::<u32>() % 1000) < 500 {
            let i = rng.gen_range(0..servers);
            if ts.raft(i).is_none() {
                ts.restart(&[i]);
            }
            g.connect_one(i);
        }

        if (rng.gen::<u32>() % 1000) < 200 {
            let i = rng.gen_range(0..servers);
            if ts.raft(i).is_some() {
                ts.kill(&[i]);
            }
        }

        // Make crash/restart infrequent enough that the peers can often
        // keep up, but not so infrequent that everything has settled down
        // from one change to the next.
        std::thread::sleep(RAFT_ELECTION_TIMEOUT * 7 / 10);
    }

    std::thread::sleep(RAFT_ELECTION_TIMEOUT);
    ts.cfg.get().unwrap().set_reliable(true);
    for i in 0..servers {
        if ts.raft(i).is_none() {
            ts.restart(&[i]);
        }
        g.connect_one(i);
    }

    stop.store(true, Ordering::SeqCst);

    // checking if any client has failed
    let mut values: Vec<i64> = Vec::new();
    for _ in 0..ncli {
        let vv = rx.recv_timeout(std::time::Duration::from_secs(60))
            .expect("client failed");
        values.extend(vv);
    }
    for h in handles {
        h.join().unwrap();
    }

    std::thread::sleep(RAFT_ELECTION_TIMEOUT);

    let last_index = ts.one(rng.gen(), servers, true);

    let mut really: Vec<i64> = Vec::new();
    for index in 1..=last_index {
        let v = ts.wait(index, servers, -1);
        really.push(v);
    }

    // checking if committed values observed by the clients remain in the log
    for v1 in &values {
        assert!(really.contains(v1), "didn't find a value {}", v1);
    }
    ts.cleanup();
}

#[test]
fn TestReliableChurn3C() {
    internal_churn(true);
}

#[test]
fn TestUnreliableChurn3C() {
    internal_churn(false);
}

// ------------------------------ 3D --------------------------------------

fn snapcommon(name: &str, disconnect: bool, reliable: bool, crash: bool) {
    let iters = 30;
    let servers = 3;
    let ts = RaftTest::make_test(servers, reliable, true);
    let g = ts.group();
    use rand::Rng;
    let mut rng = rand::thread_rng();

    ts.cfg.get().unwrap().begin(name);

    ts.one(rng.gen(), servers, true);
    let mut leader1 = ts.check_one_leader();

    for i in 0..iters {
        let mut victim = (leader1 + 1) % servers;
        let mut sender = leader1;
        if i % 3 == 1 {
            sender = (leader1 + 1) % servers;
            victim = leader1;
        }

        if disconnect {
            g.disconnect_all(victim);
            ts.one(rng.gen(), servers - 1, true);
        }
        if crash {
            ts.kill(&[victim]);
            ts.one(rng.gen(), servers - 1, true);
        }

        // perhaps send enough to get a snapshot
        let nn = (SNAPSHOT_INTERVAL / 2) + rng.gen_range(0..SNAPSHOT_INTERVAL);
        for _ in 0..nn {
            if let Some(rf) = ts.raft(sender) {
                rf.start(rng.gen());
            }
        }

        // let applier threads catch up with the start()'s
        if !disconnect && !crash {
            // make sure all followers have caught up, so that an
            // InstallSnapshot RPC isn't required for TestSnapshotBasic3D().
            ts.one(rng.gen(), servers, true);
        } else {
            ts.one(rng.gen(), servers - 1, true);
        }

        if ts.raft_size() >= MAXLOGSIZE {
            panic!("Raft size too large");
        }
        if disconnect {
            // reconnect a follower, who maybe behind and needs to receive a
            // snapshot to catch up.
            g.connect_one(victim);
            ts.one(rng.gen(), servers, true);
            leader1 = ts.check_one_leader();
        }
        if crash {
            ts.restart(&[victim]);
            ts.one(rng.gen(), servers, true);
            leader1 = ts.check_one_leader();
        }
    }
    ts.cleanup();
}

const MAXLOGSIZE: usize = 2000;

#[test]
fn TestSnapshotBasic3D() {
    snapcommon("Test (3D): snapshots basic", false, true, false);
}

#[test]
fn TestSnapshotInstall3D() {
    snapcommon("Test (3D): install snapshots (disconnect)", true, true, false);
}

#[test]
fn TestSnapshotInstallUnreliable3D() {
    snapcommon("Test (3D): install snapshots (disconnect)", true, false, false);
}

#[test]
fn TestSnapshotInstallCrash3D() {
    snapcommon("Test (3D): install snapshots (crash)", false, true, true);
}

#[test]
fn TestSnapshotInstallUnCrash3D() {
    snapcommon("Test (3D): install snapshots (crash)", false, false, true);
}

/// Do the servers persist the snapshots, and restart using snapshot along
/// with the tail of the log?
#[test]
fn TestSnapshotAllCrash3D() {
    let servers = 3;
    let iters = 5;
    let ts = RaftTest::make_test(servers, false, true);
    use rand::Rng;
    let mut rng = rand::thread_rng();

    ts.cfg.get().unwrap().begin("Test (3D): crash and restart all servers");

    ts.one(rng.gen(), servers, true);

    for _ in 0..iters {
        // perhaps enough to get a snapshot
        let nn = (SNAPSHOT_INTERVAL / 2) + rng.gen_range(0..SNAPSHOT_INTERVAL);
        for _ in 0..nn {
            ts.one(rng.gen(), servers, true);
        }

        let index1 = ts.one(rng.gen(), servers, true);

        // crash all
        let all = ts.group().all();
        ts.kill(&all);
        ts.restart(&all);

        let index2 = ts.one(rng.gen(), servers, true);
        assert!(
            index2 >= index1 + 1,
            "index decreased from {} to {}",
            index1,
            index2
        );
    }
    ts.cleanup();
}

/// Do servers correctly initialize their in-memory copy of the snapshot,
/// making sure that future writes to persistent state don't lose state?
#[test]
fn TestSnapshotInit3D() {
    let servers = 3;
    let ts = RaftTest::make_test(servers, false, true);
    use rand::Rng;
    let mut rng = rand::thread_rng();

    ts.cfg.get().unwrap().begin("Test (3D): snapshot initialization after crash");
    ts.one(rng.gen(), servers, true);

    // enough ops to make a snapshot
    let nn = SNAPSHOT_INTERVAL + 1;
    for _ in 0..nn {
        ts.one(rng.gen(), servers, true);
    }

    let all = ts.group().all();
    ts.kill(&all);
    ts.restart(&all);

    // a single op, to get something to be written back to persistent
    // storage.
    ts.one(rng.gen(), servers, true);

    let all = ts.group().all();
    ts.kill(&all);
    ts.restart(&all);

    // do another op to trigger potential bug
    ts.one(rng.gen(), servers, true);
    ts.cleanup();
}

// Keep Raft imported for doc purposes.
#[allow(dead_code)]
fn _t(_r: &Raft<i64>) {}
