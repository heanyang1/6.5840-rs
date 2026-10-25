//! Test-support server for the rsm tests (port of Go's
//! kvraft1/rsm/server.go; provided code, don't modify).

use super::rsm::{make_rsm, Rsm, StateMachine};
use crate::rpc;
use raft1::api::Cmd;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Inc;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IncRep {
    pub n: i64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Null;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NullRep;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Dec;

/// The request type sent through Raft by the rsm tests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Req {
    Inc(Inc),
    Null(Null),
    Dec(Dec),
}

/// The reply type returned by do_op.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Rep {
    Inc(IncRep),
    Null(NullRep),
}


/// The "service" whose state is replicated in the 4A tests: a single
/// integer counter (Go: rsmSrv + its StateMachine impl; in Rust the state
/// machine is a separate object owned by the rsm, sharing the counter with
/// the test handle).
struct CounterSm {
    me: usize,
    counter: Arc<Mutex<i64>>,
}

impl StateMachine<Req, Rep> for CounterSm {
    fn do_op(&self, req: Req) -> Rep {
        match req {
            Req::Inc(_) => {
                let mut c = self.counter.lock().unwrap();
                *c += 1;
                Rep::Inc(IncRep { n: *c })
            }
            Req::Null(_) => Rep::Null(NullRep),
            _ => panic!("DoOp should execute only Inc and Null"),
        }
    }
    fn snapshot(&self) -> Vec<u8> {
        labgob::encode(&*self.counter.lock().unwrap())
    }
    fn restore(&self, data: &[u8]) {
        match labgob::decode::<i64>(data) {
            Some(c) => *self.counter.lock().unwrap() = c,
            None => panic!("{} couldn't decode counter", self.me),
        }
    }
}

/// The handle the tests use (Go: rsmSrv); shares the counter with the
/// state machine so get_counter() observes applied operations.
pub struct RsmSrv {
    pub me: usize,
    counter: Arc<Mutex<i64>>,
    pub rsm: Arc<Rsm<Req, Rep>>,
}

impl RsmSrv {
    pub fn get_counter(&self) -> i64 {
        *self.counter.lock().unwrap()
    }
    pub fn submit(&self, req: Req) -> (rpc::Err, Option<Rep>) {
        self.rsm.submit(req)
    }
}

/// Build the rsm test server (Go: NewRSMSrv).  Returns the handle the
/// tests use and the ServerBox for the harness.
pub fn new_rsm_srv(
    args: tester::ServerArgs,
    maxraftstate: i64,
) -> (Arc<RsmSrv>, tester::ServerBox) {
    let me = args.srv;
    let counter = Arc::new(Mutex::new(0));
    let sm = Arc::new(CounterSm {
        me,
        counter: Arc::clone(&counter),
    });
    let rsm = make_rsm(args.ends.clone(), me, Arc::clone(&args.persister), maxraftstate, sm);
    let handle = Arc::new(RsmSrv {
        me,
        counter,
        rsm: Arc::clone(&rsm),
    });
    let h = Arc::clone(&handle);
    let box_ = tester::ServerBox::new()
        .with_service(raft1::raft::raft_service(&rsm.raft()))
        .with_kill(move || h.rsm.raft().kill());
    (handle, box_)
}
