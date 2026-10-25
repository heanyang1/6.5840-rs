//! Test configuration: one simulated network, named server groups that can
//! be killed/restarted, and clients whose connectivity can be controlled.
//!
//! Rust port of Go tester1's Config + ServerGrp + Clnt.  The main
//! differences from the Go labs: servers run as threads in the test
//! process (not separate daemon processes), and the harness wires up RPC
//! handlers explicitly (Go used reflection) via per-lab `start_server`
//! functions.

use crate::Persister;
use labrpc::{ClientEnd, MultiService, Network};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub type Tgid = i64;
pub const GRP0: Tgid = 0;

/// Network name of server `i` in group `gid` (Go: tester.ServerName).
pub fn server_name(gid: Tgid, i: usize) -> String {
    format!("server-{}-{}", gid, i)
}

/// What a per-lab `start_server` function receives from the harness.
pub struct ServerArgs {
    /// RPC client ends, one per peer, index = peer index (for Raft-style
    /// servers).  For single-server labs (kvsrv) it has length 1.
    pub ends: Vec<Arc<ClientEnd>>,
    pub gid: Tgid,
    pub srv: usize,
    pub persister: Arc<Persister>,
}

/// What a per-lab `start_server` function returns: the RPC services to
/// publish under this server's network name, and an optional kill closure
/// (called by the harness when the server "crashes": for Raft-based
/// servers this calls `rf.kill()`).
#[derive(Default)]
pub struct ServerBox {
    pub services: Vec<Arc<labrpc::Service>>,
    pub kill: Option<Box<dyn FnOnce() + Send>>,
}

impl ServerBox {
    pub fn new() -> ServerBox {
        ServerBox::default()
    }
    pub fn with_service(mut self, svc: labrpc::Service) -> Self {
        self.services.push(Arc::new(svc));
        self
    }
    pub fn with_kill(mut self, f: impl FnOnce() + Send + 'static) -> Self {
        self.kill = Some(Box::new(f));
        self
    }
}

pub type MakeServer = Arc<dyn Fn(ServerArgs) -> ServerBox + Send + Sync>;

/// One group of servers (the analog of Go's ServerGrp).
pub struct ServerGrp {
    pub gid: Tgid,
    pub n: usize,
    pub maxraftstate: i64,
    net: Arc<Network>,
    make_server: MakeServer,
    persisters: Vec<Arc<Persister>>,
    running: Mutex<HashMap<usize, Box<dyn FnOnce() + Send>>>,
    // ends[i][j]: the client end used by server i to reach server j.
    ends: Vec<Vec<Arc<ClientEnd>>>,
    connected: Mutex<Vec<bool>>,
}

impl ServerGrp {
    fn new(
        net: Arc<Network>,
        gid: Tgid,
        n: usize,
        maxraftstate: i64,
        make_server: MakeServer,
    ) -> Arc<ServerGrp> {
        let mut ends = Vec::with_capacity(n);
        for i in 0..n {
            let mut row = Vec::with_capacity(n);
            for j in 0..n {
                let ename = format!("{}-to-{}", server_name(gid, i), server_name(gid, j));
                let end = net.make_end(&ename);
                net.connect(&ename, &server_name(gid, j));
                net.enable(&ename, true);
                row.push(end);
            }
            ends.push(row);
        }
        let grp = Arc::new(ServerGrp {
            gid,
            n,
            maxraftstate,
            net,
            make_server,
            persisters: (0..n).map(|_| Persister::new()).collect(),
            running: Mutex::new(HashMap::new()),
            ends,
            connected: Mutex::new(vec![false; n]),
        });
        grp.start_servers(&grp.all());
        grp
    }

    pub fn all(&self) -> Vec<usize> {
        (0..self.n).collect()
    }

    pub fn srv_names(&self) -> Vec<String> {
        (0..self.n).map(|i| server_name(self.gid, i)).collect()
    }

    pub fn srv_names_to(&self, to: &[usize]) -> Vec<String> {
        to.iter().map(|&i| server_name(self.gid, i)).collect()
    }

    pub fn srv_name(&self, i: usize) -> String {
        server_name(self.gid, i)
    }

    /// The ends from server `i` to all peers (for raft.Make()).
    pub fn ends_from(&self, i: usize) -> Vec<Arc<ClientEnd>> {
        self.ends[i].clone()
    }

    fn make_instance(&self, i: usize) -> ServerBox {
        let args = ServerArgs {
            ends: self.ends_from(i),
            gid: self.gid,
            srv: i,
            persister: Arc::clone(&self.persisters[i]),
        };
        (self.make_server)(args)
    }

    /// Start (or restart) server i.
    pub fn start_server(&self, i: usize) {
        let instance = self.make_instance(i);
        let mut multi = MultiService::new();
        for svc in &instance.services {
            multi.add_service(Arc::clone(svc));
        }
        let name = self.srv_name(i);
        self.net.add_server(&name, Arc::new(multi));
        self.running
            .lock()
            .unwrap()
            .insert(i, instance.kill.unwrap_or_else(|| Box::new(|| {})));
        self.connect_one(i);
    }

    pub fn start_servers(&self, srvs: &[usize]) {
        for &i in srvs {
            self.start_server(i);
        }
    }

    /// Restart every server in the group.
    pub fn start_all(&self) {
        self.start_servers(&self.all());
    }

    /// Shut down (crash) server i: kill it and make it unreachable.
    pub fn shutdown_server(&self, i: usize) {
        self.disconnect(i, &self.all());
        self.net.delete_server(&self.srv_name(i));
        if let Some(kill) = self.running.lock().unwrap().remove(&i) {
            kill();
        }
    }

    /// Kill the listed servers (Go: ServerGrp.Kill).
    pub fn kill(&self, srvs: &[usize]) {
        for &i in srvs {
            self.shutdown_server(i);
        }
    }

    pub fn shutdown(&self) {
        self.kill(&self.all());
    }

    /// Connect server i to all servers (Go: ConnectOne).
    pub fn connect_one(&self, i: usize) {
        self.connect(i, &self.all());
    }

    pub fn connect_all(&self) {
        for i in 0..self.n {
            self.connect_one(i);
        }
    }

    /// Attach server i to the servers listed in `to` (Go: ServerGrp.connect).
    pub fn connect(&self, i: usize, to: &[usize]) {
        self.connected.lock().unwrap()[i] = true;
        for &j in to {
            // outgoing end points
            let ename = format!("{}-to-{}", self.srv_name(i), self.srv_name(j));
            self.net.enable(&ename, true);
            // incoming end points to me
            if self.is_connected(j) {
                let rname = format!("{}-to-{}", self.srv_name(j), self.srv_name(i));
                self.net.enable(&rname, true);
            }
        }
    }

    /// Detach server i from the servers listed in `from`.
    pub fn disconnect(&self, i: usize, from: &[usize]) {
        self.connected.lock().unwrap()[i] = false;
        for &j in from {
            let ename = format!("{}-to-{}", self.srv_name(i), self.srv_name(j));
            self.net.enable(&ename, false);
            // Also cut the incoming direction, exactly like the Go
            // tester's ServerGrp.disconnect.
            let rname = format!("{}-to-{}", self.srv_name(j), self.srv_name(i));
            self.net.enable(&rname, false);
        }
    }

    /// Disconnect server i from all servers.
    pub fn disconnect_all(&self, i: usize) {
        self.disconnect(i, &self.all());
    }

    pub fn is_connected(&self, i: usize) -> bool {
        self.connected.lock().unwrap()[i]
    }

    pub fn get_connected(&self) -> Vec<bool> {
        self.connected.lock().unwrap().clone()
    }

    /// Partition the servers into two groups.
    pub fn partition(&self, p1: &[usize], p2: &[usize]) {
        for &i in p1 {
            self.disconnect(i, p2);
            self.connect(i, p1);
        }
        for &i in p2 {
            self.disconnect(i, p1);
            self.connect(i, p2);
        }
    }

    /// Partition servers into 2 groups and put current leader in minority
    /// (Go: MakePartition).
    pub fn make_partition(&self, leader: usize) -> (Vec<usize>, Vec<usize>) {
        let n = self.n;
        let mut p1 = Vec::with_capacity(n / 2 + 1);
        let mut p2 = Vec::with_capacity(n / 2);
        for i in 0..n {
            if i != leader {
                if p1.len() < n / 2 + 1 {
                    p1.push(i);
                } else {
                    p2.push(i);
                }
            }
        }
        p2.push(leader);
        (p1, p2)
    }

    /// Number of RPCs that reached server i.
    pub fn rpc_count(&self, i: usize) -> u64 {
        self.net.get_count(&self.srv_name(i))
    }

    /// Maximum persistent Raft state size across the group's servers
    /// (Go: ServerGrp.RaftSize takes the max).
    pub fn raft_size(&self) -> usize {
        self.persisters
            .iter()
            .map(|p| p.raft_state_size())
            .max()
            .unwrap_or(0)
    }

    /// Maximum snapshot size across the group's servers (Go:
    /// ServerGrp.SnapshotSize takes the max).
    pub fn snapshot_size(&self) -> usize {
        self.persisters
            .iter()
            .map(|p| p.snapshot_size())
            .max()
            .unwrap_or(0)
    }

    /// Memory usage of a running server is not directly observable in
    /// Rust; the kvsrv tests use tester::mem::allocated() instead.
    pub fn snapshot_state(&self, i: usize) -> Arc<Persister> {
        Arc::clone(&self.persisters[i])
    }
}

/// A client handle: lazily creates per-client ends to servers, and can
/// enable/disable them (the analog of Go's tester.Clnt).
pub struct Clnt {
    pub id: u64,
    net: Arc<Network>,
    ends: Mutex<HashMap<String, (String, Arc<ClientEnd>)>>, // server -> (endname, end)
    srvs: Mutex<Option<HashSet<String>>>, // None = all allowed
}

static NEXT_CLNT: AtomicU64 = AtomicU64::new(1);

impl Clnt {
    fn new(net: Arc<Network>, srvs: Option<Vec<String>>) -> Arc<Clnt> {
        Arc::new(Clnt {
            id: NEXT_CLNT.fetch_add(1, Ordering::SeqCst),
            net,
            ends: Mutex::new(HashMap::new()),
            srvs: Mutex::new(srvs.map(|v| v.into_iter().collect())),
        })
    }

    fn allowed(&self, server: &str) -> bool {
        match self.srvs.lock().unwrap().as_ref() {
            None => true,
            Some(set) => set.contains(server),
        }
    }

    fn make_end(&self, server: &str) -> Arc<ClientEnd> {
        let mut g = self.ends.lock().unwrap();
        if let Some((_, e)) = g.get(server) {
            return Arc::clone(e);
        }
        let ename = format!("clnt-{}-{}", self.id, server);
        let end = self.net.make_end(&ename);
        self.net.connect(&ename, server);
        let allowed = self.allowed(server);
        self.net.enable(&ename, allowed);
        g.insert(server.to_string(), (ename, Arc::clone(&end)));
        end
    }

    /// Send an RPC to `server` (a server name as from `server_name()`),
    /// e.g. `clnt.call(&srv, "KVServer.Get", &args)`; `None` if the RPC or
    /// its reply was lost or the server is unreachable.
    pub fn call<Req, Rep>(&self, server: &str, svc_meth: &str, args: &Req) -> Option<Rep>
    where
        Req: serde::Serialize,
        Rep: serde::de::DeserializeOwned,
    {
        let end = self.make_end(server);
        end.call(svc_meth, args)
    }

    /// (Re-)enable all existing ends.
    pub fn connect_all(&self) {
        *self.srvs.lock().unwrap() = None;
        let g = self.ends.lock().unwrap();
        for (ename, _) in g.values() {
            self.net.enable(ename, true);
        }
    }

    /// Restrict connectivity to `srvs` and enable those ends.
    pub fn connect_to(&self, srvs: &[String]) {
        *self.srvs.lock().unwrap() = Some(srvs.iter().cloned().collect());
        let g = self.ends.lock().unwrap();
        for (srv, (ename, _)) in g.iter() {
            if self.allowed(srv) {
                self.net.enable(ename, true);
            }
        }
    }

    /// Disable the end to `server`.
    pub fn disconnect(&self, server: &str) {
        let g = self.ends.lock().unwrap();
        if let Some((ename, _)) = g.get(server) {
            self.net.enable(ename, false);
        }
    }

    /// Disable all ends.
    pub fn disconnect_all(&self) {
        *self.srvs.lock().unwrap() = Some(HashSet::new());
        let g = self.ends.lock().unwrap();
        for (ename, _) in g.values() {
            self.net.enable(ename, false);
        }
    }

    pub(crate) fn remove(&self) {
        let g = self.ends.lock().unwrap();
        for (ename, _) in g.values() {
            self.net.delete_end(ename);
        }
    }
}

/// The whole test configuration: network + named groups + clients.
pub struct Config {
    pub net: Arc<Network>,
    pub prog: String,
    make_server: MakeServer,
    groups: Mutex<HashMap<Tgid, Arc<ServerGrp>>>,
    clnts: Mutex<Vec<Arc<Clnt>>>,
    t0: Instant,
    reliable: AtomicBool,
    long_delays: AtomicBool,
    long_reordering: AtomicBool,
}

impl Config {
    /// Create a configuration with a group `GRP0` of `n` servers, all
    /// started and fully connected (Go: tester.MakeConfig).
    pub fn new(prog: &str, n: usize, reliable: bool, make_server: MakeServer) -> Arc<Config> {
        let net = Network::new();
        net.reliable(reliable);
        let cfg = Arc::new(Config {
            net: Arc::clone(&net),
            prog: prog.to_string(),
            make_server: Arc::clone(&make_server),
            groups: Mutex::new(HashMap::new()),
            clnts: Mutex::new(Vec::new()),
            t0: Instant::now(),
            reliable: AtomicBool::new(reliable),
            long_delays: AtomicBool::new(false),
            long_reordering: AtomicBool::new(false),
        });
        let grp = ServerGrp::new(net, GRP0, n, -1, make_server);
        cfg.groups.lock().unwrap().insert(GRP0, grp);
        cfg
    }

    pub fn set_reliable(&self, yes: bool) {
        self.reliable.store(yes, Ordering::SeqCst);
        self.net.reliable(yes);
    }
    pub fn is_reliable(&self) -> bool {
        self.reliable.load(Ordering::SeqCst)
    }
    pub fn set_long_delays(&self, yes: bool) {
        self.long_delays.store(yes, Ordering::SeqCst);
        self.net.long_delays(yes);
    }
    pub fn set_long_reordering(&self, yes: bool) {
        self.long_reordering.store(yes, Ordering::SeqCst);
        self.net.long_reordering(yes);
    }

    pub fn group(&self, gid: Tgid) -> Arc<ServerGrp> {
        self.groups
            .lock()
            .unwrap()
            .get(&gid)
            .cloned()
            .expect("Config: no such group")
    }

    pub fn has_group(&self, gid: Tgid) -> bool {
        self.groups.lock().unwrap().contains_key(&gid)
    }

    /// Create a new server group (Go: MakeGroupStart) using this config's
    /// default (per-lab) server constructor.
    pub fn make_group(&self, gid: Tgid, n: usize, maxraftstate: i64) -> Arc<ServerGrp> {
        self.make_group_with(gid, n, maxraftstate, Arc::clone(&self.make_server))
    }

    /// Create a new server group with an explicit constructor (used by the
    /// shardkv lab, whose groups run different server code than GRP0).
    pub fn make_group_with(
        &self,
        gid: Tgid,
        n: usize,
        maxraftstate: i64,
        make_server: MakeServer,
    ) -> Arc<ServerGrp> {
        let grp = ServerGrp::new(Arc::clone(&self.net), gid, n, maxraftstate, make_server);
        self.groups.lock().unwrap().insert(gid, Arc::clone(&grp));
        grp
    }

    /// Shut down a group and forget about it (Go: ExitGroup).
    pub fn exit_group(&self, gid: Tgid) {
        if let Some(grp) = self.groups.lock().unwrap().remove(&gid) {
            grp.shutdown();
        }
    }

    /// Make a client that may talk to every server in group GRP0.
    pub fn make_client(&self) -> Arc<Clnt> {
        self.make_client_to(&[])
    }

    /// Make a client restricted to the named servers ([] = none allowed).
    pub fn make_client_to(&self, srvs: &[String]) -> Arc<Clnt> {
        let c = Clnt::new(
            Arc::clone(&self.net),
            if srvs.is_empty() {
                None
            } else {
                Some(srvs.to_vec())
            },
        );
        self.clnts.lock().unwrap().push(Arc::clone(&c));
        c
    }

    pub fn delete_client(&self, c: &Arc<Clnt>) {
        c.remove();
        self.clnts.lock().unwrap().retain(|x| !Arc::ptr_eq(x, c));
    }

    pub fn rpc_total(&self) -> u64 {
        self.net.get_total_count()
    }

    pub fn bytes_total(&self) -> u64 {
        self.net.get_total_bytes()
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.t0.elapsed().as_secs_f64()
    }

    /// Announce the start of a check (Go: Config.Begin).
    pub fn begin(&self, description: &str) {
        println!("\n{}", description);
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }

    /// Announce the end of a test, with stats like the Go tester.
    pub fn end(&self) {
        let npeers: usize = self
            .groups
            .lock()
            .unwrap()
            .values()
            .map(|g| g.n)
            .sum();
        println!(
            "  ... Passed --  time {:4.1}s #peers {:3} #RPCs {:5} #Ops {:5}",
            self.elapsed_secs(),
            npeers,
            self.rpc_total(),
            crate::op_count()
        );
    }

    /// Fatal error inside a test (Go: cfg.Fatalf => t.Fatalf).
    pub fn fatalf(&self, msg: &str) -> ! {
        panic!("{}", msg)
    }

    /// Clean up: shut down all groups and delete all clients.
    pub fn cleanup(&self) {
        let gids: Vec<Tgid> = self.groups.lock().unwrap().keys().cloned().collect();
        for gid in gids {
            self.exit_group(gid);
        }
        let clnts: Vec<Arc<Clnt>> = self.clnts.lock().unwrap().drain(..).collect();
        for c in clnts {
            c.remove();
        }
    }
}
