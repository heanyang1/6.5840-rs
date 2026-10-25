//! labrpc: a simulated, unreliable RPC network, for 6.5840 labs.
//!
//! Rust port of the Go `labrpc` package.  It simulates a network that can
//! lose requests, lose replies, delay messages, and entirely disconnect
//! particular hosts.  RPCs are delivered by background threads, so
//! concurrent `call()`s are possible, and replies may be reordered.
//!
//! Typical use (this is what the `tester` crate does for you):
//!
//! ```ignore
//! let net = Network::new();
//! net.reliable(false);                       // start dropping messages
//! let end = net.make_end("clnt-end-1");      // client endpoint
//! net.connect("clnt-end-1", "server-0-0");   // wire the end to a server
//! net.enable("clnt-end-1", true);            // ...and enable it
//! net.add_server("server-0-0", server);      // publish the server
//!
//! // args/reply are serialized (nothing shared by reference):
//! let reply: Option<AppendReply> = end.call("Raft.AppendEntries", &args);
//! // Some(reply) means the server executed the RPC; None means the
//! // request or reply was lost, the end is disabled, or the server is down.
//! ```
//!
//! Services are plain structs with typed handlers registered by name,
//! mirroring Go's reflection-based `MakeService(obj)`:
//!
//! ```ignore
//! let mut svc = Service::new("KVServer");
//! svc.handle("Get", move |args: GetArgs| kv.get(&args));
//! svc.handle("Put", move |args: PutArgs| kv.put(&args));
//! ```

use labgob;
use rand::Rng;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SHORT_DELAY_MS: u64 = 27;
const LONG_DELAY_MS: u64 = 7000;
/// If a handler does not answer within this deadline, the RPC is treated
/// as lost.  Generous enough that only a genuinely stuck handler trips it
/// (Go's labrpc has the same property, except it blocks forever).
const REPLY_TIMEOUT_MS: u64 = 8000;

/// A server that can dispatch `"Method"` names to handlers over the
/// simulated network, operating on serialized (bytes) arguments.
pub trait RpcService: Send + Sync {
    /// Dispatch `method` (name without the service prefix).
    /// Returns `None` if the handler does not exist (treated as a lost RPC,
    /// but a loud warning is printed since it indicates a bug).
    fn dispatch(&self, method: &str, args: &[u8]) -> Option<Vec<u8>>;
}

trait Handler: Send + Sync {
    fn invoke(&self, args: &[u8]) -> Result<Vec<u8>, String>;
}

impl<F> Handler for F
where
    F: Fn(Vec<u8>) -> Result<Vec<u8>, String> + Send + Sync + 'static,
{
    fn invoke(&self, args: &[u8]) -> Result<Vec<u8>, String> {
        self(args.to_vec())
    }
}

/// A named collection of typed handlers, one server-side service.
/// The analog of Go's `labrpc.MakeService(obj)`.  Handlers are looked up
/// by method name (the part after the service prefix in RPC calls like
/// "KVServer.Get").
pub struct Service {
    name: String,
    methods: Mutex<HashMap<String, Arc<dyn Handler>>>,
}

impl Service {
    pub fn new(name: &str) -> Service {
        Service {
            name: name.to_string(),
            methods: Mutex::new(HashMap::new()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Register a handler for `method`.  `f` takes the deserialized request
    /// and returns the reply; both must derive Serialize/Deserialize.
    pub fn handle<Req, Rep, F>(&self, method: &str, f: F) -> &Self
    where
        Req: DeserializeOwned + 'static,
        Rep: Serialize + 'static,
        F: Fn(Req) -> Rep + Send + Sync + 'static,
    {
        let wrapped = move |args: Vec<u8>| -> Result<Vec<u8>, String> {
            let req: Req = labgob::decode(&args).ok_or("cannot decode args")?;
            let rep = f(req);
            Ok(labgob::encode(&rep))
        };
        self.methods
            .lock()
            .unwrap()
            .insert(method.to_string(), Arc::new(wrapped));
        self
    }
}

impl RpcService for Service {
    fn dispatch(&self, method: &str, args: &[u8]) -> Option<Vec<u8>> {
        // Accept both the bare method name and the full
        // "Service.Method" form.
        let method = match method.rsplit_once('.') {
            Some((svc, m)) if svc == self.name => m,
            Some(_) => {
                eprintln!("labrpc: dispatch: wrong service for {}", method);
                return None;
            }
            None => method,
        };
        let h = {
            let guard = self.methods.lock().unwrap();
            guard.get(method).cloned()
        };
        match h {
            Some(h) => match h.invoke(args) {
                Ok(rep) => Some(rep),
                Err(e) => {
                    eprintln!("labrpc: {}.{} handler error: {}", self.name, method, e);
                    None
                }
            },
            None => {
                eprintln!(
                    "labrpc: {}.{} does not exist",
                    self.name, method
                );
                None
            }
        }
    }
}

/// A server hosting one or more services; dispatches on the
/// `"Service.Method"` name, like Go's `labrpc.Server`.
pub struct MultiService {
    services: Vec<Arc<Service>>,
}

impl MultiService {
    pub fn new() -> MultiService {
        MultiService {
            services: Vec::new(),
        }
    }
    pub fn add_service(&mut self, svc: Arc<Service>) {
        self.services.push(svc);
    }
}

impl Default for MultiService {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcService for MultiService {
    fn dispatch(&self, svc_meth: &str, args: &[u8]) -> Option<Vec<u8>> {
        let (svc_name, meth) = match svc_meth.rsplit_once('.') {
            Some(x) => x,
            None => {
                eprintln!("labrpc: dispatch: bad method name {}", svc_meth);
                return None;
            }
        };
        for s in &self.services {
            if s.name() == svc_name {
                return s.dispatch(meth, args);
            }
        }
        eprintln!("labrpc: unknown service {} in {}", svc_name, svc_meth);
        None
    }
}

struct EndInfo {
    server: String,
    enabled: bool,
}

struct ServerEntry {
    svc: Option<Arc<dyn RpcService>>,
    count: u64,
}

/// The simulated network.
pub struct Network {
    inner: Mutex<Inner>,
}

struct Inner {
    reliable: bool,
    long_delays: bool,
    long_reordering: bool,
    ends: HashMap<String, EndInfo>,
    servers: HashMap<String, ServerEntry>,
    total_count: u64,
    total_bytes: u64,
}

impl Network {
    pub fn new() -> Arc<Network> {
        Arc::new(Network {
            inner: Mutex::new(Inner {
                reliable: true,
                long_delays: false,
                long_reordering: false,
                ends: HashMap::new(),
                servers: HashMap::new(),
                total_count: 0,
                total_bytes: 0,
            }),
        })
    }

    /// If false, messages may be dropped, delayed and reordered.
    pub fn reliable(&self, yes: bool) {
        self.inner.lock().unwrap().reliable = yes;
    }
    pub fn is_reliable(&self) -> bool {
        self.inner.lock().unwrap().reliable
    }
    /// Sometimes delay server-side handling by a long time.
    pub fn long_delays(&self, yes: bool) {
        self.inner.lock().unwrap().long_delays = yes;
    }
    /// Sometimes delay replies a long time, so they arrive out of order.
    pub fn long_reordering(&self, yes: bool) {
        self.inner.lock().unwrap().long_reordering = yes;
    }

    /// Create a client end-point named `endname` (initially disabled).
    pub fn make_end(self: &Arc<Self>, endname: &str) -> Arc<ClientEnd> {
        self.inner.lock().unwrap().ends.insert(
            endname.to_string(),
            EndInfo {
                server: String::new(),
                enabled: false,
            },
        );
        Arc::new(ClientEnd {
            endname: endname.to_string(),
            net: Arc::clone(self),
        })
    }

    /// Connect an end to a server (an end can only be connected once).
    pub fn connect(&self, endname: &str, servername: &str) {
        let mut g = self.inner.lock().unwrap();
        match g.ends.get_mut(endname) {
            Some(e) => e.server = servername.to_string(),
            None => panic!("labrpc: connect: unknown end {}", endname),
        }
    }

    /// Enable/disable an end.
    pub fn enable(&self, endname: &str, enabled: bool) {
        let mut g = self.inner.lock().unwrap();
        match g.ends.get_mut(endname) {
            Some(e) => e.enabled = enabled,
            None => panic!("labrpc: enable: unknown end {}", endname),
        }
    }

    pub fn is_enabled(&self, endname: &str) -> bool {
        self.inner
            .lock()
            .unwrap()
            .ends
            .get(endname)
            .map(|e| e.enabled)
            .unwrap_or(false)
    }

    pub fn delete_end(&self, endname: &str) {
        self.inner.lock().unwrap().ends.remove(endname);
    }

    /// Publish a server under `servername`.
    pub fn add_server(self: &Arc<Self>, servername: &str, svc: Arc<dyn RpcService>) {
        self.inner.lock().unwrap().servers.insert(
            servername.to_string(),
            ServerEntry { svc: Some(svc), count: 0 },
        );
    }

    /// Remove a server; calls to it fail (this is how "server down" works).
    pub fn delete_server(&self, servername: &str) {
        if let Some(e) = self.inner.lock().unwrap().servers.get_mut(servername) {
            e.svc = None;
        }
    }

    pub fn get_count(&self, servername: &str) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .servers
            .get(servername)
            .map(|s| s.count)
            .unwrap_or(0)
    }

    pub fn get_total_count(&self) -> u64 {
        self.inner.lock().unwrap().total_count
    }

    pub fn get_total_bytes(&self) -> u64 {
        self.inner.lock().unwrap().total_bytes
    }

    /// The fast path of [`ClientEnd::call`]: checks connectivity, applies
    /// unreliability, runs the handler on a fresh thread, and waits for the
    /// reply.  Returns `None` if anything gets lost.
    fn forward(
        self: &Arc<Self>,
        endname: &str,
        svc_meth: &str,
        args: Vec<u8>,
    ) -> Option<Vec<u8>> {
        let mut rng = rand::thread_rng();
        let (servername, svc, enabled, reliable, long_delays, long_reordering) = {
            let mut g = self.inner.lock().unwrap();
            let (enabled, server) = match g.ends.get(endname) {
                Some(e) => (e.enabled, e.server.clone()),
                None => return None, // end was deleted
            };
            let entry = g.servers.get_mut(&server);
            let svc = match entry {
                Some(srv) => match &srv.svc {
                    Some(s) => Some(Arc::clone(s)),
                    None => None,
                },
                None => None,
            };
            let counted = enabled && svc.is_some();
            if counted {
                let srv = g.servers.get_mut(&server).unwrap();
                srv.count += 1;
                g.total_count += 1;
                g.total_bytes += args.len() as u64;
            }
            (
                server,
                svc,
                enabled,
                g.reliable,
                g.long_delays,
                g.long_reordering,
            )
        };

        // End disabled (disconnected), server deleted (down), or never
        // connected: the RPC is not delivered.
        if !enabled || svc.is_none() || servername.is_empty() {
            if !reliable && long_delays {
                // Simulate a long pause when talking to a down host.
                let ms = LONG_DELAY_MS / 10 + rng.gen_range(0..LONG_DELAY_MS / 2);
                std::thread::sleep(Duration::from_millis(ms));
            } else if !reliable {
                std::thread::sleep(Duration::from_millis(rng.gen_range(0..SHORT_DELAY_MS)));
            }
            return None;
        }
        let svc = svc.unwrap();

        // Unreliable network: maybe drop the request, after a delay.
        if !reliable {
            std::thread::sleep(Duration::from_millis(rng.gen_range(0..SHORT_DELAY_MS)));
            if rng.gen_ratio(1, 8) {
                return None; // lost request
            }
        }

        // Run the handler on its own thread so that concurrent RPCs are
        // possible and replies can be reordered.
        let method = svc_meth.to_string();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let rep = svc.dispatch(&method, &args);
            let mut rng = rand::thread_rng();
            if !reliable {
                if long_reordering && rng.gen_ratio(1, 9) {
                    // Delay the reply a long time, so that replies to
                    // concurrent RPCs arrive out of order.
                    std::thread::sleep(Duration::from_millis(rng.gen_range(0..1000)));
                } else if rng.gen_ratio(1, 8) {
                    return; // lost reply
                } else {
                    std::thread::sleep(Duration::from_millis(rng.gen_range(0..SHORT_DELAY_MS)));
                }
            }
            let _ = tx.send(rep);
        });

        match rx.recv_timeout(Duration::from_millis(REPLY_TIMEOUT_MS)) {
            Ok(Some(rep)) => {
                self.inner.lock().unwrap().total_bytes += rep.len() as u64;
                Some(rep)
            }
            _ => None, // lost or too-slow reply
        }
    }
}

impl Default for Network {
    fn default() -> Self {
        Network {
            inner: Mutex::new(Inner {
                reliable: true,
                long_delays: false,
                long_reordering: false,
                ends: HashMap::new(),
                servers: HashMap::new(),
                total_count: 0,
                total_bytes: 0,
            }),
        }
    }
}

/// One client end-point, connected to (at most) one server.  Cloneable;
/// safe to make concurrent `call()`s on the same end.
pub struct ClientEnd {
    endname: String,
    net: Arc<Network>,
}

impl ClientEnd {
    /// Send an RPC to `svc_meth` (e.g. `"Raft.RequestVote"`), wait for the
    /// reply.  Returns `Some(reply)` if the server executed the request;
    /// `None` if the request or reply was lost, the end is disabled, or the
    /// server is down.  Guaranteed to return (perhaps after a delay).
    pub fn call<Req, Rep>(&self, svc_meth: &str, args: &Req) -> Option<Rep>
    where
        Req: Serialize,
        Rep: DeserializeOwned,
    {
        let bytes = labgob::encode(args);
        let rep = self.net.forward(&self.endname, svc_meth, bytes)?;
        labgob::decode(&rep)
    }
}
