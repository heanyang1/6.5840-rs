//! Unix-socket RPC plumbing for the MapReduce lab.
//!
//! This is the analog of Go's `net/rpc` over a unix domain socket, which
//! the Go lab gives you for free.  You should not need to modify this
//! file: it provides [`RpcServer`] (used by the coordinator) and
//! [`call`] (used by the worker).

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::Arc;

type Handler = Box<dyn Fn(Vec<u8>) -> Option<Vec<u8>> + Send + Sync>;

/// Write a length-prefixed blob.
fn write_blob(w: &mut impl Write, b: &[u8]) -> std::io::Result<()> {
    w.write_all(&(b.len() as u32).to_le_bytes())?;
    w.write_all(b)
}

/// Read a length-prefixed blob.
fn read_blob(r: &mut impl Read) -> std::io::Result<Vec<u8>> {
    let mut lenb = [0u8; 4];
    r.read_exact(&mut lenb)?;
    let len = u32::from_le_bytes(lenb) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// An RPC server bound to a unix socket.  Register handlers with
/// [`RpcServer::register`], then call [`RpcServer::serve`], which listens
/// forever on a background thread (one thread per connection, like Go's
/// net/rpc, so handlers run concurrently).
pub struct RpcServer {
    handlers: HashMap<String, Arc<Handler>>,
}

impl Default for RpcServer {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcServer {
    pub fn new() -> RpcServer {
        RpcServer {
            handlers: HashMap::new(),
        }
    }

    /// Register a handler for `method` (e.g. "Coordinator.GetTask").
    pub fn register<Req, Rep, F>(&mut self, method: &str, f: F)
    where
        Req: DeserializeOwned + 'static,
        Rep: Serialize + 'static,
        F: Fn(Req) -> Rep + Send + Sync + 'static,
    {
        let f: Arc<F> = Arc::new(f);
        let wrapped: Handler = Box::new(move |args: Vec<u8>| {
            let req: Req = bincode::deserialize(&args).ok()?;
            Some(bincode::serialize(&f(req)).ok()?)
        });
        self.handlers.insert(method.to_string(), Arc::new(wrapped));
    }

    /// Bind the unix socket at `sock` and serve RPCs on a background
    /// thread.  Panics if the socket cannot be created.
    pub fn serve(self, sock: &str) {
        let _ = std::fs::remove_file(sock); // remove stale socket, like Go
        let listener = UnixListener::bind(sock)
            .unwrap_or_else(|e| panic!("listen error {}: {}", sock, e));
        let handlers = Arc::new(self.handlers);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(s) => {
                        let handlers = Arc::clone(&handlers);
                        std::thread::spawn(move || {
                            let _ = serve_conn(s, &handlers);
                        });
                    }
                    Err(_) => break,
                }
            }
        });
    }
}

fn serve_conn(
    mut stream: UnixStream,
    handlers: &HashMap<String, Arc<Handler>>,
) -> std::io::Result<()> {
    loop {
        let method = match read_blob(&mut stream) {
            Ok(m) => m,
            Err(_) => return Ok(()), // connection closed
        };
        let method = String::from_utf8_lossy(&method).to_string();
        let args = read_blob(&mut stream)?;
        let reply = match handlers.get(&method) {
            Some(h) => match h(args) {
                Some(r) => r,
                None => return Ok(()), // handler errored: hang up
            },
            None => {
                eprintln!("mr net: unknown method {}", method);
                return Ok(());
            }
        };
        stream.write_all(&[1u8])?;
        write_blob(&mut stream, &reply)?;
        stream.flush()?;
    }
}

/// Send an RPC to the server listening on `sock`; returns `None` if the
/// call could not be completed (e.g. the coordinator has exited, which is
/// how workers learn that the job is done).
pub fn call<Req, Rep>(sock: &str, method: &str, args: &Req) -> Option<Rep>
where
    Req: Serialize,
    Rep: DeserializeOwned,
{
    let mut stream = UnixStream::connect(sock).ok()?;
    let ab = bincode::serialize(args).ok()?;
    write_blob(&mut stream, method.as_bytes()).ok()?;
    write_blob(&mut stream, &ab).ok()?;
    stream.flush().ok()?;
    let mut status = [0u8; 1];
    stream.read_exact(&mut status).ok()?;
    if status[0] != 1 {
        return None;
    }
    let rep = read_blob(&mut stream).ok()?;
    bincode::deserialize(&rep).ok()
}
