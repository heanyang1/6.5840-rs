//! The MapReduce worker.
//!
//! The worker calls the application Map and Reduce functions and handles
//! reading and writing files.  main/mr-worker.rs calls [`worker`].

use crate::rpc;
use crate::KeyValue;
use crate::net;

/// use ihash(key) % NReduce to choose the reduce task number for each
/// KeyValue emitted by Map.
pub fn ihash(key: &str) -> usize {
    // FNV-1a 32-bit, the same hash as Go's fnv.New32a().
    let mut h: u32 = 2166136261;
    for b in key.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(16777619);
    }
    (h & 0x7fffffff) as usize
}

/// main/mr-worker.rs calls this function.  `sockname` is the coordinator's
/// unix socket; `mapf` and `reducef` are the application's Map and Reduce
/// functions (selected by app name in the mr-worker binary).
pub fn worker(
    sockname: &str,
    mapf: fn(&str, &str) -> Vec<KeyValue>,
    reducef: fn(&str, &[String]) -> String,
) {
    let _ = (sockname, mapf, reducef);

    // Your worker implementation here.

    // uncomment to send the Example RPC to the coordinator.
    // call_example(sockname);
}

/// Example function to show how to make an RPC call to the coordinator.
///
/// The RPC argument and reply types are defined in rpc.rs.
#[allow(dead_code)]
fn call_example(sockname: &str) {
    let args = rpc::ExampleArgs { x: 99 };
    // The "Coordinator.Example" tells the receiving server that we'd like
    // to call the example() method of the Coordinator.
    if let Some(reply) = net::call::<_, rpc::ExampleReply>(sockname, "Coordinator.Example", &args) {
        // reply.y should be 100.
        println!("reply.y {}", reply.y);
    } else {
        println!("call failed!");
    }
}

/// Send an RPC request to the coordinator and wait for the response.
/// Usually returns the reply; returns `None` if something goes wrong
/// (typically: the coordinator has exited because the job is done, and
/// the worker should terminate).
///
/// (Go: mr/worker.go's call()).
pub fn call<Req, Rep>(sockname: &str, method: &str, args: &Req) -> Option<Rep>
where
    Req: serde::Serialize,
    Rep: serde::de::DeserializeOwned,
{
    net::call(sockname, method, args)
}
