//! A distributed lock built on the key/value service (port of Go's
//! kvsrv1/lock/lock.go).

use tester::kvtest::IKVClerk;
use std::sync::Arc;

pub struct Lock {
    /// The k/v clerk; your code can perform a Put or Get by calling
    /// `self.ck.put(...)` / `self.ck.get(...)`.
    #[allow(dead_code)]
    ck: Arc<dyn IKVClerk>,
    // You may add code here
}

/// The tester calls make_lock() and passes in a k/v clerk.
///
/// This interface supports multiple locks by means of the `lockname`
/// argument; locks with different names should be independent.
pub fn make_lock(ck: Arc<dyn IKVClerk>, _lockname: &str) -> Lock {
    let lk = Lock { ck };
    // You may add code here
    lk
}

impl Lock {
    /// Block until this client holds the lock.
    pub fn acquire(&self) {
        // Your code here
    }

    /// Release the lock.
    pub fn release(&self) {
        // Your code here
    }
}
