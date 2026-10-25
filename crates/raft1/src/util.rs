//! Debugging helpers.

use std::sync::atomic::{AtomicBool, Ordering};

pub static DEBUG: AtomicBool = AtomicBool::new(false);

/// Turn on debug printing (call from your test/main, or set a breakpoint).
pub fn set_debug(on: bool) {
    DEBUG.store(on, Ordering::SeqCst);
}

#[macro_export]
macro_rules! dprintf {
    ($($arg:tt)*) => {
        if $crate::util::DEBUG.load(std::sync::atomic::Ordering::SeqCst) {
            println!($($arg)*);
        }
    };
}
