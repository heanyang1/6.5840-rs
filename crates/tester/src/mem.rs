//! Allocation counting, a Rust-specific replacement for the Go labs'
//! `runtime.ReadMemStats`-based memory tests (Lab 2's
//! TestMemPutManyClientsReliable).  When enabled, tracks the number of
//! bytes currently allocated on the heap.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static CUR: AtomicU64 = AtomicU64::new(0);
static ENABLED: AtomicBool = AtomicBool::new(false);

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() && ENABLED.load(Ordering::Relaxed) {
            CUR.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        if ENABLED.load(Ordering::Relaxed) {
            CUR.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        }
        System.dealloc(p, layout)
    }
}

#[global_allocator]
static A: CountingAlloc = CountingAlloc;

/// Start counting allocations.  Must be called before the measurements in
/// a memory test; counting overhead applies only while enabled.
pub fn enable_counting_alloc() {
    ENABLED.store(true, Ordering::Relaxed);
}

pub fn counting_alloc_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Bytes currently allocated (while counting is enabled).
pub fn allocated() -> u64 {
    CUR.load(Ordering::Relaxed)
}
