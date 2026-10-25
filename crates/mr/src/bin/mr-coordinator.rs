//! Start the coordinator process, implemented in mr/src/coordinator.rs:
//! `mr-coordinator <sockname> <inputfiles...>`
//!
//! (The Rust version of Go's src/main/mrcoordinator.go; don't change.)

use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: mr-coordinator <sockname> <inputfiles...>");
        std::process::exit(1);
    }

    let _m = mr::coordinator::make_coordinator(&args[1], args[2..].to_vec(), 10);
    loop {
        std::thread::sleep(Duration::from_secs(1));
        if _m.done() {
            break;
        }
    }
    std::thread::sleep(Duration::from_secs(1));
}
