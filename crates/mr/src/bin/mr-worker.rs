//! Start a worker process, implemented in mr/src/worker.rs:
//! `mr-worker <app> <sockname>`
//!
//! Typically there will be multiple worker processes, talking to one
//! coordinator.  (The Rust version of Go's src/main/mrworker.go; don't
//! change.  In Go the app is a plugin .so file; here it is a name looked
//! up in mr::apps.)

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: mr-worker <app> <sockname>");
        std::process::exit(1);
    }

    let (mapf, reducef) = mr::apps::lookup(&args[1]).unwrap_or_else(|| {
        eprintln!("cannot load app {}", args[1]);
        std::process::exit(1);
    });

    mr::worker::worker(&args[2], mapf, reducef);
}
