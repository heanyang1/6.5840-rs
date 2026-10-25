//! MapReduce applications ("plugins" in the Go labs).
//!
//! Go loads these at run time from `.so` plugin files; Rust has no stable
//! plugin ABI, so the applications are compiled into the worker binary and
//! selected by name (`mr-worker wc sock123`, like `mrworker wc.so sock123`).
//!
//! To add your own application: write `fn map(...)`, `fn reduce(...)` and
//! register it in [`lookup`].

use crate::KeyValue;

pub type MapFn = fn(&str, &str) -> Vec<KeyValue>;
pub type ReduceFn = fn(&str, &[String]) -> String;

/// Look up an application by name.  Names mirror the Go plugin files:
/// "wc", "indexer", "mtiming", "rtiming", "jobcount", "early_exit",
/// "crash", "nocrash".
pub fn lookup(app: &str) -> Option<(MapFn, ReduceFn)> {
    match app {
        "wc" => Some((wc::map, wc::reduce)),
        "indexer" => Some((indexer::map, indexer::reduce)),
        "mtiming" => Some((mtiming::map, mtiming::reduce)),
        "rtiming" => Some((rtiming::map, rtiming::reduce)),
        "jobcount" => Some((jobcount::map, jobcount::reduce)),
        "early_exit" => Some((early_exit::map, early_exit::reduce)),
        "crash" => Some((crash::map, crash::reduce)),
        "nocrash" => Some((nocrash::map, nocrash::reduce)),
        _ => None,
    }
}

// ---------------------------------------------------------------- wc ----

mod wc {
    use super::*;

    fn split_words(s: &str) -> Vec<String> {
        s.split(|r: char| !r.is_alphabetic())
            .filter(|w| !w.is_empty())
            .map(|w| w.to_string())
            .collect()
    }

    pub fn map(_filename: &str, contents: &str) -> Vec<KeyValue> {
        split_words(contents)
            .into_iter()
            .map(|w| KeyValue { key: w, value: "1".to_string() })
            .collect()
    }

    pub fn reduce(_key: &str, values: &[String]) -> String {
        values.len().to_string()
    }
}

// ------------------------------------------------------------ indexer ----

mod indexer {
    use super::*;
    use std::collections::HashSet;

    pub fn map(document: &str, value: &str) -> Vec<KeyValue> {
        let words: HashSet<&str> = value
            .split(|r: char| !r.is_alphabetic())
            .filter(|w| !w.is_empty())
            .collect();
        words
            .into_iter()
            .map(|w| KeyValue { key: w.to_string(), value: document.to_string() })
            .collect()
    }

    pub fn reduce(key: &str, values: &[String]) -> String {
        let mut values = values.to_vec();
        values.sort();
        format!("{} {}", values.len(), values.join(","))
    }
}

// The following apps use process-global state (like Go's) and thus assume
// each app runs in a single-threaded Map/Reduce context per process, which
// holds for the lab's sequential use inside one task.

// ------------------------------------------------------------ mtiming ----

mod mtiming {
    use super::*;

    pub fn map(filename: &str, contents: &str) -> Vec<KeyValue> {
        let pid = std::process::id();
        let n = super::nparallel("map");
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        vec![
            KeyValue { key: format!("times-{}", pid), value: format!("{:.1}", ts) },
            KeyValue { key: format!("parallel-{}", pid), value: format!("{}", n) },
        ]
    }

    pub fn reduce(_key: &str, values: &[String]) -> String {
        let mut vv = values.to_vec();
        vv.sort();
        vv.join(" ")
    }
}

// ------------------------------------------------------------ rtiming ----

mod rtiming {
    use super::*;

    pub fn map(_filename: &str, _contents: &str) -> Vec<KeyValue> {
        "abcdefghij"
            .chars()
            .map(|c| KeyValue { key: c.to_string(), value: "1".to_string() })
            .collect()
    }

    pub fn reduce(_key: &str, _values: &[String]) -> String {
        let n = super::nparallel("reduce");
        format!("{}", n)
    }
}

// ----------------------------------------------------------- jobcount ----

mod jobcount {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNT: AtomicUsize = AtomicUsize::new(0);

    pub fn map(filename: &str, _contents: &str) -> Vec<KeyValue> {
        let me = std::process::id();
        let c = COUNT.fetch_add(1, Ordering::SeqCst);
        let f = format!("mr-worker-jobcount-{}-{}", me, c);
        std::fs::write(&f, b"x").expect("jobcount: write failed");
        use rand::Rng;
        let ms = 2000 + rand::thread_rng().gen_range(0..3000);
        std::thread::sleep(std::time::Duration::from_millis(ms));
        let _ = filename;
        vec![KeyValue { key: "a".to_string(), value: "x".to_string() }]
    }

    pub fn reduce(_key: &str, _values: &[String]) -> String {
        let mut invocations = 0;
        for e in std::fs::read_dir(".").expect("jobcount: readdir failed") {
            let name = e.unwrap().file_name();
            let name = name.to_string_lossy();
            if name.starts_with("mr-worker-jobcount") {
                invocations += 1;
            }
        }
        invocations.to_string()
    }
}

// --------------------------------------------------------- early_exit ----

mod early_exit {
    use super::*;

    pub fn map(filename: &str, _contents: &str) -> Vec<KeyValue> {
        vec![KeyValue { key: filename.to_string(), value: "1".to_string() }]
    }

    pub fn reduce(key: &str, values: &[String]) -> String {
        if key.contains("sherlock") || key.contains("tom") {
            std::thread::sleep(std::time::Duration::from_secs(3));
        }
        values.len().to_string()
    }
}

// -------------------------------------------------------- crash/nocrash --

fn maybe_crash(can_crash: bool) {
    use rand::Rng;
    let rr: u64 = rand::thread_rng().gen_range(0..1000);
    if can_crash && rr < 330 {
        // crash!
        std::process::exit(1);
    } else if can_crash && rr < 660 {
        // delay for a while.
        let ms = rand::thread_rng().gen_range(0..10_000);
        std::thread::sleep(std::time::Duration::from_millis(ms));
    }
}

fn crash_map(can_crash: bool, filename: &str, contents: &str) -> Vec<KeyValue> {
    maybe_crash(can_crash);
    vec![
        KeyValue { key: "a".to_string(), value: filename.to_string() },
        KeyValue { key: "b".to_string(), value: filename.len().to_string() },
        KeyValue { key: "c".to_string(), value: contents.len().to_string() },
        KeyValue { key: "d".to_string(), value: "xyzzy".to_string() },
    ]
}

fn crash_reduce(can_crash: bool, _key: &str, values: &[String]) -> String {
    maybe_crash(can_crash);
    let mut vv = values.to_vec();
    vv.sort();
    vv.join(" ")
}

mod crash {
    use super::*;
    pub fn map(filename: &str, contents: &str) -> Vec<KeyValue> {
        crash_map(true, filename, contents)
    }
    pub fn reduce(key: &str, values: &[String]) -> String {
        crash_reduce(true, key, values)
    }
}

mod nocrash {
    use super::*;
    pub fn map(filename: &str, contents: &str) -> Vec<KeyValue> {
        crash_map(false, filename, contents)
    }
    pub fn reduce(key: &str, values: &[String]) -> String {
        crash_reduce(false, key, values)
    }
}

// ------------------------------------------------------------------------

/// Are any other workers running (Go: mrapps nparallel())?  Writes a
/// marker file with our pid, looks for marker files of live processes,
/// sleeps 1s (so peers get a chance to appear), then removes our marker.
fn nparallel(phase: &str) -> usize {
    let pid = std::process::id();
    let myfilename = format!("mr-worker-{}-{}", phase, pid);
    std::fs::write(&myfilename, b"x").expect("nparallel: write failed");

    let mut ret = 0;
    let entries = match std::fs::read_dir(".") {
        Ok(e) => e,
        Err(_) => {
            let _ = std::fs::remove_file(&myfilename);
            return 0;
        }
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        let prefix = format!("mr-worker-{}-", phase);
        if let Some(rest) = name.strip_prefix(&prefix) {
            if let Ok(xpid) = rest.parse::<i32>() {
                // is xpid alive?
                let alive = unsafe { libc::kill(xpid, 0) } == 0;
                if alive {
                    ret += 1;
                }
            }
        }
    }
    std::thread::sleep(std::time::Duration::from_secs(1));
    let _ = std::fs::remove_file(&myfilename);
    ret
}
