#![allow(non_snake_case, unused_variables, unused_imports)]
//! Tests for Lab 1 (port of Go's mr/mr_test.go + util.go).
//!
//! Each test runs your coordinator and worker as separate processes in a
//! fresh temp directory, talks over a unix socket, and compares the
//! merged `mr-out-*` output against output produced by the sequential
//! implementation.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const BIN_COORD: &str = env!("CARGO_BIN_EXE_mr-coordinator");
const BIN_WORKER: &str = env!("CARGO_BIN_EXE_mr-worker");
const BIN_SEQ: &str = env!("CARGO_BIN_EXE_mr-sequential");

fn testdata() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("pg-") && n.ends_with(".txt"))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    files
}

// --------------------------- util.go ports ------------------------------

struct Tmp {
    path: PathBuf,
}

impl Tmp {
    fn new() -> Tmp {
        // Lab 1 tests are subprocess-driven and finish in well under a
        // minute when passing; a hung test means the skeleton isn't
        // implemented (e.g. Done() still returns false).
        tester::watchdog(300);
        let t = std::env::temp_dir().join(format!("mr-tmp-{}", tester::randstring(8)));
        std::fs::create_dir_all(&t).unwrap();
        Tmp { path: t }
    }
    fn join(&self, name: impl AsRef<Path>) -> PathBuf {
        self.path.join(name)
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn rand_sock() -> String {
    format!("/tmp/5840-mr-{}", tester::randstring(20))
}

fn coordinator_sock() -> String {
    rand_sock()
}

fn find_files(dir: &Path, pattern: &str) -> Vec<PathBuf> {
    // pattern is like "mr-out-[0-9]" or "mr-*" (a glob name filter)
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if glob_match(pattern, &name) {
                out.push(e.path());
            }
        }
    }
    out.sort();
    out
}

/// Glob matcher supporting '*', '?', and character classes like "[0-9]"
/// (enough for the lab's "mr-out-[0-9]"-style patterns, matching what
/// `find -name` does in the Go tests).
fn glob_match(pattern: &str, name: &str) -> bool {
    fn rec(p: &[char], n: &[char]) -> bool {
        if p.is_empty() {
            return n.is_empty();
        }
        match p[0] {
            '*' => {
                for i in 0..=n.len() {
                    if rec(&p[1..], &n[i..]) {
                        return true;
                    }
                }
                false
            }
            '?' => !n.is_empty() && rec(&p[1..], &n[1..]),
            '[' => {
                // find the closing ']'
                let close = match p.iter().skip(1).position(|&c| c == ']') {
                    Some(x) => x,
                    None => return false,
                };
                if n.is_empty() {
                    return false;
                }
                let mut class = &p[1..1 + close];
                let negated = match class.first() {
                    Some('^') | Some('!') => {
                        class = &class[1..];
                        true
                    }
                    _ => false,
                };
                let c = n[0];
                let mut matched = false;
                let mut i = 0;
                while i < class.len() {
                    if i + 2 < class.len() && class[i + 1] == '-' {
                        if c >= class[i] && c <= class[i + 2] {
                            matched = true;
                        }
                        i += 3;
                    } else {
                        if c == class[i] {
                            matched = true;
                        }
                        i += 1;
                    }
                }
                if matched != negated {
                    rec(&p[close + 2..], &n[1..])
                } else {
                    false
                }
            }
            c => !n.is_empty() && n[0] == c && rec(&p[1..], &n[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    rec(&p, &n)
}

fn sort_files_into(files: &[PathBuf], out: &Path) {
    let mut lines: Vec<String> = Vec::new();
    for f in files {
        let data = std::fs::read_to_string(f).unwrap();
        lines.extend(data.lines().map(|l| l.to_string()));
    }
    lines.sort();
    let mut of = std::fs::File::create(out).unwrap();
    for l in lines {
        writeln!(of, "{}", l).unwrap();
    }
}

/// Generate correct output for a test via the sequential implementation.
fn mk_correct_output(tmp: &Tmp, files: &[PathBuf], app: &str, out: &str) {
    let status = Command::new(BIN_SEQ)
        .args(std::iter::once(app.to_string()).chain(files.iter().map(|f| f.to_string_lossy().to_string())))
        .current_dir(&tmp.path)
        .status()
        .expect("mrsequential failed to start");
    assert!(status.success(), "mrsequential failed");
    let mr_out = tmp.join("mr-out-0");
    sort_files_into(&[mr_out], &tmp.join(out));
    let _ = std::fs::remove_file(tmp.join("mr-out-0"));
}

fn merge_output(tmp: &Tmp, out: &str) {
    let files = find_files(&tmp.path, "mr-out-[0-9]");
    assert!(!files.is_empty(), "reduce created no mr-out-X output files!");
    sort_files_into(&files, &tmp.join(out));
}

fn run_cmp(f1: &Path, f2: &Path, msg: &str) {
    let a = std::fs::read(f1).unwrap();
    let b = std::fs::read(f2).unwrap();
    assert_eq!(a, b, "{}", msg);
}

fn count_pattern(files: &[PathBuf], pattern: &str) -> usize {
    let re = regex::Regex::new(pattern).unwrap();
    let mut n = 0;
    for f in files {
        let data = std::fs::read_to_string(f).unwrap();
        n += re.find_iter(&data).count();
    }
    n
}

// ------------------------- running coordinator/workers ------------------

fn start_worker(app: &str, sock: &str, tmp: &Tmp, i: usize) -> Child {
    let out = std::fs::File::create(tmp.join(format!("worker-{}.log", i))).unwrap();
    let err = out.try_clone().unwrap();
    Command::new(BIN_WORKER)
        .args([app, sock])
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .current_dir(&tmp.path)
        .spawn()
        .expect("mrworker failed to start")
}

struct MrRun {
    coord: Child,
    workers: Vec<Child>,
}

fn run_mrchan(
    tmp: &Tmp,
    files: &[PathBuf],
    app: &str,
    n: usize,
    sock: &str,
) -> MrRun {
    let out = std::fs::File::create(tmp.join("coord.log")).unwrap();
    let err = out.try_clone().unwrap();
    let coord = Command::new(BIN_COORD)
        .args(std::iter::once(sock.to_string()).chain(files.iter().map(|f| f.to_string_lossy().to_string())))
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .current_dir(&tmp.path)
        .spawn()
        .expect("mrcoordinator failed to start");

    // Give the coordinator time to create the socket (Go sleeps 1s).
    std::thread::sleep(Duration::from_millis(1000));

    let mut workers = Vec::new();
    for i in 0..n {
        workers.push(start_worker(app, sock, tmp, i));
    }
    MrRun { coord, workers }
}

/// Wait for the coordinator to exit (meaning Done() returned true), then
/// wait for all workers.
fn wait_mr(run: &mut MrRun, tmp: &Tmp) {
    let status = run.coord.wait().expect("coordinator wait failed");
    for mut w in run.workers.drain(..) {
        let _ = w.wait();
    }
    if !status.success() || std::fs::read_dir(&tmp.path).is_err() {
        panic!("coordinator exited with failure; is Done() implemented?");
    }
    for f in ["coord.log", "worker-0.log", "worker-1.log", "worker-2.log"] {
        if let Ok(data) = std::fs::read_to_string(tmp.join(f)) {
            eprintln!("--- {} ---\n{}", f, data);
        }
    }
    assert!(status.success());
}

// -------------------------------- tests ----------------------------------

#[test]
fn TestWc() {
    let tmp = Tmp::new();
    let files = testdata();
    mk_correct_output(&tmp, &files, "wc", "mr-wc-correct.txt");
    let sock = coordinator_sock();
    let mut run = run_mrchan(&tmp, &files, "wc", 3, &sock);
    wait_mr(&mut run, &tmp);
    merge_output(&tmp, "mr-wc-all.txt");
    run_cmp(
        &tmp.join("mr-wc-all.txt"),
        &tmp.join("mr-wc-correct.txt"),
        "incorrect combined reduce output: mr-wc-all.txt vs mr-wc-correct.txt",
    );
    let _ = std::fs::remove_file(&sock);
}

#[test]
fn TestIndexer() {
    let tmp = Tmp::new();
    let files = testdata();
    mk_correct_output(&tmp, &files, "indexer", "mr-indexer-correct.txt");
    let sock = coordinator_sock();
    let mut run = run_mrchan(&tmp, &files, "indexer", 2, &sock);
    wait_mr(&mut run, &tmp);
    merge_output(&tmp, "mr-indexer-all.txt");
    run_cmp(
        &tmp.join("mr-indexer-all.txt"),
        &tmp.join("mr-indexer-correct.txt"),
        "incorrect combined reduce output: mr-indexer-all.txt vs mr-indexer-correct.txt",
    );
    let _ = std::fs::remove_file(&sock);
}

/// Test mappers run in parallel.
#[test]
fn TestMapParallel() {
    let tmp = Tmp::new();
    let files = testdata();
    let sock = coordinator_sock();
    let mut run = run_mrchan(&tmp, &files, "mtiming", 2, &sock);
    wait_mr(&mut run, &tmp);
    let outs = find_files(&tmp.path, "mr-out*");
    assert_eq!(count_pattern(&outs, "times-"), 2, "saw wrong number of workers instead of 2");
    assert!(count_pattern(&outs, "parallel.") > 0, "map workers did not run in parallel");
    let _ = std::fs::remove_file(&sock);
}

/// Test reducers run in parallel.
#[test]
fn TestReduceParallel() {
    let tmp = Tmp::new();
    let files = testdata();
    let sock = coordinator_sock();
    let mut run = run_mrchan(&tmp, &files, "rtiming", 2, &sock);
    wait_mr(&mut run, &tmp);
    let outs = find_files(&tmp.path, "mr-out*");
    assert!(
        count_pattern(&outs, "[a-z] 2") >= 2,
        "too few parallel reduces"
    );
    let _ = std::fs::remove_file(&sock);
}

/// Test counts number of map jobs.
#[test]
fn TestJobCount() {
    let tmp = Tmp::new();
    let files = testdata();
    let sock = coordinator_sock();
    let mut run = run_mrchan(&tmp, &files, "jobcount", 4, &sock);
    wait_mr(&mut run, &tmp);
    let outs = find_files(&tmp.path, "mr-out*");
    assert_eq!(count_pattern(&outs, "a 8"), 1, "map jobs ran incorrect number of times");
    let _ = std::fs::remove_file(&sock);
}

/// Test whether any worker or coordinator exits before the task has
/// completed (i.e., all output files have been finalized).  The app's
/// reduce functions sleep for the slow keys (sherlock/tom), so if a worker
/// exited early the output would be incomplete and could still change
/// after the coordinator exits.
#[test]
fn TestEarlyExit() {
    const N: usize = 4;
    let tmp = Tmp::new();
    let files = testdata();
    let sock = coordinator_sock();

    let mut run = run_mrchan(&tmp, &files, "early_exit", N, &sock);
    // Wait for the workers, then the coordinator (whose exit means the
    // job is done).
    for w in run.workers.iter_mut() {
        let _ = w.wait();
    }
    let status = run.coord.wait().expect("coordinator wait failed");
    assert!(status.success(), "coordinator exited with failure");
    let _ = std::fs::remove_file(&sock);

    // Snapshot the output right when everything has exited.
    merge_output(&tmp, "mr-wc-initial.txt");

    // The output must be final at that point: each of the 8 input files
    // must appear exactly once with count 1.
    let initial = std::fs::read_to_string(tmp.join("mr-wc-initial.txt")).unwrap();
    let lines: Vec<&str> = initial.lines().collect();
    assert_eq!(lines.len(), files.len(), "output not complete at coordinator exit: {:?}", lines);
    for l in &lines {
        assert!(l.ends_with(" 1"), "unexpected output line {:?}", l);
    }

    // Give any straggler a chance to write more output (it shouldn't).
    std::thread::sleep(Duration::from_secs(1));
    merge_output(&tmp, "mr-wc-final.txt");
    run_cmp(
        &tmp.join("mr-wc-initial.txt"),
        &tmp.join("mr-wc-final.txt"),
        "reduce output changed: mr-wc-initial.txt vs mr-wc-final.txt",
    );
}

#[test]
fn TestCrashWorker() {
    const N: usize = 3;
    let tmp = Tmp::new();
    let files = testdata();
    mk_correct_output(&tmp, &files, "nocrash", "mr-crash-correct.txt");
    let sock = coordinator_sock();
    let mut run = run_mrchan(&tmp, &files, "crash", N, &sock);
    // Workers may crash randomly (app "crash"); restart them, like the Go
    // test does, until the coordinator finishes.
    loop {
        let mut all_done = true;
        for w in run.workers.iter_mut() {
            match w.try_wait().unwrap() {
                Some(_status) => {} // exited (crashed or finished)
                None => {
                    all_done = false;
                }
            }
        }
        if run.coord.try_wait().unwrap().is_some() {
            break; // coordinator done
        }
        if all_done {
            // all workers exited but coordinator still running: restart
            for w in run.workers.iter_mut() {
                *w = start_worker("crash", &sock, &tmp, 99);
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    for mut w in run.workers.drain(..) {
        let _ = w.wait();
    }
    run.coord.wait().expect("coordinator wait failed");
    merge_output(&tmp, "mr-crash-all.txt");
    run_cmp(
        &tmp.join("mr-crash-all.txt"),
        &tmp.join("mr-crash-correct.txt"),
        "incorrect combined reduce output: mr-crash-all.txt vs mr-crash-correct.txt",
    );
    let _ = std::fs::remove_file(&sock);
}
