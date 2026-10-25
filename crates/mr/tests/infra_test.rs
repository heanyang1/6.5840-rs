//! Smoke test for the provided RPC plumbing and the example handler.
//! This does not test student code; it verifies the infrastructure works
//! so that failures in the real tests point at the lab code.

use std::time::Duration;

#[test]
fn net_smoke() {
    let sock = format!("/tmp/5840-mr-smoke-{}", tester::randstring(12));
    let c = mr::coordinator::make_coordinator(&sock, vec![], 10);
    // give the listener a moment
    std::thread::sleep(Duration::from_millis(100));
    let args = mr::rpc::ExampleArgs { x: 99 };
    let rep = mr::net::call::<_, mr::rpc::ExampleReply>(&sock, "Coordinator.Example", &args);
    assert!(rep.is_some(), "example RPC failed");
    assert_eq!(rep.unwrap().y, 100);
    // done() is false in the skeleton
    assert!(!c.done());
    let _ = std::fs::remove_file(&sock);
}

#[test]
fn sequential_wc_smoke() {
    let dir = std::env::temp_dir().join(format!("mr-seq-{}", tester::randstring(8)));
    std::fs::create_dir_all(&dir).unwrap();
    let input = dir.join("in.txt");
    std::fs::write(&input, "the quick the lazy the end").unwrap();

    // Run mr-sequential via the same code path as the binary.
    let bin = env!("CARGO_BIN_EXE_mr-sequential");
    let status = std::process::Command::new(bin)
        .args(["wc", input.to_str().unwrap()])
        .current_dir(&dir)
        .status()
        .unwrap();
    assert!(status.success());
    let out = std::fs::read_to_string(dir.join("mr-out-0")).unwrap();
    let mut lines: Vec<&str> = out.lines().collect();
    lines.sort_unstable();
    assert_eq!(lines, vec!["end 1", "lazy 1", "quick 1", "the 3"]);
    let _ = std::fs::remove_dir_all(&dir);
}
