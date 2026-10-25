//! Shared test setup for the kvsrv1 tests (port of Go's kvsrv1/test.go,
//! TestKV / MakeTestKV).

use kvsrv1::client;
use std::sync::Arc;
use tester::config::{server_name, Config, GRP0};
use tester::kvtest::{IKVClerk, KvTest};

pub fn make_test_kv(reliable: bool) -> Arc<KvTest> {
    tester::watchdog(600);
    let cfg = Config::new("kvsrv1", 1, reliable, Arc::new(kvsrv1::server::start_server));
    cfg.set_long_delays(true);
    let ts = KvTest::new(cfg, false);
    ts.cfg.begin("Setup kvsrv");
    ts.cfg.end();
    ts
}

/// Make a Clerk (the raw clerk, wrapped by the KvTest recording helpers).
pub fn make_clerk(ts: &KvTest) -> Arc<dyn IKVClerk> {
    let clnt = ts.cfg.make_client();
    let ck = client::make_clerk(clnt, &server_name(GRP0, 0));
    Arc::new(ck)
}

/// Clerk-maker for spawn_clients_with.
pub fn clerk_maker(ts: Arc<KvTest>) -> impl Fn() -> Arc<dyn IKVClerk> + Send + Sync + 'static {
    move || make_clerk(&ts)
}
