//! Shard configuration description (port of Go's shardkv1/shardcfg;
//! provided code, please don't change).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type Tshid = i64;
pub type Tnum = i64;

/// The number of shards.
pub const NSHARDS: usize = 12;
pub const NUM_FIRST: Tnum = 1;

pub const GID1: i64 = 1;

/// Which shard is a key in?  Please use this function, and please do not
/// change it.
pub fn key2shard(key: &str) -> Tshid {
    // FNV-1a 32-bit, the same hash as Go's fnv.New32a().
    let mut h: u32 = 2166136261;
    for b in key.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(16777619);
    }
    (h % NSHARDS as u32) as Tshid
}

/// A configuration -- an assignment of shards to groups.  Please don't
/// change this.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardConfig {
    pub num: Tnum,                                    // config number
    pub shards: [i64; NSHARDS],                       // shard -> gid
    pub groups: HashMap<i64, Vec<String>>,            // gid -> servers[]
}

pub fn make_shard_config() -> ShardConfig {
    ShardConfig {
        num: 0,
        shards: [0; NSHARDS],
        groups: HashMap::new(),
    }
}

impl ShardConfig {
    /// Serialize to a string, for storage in the controller's kvsrv (Go:
    /// ShardConfig.String).
    pub fn to_string_json(&self) -> String {
        serde_json::to_string(self).expect("marshal ShardConfig")
    }

    /// Parse a string produced by to_string_json (Go: FromString).
    pub fn from_string(s: &str) -> ShardConfig {
        serde_json::from_str(s).expect("unmarshal ShardConfig")
    }

    pub fn copy(&self) -> ShardConfig {
        self.clone()
    }

    /// Return the gid and servers of the group serving shard `sh`, or
    /// (gid, [], false) if the group isn't in the config.
    pub fn gid_servers(&self, sh: Tshid) -> (i64, Vec<String>, bool) {
        let gid = self.shards[sh as usize];
        match self.groups.get(&gid) {
            Some(srvs) => (gid, srvs.clone(), true),
            None => (gid, Vec::new(), false),
        }
    }

    /// Join adds a new group (Go: Join).  Modifies self; returns false if
    /// any gid was already present (in which case self is unchanged).
    pub fn join(&mut self, servers: &HashMap<i64, Vec<String>>) -> bool {
        let mut changed = false;
        for (&gid, servers) in servers {
            if self.groups.contains_key(&gid) {
                println!("re-Join {}", gid);
                return false;
            }
            self.groups.insert(gid, servers.clone());
            changed = true;
        }
        assert!(changed, "Join but no change");
        self.num += 1;
        true
    }

    /// Leave removes groups (Go: Leave).  Modifies self; returns false if
    /// any gid wasn't present.
    pub fn leave(&mut self, gids: &[i64]) -> bool {
        let mut changed = false;
        for &gid in gids {
            if self.groups.remove(&gid).is_none() {
                // already no GID!
                println!("Leave({}) but not in config", gid);
                return false;
            }
            changed = true;
        }
        assert!(changed, "Leave but no change");
        self.num += 1;
        true
    }

    /// Join, then rebalance (Go: JoinBalance).
    pub fn join_balance(&mut self, servers: &HashMap<i64, Vec<String>>) -> bool {
        if !self.join(servers) {
            return false;
        }
        self.rebalance();
        true
    }

    /// Leave, then rebalance (Go: LeaveBalance).
    pub fn leave_balance(&mut self, gids: &[i64]) -> bool {
        if !self.leave(gids) {
            return false;
        }
        self.rebalance();
        true
    }

    /// Is gid responsible for at least one shard?
    pub fn is_member(&self, gid: i64) -> bool {
        self.shards.iter().any(|&g| g == gid)
    }

    // ---------------- rebalancing (provided) ----------------

    /// (mostgroup, mostn, leastgroup, leastn)
    fn analyze(&self) -> (i64, i64, i64, i64) {
        let mut counts: HashMap<i64, i64> = HashMap::new();
        for &g in &self.shards {
            *counts.entry(g).or_insert(0) += 1;
        }

        let mut mn = -1i64;
        let mut mg = -1i64;
        let mut ln = 257i64;
        let mut lg = -1i64;
        // Enforce deterministic ordering (Go sorts group ids).
        let mut groups: Vec<i64> = self.groups.keys().cloned().collect();
        groups.sort_unstable();
        for g in groups {
            let c = counts.get(&g).copied().unwrap_or(0);
            if c < ln {
                ln = c;
                lg = g;
            }
            if c > mn {
                mn = c;
                mg = g;
            }
        }
        (mg, mn, lg, ln)
    }

    fn least(&self) -> i64 {
        self.analyze().2
    }

    /// Balance assignment of shards to groups.  Modifies self.
    pub fn rebalance(&mut self) {
        // If no groups, un-assign all shards.
        if self.groups.is_empty() {
            for s in self.shards.iter_mut() {
                *s = 0;
            }
            return;
        }

        // Assign all unassigned shards.
        for s in 0..NSHARDS {
            let g = self.shards[s];
            if !self.groups.contains_key(&g) {
                let lg = self.least();
                self.shards[s] = lg;
            }
        }

        // Move shards from most to least heavily loaded.
        loop {
            let (mg, mn, lg, ln) = self.analyze();
            if mn < ln + 2 {
                break;
            }
            // Move 1 shard from mg to lg.
            for s in 0..NSHARDS {
                if self.shards[s] == mg {
                    self.shards[s] = lg;
                    break;
                }
            }
        }
    }

    /// Test helper (Go: CheckConfig): check that the config has the given
    /// groups and is roughly balanced.
    pub fn check_config(&self, groups: &[i64]) {
        assert_eq!(
            self.groups.len(),
            groups.len(),
            "wanted {} groups, got {}",
            groups.len(),
            self.groups.len()
        );
        for &g in groups {
            assert!(self.groups.contains_key(&g), "missing group {}", g);
        }
        if !groups.is_empty() {
            for (s, &g) in self.shards.iter().enumerate() {
                assert!(
                    self.groups.contains_key(&g),
                    "shard {} -> invalid group {}",
                    s,
                    g
                );
            }
        }
        let mut counts: HashMap<i64, i64> = HashMap::new();
        for &g in &self.shards {
            *counts.entry(g).or_insert(0) += 1;
        }
        let mut min = 257i64;
        let mut max = 0i64;
        for &g in groups {
            let c = counts.get(&g).copied().unwrap_or(0);
            if c > max {
                max = c;
            }
            if c < min {
                min = c;
            }
        }
        assert!(max <= min + 1, "max {} too much larger than min {}", max, min);
    }
}
