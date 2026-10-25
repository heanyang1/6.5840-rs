//! A linearizability checker for key/value histories, ported from the
//! `porcupine` package (github.com/anishathalye/porcupine) and the KV
//! model in the Go labs' `models1` package.
//!
//! The checker implements the standard Wing & Gong graph-search algorithm
//! with memoization (the same one porcupine uses).  Histories are
//! partitioned by key, and each partition is checked independently, which
//! keeps the state space small.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct KvInput {
    pub op: u8, // 0 => get, 1 => put
    pub key: String,
    pub value: String,
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct KvOutput {
    pub value: String,
    pub version: u64,
    pub err: String,
}

#[derive(Clone, Debug)]
pub struct Operation {
    pub input: KvInput,
    pub output: KvOutput,
    pub call: u64,  // microseconds since test start
    pub ret: u64,
    pub client: i64,
}

/// A single key's state: (value, version).
type State = (String, u64);

/// Apply the sequential KV model to a state; returns whether the output
/// matches, and the next state (models1/kv.go's Step).
fn step(state: &State, input: &KvInput, output: &KvOutput) -> (bool, State) {
    match input.op {
        0 => {
            // get
            (output.value == state.0, state.clone())
        }
        1 => {
            // put
            if state.1 == input.version {
                (
                    output.err == "OK" || output.err == "ErrMaybe",
                    (input.value.clone(), state.1 + 1),
                )
            } else {
                (output.err == "ErrVersion" || output.err == "ErrMaybe", state.clone())
            }
        }
        _ => (false, state.clone()),
    }
}

struct Entry {
    call_rank: usize, // position in call-time order
    input: KvInput,
    output: KvOutput,
}

/// Check whether `ops` is linearizable w.r.t. the KV model.  Returns:
/// - `Some(true)` if linearizable,
/// - `Some(false)` if a violation was found,
/// - `None` if the check timed out (callers typically assume OK, as the
///   Go tester does).
pub fn check(ops: &[Operation], timeout: Duration) -> Option<bool> {
    // The checker's search recurses once per (call, complete) event, so
    // long histories need a big stack: run it on a dedicated thread.
    let ops = ops.to_vec();
    let handle = std::thread::Builder::new()
        .name("porcupine".into())
        .stack_size(512 * 1024 * 1024)
        .spawn(move || check_inner(&ops, timeout))
        .expect("spawn porcupine thread");
    handle.join().expect("porcupine thread panicked")
}

fn check_inner(ops: &[Operation], timeout: Duration) -> Option<bool> {
    // Partition by key.
    let mut by_key: HashMap<String, Vec<Operation>> = HashMap::new();
    for op in ops {
        by_key.entry(op.input.key.clone()).or_default().push(op.clone());
    }
    let deadline = Instant::now() + timeout;
    for (_, hops) in by_key.iter_mut() {
        hops.sort_by_key(|o| o.call);
        match check_partition(hops, deadline) {
            Some(true) => {}
            other => return other,
        }
    }
    Some(true)
}

fn check_partition(ops: &[Operation], deadline: Instant) -> Option<bool> {
    let n = ops.len();
    if n == 0 {
        return Some(true);
    }
    // Entries sorted by return time (the order in which we try to
    // complete them).
    let mut ret_order: Vec<usize> = (0..n).collect();
    ret_order.sort_by_key(|&i| ops[i].ret);
    // call_rank[i] = position of op i in call-time order.
    let mut call_order: Vec<usize> = (0..n).collect();
    call_order.sort_by_key(|&i| ops[i].call);
    let mut call_rank = vec![0usize; n];
    for (rank, &op) in call_order.iter().enumerate() {
        call_rank[op] = rank;
    }
    // Real-time constraint: entry j may be called only after every entry
    // that returned before j was called is completed.  For each entry,
    // compute how many of the earliest-returned entries (a prefix of
    // ret_order) must already be completed before it can be called.
    let mut min_completed = vec![0usize; n]; // indexed by entry index
    for &j in &call_order {
        let mut cnt = 0usize;
        for &i in ret_order.iter() {
            if ops[i].ret < ops[j].call {
                cnt += 1;
            } else {
                break; // ret_order is sorted by ret
            }
        }
        min_completed[j] = cnt;
    }
    let entries: Vec<Entry> = (0..n)
        .map(|i| Entry {
            call_rank: call_rank[i],
            input: ops[i].input.clone(),
            output: ops[i].output.clone(),
        })
        .collect();

    let completed: BTreeSet<usize> = BTreeSet::new(); // indices into entries
    let next_call = 0usize; // how many entries have been called
    let mut memo: HashSet<(String, u64, Vec<usize>, usize)> = HashSet::new();
    let mut budget: u64 = 20_000_000; // step budget, plus a wall-clock deadline

    // DFS over interleavings.  `live` is implicit: called = next_call
    // entries in call order, minus completed.
    #[allow(clippy::too_many_arguments)]
    fn dfs(
        state: State,
        completed: &BTreeSet<usize>,
        next_call: usize,
        entries: &[Entry],
        ret_order: &[usize],
        min_completed: &[usize],
        ops: &[Operation],
        memo: &mut HashSet<(String, u64, Vec<usize>, usize)>,
        deadline: Instant,
        budget: &mut u64,
    ) -> Option<bool> {
        if completed.len() == entries.len() {
            return Some(true);
        }
        if *budget == 0 || Instant::now() > deadline {
            return None; // give up
        }
        *budget -= 1;

        let key = (state.0.clone(), state.1, completed.iter().cloned().collect(), next_call);
        if !memo.insert(key) {
            return Some(false); // dead end already explored
        }

        // Try entries in return-time order.
        for &e in ret_order {
            let entry = &entries[e];
            let called = entry.call_rank < next_call;
            let done = completed.contains(&e);
            if done {
                continue;
            }
            if called {
                // Complete this entry now.
                let (ok, state2) = step(&state, &entry.input, &entry.output);
                if ok {
                    let mut completed2 = completed.clone();
                    completed2.insert(e);
                    // Some(false) = dead end: keep searching other options.
                    match dfs(
                        state2,
                        &completed2,
                        next_call,
                        entries,
                        ret_order,
                        min_completed,
                        ops,
                        memo,
                        deadline,
                        budget,
                    ) {
                        Some(true) => return Some(true),
                        None => return None,
                        Some(false) => {}
                    }
                }
            } else if entry.call_rank == next_call {
                // Call the next entry (calls must happen in call order),
                // but only once everything that returned before its call
                // has been completed (real-time order).
                let need = min_completed[e];
                let ok_realtime = ret_order[..need]
                    .iter()
                    .all(|&i| completed.contains(&i));
                if !ok_realtime {
                    continue;
                }
                match dfs(
                    state,
                    completed,
                    next_call + 1,
                    entries,
                    ret_order,
                    min_completed,
                    ops,
                    memo,
                    deadline,
                    budget,
                ) {
                    Some(true) => return Some(true),
                    None => return None,
                    Some(false) => {}
                }
                break; // can't call any other entry before this one
            }
        }
        Some(false)
    }

    dfs(
        (String::new(), 0),
        &completed,
        next_call,
        &entries,
        &ret_order,
        &min_completed,
        ops,
        &mut memo,
        deadline,
        &mut budget,
    )
}
