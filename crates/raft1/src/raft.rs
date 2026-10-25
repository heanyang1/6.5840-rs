//! A Raft implementation, ported from Go's raft1/raft.go.
//!
//! The file crate::api defines the ApplyMsg type that Raft sends to the
//! service (or tester); see the comments there for details.
//!
//! A `Raft<C>` object implements a single Raft peer.  The peers argument
//! to make() holds RPC end points for all peers (including this one);
//! `me` is this peer's index into peers[].

use crate::api::{ApplyMsg, Cmd};
use labrpc::ClientEnd;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tester::Persister;

/// Example RequestVote RPC arguments structure.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    // Your data here (3A, 3B).
}

/// Example RequestVote RPC reply structure.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestVoteReply {
    // Your data here (3A).
}

/// The mutable state protected by `mu`.  Replace the placeholder with your
/// own fields (current_term, voted_for, log, ...).
pub struct RaftState<C: Cmd> {
    // Your data here (3A, 3B, 3C).
    //
    // Look at the paper's Figure 2 for a description of what state a Raft
    // server must maintain.
    #[allow(dead_code)]
    marker: std::marker::PhantomData<C>,
}

/// A Go object implementing a single Raft peer.
pub struct Raft<C: Cmd> {
    mu: Mutex<RaftState<C>>,    // Lock to protect shared access to this peer's state
    peers: Vec<Arc<ClientEnd>>, // RPC end points of all peers
    persister: Arc<Persister>,  // Object to hold this peer's persisted state
    me: usize,                  // this peer's index into peers[]

    dead: AtomicBool, // set by kill(); provided by the tester

    // The apply channel, behind its own lock (NOT behind `mu`): this lets
    // you call send_apply() while holding `mu` without deadlocking.  It is
    // closed (set to None) by kill(), which is how the service learns to
    // stop.
    apply_ch: Mutex<Option<crossbeam_channel::Sender<ApplyMsg<C>>>>,

    #[allow(dead_code)]
    marker: std::marker::PhantomData<C>,
}

impl<C: Cmd> Raft<C> {
    /// Return currentTerm and whether this server believes it is the
    /// leader.
    pub fn get_state(&self) -> (i64, bool) {
        let _term: i64 = 0;
        let _isleader: bool = false;
        // Your code here (3A).
        (0, false)
    }

    /// Save Raft's persistent state to stable storage, where it can later
    /// be retrieved after a crash and restart.  See the paper's Figure 2
    /// for a description of what should be persistent.
    ///
    /// Before you've implemented snapshots, you should pass None as the
    /// second argument to persister.save(); after you've implemented
    /// snapshots, pass the current snapshot (or None if there's not yet a
    /// snapshot).
    fn persist(&self) {
        // Your code here (3C).
        //
        // Example:
        //   let state = self.mu.lock().unwrap();
        //   let data = labgob::encode(&(*state).persistent_fields());
        //   self.persister.save(&data, None);
    }

    /// Restore previously persisted state.
    fn read_persist(&self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (3C).
        //
        // Example:
        //   if let Some((term, voted_for, log)) = labgob::decode(data) {
        //       let mut state = self.mu.lock().unwrap();
        //       ...
        //   }
    }

    /// How many bytes in Raft's persisted log?
    pub fn persist_bytes(&self) -> usize {
        let _state = self.mu.lock().unwrap();
        self.persister.raft_state_size()
    }

    /// The service says it has created a snapshot that has all info up to
    /// and including `index`.  This means the service no longer needs the
    /// log through (and including) that index.  Raft should now trim its
    /// log as much as possible.
    pub fn snapshot(&self, _index: usize, _snapshot: &[u8]) {
        // Your code here (3D).
    }

    /// Example RequestVote RPC handler.
    pub fn request_vote(&self, _args: &RequestVoteArgs) -> RequestVoteReply {
        // Your code here (3A, 3B).
        RequestVoteReply {}
    }

    /// Example code to send a RequestVote RPC to a server.
    ///
    /// `server` is the index of the target server in self.peers[].  The
    /// return value indicates success; None means that no reply was
    /// received from the server (a dead server, a lost request, or a lost
    /// reply).
    ///
    /// call() is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return, so there
    /// is no need to implement your own timeouts around it.
    pub fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
    ) -> Option<RequestVoteReply> {
        self.peers[server].call("Raft.RequestVote", args)
    }

    /// The service using Raft (e.g. a k/v server) wants to start agreement
    /// on the next command to be appended to Raft's log.  If this server
    /// isn't the leader, returns false.  Otherwise start the agreement and
    /// return immediately.  There is no guarantee that this command will
    /// ever be committed to the Raft log, since the leader may fail or
    /// lose an election.
    ///
    /// The first return value is the index that the command will appear at
    /// if it's ever committed.  The second return value is the current
    /// term.  The third return value is true if this server believes it is
    /// the leader.
    pub fn start(&self, _command: C) -> (usize, i64, bool) {
        let index: usize = 0;
        let term: i64 = 0;
        let is_leader: bool = false;

        // Your code here (3B).

        (index, term, is_leader)
    }

    /// A background thread that kicks off leader election periodically by
    /// sending out RequestVote RPCs when it hasn't heard from another peer
    /// for a while.
    ///
    /// Keep this loop structure: sleep between checks (do NOT busy-loop;
    /// the tests fail if your implementation burns CPU).
    fn ticker(self: &Arc<Self>) {
        loop {
            if self.killed() {
                return;
            }

            // Your code here (3A)
            // Check if a leader election should be started.

            // Pause for a random amount of time between 50 and 350
            // milliseconds.
            use rand::Rng;
            let ms = 50 + rand::thread_rng().gen_range(0..300);
            std::thread::sleep(std::time::Duration::from_millis(ms));
        }
    }

    /// Send an ApplyMsg to the service.  Returns false if the apply
    /// channel has been closed, i.e., this peer was killed (the service
    /// will stop reading).
    ///
    /// You must use this helper (not the channel directly) so that the
    /// tester can shut your peer down cleanly.
    pub fn send_apply(&self, m: ApplyMsg<C>) -> bool {
        let g = self.apply_ch.lock().unwrap();
        match &*g {
            Some(s) => s.send(m).is_ok(),
            None => false,
        }
    }

    /// The tester calls kill() when a Raft instance won't be needed again.
    /// You must ensure that all your background threads exit and that the
    /// apply channel is closed, so that a service reading the channel
    /// terminates.  (The kvraft lab's shutdown test depends on this.)
    pub fn kill(&self) {
        self.dead.store(true, Ordering::SeqCst);
        // Close the apply channel: this tells the service (and tester)
        // that no more entries will be applied.
        *self.apply_ch.lock().unwrap() = None;
        // Your code here, if desired (e.g., unblock any waiting threads).
    }

    pub fn killed(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    /// This peer's index into peers[].
    pub fn me(&self) -> usize {
        self.me
    }

    /// RPC end points of all peers (peers[self.me()] is this peer).
    pub fn peers(&self) -> &[Arc<ClientEnd>] {
        &self.peers
    }
}

/// The service or tester creates a Raft server via make().  The ports of
/// all the Raft servers (including this one) are in `peers`.  This
/// server's port is `peers[me]`.  All the servers' peers[] arrays have the
/// same order.  `persister` is a place for this server to save its
/// persistent state, and also initially holds the most recent saved state,
/// if any.  `apply_ch` is a channel on which the tester or service expects
/// Raft to send ApplyMsg messages.  make() must return quickly, so it
/// should start threads for any long-running work.
pub fn make<C: Cmd>(
    peers: Vec<Arc<ClientEnd>>,
    me: usize,
    persister: Arc<Persister>,
    apply_ch: crossbeam_channel::Sender<ApplyMsg<C>>,
) -> Arc<Raft<C>> {
    let rf = Arc::new(Raft {
        mu: Mutex::new(RaftState {
            marker: std::marker::PhantomData,
        }),
        peers,
        persister,
        me,
        dead: AtomicBool::new(false),
        apply_ch: Mutex::new(Some(apply_ch)),
        marker: std::marker::PhantomData,
    });

    // Your initialization code here (3A, 3B, 3C).

    // Initialize from state persisted before a crash.
    let data = rf.persister.read_raft_state();
    rf.read_persist(&data);

    // Start ticker thread to start elections.
    let rf2 = Arc::clone(&rf);
    std::thread::spawn(move || rf2.ticker());

    rf
}

/// Provided: build the "Raft" RPC service for this peer, registering the
/// RPC handlers.  When you add new RPC handlers (AppendEntries in 3A,
/// InstallSnapshot in 3D), register them here too, e.g.:
///
/// ```ignore
/// svc.handle("AppendEntries", { let rf = Arc::clone(rf); move |args: AppendEntriesArgs| rf.append_entries(&args) });
/// ```
pub fn raft_service<C: Cmd>(rf: &Arc<Raft<C>>) -> labrpc::Service {
    let svc = labrpc::Service::new("Raft");
    {
        let rf = Arc::clone(rf);
        svc.handle("RequestVote", move |args: RequestVoteArgs| {
            rf.request_vote(&args)
        });
    }
    svc
}
