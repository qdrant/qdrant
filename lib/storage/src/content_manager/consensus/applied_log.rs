//! Diagnostic record of the consensus entries this peer has applied.
//!
//! Consensus never reads this back: it exists so peers can be compared against each other by
//! `/profiler/consensus_lag`, which lines up the entry indices they have in common. Kept in
//! memory rather than alongside the persisted consensus state, which is rewritten on every
//! applied entry.

use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

use parking_lot::Mutex;
use raft::eraftpb::Entry as RaftEntry;
use serde::Serialize;

/// How many recently applied consensus entries each peer remembers.
///
/// Only bounds memory. A peer further behind than this many entries can still be placed by
/// index, but not timed, because no entry it applied is still remembered elsewhere.
pub const APPLIED_LOG_SIZE: usize = 32;

/// A consensus entry this peer has finished applying.
///
/// Timestamps come from this peer's own wall clock, so comparing them against another peer's
/// includes whatever clock skew exists between the two machines.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct AppliedEntry {
    /// Raft log index of the entry.
    pub index: u64,
    /// Raft term the entry was proposed in.
    pub term: u64,
    /// Wall clock when the entry finished applying, milliseconds since the Unix epoch.
    pub applied_at_ms: u64,
    /// How long this single entry took to apply.
    pub took_ms: u64,
}

/// Snapshot of a peer's applied-entry log, as served to `/profiler/consensus_lag`.
#[derive(Debug, Clone, Serialize)]
pub struct AppliedLog {
    /// Recently applied entries, oldest first, at most [`APPLIED_LOG_SIZE`] of them.
    pub entries: Vec<AppliedEntry>,
    /// Wall clock on this peer when the log was read, milliseconds since the Unix epoch.
    ///
    /// Lets a reader age the newest entry against the clock that stamped it, rather than its own.
    pub now_ms: u64,
    /// Entries committed but not yet applied on this peer.
    pub pending_operations: usize,
    /// Highest entry index this peer has applied, from the consensus state rather than the
    /// entries above, which cover only this process. `None` if it has never applied one.
    pub last_applied_index: Option<u64>,
}

/// Ring of the entries this peer applied most recently, oldest first.
pub struct AppliedEntryRing(Mutex<VecDeque<AppliedEntry>>);

impl Default for AppliedEntryRing {
    fn default() -> Self {
        Self(Mutex::new(VecDeque::with_capacity(APPLIED_LOG_SIZE)))
    }
}

impl AppliedEntryRing {
    /// Record that `entry` finished applying, evicting the oldest entry once the ring is full.
    pub fn record(&self, entry: &RaftEntry, took: Duration) {
        let applied = AppliedEntry {
            index: entry.index,
            term: entry.term,
            applied_at_ms: now_ms(),
            took_ms: took.as_millis() as u64,
        };

        let mut entries = self.0.lock();
        while entries.len() >= APPLIED_LOG_SIZE {
            entries.pop_front();
        }
        entries.push_back(applied);
    }

    /// Snapshot the ring for a lag report.
    ///
    /// The ring is empty until this peer applies its first entry after startup, so a freshly
    /// restarted peer has nothing to time, however far it has caught up. Both `pending_operations`
    /// and `last_applied_index` are supplied by the caller from the consensus state, which knows
    /// where the peer stands whether or not this process applied anything.
    pub fn snapshot(
        &self,
        pending_operations: usize,
        last_applied_index: Option<u64>,
    ) -> AppliedLog {
        let entries = self.0.lock().iter().copied().collect();

        AppliedLog {
            entries,
            now_ms: now_ms(),
            pending_operations,
            last_applied_index,
        }
    }
}

/// Current wall clock in milliseconds since the Unix epoch, saturating at 0 before it.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since_epoch| since_epoch.as_millis() as u64)
}
