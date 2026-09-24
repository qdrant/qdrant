//! Statistics for remote data fetches made by the disk cache. These count logical fetches,
//! not backend HTTP requests, metadata operations, retries, or wire bytes. Local memory reads
//! are not counted.
use std::time::Instant;

use crate::universal_io::{OpGuard, OpStats, OpStatsSnapshot};

/// Cloneable observer shared by a filesystem, its clones, and all files it opens.
#[derive(Clone, Debug, Default)]
pub struct DiskCacheStats(OpStats);

impl DiskCacheStats {
    pub fn snapshot(&self) -> OpStatsSnapshot {
        self.0.snapshot()
    }

    /// Fetches are scheduled or async reads started, including whole-file reads of empty files.
    /// Their observed duration includes queueing and delayed collection in `wait`, but excludes
    /// local mirror writes. Pending fetches dropped after a pipeline failure count as abandoned.
    pub(super) fn fetch(&self, started: Instant) -> OpGuard {
        self.0.start(started)
    }
}
