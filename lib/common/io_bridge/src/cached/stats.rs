//! Statistics of a [`CachedBlobFs`](super::CachedBlobFs): the disk cache's logical fetches
//! plus every remote request, reads and writes alike.

use common::universal_io::{DiskCacheStats, OpStatsSnapshot};

use crate::stats::{RemoteIoStats, RemoteIoStatsSnapshot};

/// Cloneable observer; see [`DiskCacheStats`] and [`RemoteIoStats`] for what each half counts.
/// A cache fetch and the remote read it issues are counted once in each half.
#[derive(Clone, Debug, Default)]
pub struct CachedBlobStats {
    pub cache: DiskCacheStats,
    pub remote: RemoteIoStats,
}

impl CachedBlobStats {
    pub fn snapshot(&self) -> CachedBlobStatsSnapshot {
        CachedBlobStatsSnapshot {
            cache: self.cache.snapshot(),
            remote: self.remote.snapshot(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CachedBlobStatsSnapshot {
    pub cache: OpStatsSnapshot,
    pub remote: RemoteIoStatsSnapshot,
}

impl CachedBlobStatsSnapshot {
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty() && self.remote.is_empty()
    }

    /// Interval counters against an earlier snapshot of the same observer.
    pub fn delta_since(&self, earlier: &Self) -> Self {
        Self {
            cache: self.cache.delta_since(&earlier.cache),
            remote: self.remote.delta_since(&earlier.remote),
        }
    }

    /// Human-readable multi-line dump; `None` when nothing was recorded.
    pub fn format_compact(&self) -> Option<String> {
        let mut sections = Vec::new();
        if let Some(cache) = self.cache.format_compact() {
            sections.push(format!("cache fetches: {cache}"));
        }
        if let Some(remote) = self.remote.format_compact() {
            let indented = remote.replace('\n', "\n  ");
            sections.push(format!("remote requests:\n  {indented}"));
        }
        (!sections.is_empty()).then(|| sections.join("\n"))
    }
}
