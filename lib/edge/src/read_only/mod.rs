//! Read-only follower of [`EdgeShard`](crate::EdgeShard).
//!
//! A leader process owns a read-write `EdgeShard` and writes to an on-disk directory. One or more
//! follower processes open the *same* directory as a [`ReadOnlyEdgeShard`] and serve reads. A
//! follower never writes: no WAL, no optimization, no segment creation. It refreshes by
//! [`refresh`](ReadOnlyEdgeShard::refresh)ing — rescanning the `segments/` directory to pick up
//! segments the leader created/removed, and [`live_reload`](segment::segment::read_only::ReadOnlySegment::live_reload)ing
//! the survivors to fold in the leader's flushed in-place appends and deletes.
//!
//! This mirrors the segment-level read-only design one level up: just as `ReadOnlySegment` is the
//! read-only counterpart of `Segment`, `ReadOnlyEdgeShard` is the read-only counterpart of
//! `EdgeShard`. Both expose a consistent [`EdgeReadView`](crate::EdgeReadView) and share its read
//! logic.

mod enumerate;
mod holder;
mod lifecycle;
mod load;
mod refresh;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;

use crate::EdgeConfig;
pub use crate::read_only::enumerate::{
    LocalSegmentEnumerator, ManifestSegmentEnumerator, SegmentEnumerator,
};
use crate::read_only::holder::ReadOnlySegmentHolder;
use crate::read_view::EdgeShardRead;

/// Read-only follower view over an edge shard's on-disk directory.
///
/// Generic over the read backend `S` (e.g. `MmapFile` for local memory-mapped files; the same
/// abstraction `ReadOnlySegment` uses, so blob/S3 backends are possible). Use
/// [`open_mmap`](ReadOnlyEdgeShard::open_mmap) for the common local case.
pub struct ReadOnlyEdgeShard<S: UniversalReadExt + 'static> {
    path: PathBuf,
    /// Read backend handle; passed to segment `open` and `live_reload`.
    fs: S::Fs,
    /// Config snapshot, derived from the segments (a follower has no `edge_config.json`) and
    /// re-derived on each refresh. Stored as an `Arc` so a read view can cheaply clone the current
    /// snapshot while a refresh swaps in a new one.
    config: RwLock<Arc<EdgeConfig>>,
    segments: RwLock<ReadOnlySegmentHolder<S>>,
    /// Discovers the current segment directories. Injected because segment discovery is
    /// backend-specific (see [`SegmentEnumerator`]) until an on-disk manifest exists.
    enumerator: Box<dyn SegmentEnumerator>,
}

impl<S: UniversalReadExt + 'static> ReadOnlyEdgeShard<S> {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of segments currently open in the follower.
    pub fn segments_count(&self) -> usize {
        self.segments.read().uuids().len()
    }
}

/// The follower's segments are homogeneous, so its handle is the concrete
/// `Arc<RwLock<ReadOnlySegment<S>>>` — the read path is fully monomorphized, no dynamic dispatch.
impl<S: UniversalReadExt + 'static> EdgeShardRead for ReadOnlyEdgeShard<S> {
    type Handle = Arc<RwLock<ReadOnlySegment<S>>>;

    fn read_segments(&self) -> Vec<Arc<RwLock<ReadOnlySegment<S>>>> {
        self.segments.read().read_handles()
    }

    fn config_snapshot(&self) -> Arc<EdgeConfig> {
        self.config.read().clone()
    }

    fn path(&self) -> &Path {
        &self.path
    }
}
