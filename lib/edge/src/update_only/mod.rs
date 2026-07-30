//! Update-only shard: a batch writer over an edge-shard directory, the mirror
//! image of [`ReadOnlyEdgeShard`](crate::ReadOnlyEdgeShard). Its whole public
//! surface is [`apply_batch`].
//!
//! Built for the serverless updater's cost model — batches of many tiny
//! operations, remote per-file reads, no long-lived process:
//!
//! * a batch is folded before it is applied: a point is read at most once and
//!   written at most once, however many operations named it;
//! * only the components a write needs are opened, and every lookup is one
//!   batched pass per component over the whole point set;
//! * there is no WAL: a batch is durable when the storages are flushed.
//!
//! Storage is append-only throughout. Updating a point appends it in full and
//! tombstones its old slot; a deletion writes nothing but the deleted-points
//! bitmask.
//!
//! [`apply_batch`]: UpdateOnlyEdgeShard::apply_batch

mod apply;
mod batch;
mod holder;
mod lifecycle;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::universal_io::UniversalRead;
use parking_lot::RwLock;
use rayon::ThreadPool;

pub use self::apply::UpdateBatchOutcome;
pub use self::batch::{PointUpdates, UpdateBatchPlan};
use self::holder::UpdateOnlySegmentHolder;

/// A batch writer over the segments of one shard directory, generic over the
/// backend `S`.
///
/// Compared to [`EdgeShard`](crate::EdgeShard), there is no WAL, no
/// optimizers, and no `EdgeConfig` — the write target's own segment config is
/// the only configuration a write needs.
pub struct UpdateOnlyEdgeShard<S: UniversalRead + 'static> {
    path: PathBuf,
    /// Backend the segments were opened on, and the one their appends go
    /// through. Unread until the writer can create the appendable segment a
    /// fresh directory needs.
    #[allow(dead_code)]
    fs: S::Fs,
    segments: RwLock<UpdateOnlySegmentHolder<S>>,
    /// Thread pool the per-segment work of a batch runs on: on a remote
    /// backend each segment's reads block on the network, so segments are
    /// visited in parallel.
    pool: Arc<ThreadPool>,
}

impl<S: UniversalRead + 'static> UpdateOnlyEdgeShard<S> {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of segments the writer has open.
    pub fn segments_count(&self) -> usize {
        self.segments.read().len()
    }
}
