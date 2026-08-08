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
//! retires its old copy; a deletion writes no point data at all — it records
//! a retirement in the appendable segment's mappings log, or marks the
//! deleted-points bitmask of an immutable one.
//!
//! [`apply_batch`]: UpdateOnlyEdgeShard::apply_batch

mod apply;
mod batch;
mod holder;
mod lifecycle;
mod preview;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::universal_io::UniversalRead;
use parking_lot::RwLock;
use rayon::ThreadPool;
use segment::types::SegmentConfig;
use uuid::Uuid;

pub use self::apply::UpdateBatchOutcome;
pub use self::batch::{PointUpdates, UpdateBatchPlan};
use self::holder::LookupSegmentHolder;
pub use self::preview::{PointAction, PointCopy, PointPreview, UpdateBatchPreview};

/// A batch writer over the segments of one shard directory, generic over the
/// backend `S`.
///
/// Compared to [`EdgeShard`](crate::EdgeShard), there is no WAL, no
/// optimizers, and no `EdgeConfig` — the write target's own segment config is
/// the only configuration a write needs.
pub struct UpdateOnlyEdgeShard<S: UniversalRead + 'static> {
    path: PathBuf,
    /// Backend the segments were opened on, and the one a batch's writers go
    /// through.
    fs: S::Fs,
    segments: RwLock<LookupSegmentHolder<S>>,
    /// Thread pool the per-segment work of a batch runs on: on a remote
    /// backend each segment's reads block on the network, so segments are
    /// visited in parallel.
    pool: Arc<ThreadPool>,
}

/// One segment's schema, as reported by
/// [`UpdateOnlyEdgeShard::segment_configs`].
pub struct SegmentConfigInfo {
    pub uuid: Uuid,
    /// Whether this segment is the write target — the one every write in a
    /// batch is appended to.
    pub is_write_target: bool,
    pub config: SegmentConfig,
}

impl<S: UniversalRead + 'static> UpdateOnlyEdgeShard<S> {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of segments the writer has open.
    pub fn segments_count(&self) -> usize {
        self.segments.read().len()
    }

    /// Every segment's config, cloned out, with the write target marked.
    /// Order is unspecified. The write target's config is the schema a write
    /// must conform to: the named vectors a point carries, and their shapes.
    pub fn segment_configs(&self) -> Vec<SegmentConfigInfo> {
        let segments = self.segments.read();
        let write_target = segments.write_target_uuid();
        segments
            .iter()
            .map(|(uuid, segment)| SegmentConfigInfo {
                uuid,
                is_write_target: Some(uuid) == write_target,
                config: segment.read().segment_config.clone(),
            })
            .collect()
    }
}
