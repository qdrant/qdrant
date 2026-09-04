//! Update-only shard: a batch writer over an edge-shard directory, the mirror
//! image of [`ReadOnlyEdgeShard`](crate::ReadOnlyEdgeShard). Its whole public
//! surface is [`apply_batch`].
//!
//! Built for the serverless updater's cost model — batches of many tiny
//! operations, remote per-file reads, no long-lived process:
//!
//! * a batch is folded before it is applied: a point is read at most once and
//!   written at most once, however many operations named it;
//! * writers are opened once, at shard open, next to the segments they resume
//!   from; the store components only on the first point actually stored. Every
//!   lookup is one batched pass per component over the whole point set;
//! * there is no WAL: a batch is durable when the storages are flushed;
//! * an upsert's `update_mode` is honored off the same lookup: whether a point
//!   exists is what locating it already answers, so `insert_only` costs
//!   nothing beyond a plain upsert — and less, since the points it rejects are
//!   never read. A conditional upsert carrying a real filter is rejected:
//!   evaluating one needs payload indexes the writer never fetches.
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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::universal_io::UniversalAppendFs;
use parking_lot::RwLock;
use rayon::ThreadPool;
use segment::segment::update_only::UpdateOnlySegmentEnum;
use segment::types::SegmentConfig;
use uuid::Uuid;

pub use self::apply::{PointApplyKind, PointApplyRecord, UpdateBatchOutcome};
pub use self::batch::{PointUpdates, UpdateBatchPlan};
use self::holder::LookupSegmentHolder;
pub use self::preview::{PointAction, PointCopy, PointPreview, UpdateBatchPreview};

/// A batch writer over the segments of one shard directory.
///
/// Compared to [`EdgeShard`](crate::EdgeShard), there is no WAL, no
/// optimizers, and no `EdgeConfig` — the write target's own segment config is
/// the only configuration a write needs.
pub struct UpdateOnlyEdgeShard<Fs: UniversalAppendFs> {
    path: PathBuf,
    /// Backend the segments were opened on; live-reloads their lookup
    /// halves after a batch writes to them.
    fs: Fs,
    segments: RwLock<LookupSegmentHolder<Fs::File>>,
    /// One writer per segment, opened at shard open from the state its
    /// [`LookupSegment`](segment::segment::update_only::LookupSegment)
    /// observed — which also decided whether the segment accepts appends or
    /// deletes only.
    writers: HashMap<Uuid, UpdateOnlySegmentEnum<Fs>>,
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

impl<Fs: UniversalAppendFs> UpdateOnlyEdgeShard<Fs> {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of segments the writer has open.
    pub fn segments_count(&self) -> usize {
        self.segments.read().len()
    }

    /// The append target; `None` when every appendable is claimed by a rebuild.
    pub fn write_target(&self) -> Option<Uuid> {
        self.segments.read().write_target_uuid()
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
