//! Update-only shard: a batch writer over an edge-shard directory.
//!
//! The mirror image of [`ReadOnlyEdgeShard`](crate::ReadOnlyEdgeShard). Where a
//! follower opens a shard directory to serve reads and never writes, an
//! [`UpdateOnlyEdgeShard`] opens one to apply writes and exposes no read API at
//! all — its whole public surface is [`apply_batch`].
//!
//! It exists for the serverless updater, whose cost model is unlike the
//! server's in three ways, each of which shows up in the design:
//!
//! * **Updates arrive in batches of small operations.** So a batch is folded
//!   before it is applied (see `batch`): all operations on a point collapse
//!   into one, and the point is read at most once and written at most once,
//!   regardless of how many operations named it.
//! * **Reads are remote and per-file.** So only components that are actually
//!   needed are opened at all — no vector indexes, no quantized vectors, and no
//!   payload index on the segments the writer only reads from — and every
//!   lookup is batched, one pass per component over the whole point set rather
//!   than one round-trip per point.
//! * **There is no long-lived process to recover.** So there is no WAL: a batch
//!   is durable when the storages are flushed, and the operations it applied
//!   live in the caller's queue until then.
//!
//! Storage is append-only throughout. Updating a point never rewrites its slot:
//! the point is resolved in full, appended to the write target, and its old
//! slot is tombstoned. Deletions in the segments the writer does not append to
//! are tombstone-only — the payload row, the vectors and the field indexes at
//! that slot are never touched, so the writer never has to fetch them.
//!
//! [`apply_batch`]: UpdateOnlyEdgeShard::apply_batch

mod apply;
mod batch;
mod holder;
mod lifecycle;

use std::path::{Path, PathBuf};

use common::universal_io::UniversalRead;
use parking_lot::RwLock;

pub use self::apply::UpdateBatchOutcome;
pub use self::batch::{PointUpdates, UpdateBatchPlan};
use self::holder::UpdateOnlySegmentHolder;

/// A batch writer over the segments of one shard directory.
///
/// Generic over the backend `S` exactly like the read-only follower, since the
/// serverless updater runs against object storage while a local process runs
/// against memory-mapped files.
///
/// Deliberately absent, compared to [`EdgeShard`](crate::EdgeShard): no WAL
/// (see the module docs), no optimizers (the writer appends; rebuilding
/// segments is someone else's job), and no `EdgeConfig` — the write target's
/// own segment config is the only configuration a write needs.
pub struct UpdateOnlyEdgeShard<S: UniversalRead + 'static> {
    path: PathBuf,
    /// Backend the segments were opened on, and the one their appends go
    /// through. Unread until the writer can create the appendable segment a
    /// fresh directory needs — that is the one write that is not a segment's
    /// own.
    #[allow(dead_code)]
    fs: S::Fs,
    segments: RwLock<UpdateOnlySegmentHolder<S>>,
}

impl<S: UniversalRead + 'static> UpdateOnlyEdgeShard<S> {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of segments the writer has open — the one it appends to plus the
    /// ones it reads points from.
    pub fn segments_count(&self) -> usize {
        self.segments.read().len()
    }
}
