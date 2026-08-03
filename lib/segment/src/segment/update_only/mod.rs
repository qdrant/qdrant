//! Update-only segment: the write-side counterpart of
//! [`ReadOnlySegment`](crate::segment::read_only::ReadOnlySegment).
//!
//! Its public surface is storing points and tombstoning slots; internally it
//! still *reads*, because an operation like `set_payload` names a point but
//! not its vectors. It opens exactly what that requires — the id tracker, the
//! payload storage and the vector storages. No vector index, no quantized
//! vectors, no payload index: on a remote backend those files are never
//! fetched.

mod append;
mod lifecycle;
mod resolve;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::UniversalRead;
use uuid::Uuid;

use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::{SegmentConfig, VectorNameBuf};
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// A segment open for updates: read components to resolve points with,
/// append-only components to store them through. Generic over the backend `S`,
/// like `ReadOnlySegment`.
pub struct UpdateOnlySegment<S: UniversalRead + 'static> {
    pub uuid: Uuid,
    /// Path to the segment directory.
    pub segment_path: PathBuf,
    /// Backend the segment was opened on. Retained because appends need a
    /// filesystem handle of their own: the caching wrapper an open may go
    /// through only lives for that open.
    pub fs: S::Fs,

    pub id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    pub payload_storage: Arc<AtomicRefCell<ReadOnlyPayloadStorage<S>>>,
    pub vector_data: HashMap<VectorNameBuf, UpdateOnlyVectorData<S>>,

    pub segment_config: SegmentConfig,
    /// Whether this segment can accept appends and therefore be the target of
    /// a write.
    appendable: bool,
}

/// A single named vector of an [`UpdateOnlySegment`]: storage only — no vector
/// index, no quantized vectors.
pub struct UpdateOnlyVectorData<S: UniversalRead + 'static> {
    pub vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
}

impl<S: UniversalRead + 'static> UpdateOnlySegment<S> {
    /// Whether this segment accepts appends, and can therefore be the target of
    /// a write.
    pub fn is_appendable(&self) -> bool {
        self.appendable
    }
}
