//! The read phase of an update: the components a batch resolves its points
//! against, and nothing that writes.

mod lifecycle;
mod live_reload;
mod resolve;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::{CachedFs, UniversalReadFsAsync};

pub(super) use self::lifecycle::{WRITER_POPULATE, build_cached_fs};
use super::WriterIdTrackerState;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::{SegmentConfig, VectorNameBuf};
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// A segment opened to resolve updates against: read components only. Generic
/// over the backend `S`, like `ReadOnlySegment`, and requiring no more of it
/// than reads — every segment of a shard is opened this way, including those a
/// batch never writes to.
pub struct LookupSegment<Fs: UniversalReadFsAsync> {
    fs: CachedFs<Fs>,

    /// Path to the segment directory.
    pub segment_path: PathBuf,

    pub id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<Fs::File>>>,
    pub payload_storage: Arc<AtomicRefCell<ReadOnlyPayloadStorage<Fs::File>>>,
    /// One storage per named vector — no vector index, no quantized vectors.
    pub vector_data: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageReadEnum<Fs::File>>>>,

    pub segment_config: SegmentConfig,
    /// Whether this segment accepts appends, and can therefore be the target
    /// of a write.
    pub appendable: bool,
}

impl<Fs: UniversalReadFsAsync> LookupSegment<Fs> {
    /// The id-tracker state a writer resuming this segment picks up from, for
    /// [`UpdateOnlySegmentEnum::open`](super::UpdateOnlySegmentEnum::open).
    ///
    /// Taken from this segment's own reads rather than re-read by the writer:
    /// a second read costs another round-trip on a remote backend and may
    /// land on a different state than the batch resolved against. The deleted
    /// mask is handed over only when already in memory — the disk-resident
    /// tracker deliberately avoids materializing it.
    pub fn writer_state(&self) -> WriterIdTrackerState {
        WriterIdTrackerState::of(&self.id_tracker.borrow())
    }
}
