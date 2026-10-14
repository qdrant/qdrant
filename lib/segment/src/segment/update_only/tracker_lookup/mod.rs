//! A segment opened with its id tracker only: enough to find points and delete
//! them, without opening any storage.

mod lifecycle;
mod live_reload;
mod resolve;

use std::path::PathBuf;

use common::universal_io::{CachedFs, UniversalReadFsAsync};

pub(super) use self::resolve::{locate_points, point_versions};
use super::WriterIdTrackerState;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::types::SegmentConfig;

/// A [`LookupSegment`](super::LookupSegment) without storages.
pub struct TrackerLookup<Fs: UniversalReadFsAsync> {
    fs: CachedFs<Fs>,

    /// Path to the segment directory.
    pub segment_path: PathBuf,

    id_tracker: ReadOnlyIdTrackerEnum<Fs::File>,

    /// Passed to the writer; unused when it only deletes.
    pub segment_config: SegmentConfig,
    pub appendable: bool,
}

impl<Fs: UniversalReadFsAsync> TrackerLookup<Fs> {
    /// See [`LookupSegment::writer_state`](super::LookupSegment::writer_state).
    pub fn writer_state(&self) -> WriterIdTrackerState {
        WriterIdTrackerState::of(&self.id_tracker)
    }
}
