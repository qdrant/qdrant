use std::collections::HashMap;
use std::path::Path;

use common::ambient;
use common::reason::Reason;
use common::universal_io::{MmapFs, Populate, UniversalReadFs};
use segment::common::operation_error::OperationResult;
use segment::id_tracker::IdTrackerRead as _;
use segment::id_tracker::delete_versions::DeleteGating;
use segment::id_tracker::id_tracker_base::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use segment::types::{PointIdType, SeqNumberType};

pub struct ReadOnlyIdTracker<Fs: UniversalReadFs> {
    fs: Fs,
    tracker: ReadOnlyIdTrackerEnum<Fs::File>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PointCounts {
    pub total: usize,
    pub available: usize,
    pub deleted: usize,
}

impl ReadOnlyIdTracker<MmapFs> {
    pub fn open_mmap(segment_path: &Path) -> OperationResult<Self> {
        Self::open(MmapFs, segment_path)
    }
}

impl<Fs: UniversalReadFs> ReadOnlyIdTracker<Fs> {
    pub fn open(fs: Fs, segment_path: &Path) -> OperationResult<Self> {
        let _scope = ambient::unmeasured_guard(Reason::EDGE_UNMEASURED);
        let tracker = ReadOnlyIdTrackerEnum::detect_and_load(
            &fs,
            segment_path,
            None,
            None,
            Populate::Blocking,
            DeleteGating::Immediate,
        )?;
        Ok(Self { fs, tracker })
    }

    pub fn point_counts(&self) -> PointCounts {
        let _scope = ambient::unmeasured_guard(Reason::EDGE_UNMEASURED);
        PointCounts {
            total: self.tracker.total_point_count(),
            available: self.tracker.available_point_count(),
            deleted: self.tracker.deleted_point_count(),
        }
    }

    pub fn point_versions(&self) -> HashMap<PointIdType, SeqNumberType> {
        let _scope = ambient::unmeasured_guard(Reason::EDGE_UNMEASURED);
        self.tracker
            .point_mappings()
            .iter_from(None)
            .map(|(external, internal)| {
                let version = self.tracker.internal_version(internal).unwrap_or(0);
                (external, version)
            })
            .collect()
    }

    pub fn live_reload(&mut self) -> OperationResult<bool> {
        let _scope = ambient::unmeasured_guard(Reason::EDGE_UNMEASURED);
        let delta = self.tracker.live_reload(&self.fs, None)?;
        self.tracker.publish_staged();
        Ok(!delta.is_empty())
    }
}
