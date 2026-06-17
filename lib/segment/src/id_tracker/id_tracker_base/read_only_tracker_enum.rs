use std::path::Path;

use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::read_only::ReadOnlyImmutableIdTracker;
use crate::id_tracker::mutable_id_tracker::read_only::{
    LiveReloadResult, ReadOnlyAppendableIdTracker,
};
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum};
use crate::types::{PointIdType, SeqNumberType};

pub enum ReadOnlyIdTrackerEnum<S: UniversalRead> {
    Appendable(ReadOnlyAppendableIdTracker<S>),
    Immutable(ReadOnlyImmutableIdTracker<S>),
}

impl<S: UniversalRead> ReadOnlyIdTrackerEnum<S> {
    /// Open the read-only ID tracker, mirroring the writable `create_segment_id_tracker` selection.
    pub fn open(
        fs: &S::Fs,
        segment_path: &Path,
        use_appendable: bool,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<Self> {
        if use_appendable {
            Ok(Self::Appendable(ReadOnlyAppendableIdTracker::open(
                fs,
                segment_path,
                deferred_internal_id,
            )?))
        } else {
            Ok(Self::Immutable(ReadOnlyImmutableIdTracker::open(
                fs,
                segment_path,
            )?))
        }
    }

    /// Reload externally-applied changes, dispatching to the active variant.
    pub fn live_reload(&mut self) -> OperationResult<LiveReloadResult> {
        match self {
            Self::Appendable(id_tracker) => id_tracker.live_reload(),
            Self::Immutable(id_tracker) => id_tracker.live_reload(),
        }
    }
}

impl<S: UniversalRead> IdTrackerRead for ReadOnlyIdTrackerEnum<S> {
    fn point_mappings(&self) -> PointMappingsRefEnum<'_> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.point_mappings(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.point_mappings(),
        }
    }

    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                id_tracker.internal_version(internal_id)
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                id_tracker.internal_version(internal_id)
            }
        }
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        deferred_behavior: common::types::DeferredBehavior,
    ) -> Option<PointOffsetType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior)
            }
            ReadOnlyIdTrackerEnum::Immutable(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior)
            }
        }
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.external_id(internal_id),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.external_id(internal_id),
        }
    }

    fn total_point_count(&self) -> usize {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.total_point_count(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.total_point_count(),
        }
    }

    fn available_point_count(&self) -> usize {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.available_point_count(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.available_point_count(),
        }
    }

    fn deleted_point_count(&self) -> usize {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.deleted_point_count(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.deleted_point_count(),
        }
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.deleted_point_bitslice(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.deleted_point_bitslice(),
        }
    }

    fn is_deleted_point(&self, internal_id: PointOffsetType) -> bool {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => {
                id_tracker.is_deleted_point(internal_id)
            }
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => {
                id_tracker.is_deleted_point(internal_id)
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.name(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.name(),
        }
    }

    fn iter_internal_versions(
        &self,
    ) -> Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.iter_internal_versions(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.iter_internal_versions(),
        }
    }

    fn deferred_internal_id(&self) -> Option<PointOffsetType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.deferred_internal_id(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.deferred_internal_id(),
        }
    }

    fn deferred_deleted_count(&self) -> usize {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.deferred_deleted_count(),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.deferred_deleted_count(),
        }
    }
}
