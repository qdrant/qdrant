use common::bitvec::BitSlice;
use common::types::PointOffsetType;

use crate::id_tracker::immutable_id_tracker::read_only::ReadOnlyImmutableIdTracker;
use crate::id_tracker::mutable_id_tracker::read_only::ReadOnlyAppendableIdTracker;
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum};
use crate::types::{PointIdType, SeqNumberType};

#[allow(clippy::large_enum_variant)]
pub enum ReadOnlyIdTrackerEnum {
    Appendable(ReadOnlyAppendableIdTracker),
    Immutable(ReadOnlyImmutableIdTracker),
}

impl IdTrackerRead for ReadOnlyIdTrackerEnum {
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

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        match self {
            ReadOnlyIdTrackerEnum::Appendable(id_tracker) => id_tracker.internal_id(external_id),
            ReadOnlyIdTrackerEnum::Immutable(id_tracker) => id_tracker.internal_id(external_id),
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
}
