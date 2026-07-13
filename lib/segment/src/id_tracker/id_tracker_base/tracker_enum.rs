use std::path::PathBuf;

use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;

use super::point_mappings_ref::PointMappingsRefEnum;
use super::trait_def::{IdTracker, IdTrackerRead};
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::DiskIdTracker;
use crate::id_tracker::immutable_id_tracker::ImmutableIdTracker;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::mutable_id_tracker::MutableIdTracker;
use crate::types::{PointIdType, SeqNumberType};

#[derive(Debug)]
pub enum IdTrackerEnum {
    MutableIdTracker(MutableIdTracker),
    ImmutableIdTracker(ImmutableIdTracker<MmapFile>),
    InMemoryIdTracker(InMemoryIdTracker),
    DiskIdTracker(DiskIdTracker<MmapFile>),
}

impl IdTrackerRead for IdTrackerEnum {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.internal_version(internal_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => {
                id_tracker.internal_version(internal_id)
            }
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => {
                id_tracker.internal_version(internal_id)
            }
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.internal_version(internal_id),
        }
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        deferred_behavior: common::types::DeferredBehavior,
    ) -> Option<PointOffsetType> {
        match self {
            IdTrackerEnum::MutableIdTracker(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior)
            }
            IdTrackerEnum::ImmutableIdTracker(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior)
            }
            IdTrackerEnum::InMemoryIdTracker(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior)
            }
            IdTrackerEnum::DiskIdTracker(t) => {
                t.internal_id_with_behavior(external_id, deferred_behavior)
            }
        }
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.external_id(internal_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.external_id(internal_id),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.external_id(internal_id),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.external_id(internal_id),
        }
    }

    fn internal_versions_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
    ) -> Vec<Option<SeqNumberType>> {
        match self {
            IdTrackerEnum::MutableIdTracker(t) => t.internal_versions_batch(internal_ids),
            IdTrackerEnum::ImmutableIdTracker(t) => t.internal_versions_batch(internal_ids),
            IdTrackerEnum::InMemoryIdTracker(t) => t.internal_versions_batch(internal_ids),
            IdTrackerEnum::DiskIdTracker(t) => t.internal_versions_batch(internal_ids),
        }
    }

    fn external_ids_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
    ) -> Vec<Option<PointIdType>> {
        match self {
            IdTrackerEnum::MutableIdTracker(t) => t.external_ids_batch(internal_ids),
            IdTrackerEnum::ImmutableIdTracker(t) => t.external_ids_batch(internal_ids),
            IdTrackerEnum::InMemoryIdTracker(t) => t.external_ids_batch(internal_ids),
            IdTrackerEnum::DiskIdTracker(t) => t.external_ids_batch(internal_ids),
        }
    }

    fn resolve_external_ids(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        deferred_behavior: common::types::DeferredBehavior,
        callback: impl FnMut(PointIdType, PointOffsetType),
    ) {
        match self {
            IdTrackerEnum::MutableIdTracker(t) => {
                t.resolve_external_ids(point_ids, deferred_behavior, callback)
            }
            IdTrackerEnum::ImmutableIdTracker(t) => {
                t.resolve_external_ids(point_ids, deferred_behavior, callback)
            }
            IdTrackerEnum::InMemoryIdTracker(t) => {
                t.resolve_external_ids(point_ids, deferred_behavior, callback)
            }
            IdTrackerEnum::DiskIdTracker(t) => {
                t.resolve_external_ids(point_ids, deferred_behavior, callback)
            }
        }
    }

    type Backend = MmapFile;

    fn point_mappings(&self) -> PointMappingsRefEnum<'_, Self::Backend> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.point_mappings(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.point_mappings(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.point_mappings(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.point_mappings(),
        }
    }

    fn total_point_count(&self) -> usize {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.total_point_count(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.total_point_count(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.total_point_count(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.total_point_count(),
        }
    }

    fn deleted_point_count(&self) -> usize {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.deleted_point_count(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.deleted_point_count(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.deleted_point_count(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.deleted_point_count(),
        }
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.deleted_point_bitslice(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.deleted_point_bitslice(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.deleted_point_bitslice(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.deleted_point_bitslice(),
        }
    }

    fn is_deleted_point(&self, internal_id: PointOffsetType) -> bool {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.is_deleted_point(internal_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => {
                id_tracker.is_deleted_point(internal_id)
            }
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => {
                id_tracker.is_deleted_point(internal_id)
            }
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.is_deleted_point(internal_id),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.name(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.name(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.name(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.name(),
        }
    }

    fn iter_internal_versions(
        &self,
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.iter_internal_versions(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.iter_internal_versions(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.iter_internal_versions(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.iter_internal_versions(),
        }
    }

    fn deferred_internal_id(&self) -> Option<PointOffsetType> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.deferred_internal_id(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.deferred_internal_id(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.deferred_internal_id(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.deferred_internal_id(),
        }
    }

    fn deferred_deleted_count(&self) -> usize {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.deferred_deleted_count(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.deferred_deleted_count(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.deferred_deleted_count(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.deferred_deleted_count(),
        }
    }
}

impl IdTracker for IdTrackerEnum {
    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => {
                id_tracker.set_internal_version(internal_id, version)
            }
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => {
                id_tracker.set_internal_version(internal_id, version)
            }
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => {
                id_tracker.set_internal_version(internal_id, version)
            }
            IdTrackerEnum::DiskIdTracker(id_tracker) => {
                id_tracker.set_internal_version(internal_id, version)
            }
        }
    }

    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => {
                id_tracker.set_link(external_id, internal_id)
            }
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => {
                id_tracker.set_link(external_id, internal_id)
            }
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => {
                id_tracker.set_link(external_id, internal_id)
            }
            IdTrackerEnum::DiskIdTracker(id_tracker) => {
                id_tracker.set_link(external_id, internal_id)
            }
        }
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.drop(external_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.drop(external_id),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.drop(external_id),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.drop(external_id),
        }
    }

    fn drop_internal(&mut self, internal_id: PointOffsetType) -> OperationResult<()> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.drop_internal(internal_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.drop_internal(internal_id),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.drop_internal(internal_id),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.drop_internal(internal_id),
        }
    }

    fn mapping_flusher(&self) -> Flusher {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.mapping_flusher(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.mapping_flusher(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.mapping_flusher(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.mapping_flusher(),
        }
    }

    fn versions_flusher(&self) -> Flusher {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.versions_flusher(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.versions_flusher(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.versions_flusher(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.versions_flusher(),
        }
    }

    fn fix_inconsistencies(&mut self) -> OperationResult<Vec<PointOffsetType>> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.fix_inconsistencies(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.fix_inconsistencies(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.fix_inconsistencies(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.fix_inconsistencies(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.files(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.files(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.files(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.files(),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.immutable_files(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.immutable_files(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.immutable_files(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.immutable_files(),
        }
    }

    fn clear_cache(&self) -> OperationResult<()> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.clear_cache(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.clear_cache(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.clear_cache(),
            IdTrackerEnum::DiskIdTracker(id_tracker) => id_tracker.clear_cache(),
        }
    }
}
