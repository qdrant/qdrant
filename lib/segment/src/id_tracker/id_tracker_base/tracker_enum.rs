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

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            IdTrackerEnum::MutableIdTracker($pattern) => $result,
            IdTrackerEnum::ImmutableIdTracker($pattern) => $result,
            IdTrackerEnum::InMemoryIdTracker($pattern) => $result,
            IdTrackerEnum::DiskIdTracker($pattern) => $result,
        }
    };
}

impl IdTrackerRead for IdTrackerEnum {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        for_all!(self, id_tracker => id_tracker.internal_version(internal_id))
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        deferred_behavior: common::types::DeferredBehavior,
    ) -> Option<PointOffsetType> {
        for_all!(self, t => t.internal_id_with_behavior(external_id, deferred_behavior))
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        for_all!(self, id_tracker => id_tracker.external_id(internal_id))
    }

    fn internal_versions_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, SeqNumberType),
    ) -> OperationResult<()> {
        for_all!(self, t => t.internal_versions_batch(internal_ids, callback))
    }

    fn external_ids_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, PointIdType),
    ) -> OperationResult<()> {
        for_all!(self, t => t.external_ids_batch(internal_ids, callback))
    }

    fn resolve_external_ids(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        deferred_behavior: common::types::DeferredBehavior,
        callback: impl FnMut(PointIdType, PointOffsetType),
    ) -> OperationResult<()> {
        for_all!(self, t => t.resolve_external_ids(point_ids, deferred_behavior, callback))
    }

    type Backend = MmapFile;

    fn point_mappings(&self) -> PointMappingsRefEnum<'_, Self::Backend> {
        for_all!(self, id_tracker => id_tracker.point_mappings())
    }

    fn total_point_count(&self) -> usize {
        for_all!(self, id_tracker => id_tracker.total_point_count())
    }

    fn deleted_point_count(&self) -> usize {
        for_all!(self, id_tracker => id_tracker.deleted_point_count())
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        for_all!(self, id_tracker => id_tracker.deleted_point_bitslice())
    }

    fn is_deleted_point(&self, internal_id: PointOffsetType) -> bool {
        for_all!(self, id_tracker => id_tracker.is_deleted_point(internal_id))
    }

    fn name(&self) -> &'static str {
        for_all!(self, id_tracker => id_tracker.name())
    }

    fn iter_internal_versions(
        &self,
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        for_all!(self, id_tracker => id_tracker.iter_internal_versions())
    }

    fn deferred_internal_id(&self) -> Option<PointOffsetType> {
        for_all!(self, id_tracker => id_tracker.deferred_internal_id())
    }

    fn deferred_deleted_count(&self) -> usize {
        for_all!(self, id_tracker => id_tracker.deferred_deleted_count())
    }
}

impl IdTracker for IdTrackerEnum {
    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        for_all!(self, id_tracker => id_tracker.set_internal_version(internal_id, version))
    }

    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        for_all!(self, id_tracker => id_tracker.set_link(external_id, internal_id))
    }

    fn drop(
        &mut self,
        external_id: PointIdType,
        version: Option<SeqNumberType>,
    ) -> OperationResult<()> {
        for_all!(self, id_tracker => id_tracker.drop(external_id, version))
    }

    fn drop_internal(
        &mut self,
        internal_id: PointOffsetType,
        version: Option<SeqNumberType>,
    ) -> OperationResult<()> {
        for_all!(self, id_tracker => id_tracker.drop_internal(internal_id, version))
    }

    fn mapping_flusher(&self) -> Flusher {
        for_all!(self, id_tracker => id_tracker.mapping_flusher())
    }

    fn versions_flusher(&self) -> Flusher {
        for_all!(self, id_tracker => id_tracker.versions_flusher())
    }

    fn fix_inconsistencies(&mut self) -> OperationResult<Vec<PointOffsetType>> {
        for_all!(self, id_tracker => id_tracker.fix_inconsistencies())
    }

    fn files(&self) -> Vec<PathBuf> {
        for_all!(self, id_tracker => id_tracker.files())
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        for_all!(self, id_tracker => id_tracker.immutable_files())
    }

    fn clear_cache(&self) -> OperationResult<()> {
        for_all!(self, id_tracker => id_tracker.clear_cache())
    }
}
