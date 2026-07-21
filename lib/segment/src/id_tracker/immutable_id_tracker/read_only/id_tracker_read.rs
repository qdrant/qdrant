use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::read_only::ReadOnlyImmutableIdTracker;
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum, default_internal_versions_batch};
use crate::types::{PointIdType, SeqNumberType};

impl<S: UniversalRead> IdTrackerRead for ReadOnlyImmutableIdTracker<S> {
    type Backend = S;

    fn point_mappings(&self) -> PointMappingsRefEnum<'_, Self::Backend> {
        PointMappingsRefEnum::Compressed(&self.mappings)
    }

    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        self.internal_to_version.get(internal_id)
    }

    fn internal_versions_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, SeqNumberType),
    ) -> OperationResult<()> {
        default_internal_versions_batch(self, internal_ids, callback)
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        _deferred_behavior: common::types::DeferredBehavior,
    ) -> Option<PointOffsetType> {
        // Immutable mappings never carry deferred heads; behavior is moot.
        self.mappings.internal_id(&external_id)
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        self.mappings.external_id(internal_id)
    }

    fn total_point_count(&self) -> usize {
        self.mappings.total_point_count()
    }

    fn available_point_count(&self) -> usize {
        self.mappings.available_point_count()
    }

    fn deleted_point_count(&self) -> usize {
        self.total_point_count() - self.available_point_count()
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        self.mappings.deleted()
    }

    fn is_deleted_point(&self, internal_id: PointOffsetType) -> bool {
        self.mappings.is_deleted_point(internal_id)
    }

    fn name(&self) -> &'static str {
        "read-only immutable id tracker"
    }

    fn iter_internal_versions(
        &self,
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        Ok(Box::new(self.internal_to_version.iter()))
    }
}
