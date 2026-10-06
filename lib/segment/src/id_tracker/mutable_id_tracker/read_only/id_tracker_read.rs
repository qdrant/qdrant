use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::mutable_id_tracker::read_only::ReadOnlyAppendableIdTracker;
use crate::id_tracker::{
    IdTrackerRead, PointMappingsRefEnum, default_external_ids_batch,
    default_internal_versions_batch,
};
use crate::types::{PointIdType, SeqNumberType};

impl<S: UniversalRead> IdTrackerRead for ReadOnlyAppendableIdTracker<S> {
    type Backend = S;

    fn point_mappings(
        &self,
        _hw_counter: &HardwareCounterCell,
    ) -> PointMappingsRefEnum<'_, Self::Backend> {
        PointMappingsRefEnum::Plain(&self.mappings)
    }

    fn internal_version(
        &self,
        internal_id: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> Option<SeqNumberType> {
        self.internal_to_version.get(internal_id as usize).copied()
    }

    fn internal_versions_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, SeqNumberType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        default_internal_versions_batch(self, internal_ids, callback, hw_counter)
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        deferred_behavior: common::types::DeferredBehavior,
        _hw_counter: &HardwareCounterCell,
    ) -> Option<PointOffsetType> {
        self.mappings
            .internal_id_with_behavior(&external_id, deferred_behavior)
    }

    fn external_id(
        &self,
        internal_id: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> Option<PointIdType> {
        self.mappings.external_id(internal_id)
    }

    fn external_ids_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, PointIdType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        default_external_ids_batch(self, internal_ids, callback, hw_counter)
    }

    fn total_point_count(&self) -> usize {
        self.mappings.total_point_count()
    }

    fn available_point_count(&self, _hw_counter: &HardwareCounterCell) -> usize {
        self.mappings.available_point_count()
    }

    fn deleted_point_count(&self, hw_counter: &HardwareCounterCell) -> usize {
        self.total_point_count() - self.available_point_count(hw_counter)
    }

    fn deleted_point_bitslice(&self, _hw_counter: &HardwareCounterCell) -> &BitSlice {
        self.mappings.deleted()
    }

    fn is_deleted_point(
        &self,
        internal_id: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> bool {
        self.mappings.is_deleted_point(internal_id)
    }

    fn name(&self) -> &'static str {
        "read-only appendable id tracker"
    }

    fn deferred_internal_id(&self) -> Option<PointOffsetType> {
        self.mappings.deferred_internal_id()
    }

    fn deferred_deleted_count(&self) -> usize {
        self.mappings.deferred_deleted_count()
    }

    fn iter_internal_versions(
        &self,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        Ok(Box::new(
            self.internal_to_version
                .iter()
                .enumerate()
                .map(|(i, version)| (i as PointOffsetType, *version)),
        ))
    }
}
