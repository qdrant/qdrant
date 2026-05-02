use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::PointIdType;

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    pub(crate) fn lookup_internal_id(
        &self,
        point_id: PointIdType,
    ) -> OperationResult<PointOffsetType> {
        self.id_tracker
            .internal_id(point_id)
            .ok_or(OperationError::PointIdError {
                missed_point_id: point_id,
            })
    }

    fn deferred_internal_id(&self) -> Option<PointOffsetType> {
        self.deferred_point_status.map(|s| s.deferred_internal_id)
    }

    fn deferred_deleted_count(&self) -> Option<usize> {
        self.deferred_point_status.map(|s| s.deferred_deleted_count)
    }

    pub fn deferred_point_count(&self) -> usize {
        match self.deferred_internal_id() {
            Some(internal_id) => self
                .id_tracker
                .total_point_count()
                .saturating_sub(internal_id as usize)
                .saturating_sub(self.deferred_deleted_count().unwrap_or_default()),
            None => 0,
        }
    }

    pub fn has_deferred_points(&self) -> bool {
        self.deferred_internal_id().is_some_and(|deferred_from| {
            self.id_tracker.total_point_count() > deferred_from as usize
        })
    }

    pub fn point_is_deferred(&self, point_id: PointIdType) -> bool {
        if let Some(deferred_from) = self.deferred_internal_id()
            && let Some(internal_id) = self.id_tracker.internal_id(point_id)
        {
            return self.appendable_flag && internal_id >= deferred_from;
        };
        false
    }

    pub fn deferred_point_ids(&self) -> Vec<PointIdType> {
        let Some(deferred_from) = self.deferred_internal_id() else {
            return vec![];
        };
        if self.deferred_point_count() == 0 {
            return vec![];
        }

        let mappings = self.id_tracker.point_mappings();
        mappings
            .iter_internal()
            .skip_while(|&internal_id| internal_id < deferred_from)
            .filter_map(|internal_id| self.id_tracker.external_id(internal_id))
            .collect()
    }

    pub fn available_point_count_without_deferred(&self) -> usize {
        self.id_tracker
            .available_point_count()
            .saturating_sub(self.deferred_point_count())
    }
}
