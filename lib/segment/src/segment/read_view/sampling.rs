use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use rand::seq::{IteratorRandom, SliceRandom};

use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{Filter, PointIdType};

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    pub fn read_by_random_id(&self, limit: usize) -> Vec<PointIdType> {
        self.id_tracker
            .point_mappings()
            .iter_random_visible(self.deferred_internal_id())
            .map(|x| x.0)
            .take(limit)
            .collect()
    }

    pub fn filtered_read_by_index_shuffled(
        &self,
        limit: usize,
        condition: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<PointIdType>> {
        let point_mappings = self.id_tracker.point_mappings();
        let cardinality_estimation = self
            .payload_index
            .estimate_cardinality(condition, hw_counter)?;
        let ids_iterator = self
            .payload_index
            .iter_filtered_points(
                condition,
                self.id_tracker,
                &point_mappings,
                &cardinality_estimation,
                hw_counter,
                is_stopped,
                self.deferred_internal_id(),
            )?
            .filter_map(|internal_id| self.id_tracker.external_id(internal_id));

        let mut rng = rand::rng();
        let mut shuffled = ids_iterator.sample(&mut rng, limit);
        shuffled.shuffle(&mut rng);
        Ok(shuffled)
    }

    pub fn filtered_read_by_random_stream(
        &self,
        limit: usize,
        condition: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<PointIdType>> {
        let filter_context = self.payload_index.filter_context(condition, hw_counter)?;
        Ok(self
            .id_tracker
            .point_mappings()
            .iter_random_visible(self.deferred_internal_id())
            .stop_if(is_stopped)
            .filter(move |(_, internal_id)| filter_context.check(*internal_id))
            .map(|(external_id, _)| external_id)
            .take(limit)
            .collect())
    }

    pub fn read_random_filtered(
        &self,
        limit: usize,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<PointIdType>> {
        match filter {
            None => Ok(self.read_by_random_id(limit)),
            Some(condition) => {
                if self.should_pre_filter(condition, Some(limit), hw_counter)? {
                    self.filtered_read_by_index_shuffled(limit, condition, is_stopped, hw_counter)
                } else {
                    self.filtered_read_by_random_stream(limit, condition, is_stopped, hw_counter)
                }
            }
        }
    }
}
