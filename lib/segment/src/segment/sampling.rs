use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use rand::seq::{IteratorRandom, SliceRandom};

use super::Segment;
use crate::index::PayloadIndex;
use crate::types::{Filter, PointIdType};

impl Segment {
    pub(super) fn filtered_read_by_index_shuffled(
        &self,
        limit: usize,
        condition: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType> {
        let payload_index = self.payload_index.borrow();
        let id_tracker = self.id_tracker.borrow();

        let cardinality_estimation = payload_index.estimate_cardinality(condition, hw_counter);
        let ids_iterator = payload_index
            .iter_filtered_points(condition, &*id_tracker, &cardinality_estimation, hw_counter)
            .check_stop(|| is_stopped.load(Ordering::Relaxed))
            .filter_map(|internal_id| id_tracker.external_id(internal_id));

        let mut rng = rand::rng();
        let mut shuffled = ids_iterator.choose_multiple(&mut rng, limit);
        shuffled.shuffle(&mut rng);

        shuffled
    }

    pub fn filtered_read_by_random_stream(
        &self,
        limit: usize,
        condition: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType> {
        let payload_index = self.payload_index.borrow();
        let filter_context = payload_index.filter_context(condition, hw_counter);
        self.id_tracker
            .borrow()
            .iter_random()
            .check_stop(|| is_stopped.load(Ordering::Relaxed))
            .filter(move |(_, internal_id)| filter_context.check(*internal_id))
            .map(|(external_id, _)| external_id)
            .take(limit)
            .collect()
    }

    pub(super) fn read_by_random_id(&self, limit: usize) -> Vec<PointIdType> {
        self.id_tracker
            .borrow()
            .iter_random()
            .map(|x| x.0)
            .take(limit)
            .collect()
    }
}
