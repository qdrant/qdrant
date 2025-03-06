use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use itertools::Either;

use super::Segment;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::order_by::{Direction, OrderBy, OrderValue};
use crate::index::PayloadIndex;
use crate::index::field_index::numeric_index::StreamRange;
use crate::spaces::tools::{peek_top_largest_iterable, peek_top_smallest_iterable};
use crate::types::{Filter, PointIdType};

impl Segment {
    pub fn filtered_read_by_index_ordered(
        &self,
        order_by: &OrderBy,
        limit: Option<usize>,
        condition: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>> {
        let payload_index = self.payload_index.borrow();
        let id_tracker = self.id_tracker.borrow();

        let numeric_index = payload_index
            .field_indexes
            .get(&order_by.key)
            .and_then(|indexes| indexes.iter().find_map(|index| index.as_numeric()))
            .ok_or_else(|| OperationError::MissingRangeIndexForOrderBy {
                key: order_by.key.to_string(),
            })?;

        let cardinality_estimation = payload_index.estimate_cardinality(condition, hw_counter);

        let start_from = order_by.start_from();

        let values_ids_iterator = payload_index
            .iter_filtered_points(condition, &*id_tracker, &cardinality_estimation, hw_counter)
            .check_stop(|| is_stopped.load(Ordering::Relaxed))
            .flat_map(|internal_id| {
                // Repeat a point for as many values as it has
                numeric_index
                    .get_ordering_values(internal_id)
                    // But only those which start from `start_from`
                    .filter(|value| match order_by.direction() {
                        Direction::Asc => value >= &start_from,
                        Direction::Desc => value <= &start_from,
                    })
                    .map(move |ordering_value| (ordering_value, internal_id))
            })
            .filter_map(|(value, internal_id)| {
                id_tracker
                    .external_id(internal_id)
                    .map(|external_id| (value, external_id))
            });

        let page = match order_by.direction() {
            Direction::Asc => {
                let mut page = match limit {
                    Some(limit) => peek_top_smallest_iterable(values_ids_iterator, limit),
                    None => values_ids_iterator.collect(),
                };
                page.sort_unstable_by(|(value_a, _), (value_b, _)| value_a.cmp(value_b));
                page
            }
            Direction::Desc => {
                let mut page = match limit {
                    Some(limit) => peek_top_largest_iterable(values_ids_iterator, limit),
                    None => values_ids_iterator.collect(),
                };
                page.sort_unstable_by(|(value_a, _), (value_b, _)| value_b.cmp(value_a));
                page
            }
        };

        Ok(page)
    }

    pub fn filtered_read_by_value_stream(
        &self,
        order_by: &OrderBy,
        limit: Option<usize>,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>> {
        let payload_index = self.payload_index.borrow();

        let numeric_index = payload_index
            .field_indexes
            .get(&order_by.key)
            .and_then(|indexes| indexes.iter().find_map(|index| index.as_numeric()))
            .ok_or_else(|| OperationError::MissingRangeIndexForOrderBy {
                key: order_by.key.to_string(),
            })?;

        let range_iter = numeric_index.stream_range(&order_by.as_range());

        let directed_range_iter = match order_by.direction() {
            Direction::Asc => Either::Left(range_iter),
            Direction::Desc => Either::Right(range_iter.rev()),
        };

        let id_tracker = self.id_tracker.borrow();

        let filtered_iter = match filter {
            None => Either::Left(directed_range_iter),
            Some(filter) => {
                let filter_context = payload_index.filter_context(filter, hw_counter);

                Either::Right(
                    directed_range_iter
                        .filter(move |(_, internal_id)| filter_context.check(*internal_id)),
                )
            }
        };

        let reads = filtered_iter
            .check_stop(|| is_stopped.load(Ordering::Relaxed))
            .filter_map(|(value, internal_id)| {
                id_tracker
                    .external_id(internal_id)
                    .map(|external_id| (value, external_id))
            })
            .take(limit.unwrap_or(usize::MAX))
            .collect();
        Ok(reads)
    }
}
