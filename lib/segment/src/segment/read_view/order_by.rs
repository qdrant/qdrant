use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::{DeferredBehavior, PointOffsetType};
use itertools::Either;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::order_by::{Direction, OrderBy, OrderValue};
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::numeric_index::NumericFieldIndexRead;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::spaces::tools::{peek_top_largest_iterable, peek_top_smallest_iterable};
use crate::types::{Filter, PointIdType};

fn effective_order_value(
    values: impl IntoIterator<Item = OrderValue>,
    direction: Direction,
    start_from: &OrderValue,
) -> Option<OrderValue> {
    let filtered: Vec<_> = values
        .into_iter()
        .filter(|value| match direction {
            Direction::Asc => value >= start_from,
            Direction::Desc => value <= start_from,
        })
        .collect();
    match direction {
        Direction::Asc => filtered.into_iter().min(),
        Direction::Desc => filtered.into_iter().max(),
    }
}

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    pub fn filtered_read_by_index_ordered(
        &self,
        order_by: &OrderBy,
        limit: Option<usize>,
        condition: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>> {
        let numeric_index = self
            .payload_index
            .numeric_index_for(&order_by.key)
            .ok_or_else(|| OperationError::MissingRangeIndexForOrderBy {
                key: order_by.key.to_string(),
            })?;

        let cardinality_estimation = self
            .payload_index
            .estimate_cardinality(condition, hw_counter)?;

        let start_from = order_by.start_from();

        let values_ids_iterator = self
            .payload_index
            .iter_filtered_points(
                condition,
                &cardinality_estimation,
                hw_counter,
                is_stopped,
                deferred_behavior,
            )
            .filter_map(|internal_id| {
                let ordering_value = effective_order_value(
                    numeric_index.get_ordering_values(internal_id),
                    order_by.direction(),
                    &start_from,
                )?;
                self.id_tracker
                    .external_id(internal_id)
                    .map(|external_id| (ordering_value, external_id))
            });

        Ok(match order_by.direction() {
            Direction::Asc => {
                let mut page = match limit {
                    Some(limit) => peek_top_smallest_iterable(values_ids_iterator, limit),
                    None => values_ids_iterator.collect(),
                };
                page.sort_unstable_by_key(|(value, _)| *value);
                page
            }
            Direction::Desc => {
                let mut page = match limit {
                    Some(limit) => peek_top_largest_iterable(values_ids_iterator, limit),
                    None => values_ids_iterator.collect(),
                };
                page.sort_unstable_by_key(|(value, _)| Reverse(*value));
                page
            }
        })
    }

    pub fn filtered_read_by_value_stream(
        &self,
        order_by: &OrderBy,
        limit: Option<usize>,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>> {
        let numeric_index = self
            .payload_index
            .numeric_index_for(&order_by.key)
            .ok_or_else(|| OperationError::MissingRangeIndexForOrderBy {
                key: order_by.key.to_string(),
            })?;

        let range_iter = numeric_index
            .stream_range(&order_by.as_range())?
            // We can't early-stop the iterator for deferred points because the items are sorted
            // lexicographically by type `(T, internalID)`.
            .filter(|&(_, internal_id)| {
                deferred_behavior.with_deferred_points()
                    || internal_id < self.deferred_internal_id().unwrap_or(PointOffsetType::MAX)
            });

        let directed_range_iter = match order_by.direction() {
            Direction::Asc => Either::Left(range_iter),
            Direction::Desc => Either::Right(range_iter.rev()),
        };

        let filtered_iter = match filter {
            None => Either::Left(directed_range_iter),
            Some(filter) => {
                let filter_context = self.payload_index.filter_context(filter, hw_counter)?;

                Either::Right(
                    directed_range_iter
                        .filter(move |(_, internal_id)| filter_context.check(*internal_id)),
                )
            }
        };

        let mut best_value_per_point: HashMap<PointOffsetType, OrderValue> = HashMap::new();
        for (value, internal_id) in filtered_iter.stop_if(is_stopped) {
            match order_by.direction() {
                Direction::Asc => {
                    best_value_per_point
                        .entry(internal_id)
                        .and_modify(|current| {
                            if value < *current {
                                *current = value;
                            }
                        })
                        .or_insert(value);
                }
                Direction::Desc => {
                    best_value_per_point
                        .entry(internal_id)
                        .and_modify(|current| {
                            if value > *current {
                                *current = value;
                            }
                        })
                        .or_insert(value);
                }
            }
        }

        let mut reads: Vec<_> = best_value_per_point
            .into_iter()
            .filter_map(|(internal_id, value)| {
                self.id_tracker
                    .external_id(internal_id)
                    .map(|external_id| (value, external_id))
            })
            .collect();

        match order_by.direction() {
            Direction::Asc => reads.sort_unstable_by_key(|(value, _)| *value),
            Direction::Desc => reads.sort_unstable_by_key(|(value, _)| Reverse(*value)),
        }

        if let Some(limit) = limit {
            reads.truncate(limit);
        }

        Ok(reads)
    }

    pub fn read_ordered_filtered<'a>(
        &'a self,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        order_by: &'a OrderBy,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>> {
        match filter {
            None => self.filtered_read_by_value_stream(
                order_by,
                limit,
                None,
                is_stopped,
                hw_counter,
                deferred_behavior,
            ),
            Some(filter) => {
                if self.should_pre_filter(filter, limit, hw_counter)? {
                    self.filtered_read_by_index_ordered(
                        order_by,
                        limit,
                        filter,
                        is_stopped,
                        hw_counter,
                        deferred_behavior,
                    )
                } else {
                    self.filtered_read_by_value_stream(
                        order_by,
                        limit,
                        Some(filter),
                        is_stopped,
                        hw_counter,
                        deferred_behavior,
                    )
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ordered_float::OrderedFloat;

    use super::*;

    /// Regression test for <https://github.com/qdrant/qdrant/issues/9192>
    #[test]
    fn effective_order_value_picks_single_value_per_point() {
        let start_from = OrderValue::Float(OrderedFloat(0.0));
        let values = [
            OrderValue::Float(OrderedFloat(1.0)),
            OrderValue::Float(OrderedFloat(3.0)),
        ];

        let asc = effective_order_value(values, Direction::Asc, &start_from).unwrap();
        assert_eq!(asc, OrderValue::Float(OrderedFloat(1.0)));

        let desc = effective_order_value(values, Direction::Desc, &start_from).unwrap();
        assert_eq!(desc, OrderValue::Float(OrderedFloat(3.0)));
    }
}
