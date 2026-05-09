use std::cmp::Reverse;
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

        let effective_deferred_id = deferred_behavior.apply(self.deferred_internal_id());

        let point_mappings = self.id_tracker.point_mappings();
        let values_ids_iterator = self
            .payload_index
            .iter_filtered_points(
                condition,
                self.id_tracker,
                &point_mappings,
                &cardinality_estimation,
                hw_counter,
                is_stopped,
                effective_deferred_id,
            )?
            .flat_map(|internal_id| {
                // Repeat a point for as many values as it has.
                numeric_index
                    .get_ordering_values(internal_id)
                    // But only those which start from `start_from`.
                    .filter(|value| match order_by.direction() {
                        Direction::Asc => value >= &start_from,
                        Direction::Desc => value <= &start_from,
                    })
                    .map(move |ordering_value| (ordering_value, internal_id))
            })
            .filter_map(|(value, internal_id)| {
                self.id_tracker
                    .external_id(internal_id)
                    .map(|external_id| (value, external_id))
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
                deferred_behavior.include_all_points()
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

        let reads = filtered_iter
            .stop_if(is_stopped)
            .filter_map(|(value, internal_id)| {
                self.id_tracker
                    .external_id(internal_id)
                    .map(|external_id| (value, external_id))
            })
            .take(limit.unwrap_or(usize::MAX))
            .collect();
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
