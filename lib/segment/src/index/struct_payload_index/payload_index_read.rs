use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::either_variant::EitherVariant;
use common::iterator_ext::IteratorExt;
use common::types::{PointOffsetType, ScoreType};

use super::StructPayloadIndex;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum};
use crate::index::PayloadIndexRead;
use crate::index::field_index::numeric_index::NumericFieldIndexRead;
use crate::index::field_index::{CardinalityEstimation, FacetIndex, PayloadBlockCondition};
use crate::index::query_estimator::estimate_filter;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::query_optimization::rescore_formula::FormulaScorer;
use crate::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use crate::json_path::JsonPath;
use crate::payload_storage::{FilterContext, PayloadStorageRead};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    Condition, Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef,
};

impl PayloadIndexRead for StructPayloadIndex {
    fn indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        self.config().indices.to_schemas()
    }

    fn estimate_cardinality(
        &self,
        query: &Filter,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        let available_points = self.available_point_count();
        let estimator =
            |condition: &Condition| self.condition_cardinality(condition, None, hw_counter);
        estimate_filter(&estimator, query, available_points)
    }

    fn estimate_nested_cardinality(
        &self,
        query: &Filter,
        nested_path: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        let available_points = self.available_point_count();
        let estimator = |condition: &Condition| {
            self.condition_cardinality(condition, Some(nested_path), hw_counter)
        };
        estimate_filter(&estimator, query, available_points)
    }

    fn query_points(
        &self,
        filter: &Filter,
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<Vec<PointOffsetType>> {
        // Assume query is already estimated to be small enough so we can iterate over all matched ids
        let query_cardinality = self.estimate_cardinality(filter, hw_counter)?;
        let id_tracker = self.id_tracker.borrow();
        let point_mappings = id_tracker.point_mappings();
        let result = self
            .iter_filtered_points(
                filter,
                &*id_tracker,
                &point_mappings,
                &query_cardinality,
                hw_counter,
                is_stopped,
                deferred_internal_id,
            )?
            .collect();
        Ok(result)
    }

    fn numeric_index_for(&self, key: &PayloadKeyType) -> Option<impl NumericFieldIndexRead + '_> {
        self.field_indexes
            .get(key)
            .and_then(|indexes| indexes.iter().find_map(|index| index.as_numeric()))
    }

    fn get_telemetry_data(&self) -> Vec<PayloadIndexTelemetry> {
        self.field_indexes
            .iter()
            .flat_map(|(name, field)| -> Vec<PayloadIndexTelemetry> {
                field
                    .iter()
                    .map(|field| field.get_telemetry_data().set_name(name.to_string()))
                    .collect()
            })
            .collect()
    }

    fn facet_index_for(&self, key: &JsonPath) -> Option<impl FacetIndex + '_> {
        self.field_indexes
            .get(key)
            .and_then(|index| index.iter().find_map(|index| index.as_facet_index()))
    }

    fn formula_scorer<'q>(
        &'q self,
        parsed_formula: &'q ParsedFormula,
        prefetches_scores: &'q [AHashMap<PointOffsetType, ScoreType>],
        hw_counter: &'q HardwareCounterCell,
    ) -> OperationResult<FormulaScorer<'q>> {
        let ParsedFormula {
            payload_vars,
            conditions,
            defaults,
            formula,
        } = parsed_formula;

        let payload_retrievers = self.retrievers_map(payload_vars.clone(), hw_counter);

        let payload_provider = PayloadProvider::new(self.payload.clone());
        let total = self.available_point_count();
        let condition_checkers = self
            .convert_conditions(conditions, payload_provider, total, hw_counter)?
            .into_iter()
            .map(|(checker, _estimation)| checker)
            .collect();

        Ok(FormulaScorer::new(
            formula.clone(),
            prefetches_scores,
            payload_retrievers,
            condition_checkers,
            defaults.clone(),
        ))
    }

    fn iter_filtered_points<'a, I: IdTrackerRead>(
        &'a self,
        filter: &'a Filter,
        id_tracker: &'a I,
        point_mappings: &'a PointMappingsRefEnum<'a>,
        query_cardinality: &'a CardinalityEstimation,
        hw_counter: &'a HardwareCounterCell,
        is_stopped: &'a AtomicBool,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'a> {
        if query_cardinality.primary_clauses.is_empty() {
            let full_scan_iterator = point_mappings.iter_internal_visible(deferred_internal_id);

            let struct_filtered_context = self.struct_filtered_context(filter, hw_counter)?;
            // Worst case: query expected to return few matches, but index can't be used
            let matched_points = full_scan_iterator
                .stop_if(is_stopped)
                .filter(move |i| struct_filtered_context.check(*i));

            Ok(EitherVariant::A(matched_points))
        } else {
            // CPU-optimized strategy here: points are made unique before applying other filters.
            let mut visited_list = self.visited_pool.get(id_tracker.total_point_count());

            // If even one iterator is None, we should replace the whole thing with
            // an iterator over all ids.
            let primary_clause_iterators: OperationResult<Option<Vec<_>>> = query_cardinality
                .primary_clauses
                .iter()
                .map(|clause| self.query_field(clause, hw_counter))
                .collect();

            if let Some(primary_iterators) = primary_clause_iterators? {
                let all_conditions_are_primary = filter
                    .iter_conditions()
                    .all(|condition| query_cardinality.is_primary(condition));

                let joined_primary_iterator = primary_iterators
                    .into_iter()
                    // Filter out deferred points.
                    // This iterator (and each primary iterator too) can yield items in non sorted order, depending on the type of index and primary condition.
                    .flatten()
                    .filter(move |&internal_id| {
                        internal_id < deferred_internal_id.unwrap_or(PointOffsetType::MAX)
                    })
                    .stop_if(is_stopped);

                return Ok(if all_conditions_are_primary {
                    // All conditions are primary clauses,
                    // We can avoid post-filtering
                    let iter = joined_primary_iterator
                        .filter(move |&id| !visited_list.check_and_update_visited(id));
                    EitherVariant::B(iter)
                } else {
                    // Some conditions are primary clauses, some are not
                    let struct_filtered_context =
                        self.struct_filtered_context(filter, hw_counter)?;
                    let iter = joined_primary_iterator.filter(move |&id| {
                        !visited_list.check_and_update_visited(id)
                            && struct_filtered_context.check(id)
                    });
                    EitherVariant::C(iter)
                });
            }

            // We can't use primary conditions, so we fall back to iterating over all ids
            // and applying full filter.
            let struct_filtered_context = self.struct_filtered_context(filter, hw_counter)?;

            let id_tracker_iterator = point_mappings.iter_internal_visible(deferred_internal_id);

            let iter = id_tracker_iterator
                .stop_if(is_stopped)
                .measure_hw_with_cell(hw_counter, size_of::<PointOffsetType>(), |i| {
                    i.cpu_counter()
                })
                .filter(move |&id| {
                    !visited_list.check_and_update_visited(id) && struct_filtered_context.check(id)
                });

            Ok(EitherVariant::D(iter))
        }
    }

    fn indexed_points(&self, field: PayloadKeyTypeRef) -> usize {
        self.field_indexes.get(field).map_or(0, |indexes| {
            // Assume that multiple field indexes are applied to the same data type,
            // so the points indexed with those indexes are the same.
            // We will return minimal number as a worst case, to highlight possible errors in the index early.
            indexes
                .iter()
                .map(|index| index.count_indexed_points())
                .min()
                .unwrap_or(0)
        })
    }

    fn filter_context<'a>(
        &'a self,
        filter: &'a Filter,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Box<dyn FilterContext + 'a>> {
        Ok(Box::new(self.struct_filtered_context(filter, hw_counter)?))
    }

    fn for_each_payload_block(
        &self,
        field: PayloadKeyTypeRef,
        threshold: usize,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        if let Some(indexes) = self.field_indexes.get(field) {
            let field_clone = field.to_owned();
            indexes.iter().try_for_each(|field_index| {
                field_index.for_each_payload_block(threshold, field_clone.clone(), f)
            })?;
        }
        Ok(())
    }

    fn get_payload(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        self.payload.borrow().get(point_id, hw_counter)
    }

    fn get_payload_sequential(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        self.payload.borrow().get_sequential(point_id, hw_counter)
    }
}
