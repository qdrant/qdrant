use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::ambient::hw::{HwMeasurementIteratorExt, HwMetric};
use common::condition_checker::ConditionChecker;
use common::either_variant::EitherVariant;
use common::generic_consts::AccessPattern;
use common::iterator_ext::IteratorExt;
use common::types::{DeferredBehavior, PointOffsetType, ScoreType, ScoredPointOffset};

use super::StructPayloadIndexReadView;
use crate::common::operation_error::OperationResult;
use crate::data_types::query_context::{TextFieldStats, TextQueryContext};
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::full_text_index::Bm25Params;
use crate::index::field_index::numeric_index::NumericFieldIndexRead;
use crate::index::field_index::{
    CardinalityEstimation, FacetIndex, FieldIndexRead, PayloadBlockCondition,
};
use crate::index::query_estimator::estimate_filter;
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::query_optimization::rescore_formula::FormulaScorer;
use crate::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorageRead;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    Condition, Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef,
};
use crate::vector_storage::VectorStorageRead;

impl<'a, P, I, V, F> PayloadIndexRead for StructPayloadIndexReadView<'a, P, I, V, F>
where
    P: PayloadStorageRead,
    I: IdTrackerRead,
    V: VectorStorageRead,
    F: FieldIndexRead,
{
    fn indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        self.config.indices.to_schemas()
    }

    fn estimate_cardinality(&self, query: &Filter) -> OperationResult<CardinalityEstimation> {
        let available_points = self.available_point_count();
        let estimator = |condition: &Condition| {
            self.condition_cardinality(condition, None, DeferredBehavior::VisibleOnly)
        };
        estimate_filter(&estimator, query, available_points)
    }

    fn estimate_nested_cardinality(
        &self,
        query: &Filter,
        nested_path: &JsonPath,
    ) -> OperationResult<CardinalityEstimation> {
        let available_points = self.available_point_count();
        let estimator = |condition: &Condition| {
            self.condition_cardinality(condition, Some(nested_path), DeferredBehavior::VisibleOnly)
        };
        estimate_filter(&estimator, query, available_points)
    }

    fn query_points(
        &self,
        filter: &Filter,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<PointOffsetType>> {
        // Assume query is already estimated to be small enough so we can iterate over all matched ids
        let query_cardinality = self.estimate_cardinality(filter)?;
        let result = self
            .iter_filtered_points(
                filter,
                &query_cardinality,
                is_stopped,
                DeferredBehavior::VisibleOnly,
            )?
            .collect();
        Ok(result)
    }

    fn numeric_index_for(&self, key: &PayloadKeyType) -> Option<impl NumericFieldIndexRead + '_> {
        self.field_indexes
            .get(key)
            .and_then(|indexes| indexes.iter().find_map(|index| index.as_numeric()))
    }

    fn fill_text_statistics(
        &self,
        field: PayloadKeyTypeRef,
        stats: &mut TextFieldStats,
        is_stopped: &AtomicBool,
    ) -> OperationResult<()> {
        let Some(indexes) = self.field_indexes.get(field) else {
            return Ok(());
        };
        let corpus = stats
            .corpus
            .as_ref()
            .map(|corpus| self.query_points(corpus, is_stopped))
            .transpose()?;
        // At most one text index per field.
        for index in indexes {
            if index.fill_text_statistics(stats, corpus.as_deref(), is_stopped)? {
                break;
            }
        }
        Ok(())
    }

    fn score_bm25(
        &self,
        field: PayloadKeyTypeRef,
        terms: &[String],
        context: &TextQueryContext<'_>,
        params: Bm25Params,
        accept: &dyn Fn(PointOffsetType) -> bool,
        limit: usize,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let Some(indexes) = self.field_indexes.get(field) else {
            return Ok(Vec::new());
        };
        // At most one text index per field.
        for index in indexes {
            if let Some(scored) = index.score_bm25(terms, context, params, accept, limit)? {
                return Ok(scored);
            }
        }
        Ok(Vec::new())
    }

    fn get_telemetry_data(&self) -> OperationResult<Vec<PayloadIndexTelemetry>> {
        self.field_indexes
            .iter()
            .flat_map(|(name, field)| field.iter().map(move |field| (name, field)))
            .map(|(name, field)| Ok(field.get_telemetry_data()?.set_name(name.to_string())))
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
    ) -> OperationResult<FormulaScorer<'q>> {
        let ParsedFormula {
            payload_vars,
            conditions,
            defaults,
            formula,
        } = parsed_formula;

        let payload_retrievers = self.retrievers_map(payload_vars.clone())?;

        let payload_provider = PayloadProvider::new(self.payload.clone());
        let total = self.available_point_count();
        let condition_checkers = self
            .convert_conditions(
                conditions,
                payload_provider,
                total,
                DeferredBehavior::VisibleOnly,
            )?
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

    fn iter_filtered_points<'b>(
        &'b self,
        filter: &'b Filter,
        query_cardinality: &'b CardinalityEstimation,
        is_stopped: &'b AtomicBool,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'b> {
        let point_mappings = self.id_tracker.point_mappings();

        if query_cardinality.primary_clauses.is_empty() {
            let full_scan_iterator = point_mappings.iter_internal_with_behavior(deferred_behavior);

            let optimized_filter = self.optimized_filter(filter, deferred_behavior)?;
            // Worst case: query expected to return few matches, but index can't be used
            let matched_points = full_scan_iterator
                .stop_if(is_stopped)
                .filter(move |i| optimized_filter.check_infallible(*i));

            Ok(EitherVariant::A(matched_points))
        } else {
            // CPU-optimized strategy here: points are made unique before applying other filters.
            let mut visited_list = self.visited_pool.get(self.id_tracker.total_point_count());

            // If even one iterator is None, we should replace the whole thing with
            // an iterator over all ids.
            let primary_clause_iterators: OperationResult<Option<Vec<_>>> = query_cardinality
                .primary_clauses
                .iter()
                .map(|clause| self.query_field(clause))
                .collect();

            if let Some(primary_iterators) = primary_clause_iterators? {
                // Primary clauses only come from positive conditions, so they never cover a
                // `must_not` condition, even one equal to a primary clause. E.g. a proxy segment
                // reads `HasId` of its deleted points as `must: HasId(ids), must_not: HasId(ids)`.
                let all_conditions_are_primary = filter
                    .must_not
                    .as_ref()
                    .is_none_or(|conditions| conditions.is_empty())
                    && filter
                        .iter_conditions()
                        .all(|condition| query_cardinality.is_primary(condition));

                // Primary clause iterators come from field indexes and don't go through
                // the mapping, so deferred filtering must be applied to them explicitly.
                // Each primary iterator (and the flattened stream) can yield items in
                // non-sorted order depending on the field-index type and primary condition.
                let joined_primary_iterator = point_mappings
                    .filter_deferred_and_deleted(
                        primary_iterators.into_iter().flatten(),
                        deferred_behavior,
                    )
                    .stop_if(is_stopped);

                return Ok(if all_conditions_are_primary {
                    // All conditions are primary clauses,
                    // We can avoid post-filtering
                    let iter = joined_primary_iterator
                        .filter(move |&id| !visited_list.check_and_update_visited(id));
                    EitherVariant::B(iter)
                } else {
                    // Some conditions are primary clauses, some are not
                    let optimized_filter = self.optimized_filter(filter, deferred_behavior)?;
                    let iter = joined_primary_iterator.filter(move |&id| {
                        !visited_list.check_and_update_visited(id)
                            && optimized_filter.check_infallible(id)
                    });
                    EitherVariant::C(iter)
                });
            }

            // We can't use primary conditions, so we fall back to iterating over all ids
            // and applying full filter.
            let optimized_filter = self.optimized_filter(filter, deferred_behavior)?;

            let id_tracker_iterator = point_mappings.iter_internal_with_behavior(deferred_behavior);

            let iter = id_tracker_iterator
                .stop_if(is_stopped)
                .measure_hw(HwMetric::Cpu, size_of::<PointOffsetType>())
                .filter(move |&id| {
                    !visited_list.check_and_update_visited(id)
                        && optimized_filter.check_infallible(id)
                });

            Ok(EitherVariant::D(iter))
        }
    }

    fn indexed_points(&self, field: PayloadKeyTypeRef) -> OperationResult<usize> {
        let Some(indexes) = self.field_indexes.get(field) else {
            return Ok(0);
        };

        // Assume that multiple field indexes are applied to the same data type,
        // so the points indexed with those indexes are the same.
        // We will return minimal number as a worst case, to highlight possible errors in the index early.
        let counts = indexes
            .iter()
            .map(|index| index.count_indexed_points())
            .collect::<OperationResult<Vec<_>>>()?;

        Ok(counts.into_iter().min().unwrap_or(0))
    }

    fn filter_context<'b>(&'b self, filter: &'b Filter) -> OperationResult<OptimizedFilter<'b>> {
        self.optimized_filter(filter, DeferredBehavior::VisibleOnly)
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

    fn get_payload(&self, point_id: PointOffsetType) -> OperationResult<Payload> {
        self.payload.borrow().get(point_id)
    }

    fn get_payload_sequential(&self, point_id: PointOffsetType) -> OperationResult<Payload> {
        self.payload.borrow().get_sequential(point_id)
    }

    fn read_payloads<AP: AccessPattern, U: common::universal_io::UserData>(
        &self,
        point_ids: impl Iterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, Payload) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.payload
            .borrow()
            .read_payloads::<AP, _>(point_ids, callback)
    }

    fn read_payloads_raw<AP: AccessPattern, U: common::universal_io::UserData>(
        &self,
        point_ids: impl Iterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, Option<&[u8]>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.payload
            .borrow()
            .read_payloads_raw::<AP, _>(point_ids, callback)
    }
}
