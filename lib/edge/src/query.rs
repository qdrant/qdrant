use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ahash::AHashSet;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use ordered_float::OrderedFloat;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::common::reciprocal_rank_fusion::rrf_scoring;
use segment::common::score_fusion::{ScoreFusion, score_fusion};
use segment::data_types::query_context::FormulaContext;
use segment::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use segment::types::{
    Filter, HasIdCondition, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use shard::query::mmr::mmr_from_points_with_vector;
use shard::query::planned_query::*;
use shard::query::scroll::{QueryScrollRequestInternal, ScrollOrder};
use shard::query::*;
use shard::retrieve::retrieve_blocking::retrieve_blocking;
use shard::search::CoreSearchRequest;
use shard::search_result_aggregator::BatchResultAggregator;

use super::Shard;

impl Shard {
    pub fn query(&self, request: ShardQueryRequest) -> OperationResult<Vec<ShardQueryResponse>> {
        let planned_query = PlannedQuery::try_from(vec![request])?;

        let PlannedQuery {
            root_plans,
            searches,
            scrolls,
        } = planned_query;

        let mut search_results = Vec::new();
        for search in &searches {
            search_results.push(self.search(search.clone())?);
        }

        let mut scroll_results = Vec::new();
        for scroll in &scrolls {
            scroll_results.push(self.query_scroll(scroll)?);
        }

        let mut scored_points_batch = Vec::new();
        for root_plan in root_plans {
            let scored_points = self.resolve_plan(
                root_plan,
                &mut search_results,
                &mut scroll_results,
                HwMeasurementAcc::disposable(),
            )?;

            scored_points_batch.push(scored_points)
        }

        Ok(scored_points_batch)
    }

    fn resolve_plan(
        &self,
        root_plan: RootPlan,
        search_results: &mut Vec<Vec<ScoredPoint>>,
        scroll_results: &mut Vec<Vec<ScoredPoint>>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        let RootPlan {
            merge_plan,
            with_payload,
            with_vector,
        } = root_plan;

        let results = self.recurse_prefetch(
            merge_plan,
            search_results,
            scroll_results,
            0,
            hw_measurement_acc.clone(),
        )?;

        self.fill_with_payload_or_vectors(results, with_payload, with_vector, hw_measurement_acc)
    }

    fn recurse_prefetch(
        &self,
        merge_plan: MergePlan,
        search_results: &mut Vec<Vec<ScoredPoint>>,
        scroll_results: &mut Vec<Vec<ScoredPoint>>,
        depth: usize,
        hw_counter_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        let MergePlan {
            sources: merge_plan_sources,
            rescore_params,
        } = merge_plan;

        let max_len = merge_plan_sources.len();
        let mut sources = Vec::with_capacity(max_len);

        // We need to preserve the order of the sources for some fusion strategies
        for source in merge_plan_sources {
            match source {
                Source::SearchesIdx(idx) => {
                    sources.push(take_prefetched_source(search_results, idx)?)
                }

                Source::ScrollsIdx(idx) => {
                    sources.push(take_prefetched_source(scroll_results, idx)?)
                }

                Source::Prefetch(merge_plan) => {
                    let merged = self
                        .recurse_prefetch(
                            *merge_plan,
                            search_results,
                            scroll_results,
                            depth + 1,
                            hw_counter_acc.clone(),
                        )?
                        .into_iter();

                    sources.extend(merged);
                }
            }
        }

        // Rescore or return plain sources
        if let Some(rescore_params) = rescore_params {
            let rescored = self.rescore(sources, rescore_params, hw_counter_acc)?;
            Ok(vec![rescored])
        } else {
            // The sources here are passed to the next layer without any extra processing.
            // It is either a query without prefetches, or a fusion request and the intermediate results are passed to the next layer.
            debug_assert_eq!(depth, 0);
            Ok(sources)
        }
    }

    fn rescore(
        &self,
        sources: Vec<Vec<ScoredPoint>>,
        rescore_params: RescoreParams,
        hw_counter_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let RescoreParams {
            rescore,
            score_threshold,
            limit,
            params,
        } = rescore_params;

        match rescore {
            ScoringQuery::Fusion(fusion) => self.fusion_rescore(
                sources.into_iter(),
                fusion,
                score_threshold.map(OrderedFloat::into_inner),
                limit,
            ),

            ScoringQuery::OrderBy(order_by) => {
                // create single scroll request for rescoring query
                let filter = filter_by_point_ids(&sources);

                // Note: score_threshold is not used in this case, as all results will have same score,
                // but different order_value
                let scroll_request = QueryScrollRequestInternal {
                    limit,
                    filter: Some(filter),
                    with_payload: false.into(),
                    with_vector: false.into(),
                    scroll_order: ScrollOrder::ByField(order_by),
                };

                self.query_scroll(&scroll_request)
            }

            ScoringQuery::Vector(query_enum) => {
                // create single search request for rescoring query
                let filter = filter_by_point_ids(&sources);

                let search_request = CoreSearchRequest {
                    query: query_enum,
                    filter: Some(filter),
                    params,
                    limit,
                    offset: 0,
                    with_payload: None,
                    with_vector: None,
                    score_threshold: score_threshold.map(OrderedFloat::into_inner),
                };

                self.search(search_request)
            }

            ScoringQuery::Formula(formula) => {
                self.rescore_with_formula(formula, sources, limit, hw_counter_acc)
            }

            ScoringQuery::Sample(sample) => match sample {
                SampleInternal::Random => {
                    // create single scroll request for rescoring query
                    let filter = filter_by_point_ids(&sources);

                    // Note: score_threshold is not used in this case, as all results will have same score and order_value
                    let scroll_request = QueryScrollRequestInternal {
                        limit,
                        filter: Some(filter),
                        with_payload: false.into(),
                        with_vector: false.into(),
                        scroll_order: ScrollOrder::Random,
                    };

                    self.query_scroll(&scroll_request)
                }
            },

            ScoringQuery::Mmr(mmr) => self.mmr_rescore(sources, mmr, limit, hw_counter_acc),
        }
    }

    fn fusion_rescore(
        &self,
        sources: impl Iterator<Item = Vec<ScoredPoint>>,
        fusion: FusionInternal,
        score_threshold: Option<f32>,
        limit: usize,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let fused = match fusion {
            FusionInternal::RrfK(k) => rrf_scoring(sources, k),
            FusionInternal::Dbsf => score_fusion(sources, ScoreFusion::dbsf()),
        };

        let top_fused: Vec<_> = if let Some(score_threshold) = score_threshold {
            fused
                .into_iter()
                .take_while(|point| point.score >= score_threshold)
                .take(limit)
                .collect()
        } else {
            fused.into_iter().take(limit).collect()
        };

        Ok(top_fused)
    }

    pub fn rescore_with_formula(
        &self,
        formula: ParsedFormula,
        prefetches_results: Vec<Vec<ScoredPoint>>,
        limit: usize,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let ctx = FormulaContext {
            formula,
            prefetches_results,
            limit,
            is_stopped: Arc::new(AtomicBool::new(false)),
        };

        let ctx = Arc::new(ctx);
        let hw_counter = hw_measurement_acc.get_counter_cell();

        let mut rescored_results = Vec::new();

        for segment in self
            .segments
            .read()
            .non_appendable_then_appendable_segments()
        {
            let rescored_result = segment
                .get()
                .read()
                .rescore_with_formula(ctx.clone(), &hw_counter)?;

            rescored_results.push(rescored_result);
        }

        // use aggregator with only one "batch"
        let mut aggregator = BatchResultAggregator::new(std::iter::once(limit));
        aggregator.update_point_versions(rescored_results.iter().flatten());
        aggregator.update_batch_results(0, rescored_results.into_iter().flatten());

        let top =
            aggregator.into_topk().into_iter().next().ok_or_else(|| {
                OperationError::service_error("expected first result of aggregator")
            })?;

        Ok(top)
    }

    /// Maximal Marginal Relevance rescoring
    fn mmr_rescore(
        &self,
        sources: Vec<Vec<ScoredPoint>>,
        mmr: MmrInternal,
        limit: usize,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let points_with_vector = self
            .fill_with_payload_or_vectors(
                sources,
                false.into(),
                WithVector::from(mmr.using.clone()),
                hw_measurement_acc.clone(),
            )?
            .into_iter()
            .flatten();

        let vector_data_config = self.config.vector_data.get(&mmr.using).ok_or_else(|| {
            OperationError::service_error(format!(
                "vector data config for vector {} not found",
                mmr.using,
            ))
        })?;

        // Even if we have fewer points than requested, still calculate MMR.
        let mut top_mmr = mmr_from_points_with_vector(
            points_with_vector,
            mmr,
            vector_data_config.distance,
            vector_data_config.multivector_config,
            limit,
            hw_measurement_acc,
        )?;

        // strip mmr vector. We will handle user-requested vectors at root level of request.
        for point in &mut top_mmr {
            point.vector = None;
        }

        Ok(top_mmr)
    }

    fn fill_with_payload_or_vectors(
        &self,
        query_response: ShardQueryResponse,
        with_payload: WithPayloadInterface,
        with_vector: WithVector,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> OperationResult<ShardQueryResponse> {
        if !with_payload.is_required() && !with_vector.is_enabled() {
            return Ok(query_response);
        }

        // ids to retrieve (deduplication happens in the searcher)
        let point_ids: Vec<_> = query_response
            .iter()
            .flatten()
            .map(|scored_point| scored_point.id)
            .collect();

        let records_map = retrieve_blocking(
            self.segments.clone(),
            &point_ids,
            &WithPayload::from(with_payload),
            &with_vector,
            &AtomicBool::new(false),
            hw_measurement_acc,
        )?;

        // It might be possible, that we won't find all records,
        // so we need to re-collect the results
        let query_response: ShardQueryResponse = query_response
            .into_iter()
            .map(|points| {
                points
                    .into_iter()
                    .filter_map(|mut point| {
                        records_map.get(&point.id).map(|record| {
                            point.payload.clone_from(&record.payload);
                            point.vector.clone_from(&record.vector);
                            point
                        })
                    })
                    .collect()
            })
            .collect();

        Ok(query_response)
    }
}

fn take_prefetched_source<T: Default>(items: &mut [T], index: usize) -> OperationResult<T> {
    let source = items.get_mut(index).ok_or_else(|| {
        OperationError::service_error(format!("prefetched source at index {index} does not exist"))
    })?;

    Ok(mem::take(source))
}

/// Extracts point ids from sources, and creates a filter to only include those ids.
fn filter_by_point_ids(points: &[Vec<ScoredPoint>]) -> Filter {
    let point_ids: AHashSet<_> = points.iter().flatten().map(|point| point.id).collect();

    // create filter for target point ids
    Filter::new_must(segment::types::Condition::HasId(HasIdCondition::from(
        point_ids,
    )))
}
