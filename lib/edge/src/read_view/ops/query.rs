use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ahash::AHashSet;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::{DeferredBehavior, ScoreType};
use ordered_float::OrderedFloat;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::common::reciprocal_rank_fusion::rrf_scoring;
use segment::common::score_fusion::score_fusion;
use segment::data_types::query_context::FormulaContext;
use segment::entry::ReadSegmentEntry;
use segment::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use segment::types::{
    Filter, HasIdCondition, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use shard::query::mmr::mmr_from_points_with_vector;
use shard::query::planned_query::*;
use shard::query::scroll::{QueryScrollRequestInternal, ScrollOrder};
use shard::query::*;
use shard::retrieve::retrieve_blocking::retrieve_over;
use shard::search::CoreSearchRequest;
use shard::search_result_aggregator::BatchResultAggregator;

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    pub(crate) fn query(&self, request: ShardQueryRequest) -> OperationResult<Vec<ScoredPoint>> {
        let [points] =
            self.query_batch(vec![request])?
                .try_into()
                .map_err(|unconverted: Vec<_>| {
                    OperationError::service_error(format!(
                        "unexpected query batch size: expected 1, received {}",
                        unconverted.len(),
                    ))
                })?;

        Ok(points)
    }

    /// Execute several queries as one planned batch.
    ///
    /// Planning the whole batch at once puts every request's leaf searches into a single
    /// [`search_batch`](Self::search_batch): the segments are visited once for the batch, and
    /// leaves that differ only in their query vector are pushed down to each segment as one
    /// multi-vector search. Only the plan resolution on top of those leaves — fusion, rescoring,
    /// payload fetching — stays per request.
    ///
    /// Returns one result list per request, in request order.
    pub(crate) fn query_batch(
        &self,
        requests: Vec<ShardQueryRequest>,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        let planned_query = PlannedQuery::try_from(requests)?;

        let PlannedQuery {
            root_plans,
            searches,
            scrolls,
        } = planned_query;

        let mut search_results = self.search_batch(&searches)?;

        let mut scroll_results = Vec::with_capacity(scrolls.len());
        for scroll in &scrolls {
            scroll_results.push(self.query_scroll(scroll)?);
        }

        let mut scored_points_batch = Vec::with_capacity(root_plans.len());
        for root_plan in root_plans {
            let scored_points = self.resolve_plan(
                root_plan,
                &mut search_results,
                &mut scroll_results,
                HwMeasurementAcc::disposable_edge(),
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
    ) -> OperationResult<Vec<ScoredPoint>> {
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

        let [result] = self
            .fill_with_payload_or_vectors(
                vec![results],
                with_payload,
                with_vector,
                hw_measurement_acc,
            )?
            .try_into()
            .map_err(|unconverted: Vec<_>| {
                OperationError::service_error(format!(
                    "expected single result after filling payload/vectors, got {}",
                    unconverted.len(),
                ))
            })?;
        Ok(result)
    }

    fn recurse_prefetch(
        &self,
        merge_plan: MergePlan,
        search_results: &mut Vec<Vec<ScoredPoint>>,
        scroll_results: &mut Vec<Vec<ScoredPoint>>,
        depth: usize,
        hw_counter_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let MergePlan {
            sources: merge_plan_sources,
            rescore_stages,
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
                    let merged = self.recurse_prefetch(
                        *merge_plan,
                        search_results,
                        scroll_results,
                        depth + 1,
                        hw_counter_acc.clone(),
                    )?;

                    sources.push(merged);
                }
            }
        }

        if let Some(rescore_stages) = rescore_stages {
            let RescoreStages {
                shard_level,
                collection_level,
            } = rescore_stages;

            let shard_stage_result = if let Some(rescore_params) = shard_level {
                vec![self.rescore(sources, rescore_params, hw_counter_acc.clone())?]
            } else {
                sources
            };

            let collection_result = if let Some(rescore_params) = collection_level {
                self.rescore(shard_stage_result, rescore_params, hw_counter_acc)?
            } else {
                // Only one shard result is expected at this point.
                shard_stage_result.into_iter().next().unwrap_or_default()
            };

            // In Edge, both shard-level and collection-level rescoring are handled the same way.
            Ok(collection_result)
        } else {
            // The sources here are passed to the next layer without any extra processing.
            // It should be a query without prefetches.
            debug_assert_eq!(depth, 0);
            debug_assert_eq!(sources.len(), 1);
            let [result] = sources.try_into().map_err(|unconverted: Vec<_>| {
                OperationError::service_error(format!(
                    "expected single source without rescore stages, got {}",
                    unconverted.len(),
                ))
            })?;

            Ok(result)
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
            ScoringQuery::Fusion(fusion) => {
                let top_fused = Self::fusion_rescore(
                    sources,
                    fusion,
                    score_threshold.map(OrderedFloat::into_inner),
                    limit,
                )?;
                Ok(top_fused)
            }

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

            ScoringQuery::Formula(formula) => self.rescore_with_formula(
                formula,
                sources,
                limit,
                score_threshold.map(OrderedFloat::into_inner),
                hw_counter_acc,
            ),

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
        sources: Vec<Vec<ScoredPoint>>,
        fusion: FusionInternal,
        score_threshold: Option<f32>,
        limit: usize,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let fused = match fusion {
            FusionInternal::Rrf { k, ref weights } => {
                let weights_slice = weights
                    .as_ref()
                    .map(|w| w.iter().map(|f| f.into_inner()).collect::<Vec<_>>());
                rrf_scoring(sources, k, weights_slice.as_deref())?
            }
            FusionInternal::Dbsf => score_fusion(sources),
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

    pub(crate) fn rescore_with_formula(
        &self,
        formula: ParsedFormula,
        prefetches_results: Vec<Vec<ScoredPoint>>,
        limit: usize,
        score_threshold: Option<ScoreType>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let ctx = FormulaContext {
            formula,
            prefetches_results,
            limit,
            score_threshold,
            is_stopped: Arc::new(AtomicBool::new(false)),
        };

        let ctx = Arc::new(ctx);

        let rescored_results = self.par_map_segments(|segment| {
            segment
                .read_segment()
                .rescore_with_formula(ctx.clone(), &hw_measurement_acc.get_counter_cell())
        })?;

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

        let vector_data_config = self.config.vector_data_config(&mmr.using).ok_or_else(|| {
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

    /// This function always filters deferred points.
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

        let records_map = retrieve_over(
            self.segment_arcs(),
            &point_ids,
            &WithPayload::from(with_payload),
            &with_vector,
            &AtomicBool::new(false),
            hw_measurement_acc,
            DeferredBehavior::VisibleOnly,
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

#[cfg(test)]
mod tests {
    use segment::data_types::vectors::{NamedQuery, VectorInternal};
    use segment::types::{Condition, WithPayloadInterface};
    use shard::query::query_enum::QueryEnum;

    use super::*;
    use crate::test_helpers::{VECTOR_NAME, point, point_with_group, test_config, upsert};
    use crate::{EdgeShard, PrefetchBuilder, QueryRequest, QueryRequestBuilder};

    fn nearest_query(value: f32) -> ScoringQuery {
        ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
            VectorInternal::from(vec![value]),
            VECTOR_NAME.to_string(),
        )))
    }

    fn nearest(limit: usize) -> QueryRequest {
        QueryRequestBuilder::new(limit)
            .query(nearest_query(1.0))
            .build()
    }

    /// Points 1..=n, dot-product scored against `[1.0]`, so ids rank highest-first.
    fn shard_with_points(dir: &tempfile::TempDir, n: u64) -> EdgeShard {
        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
        upsert(&shard, (1..=n).map(point).collect());
        shard
    }

    /// The whole point of the batch: it must return exactly what the same requests return one by
    /// one, however the requests are grouped when pushed down to the segments.
    fn assert_matches_one_by_one(shard: &EdgeShard, requests: Vec<QueryRequest>) {
        let one_by_one: Vec<_> = requests
            .iter()
            .map(|request| shard.query(request.clone()).unwrap())
            .collect();

        let batched = shard.query_batch(requests).unwrap();

        assert_eq!(batched, one_by_one);
    }

    #[test]
    fn query_batch_returns_one_list_per_request() {
        let dir = tempfile::tempdir().unwrap();
        let shard = shard_with_points(&dir, 3);

        let batches = shard
            .query_batch(vec![nearest(1), nearest(2), nearest(3)])
            .unwrap();

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[1].len(), 2);
        assert_eq!(batches[2].len(), 3);
        // Dot product with [1.0] ranks by vector value, so highest ids first.
        assert_eq!(batches[0][0].id, 3.into());
        assert_eq!(batches[1][0].id, 3.into());
        assert_eq!(batches[1][1].id, 2.into());
    }

    #[test]
    fn query_batch_empty_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        let batches = shard.query_batch(Vec::new()).unwrap();

        assert!(batches.is_empty());
    }

    /// Requests that agree on everything but the query vector collapse into one segment call;
    /// results must still be per request.
    #[test]
    fn query_batch_groups_identical_params() {
        let dir = tempfile::tempdir().unwrap();
        let shard = shard_with_points(&dir, 5);

        let requests: Vec<_> = [1.0, -1.0, 3.0]
            .into_iter()
            .map(|value| {
                QueryRequestBuilder::new(2)
                    .query(nearest_query(value))
                    .build()
            })
            .collect();

        assert_matches_one_by_one(&shard, requests);
    }

    /// Requests whose params differ split into several segment calls; the results must still line
    /// up with the requests, including for a param that only differs in the middle of the batch.
    #[test]
    fn query_batch_preserves_order_across_groups() {
        let dir = tempfile::tempdir().unwrap();
        let shard = shard_with_points(&dir, 5);

        let only_odd_ids = Filter::new_must(Condition::HasId(HasIdCondition::from(
            [1, 3, 5]
                .map(Into::into)
                .into_iter()
                .collect::<AHashSet<_>>(),
        )));

        let requests = vec![
            // Same params as the next one: grouped together.
            QueryRequestBuilder::new(3)
                .query(nearest_query(1.0))
                .build(),
            QueryRequestBuilder::new(3)
                .query(nearest_query(2.0))
                .build(),
            // Filter, limit and offset are pushed down, so each of these starts a new group.
            QueryRequestBuilder::new(3)
                .query(nearest_query(1.0))
                .filter(only_odd_ids)
                .build(),
            QueryRequestBuilder::new(1)
                .query(nearest_query(1.0))
                .build(),
            QueryRequestBuilder::new(2)
                .query(nearest_query(1.0))
                .offset(2)
                .build(),
            // The threshold is applied to the merged result instead, so this one shares a group
            // with the request above only if the pushed-down params match — either way it must
            // be cut off for this request alone.
            QueryRequestBuilder::new(5)
                .query(nearest_query(1.0))
                .score_threshold(3.5)
                .build(),
            // Back to the params of the first group, but not adjacent to it.
            QueryRequestBuilder::new(3)
                .query(nearest_query(4.0))
                .build(),
        ];

        assert_matches_one_by_one(&shard, requests);
    }

    /// A batch of multi-leaf requests: each request contributes several searches to the same
    /// batched pass, and every root plan must pick up its own leaves.
    #[test]
    fn query_batch_resolves_prefetches_per_request() {
        let dir = tempfile::tempdir().unwrap();
        let shard = shard_with_points(&dir, 6);

        // Weighted RRF rather than DBSF: with these points DBSF produces tied fused scores, whose
        // relative order is not defined (`score_fusion` sorts the values of an `AHashMap`).
        let fusion = |limit: usize, first: f32, second: f32| {
            QueryRequestBuilder::new(limit)
                .add_prefetch(PrefetchBuilder::new(4).query(nearest_query(first)).build())
                .add_prefetch(PrefetchBuilder::new(4).query(nearest_query(second)).build())
                .query(ScoringQuery::Fusion(FusionInternal::Rrf {
                    k: 2,
                    weights: Some(vec![OrderedFloat(1.0), OrderedFloat(0.5)]),
                }))
                .build()
        };

        let requests = vec![
            fusion(3, 1.0, -1.0),
            nearest(3),
            fusion(2, 2.0, 5.0),
            // A rescore that runs its own search on top of a prefetch.
            QueryRequestBuilder::new(2)
                .add_prefetch(PrefetchBuilder::new(4).query(nearest_query(1.0)).build())
                .query(nearest_query(-1.0))
                .build(),
        ];

        assert_matches_one_by_one(&shard, requests);
    }

    /// Payload and vector fetching happens per root plan, after the shared search pass.
    #[test]
    fn query_batch_fills_payload_and_vectors_per_request() {
        let dir = tempfile::tempdir().unwrap();
        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
        upsert(
            &shard,
            vec![point_with_group(1, "a"), point_with_group(2, "b")],
        );

        let requests = vec![
            QueryRequestBuilder::new(2)
                .query(nearest_query(1.0))
                .build(),
            QueryRequestBuilder::new(2)
                .query(nearest_query(1.0))
                .with_payload(WithPayloadInterface::Bool(true))
                .build(),
            QueryRequestBuilder::new(2)
                .query(nearest_query(1.0))
                .with_vector(WithVector::Bool(true))
                .build(),
        ];

        let batches = shard.query_batch(requests).unwrap();

        assert!(batches[0].iter().all(|point| point.payload.is_none()));
        assert!(batches[1].iter().all(|point| point.payload.is_some()));
        assert!(batches[2].iter().all(|point| point.vector.is_some()));
    }

    /// An empty shard has no segments to search, so every request gets an empty list — not a
    /// short batch.
    #[test]
    fn query_batch_without_segments_returns_a_list_per_request() {
        let dir = tempfile::tempdir().unwrap();
        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        let batches = shard.query_batch(vec![nearest(1), nearest(2)]).unwrap();

        assert_eq!(batches, vec![vec![], vec![]]);
    }
}
