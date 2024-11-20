use std::collections::HashSet;
use std::time::Duration;

use api::rest::{
    SearchMatrixOffsetsResponse, SearchMatrixPair, SearchMatrixPairsResponse,
    SearchMatrixRequestInternal,
};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::vectors::{NamedVectorStruct, DEFAULT_VECTOR_NAME};
use segment::types::{
    Condition, Filter, HasIdCondition, HasVectorCondition, PointIdType, ScoredPoint, WithVector,
};

use crate::collection::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::query_enum::QueryEnum;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{CollectionResult, CoreSearchRequest, CoreSearchRequestBatch};
use crate::operations::universal_query::shard_query::{
    SampleInternal, ScoringQuery, ShardQueryRequest,
};

#[derive(Debug, Default)]
pub struct CollectionSearchMatrixResponse {
    pub sample_ids: Vec<PointIdType>,    // sampled point ids
    pub nearests: Vec<Vec<ScoredPoint>>, // nearest points for each sampled point
}

/// Internal representation of the distance matrix request, used to convert from REST and gRPC.
pub struct CollectionSearchMatrixRequest {
    pub sample_size: usize,
    pub limit_per_sample: usize,
    pub filter: Option<Filter>,
    pub using: String,
}

impl CollectionSearchMatrixRequest {
    pub const DEFAULT_LIMIT_PER_SAMPLE: usize = 3;
    pub const DEFAULT_SAMPLE: usize = 10;
}

impl From<SearchMatrixRequestInternal> for CollectionSearchMatrixRequest {
    fn from(request: SearchMatrixRequestInternal) -> Self {
        let SearchMatrixRequestInternal {
            sample,
            limit,
            filter,
            using,
        } = request;
        Self {
            sample_size: sample.unwrap_or(CollectionSearchMatrixRequest::DEFAULT_SAMPLE),
            limit_per_sample: limit
                .unwrap_or(CollectionSearchMatrixRequest::DEFAULT_LIMIT_PER_SAMPLE),
            filter,
            using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
        }
    }
}

impl From<CollectionSearchMatrixResponse> for SearchMatrixOffsetsResponse {
    fn from(response: CollectionSearchMatrixResponse) -> Self {
        let CollectionSearchMatrixResponse {
            sample_ids,
            nearests,
        } = response;
        let offset_by_id = sample_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id, i))
            .collect::<std::collections::HashMap<_, _>>();
        let mut offsets_row = Vec::with_capacity(sample_ids.len());
        let mut offsets_col = Vec::with_capacity(sample_ids.len());
        for (row_offset, scored_points) in nearests.iter().enumerate() {
            for p in scored_points {
                offsets_row.push(row_offset as u64);
                offsets_col.push(offset_by_id[&p.id] as u64);
            }
        }
        let scores = nearests
            .into_iter()
            .flat_map(|row| row.into_iter().map(|p| p.score))
            .collect();
        Self {
            offsets_row,
            offsets_col,
            scores,
            ids: sample_ids,
        }
    }
}

impl From<CollectionSearchMatrixResponse> for SearchMatrixPairsResponse {
    fn from(response: CollectionSearchMatrixResponse) -> Self {
        let CollectionSearchMatrixResponse {
            sample_ids,
            nearests,
        } = response;

        let pairs_len = nearests.iter().map(|n| n.len()).sum();
        let mut pairs = Vec::with_capacity(pairs_len);

        for (a, scored_points) in sample_ids.into_iter().zip(nearests.into_iter()) {
            for scored_point in scored_points {
                pairs.push(SearchMatrixPair {
                    a,
                    b: scored_point.id,
                    score: scored_point.score,
                });
            }
        }

        Self { pairs }
    }
}

impl From<CollectionSearchMatrixResponse> for api::grpc::qdrant::SearchMatrixPairs {
    fn from(response: CollectionSearchMatrixResponse) -> Self {
        let rest_result = SearchMatrixPairsResponse::from(response);
        let pairs = rest_result.pairs.into_iter().map(From::from).collect();
        Self { pairs }
    }
}

impl From<CollectionSearchMatrixResponse> for api::grpc::qdrant::SearchMatrixOffsets {
    fn from(response: CollectionSearchMatrixResponse) -> Self {
        let rest_result = SearchMatrixOffsetsResponse::from(response);
        Self {
            offsets_row: rest_result.offsets_row,
            offsets_col: rest_result.offsets_col,
            scores: rest_result.scores,
            ids: rest_result.ids.into_iter().map(From::from).collect(),
        }
    }
}

impl Collection {
    pub async fn search_points_matrix(
        &self,
        request: CollectionSearchMatrixRequest,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
        hw_measurement_acc: &HwMeasurementAcc,
    ) -> CollectionResult<CollectionSearchMatrixResponse> {
        let start = std::time::Instant::now();
        let CollectionSearchMatrixRequest {
            sample_size,
            limit_per_sample,
            filter,
            using,
        } = request;
        if limit_per_sample == 0 || sample_size == 0 {
            return Ok(Default::default());
        }

        // make sure the vector is present in the point
        let has_vector = Filter::new_must(Condition::HasVector(HasVectorCondition::from(
            using.clone(),
        )));

        // merge user's filter with the has_vector filter
        let filter = Some(
            filter
                .map(|filter| filter.merge(&has_vector))
                .unwrap_or(has_vector),
        );

        // sample random points
        let sampling_query = ShardQueryRequest {
            prefetches: vec![],
            query: Some(ScoringQuery::Sample(SampleInternal::Random)),
            filter,
            score_threshold: None,
            limit: sample_size,
            offset: 0,
            params: None,
            with_vector: WithVector::Selector(vec![using.clone()]), // retrieve the vector
            with_payload: Default::default(),
        };

        let mut sampled_points = self
            .query(
                sampling_query,
                read_consistency,
                shard_selection.clone(),
                timeout,
                hw_measurement_acc,
            )
            .await?;

        // if we have less than 2 points, we can't build a matrix
        if sampled_points.len() < 2 {
            return Ok(CollectionSearchMatrixResponse::default());
        }

        sampled_points.truncate(sample_size);
        // sort by id for a deterministic order
        sampled_points.sort_unstable_by_key(|p| p.id);

        // collect the sampled point ids in the same order
        let sampled_point_ids: Vec<_> = sampled_points.iter().map(|p| p.id).collect();

        // filter to only include the sampled points in the search
        // use the same filter for all requests to leverage batch search
        let filter = Filter::new_must(Condition::HasId(HasIdCondition::from(
            sampled_point_ids.iter().copied().collect::<HashSet<_>>(),
        )));

        // Perform nearest neighbor search for each sampled point
        let mut searches = Vec::with_capacity(sampled_points.len());
        for point in sampled_points {
            let vector = point
                .vector
                .as_ref()
                .and_then(|v| v.get(&using))
                .map(|v| v.to_owned())
                .expect("Vector not found in the point");

            // nearest query on the sample vector
            let named_vector = NamedVectorStruct::new_from_vector(vector, using.clone());
            let query = QueryEnum::Nearest(named_vector);

            searches.push(CoreSearchRequest {
                query,
                filter: Some(filter.clone()),
                score_threshold: None,
                limit: limit_per_sample + 1, // +1 to exclude the point itself afterward
                offset: 0,
                params: None,
                with_vector: None,
                with_payload: None,
            });
        }

        // update timeout
        let timeout = timeout.map(|timeout| timeout.saturating_sub(start.elapsed()));

        // run batch search request
        let batch_request = CoreSearchRequestBatch { searches };
        let mut nearest = self
            .core_search_batch(
                batch_request,
                read_consistency,
                shard_selection,
                timeout,
                hw_measurement_acc,
            )
            .await?;

        // postprocess the results to account for overlapping samples
        for (scores, sample_id) in nearest.iter_mut().zip(sampled_point_ids.iter()) {
            // need to remove the sample_id from the results
            if let Some(sample_pos) = scores.iter().position(|p| p.id == *sample_id) {
                scores.remove(sample_pos);
            } else {
                // if not found pop lowest score
                if scores.len() == limit_per_sample + 1 {
                    // if we have enough results, remove the last one
                    scores.pop();
                }
            }
        }

        Ok(CollectionSearchMatrixResponse {
            sample_ids: sampled_point_ids,
            nearests: nearest,
        })
    }
}

#[cfg(test)]
mod tests {
    use segment::types::ScoredPoint;

    use super::*;

    fn make_scored_point(id: u64, score: f32) -> ScoredPoint {
        ScoredPoint {
            id: id.into(),
            version: 0,
            score,
            payload: None,
            vector: None,
            shard_key: None,
            order_value: None,
        }
    }

    // 3 samples, 2 results per sample
    fn fixture_response() -> CollectionSearchMatrixResponse {
        CollectionSearchMatrixResponse {
            sample_ids: vec![1.into(), 2.into(), 3.into()],
            nearests: vec![
                vec![make_scored_point(1, 0.2), make_scored_point(2, 0.1)],
                vec![make_scored_point(2, 0.4), make_scored_point(3, 0.3)],
                vec![make_scored_point(1, 0.6), make_scored_point(3, 0.5)],
            ],
        }
    }

    #[test]
    fn test_matrix_pairs_response_conversion() {
        let response = fixture_response();
        let expected = SearchMatrixPairsResponse {
            pairs: vec![
                SearchMatrixPair::new(1, 1, 0.2),
                SearchMatrixPair::new(1, 2, 0.1),
                SearchMatrixPair::new(2, 2, 0.4),
                SearchMatrixPair::new(2, 3, 0.3),
                SearchMatrixPair::new(3, 1, 0.6),
                SearchMatrixPair::new(3, 3, 0.5),
            ],
        };

        let actual = SearchMatrixPairsResponse::from(response);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_matrix_offsets_response_conversion() {
        let response = fixture_response();
        let expected = SearchMatrixOffsetsResponse {
            offsets_row: vec![0, 0, 1, 1, 2, 2],
            offsets_col: vec![0, 1, 1, 2, 0, 2],
            scores: vec![0.2, 0.1, 0.4, 0.3, 0.6, 0.5],
            ids: vec![1.into(), 2.into(), 3.into()],
        };

        let actual = SearchMatrixOffsetsResponse::from(response);
        assert_eq!(actual, expected);
    }
}
