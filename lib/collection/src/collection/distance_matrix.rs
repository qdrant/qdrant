use std::collections::HashSet;
use std::time::Duration;

use api::rest::{
    SearchMatrixOffsetsResponse, SearchMatrixPairsResponse, SearchMatrixRequestInternal,
    SearchMatrixRow, SearchMatrixRowsResponse,
};
use segment::data_types::vectors::{NamedVectorStruct, DEFAULT_VECTOR_NAME};
use segment::types::{Condition, Filter, HasIdCondition, PointIdType, ScoredPoint, WithVector};

use crate::collection::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::query_enum::QueryEnum;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{CollectionResult, CoreSearchRequest, CoreSearchRequestBatch};
use crate::operations::universal_query::shard_query::{Sample, ScoringQuery, ShardQueryRequest};

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

impl From<SearchMatrixRequestInternal> for CollectionSearchMatrixRequest {
    fn from(request: SearchMatrixRequestInternal) -> Self {
        let SearchMatrixRequestInternal {
            sample,
            limit,
            filter,
            using,
        } = request;
        Self {
            sample_size: sample,
            limit_per_sample: limit,
            filter,
            using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
        }
    }
}

impl From<CollectionSearchMatrixResponse> for SearchMatrixRowsResponse {
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
        let mut rows = Vec::with_capacity(sample_ids.len());
        for scored_point in nearests {
            let row = SearchMatrixRow {
                offsets_id: scored_point
                    .iter()
                    .map(|p| offset_by_id[&p.id] as u64)
                    .collect(),
                scores: scored_point.into_iter().map(|p| p.score).collect(),
            };
            rows.push(row);
        }
        Self {
            rows,
            ids: sample_ids,
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
            .iter()
            .flat_map(|row| row.iter().map(|p| p.score))
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
        let offset_by_id = sample_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id, i))
            .collect::<std::collections::HashMap<_, _>>();
        let mut rows = Vec::with_capacity(sample_ids.len());
        for scored_point in nearests {
            let row = scored_point
                .into_iter()
                .map(|p| (offset_by_id[&p.id] as u64, p.score))
                .collect();
            rows.push(row);
        }
        Self {
            rows,
            ids: sample_ids,
        }
    }
}

// TODO introduce HasVector condition to avoid iterative sampling
const SAMPLING_TRIES: usize = 3;

impl Collection {
    pub async fn search_points_matrix(
        &self,
        request: CollectionSearchMatrixRequest,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
    ) -> CollectionResult<CollectionSearchMatrixResponse> {
        let CollectionSearchMatrixRequest {
            sample_size,
            limit_per_sample,
            filter,
            using,
        } = request;
        if limit_per_sample == 0 || sample_size == 0 {
            return Ok(Default::default());
        }

        let mut sampled_points: Vec<(_, _)> = Vec::with_capacity(sample_size);

        // Sampling multiple times because we might not have enough points with the named vector
        for _ in 0..SAMPLING_TRIES {
            let filter = filter.clone();
            // check if we have enough samples with right named vector
            if sampled_points.len() >= sample_size {
                break;
            }

            // exclude already sampled points
            let exclude_ids: HashSet<_> = sampled_points.iter().map(|(id, _)| *id).collect();
            let filter = if exclude_ids.is_empty() {
                filter
            } else {
                let exclude_ids = Filter::new_must_not(Condition::HasId(exclude_ids.into()));
                Some(
                    filter
                        .map(|filter| filter.merge(&exclude_ids))
                        .unwrap_or(exclude_ids),
                )
            };

            // Sample points with query API
            let sampling_query = ShardQueryRequest {
                prefetches: vec![],
                query: Some(ScoringQuery::Sample(Sample::Random)),
                filter,
                score_threshold: None,
                limit: sample_size,
                offset: 0,
                params: None,
                with_vector: WithVector::Selector(vec![using.clone()]), // retrieve the vector
                with_payload: Default::default(),
            };

            let sampling_response = self
                .query(
                    sampling_query.clone(),
                    read_consistency,
                    shard_selection.clone(),
                    timeout,
                )
                .await?;

            // select only points with the queried named vector
            let filtered = sampling_response.into_iter().filter_map(|p| {
                p.vector
                    .as_ref()
                    .and_then(|v| v.get(&using))
                    .map(|v| (p.id, v.to_owned()))
            });

            sampled_points.extend(filtered);
        }

        sampled_points.truncate(sample_size);
        // sort by id for a deterministic order
        sampled_points.sort_unstable_by(|(id1, _), (id2, _)| id1.cmp(id2));
        let sampled_point_ids: Vec<_> = sampled_points.iter().map(|(id, _)| *id).collect();

        // Perform nearest neighbor search for each sampled point
        let mut searches = Vec::with_capacity(sampled_points.len());
        for (point_id, vector) in sampled_points {
            // nearest query on the sample vector
            let named_vector = NamedVectorStruct::new_from_vector(vector, using.clone());
            let query = QueryEnum::Nearest(named_vector);

            // exclude the point itself from the possible points to score
            let req_ids: HashSet<_> = sampled_point_ids
                .iter()
                .filter(|id| *id != &point_id)
                .cloned()
                .collect();
            let only_ids = Filter::new_must(Condition::HasId(HasIdCondition::from(req_ids)));

            // update filter with the only_ids
            let req_filter = Some(
                filter
                    .as_ref()
                    .map(|filter| filter.merge(&only_ids))
                    .unwrap_or(only_ids),
            );
            searches.push(CoreSearchRequest {
                query,
                filter: req_filter,
                score_threshold: None,
                limit: limit_per_sample,
                offset: 0,
                params: None,
                with_vector: None,
                with_payload: None,
            });
        }

        // run batch search request
        let batch_request = CoreSearchRequestBatch { searches };
        let nearest = self
            .core_search_batch(batch_request, read_consistency, shard_selection, timeout)
            .await?;

        Ok(CollectionSearchMatrixResponse {
            sample_ids: sampled_point_ids,
            nearests: nearest,
        })
    }
}

#[cfg(test)]
mod tests {
    use api::rest::{SearchMatrixRow, SearchMatrixRowsResponse};
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
    fn test_matrix_row_response_conversion() {
        let response = fixture_response();
        let expected = SearchMatrixRowsResponse {
            rows: vec![
                SearchMatrixRow {
                    offsets_id: vec![0, 1],
                    scores: vec![0.2, 0.1],
                },
                SearchMatrixRow {
                    offsets_id: vec![1, 2],
                    scores: vec![0.4, 0.3],
                },
                SearchMatrixRow {
                    offsets_id: vec![0, 2],
                    scores: vec![0.6, 0.5],
                },
            ],
            ids: vec![1.into(), 2.into(), 3.into()],
        };
        let actual = SearchMatrixRowsResponse::from(response);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_matrix_pairs_response_conversion() {
        let response = fixture_response();
        let expected = SearchMatrixPairsResponse {
            rows: vec![
                vec![(0, 0.2), (1, 0.1)],
                vec![(1, 0.4), (2, 0.3)],
                vec![(0, 0.6), (2, 0.5)],
            ],
            ids: vec![1.into(), 2.into(), 3.into()],
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
