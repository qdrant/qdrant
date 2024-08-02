use std::collections::HashSet;
use std::time::Duration;

use segment::data_types::vectors::NamedVectorStruct;
use segment::types::{Condition, Filter, HasIdCondition, PointIdType, ScoredPoint, WithVector};

use crate::collection::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::query_enum::QueryEnum;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{CollectionResult, CoreSearchRequest, CoreSearchRequestBatch};
use crate::operations::universal_query::shard_query::{Sample, ScoringQuery, ShardQueryRequest};

#[derive(Debug, Default)]
pub struct DistanceMatrixResponse {
    pub sample_ids: Vec<PointIdType>,   // sampled point ids
    pub nearest: Vec<Vec<ScoredPoint>>, // nearest points for each sampled point
}

// TODO introduce HasVector condition to avoid iterative sampling
const SAMPLING_TRIES: usize = 3;

impl Collection {
    #[allow(clippy::too_many_arguments)] // TODO use request object
    pub async fn distance_matrix(
        &self,
        sample_size: usize,
        limit_per_sample: usize,
        filter: Option<&Filter>,
        using: String,
        shard_selector: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
    ) -> CollectionResult<DistanceMatrixResponse> {
        if limit_per_sample == 0 || sample_size == 0 {
            return Ok(Default::default());
        }

        let mut sampled_points: Vec<(_, _)> = Vec::with_capacity(sample_size);

        // Sampling multiple times because we might not have enough points with the named vector
        for _ in 0..SAMPLING_TRIES {
            // check if we have enough samples with right named vector
            if sampled_points.len() >= sample_size {
                break;
            }

            // exclude already sampled points
            let exclude_ids: HashSet<_> = sampled_points.iter().map(|(id, _)| *id).collect();
            let filter = if exclude_ids.is_empty() {
                filter.cloned()
            } else {
                let exclude_ids = Filter::new_must_not(Condition::HasId(exclude_ids.into()));
                Some(
                    filter
                        .as_ref()
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
                    shard_selector.clone(),
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
            .core_search_batch(batch_request, read_consistency, shard_selector, timeout)
            .await?;

        Ok(DistanceMatrixResponse {
            sample_ids: sampled_point_ids,
            nearest,
        })
    }
}
