use std::collections::HashSet;
use std::time::Duration;

use segment::data_types::vectors::{
    NamedMultiDenseVector, NamedVectorStruct, VectorStructInternal,
};
use segment::types::{Filter, HasIdCondition, PointIdType, ScoredPoint, WithVector};

use crate::collection::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::query_enum::QueryEnum;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{CollectionResult, CoreSearchRequest, CoreSearchRequestBatch};
use crate::operations::universal_query::shard_query::{Sample, ScoringQuery, ShardQueryRequest};

#[derive(Debug, Default)]
struct DistanceMatrixResponse {
    sample_ids: Vec<PointIdType>,   // sampled point ids
    nearest: Vec<Vec<ScoredPoint>>, // nearest points for each sampled point
}

impl Collection {
    #[allow(clippy::too_many_arguments)] // TODO use request object
    async fn distance_matrix(
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

        // Sample points with query API
        let sampling_query = ShardQueryRequest {
            prefetches: vec![],
            query: Some(ScoringQuery::Sample(Sample::Random)),
            filter: filter.cloned(),
            score_threshold: None,
            limit: sample_size,
            offset: 0,
            params: None,
            with_vector: WithVector::Selector(vec![using.clone()]), // retrieve the vector
            with_payload: Default::default(),
        };
        let sampling_response = self
            .query(
                sampling_query,
                read_consistency,
                shard_selector.clone(),
                timeout,
            )
            .await?;
        let sampled_point_ids: Vec<_> = sampling_response.iter().map(|point| point.id).collect();

        // Perform nearest neighbor search for each sampled point
        let mut searches = Vec::with_capacity(sampling_response.len());
        for point in sampling_response {
            if let Some(vector) = point.vector {
                let named_vector: NamedVectorStruct = match vector {
                    VectorStructInternal::Single(v) => NamedVectorStruct::from(v),
                    VectorStructInternal::MultiDense(v) => {
                        let multi = NamedMultiDenseVector {
                            name: using.clone(),
                            vector: v,
                        };
                        NamedVectorStruct::from(multi)
                    }
                    VectorStructInternal::Named(named_vectors) => {
                        // expect is safe because we just retrieved the vector
                        let v = named_vectors.get(&using).expect("Vector not found").clone();
                        NamedVectorStruct::new_from_vector(v, using.clone())
                    }
                };

                // nearest query on the sample vector
                let query = QueryEnum::Nearest(named_vector);
                // exclude the point itself from the possible points to score
                let req_ids: HashSet<_> = sampled_point_ids
                    .iter()
                    .filter(|id| *id != &point.id)
                    .cloned()
                    .collect();
                let only_ids = Filter::new_must(segment::types::Condition::HasId(
                    HasIdCondition::from(req_ids),
                ));
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
