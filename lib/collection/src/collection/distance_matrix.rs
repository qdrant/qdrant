use std::collections::HashSet;
use std::time::Duration;

use rand::prelude::SliceRandom;
use segment::data_types::vectors::{
    NamedMultiDenseVector, NamedVectorStruct, VectorStructInternal,
};
use segment::types::{Filter, HasIdCondition, PointIdType, ScoredPoint, WithVector};

use crate::collection::Collection;
use crate::operations::query_enum::QueryEnum;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{
    CollectionResult, CoreSearchRequest, CoreSearchRequestBatch, PointRequestInternal,
};

#[derive(Debug, Default)]
struct DistanceMatrixResponse {
    sample_ids: Vec<PointIdType>,   // sampled point ids
    nearest: Vec<Vec<ScoredPoint>>, // nearest points for each sampled point
}

impl Collection {
    async fn distance_matrix(
        &self,
        sample_size: usize,
        limit_per_sample: usize,
        filter: Option<&Filter>,
        using: String,
        shard_selector: ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> CollectionResult<DistanceMatrixResponse> {
        if limit_per_sample == 0 || sample_size == 0 {
            return Ok(Default::default());
        }

        let mut sampled_point_ids = Vec::with_capacity(sample_size);

        // Get local shards in random order
        let mut local_shards = self.get_local_shards().await;
        local_shards.shuffle(&mut rand::thread_rng());

        // Sample points from local shards
        for local_shard in self.get_local_shards().await {
            let guard = self.shards_holder.read().await;
            if let Some(shard) = guard.get_shard(&local_shard) {
                let sampled = shard
                    .sample_filtered_points(sample_size, filter, timeout)
                    .await?;
                sampled_point_ids.extend(sampled);
            }
        }

        // Check if duplicate in debug mode - shards should not return duplicate points
        #[cfg(debug_assertions)]
        {
            sampled_point_ids.sort();
            debug_assert!(
                sampled_point_ids.windows(2).all(|w| w[0] != w[1]),
                "Duplicate sample points"
            );
        }

        sampled_point_ids.shuffle(&mut rand::thread_rng());
        sampled_point_ids.truncate(sample_size);

        // retrieve the vectors for the sampled points
        let retrieve_request = PointRequestInternal {
            ids: sampled_point_ids.clone(),
            with_vector: WithVector::Selector(vec![using.clone()]),
            with_payload: Default::default(),
        };
        let sampled_points = self
            .retrieve(retrieve_request, None, &shard_selector)
            .await?;

        // Perform nearest neighbor search for each sampled point
        let mut searches = Vec::with_capacity(sampled_points.len());
        for point in sampled_points {
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

                // nearest query on the vector
                let query = QueryEnum::Nearest(named_vector);
                // exclude the point itself from the possible points search
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
        let batch_request = CoreSearchRequestBatch { searches };
        let nearest = self
            .core_search_batch(batch_request, None, shard_selector, timeout)
            .await?;

        Ok(DistanceMatrixResponse {
            sample_ids: sampled_point_ids,
            nearest,
        })
    }
}
