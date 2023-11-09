use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future;
use segment::spaces::tools;
use segment::types::{ExtendedPointId, Order, ScoredPoint, WithPayloadInterface, WithVector};

use super::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::*;
use crate::shards::shard::ShardId;

impl Collection {
    pub async fn search(
        &self,
        request: SearchRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        if request.limit == 0 {
            return Ok(vec![]);
        }
        // search is a special case of search_batch with a single batch
        let request_batch = SearchRequestBatch {
            searches: vec![request],
        };
        let results = self
            .do_search_batch(request_batch, read_consistency, shard_selection, timeout)
            .await?;
        Ok(results.into_iter().next().unwrap())
    }

    // ! COPY-PASTE: `core_search` is a copy-paste of `search` with different request type
    // ! please replicate any changes to both methods
    pub async fn search_batch(
        &self,
        request: SearchRequestBatch,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        // shortcuts batch if all requests with limit=0
        if request.searches.iter().all(|s| s.limit == 0) {
            return Ok(vec![]);
        }
        // A factor which determines if we need to use the 2-step search or not
        // Should be adjusted based on usage statistics.
        const PAYLOAD_TRANSFERS_FACTOR_THRESHOLD: usize = 10;

        let is_payload_required = request.searches.iter().all(|s| {
            s.with_payload
                .clone()
                .map(|p| p.is_required())
                .unwrap_or_default()
        });
        let with_vectors = request.searches.iter().all(|s| {
            s.with_vector
                .as_ref()
                .map(|wv| wv.is_some())
                .unwrap_or(false)
        });

        let metadata_required = is_payload_required || with_vectors;

        let sum_limits: usize = request.searches.iter().map(|s| s.limit).sum();
        let sum_offsets: usize = request.searches.iter().map(|s| s.offset).sum();

        // Number of records we need to retrieve to fill the search result.
        let require_transfers = self.shards_holder.read().await.len() * (sum_limits + sum_offsets);
        // Actually used number of records.
        let used_transfers = sum_limits;

        let is_required_transfer_large_enough =
            require_transfers > used_transfers * PAYLOAD_TRANSFERS_FACTOR_THRESHOLD;

        if metadata_required && is_required_transfer_large_enough {
            // If there is a significant offset, we need to retrieve the whole result
            // set without payload first and then retrieve the payload.
            // It is required to do this because the payload might be too large to send over the
            // network.
            let mut without_payload_requests = Vec::with_capacity(request.searches.len());
            for search in &request.searches {
                let mut without_payload_request = search.clone();
                without_payload_request.with_payload = None;
                without_payload_request.with_vector = None;
                without_payload_requests.push(without_payload_request);
            }
            let without_payload_batch = SearchRequestBatch {
                searches: without_payload_requests,
            };
            let without_payload_results = self
                .do_search_batch(
                    without_payload_batch,
                    read_consistency,
                    shard_selection,
                    timeout,
                )
                .await?;
            let filled_results = without_payload_results
                .into_iter()
                .zip(request.clone().searches.into_iter())
                .map(|(without_payload_result, req)| {
                    self.fill_search_result_with_payload(
                        without_payload_result,
                        req.with_payload.clone(),
                        req.with_vector.unwrap_or_default(),
                        read_consistency,
                        shard_selection,
                    )
                });
            future::try_join_all(filled_results).await
        } else {
            let result = self
                .do_search_batch(request, read_consistency, shard_selection, timeout)
                .await?;
            Ok(result)
        }
    }

    // ! COPY-PASTE: `core_search` is a copy-paste of `search` with different request type
    // ! please replicate any changes to both methods
    pub async fn core_search_batch(
        &self,
        request: CoreSearchRequestBatch,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        // shortcuts batch if all requests with limit=0
        if request.searches.iter().all(|s| s.limit == 0) {
            return Ok(vec![]);
        }
        // A factor which determines if we need to use the 2-step search or not
        // Should be adjusted based on usage statistics.
        const PAYLOAD_TRANSFERS_FACTOR_THRESHOLD: usize = 10;

        let is_payload_required = request.searches.iter().all(|s| {
            s.with_payload
                .clone()
                .map(|p| p.is_required())
                .unwrap_or_default()
        });
        let with_vectors = request.searches.iter().all(|s| {
            s.with_vector
                .as_ref()
                .map(|wv| wv.is_some())
                .unwrap_or(false)
        });

        let metadata_required = is_payload_required || with_vectors;

        let sum_limits: usize = request.searches.iter().map(|s| s.limit).sum();
        let sum_offsets: usize = request.searches.iter().map(|s| s.offset).sum();

        // Number of records we need to retrieve to fill the search result.
        let require_transfers = self.shards_holder.read().await.len() * (sum_limits + sum_offsets);
        // Actually used number of records.
        let used_transfers = sum_limits;

        let is_required_transfer_large_enough =
            require_transfers > used_transfers * PAYLOAD_TRANSFERS_FACTOR_THRESHOLD;

        if metadata_required && is_required_transfer_large_enough {
            // If there is a significant offset, we need to retrieve the whole result
            // set without payload first and then retrieve the payload.
            // It is required to do this because the payload might be too large to send over the
            // network.
            let mut without_payload_requests = Vec::with_capacity(request.searches.len());
            for search in &request.searches {
                let mut without_payload_request = search.clone();
                without_payload_request.with_payload = None;
                without_payload_request.with_vector = None;
                without_payload_requests.push(without_payload_request);
            }
            let without_payload_batch = CoreSearchRequestBatch {
                searches: without_payload_requests,
            };
            let without_payload_results = self
                .do_core_search_batch(
                    without_payload_batch,
                    read_consistency,
                    shard_selection,
                    timeout,
                )
                .await?;
            let filled_results = without_payload_results
                .into_iter()
                .zip(request.clone().searches.into_iter())
                .map(|(without_payload_result, req)| {
                    self.fill_search_result_with_payload(
                        without_payload_result,
                        req.with_payload.clone(),
                        req.with_vector.unwrap_or_default(),
                        read_consistency,
                        shard_selection,
                    )
                });
            future::try_join_all(filled_results).await
        } else {
            let result = self
                .do_core_search_batch(request, read_consistency, shard_selection, timeout)
                .await?;
            Ok(result)
        }
    }

    // ! COPY-PASTE: `do_core_search_batch` is a copy-paste of `do_search_batch` with different request type
    // ! please replicate any changes to both methods
    async fn do_search_batch(
        &self,
        request: SearchRequestBatch,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let request = Arc::new(request);

        // query all shards concurrently
        let all_searches_res = {
            let shard_holder = self.shards_holder.read().await;
            let target_shards = shard_holder.target_shard(shard_selection)?;
            let all_searches = target_shards.iter().map(|shard| {
                shard.search(
                    request.clone(),
                    read_consistency,
                    shard_selection.is_some(),
                    timeout,
                )
            });
            future::try_join_all(all_searches).await?
        };

        let request = Arc::into_inner(request)
            .expect("We have already dropped all of the Arc clones at this point")
            .into();

        self.merge_from_shards(all_searches_res, request, shard_selection)
            .await
    }

    // ! COPY-PASTE: `do_core_search_batch` is a copy-paste of `do_search_batch` with different request type
    // ! please replicate any changes to both methods
    async fn do_core_search_batch(
        &self,
        request: CoreSearchRequestBatch,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let request = Arc::new(request);

        // query all shards concurrently
        let all_searches_res = {
            let shard_holder = self.shards_holder.read().await;
            let target_shards = shard_holder.target_shard(shard_selection)?;
            let all_searches = target_shards.iter().map(|shard| {
                shard.core_search(
                    request.clone(),
                    read_consistency,
                    shard_selection.is_some(),
                    timeout,
                )
            });
            future::try_join_all(all_searches).await?
        };

        let request = Arc::into_inner(request)
            .expect("We have already dropped all of the Arc clones at this point");

        self.merge_from_shards(all_searches_res, request, shard_selection)
            .await
    }

    pub(crate) async fn fill_search_result_with_payload(
        &self,
        search_result: Vec<ScoredPoint>,
        with_payload: Option<WithPayloadInterface>,
        with_vector: WithVector,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        // short-circuit if not needed
        if let (&Some(WithPayloadInterface::Bool(false)), &WithVector::Bool(false)) =
            (&with_payload, &with_vector)
        {
            return Ok(search_result
                .into_iter()
                .map(|point| ScoredPoint {
                    payload: None,
                    vector: None,
                    ..point
                })
                .collect());
        };

        let retrieve_request = PointRequest {
            ids: search_result.iter().map(|x| x.id).collect(),
            with_payload,
            with_vector,
        };
        let retrieved_records = self
            .retrieve(retrieve_request, read_consistency, shard_selection)
            .await?;
        let mut records_map: HashMap<ExtendedPointId, Record> = retrieved_records
            .into_iter()
            .map(|rec| (rec.id, rec))
            .collect();
        let enriched_result = search_result
            .into_iter()
            .filter_map(|mut scored_point| {
                // Points might get deleted between search and retrieve.
                // But it's not a problem, because we don't want to return deleted points.
                // So we just filter out them.
                records_map.remove(&scored_point.id).map(|record| {
                    scored_point.payload = record.payload;
                    scored_point.vector = record.vector;
                    scored_point
                })
            })
            .collect();
        Ok(enriched_result)
    }

    async fn merge_from_shards(
        &self,
        mut all_searches_res: Vec<Vec<Vec<ScoredPoint>>>,
        request: CoreSearchRequestBatch,
        shard_selection: Option<u32>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let batch_size = request.searches.len();

        // merge results from shards in order
        let mut merged_results: Vec<Vec<ScoredPoint>> = vec![vec![]; batch_size];
        for shard_searches_results in all_searches_res.iter_mut() {
            for (index, shard_searches_result) in shard_searches_results.iter_mut().enumerate() {
                merged_results[index].append(shard_searches_result)
            }
        }
        let collection_params = self.collection_config.read().await.params.clone();
        let top_results: Vec<_> = merged_results
            .into_iter()
            .zip(request.searches.iter())
            .map(|(res, request)| {
                let order = match &request.query {
                    QueryEnum::Nearest(_) => collection_params
                        .get_vector_params(request.query.get_vector_name())?
                        .distance
                        .distance_order(),

                    // Score comes from special handling of the distances in a way that it doesn't
                    // directly represent distance anymore, so the order is always `LargeBetter`
                    QueryEnum::Discover(_)
                    | QueryEnum::Context(_)
                    | QueryEnum::RecommendBestScore(_) => Order::LargeBetter,
                };

                let mut top_res = match order {
                    Order::LargeBetter => {
                        tools::peek_top_largest_iterable(res, request.limit + request.offset)
                    }
                    Order::SmallBetter => {
                        tools::peek_top_smallest_iterable(res, request.limit + request.offset)
                    }
                };
                // Remove `offset` from top result only for client requests
                // to avoid applying `offset` twice in distributed mode.
                if shard_selection.is_none() && request.offset > 0 {
                    if top_res.len() >= request.offset {
                        // Panics if the end point > length of the vector.
                        top_res.drain(..request.offset);
                    } else {
                        top_res.clear()
                    }
                }
                Ok(top_res)
            })
            .collect::<CollectionResult<Vec<_>>>()?;

        Ok(top_results)
    }
}
