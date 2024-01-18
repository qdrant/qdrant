use std::sync::Arc;

use futures::{future, TryFutureExt, TryStreamExt as _};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use segment::data_types::order_by::{Direction, OrderBy};
use segment::types::{PayloadContainer, ShardKey, WithPayload, WithPayloadInterface};
use validator::Validate as _;

use super::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::point_ops::WriteOrdering;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::*;
use crate::operations::CollectionUpdateOperations;
use crate::shards::shard::ShardId;

impl Collection {
    /// Apply collection update operation to all local shards.
    /// Return None if there are no local shards
    pub async fn update_all_local(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<Option<UpdateResult>> {
        let _update_lock = self.updates_lock.read().await;
        let shard_holder_guard = self.shards_holder.read().await;

        let res: Vec<_> = shard_holder_guard
            .all_shards()
            .map(|shard| shard.update_local(operation.clone(), wait))
            .collect();

        let results: Vec<_> = future::try_join_all(res).await?;

        let result = results.into_iter().flatten().next();

        Ok(result)
    }

    /// Handle collection updates from peers.
    ///
    /// Shard transfer aware.
    pub async fn update_from_peer(
        &self,
        operation: CollectionUpdateOperations,
        shard_selection: ShardId,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        let _update_lock = self.updates_lock.read().await;
        let shard_holder_guard = self.shards_holder.read().await;

        let res = match shard_holder_guard.get_shard(&shard_selection) {
            None => None,
            Some(target_shard) => match ordering {
                WriteOrdering::Weak => target_shard.update_local(operation, wait).await?,
                WriteOrdering::Medium | WriteOrdering::Strong => Some(
                    target_shard
                        .update_with_consistency(operation, wait, ordering)
                        .await?,
                ),
            },
        };

        if let Some(res) = res {
            Ok(res)
        } else {
            Err(CollectionError::service_error(format!(
                "No target shard {shard_selection} found for update"
            )))
        }
    }

    pub async fn update_from_client_simple(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        self.update_from_client(operation, wait, ordering, None)
            .await
    }

    pub async fn update_from_client(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
        shard_keys_selection: Option<ShardKey>,
    ) -> CollectionResult<UpdateResult> {
        operation.validate()?;
        let _update_lock = self.updates_lock.read().await;

        let mut results = {
            let shards_holder = self.shards_holder.read().await;
            let shard_to_op = shards_holder.split_by_shard(operation, &shard_keys_selection)?;

            if shard_to_op.is_empty() {
                return Err(CollectionError::bad_request(
                    "Empty update request".to_string(),
                ));
            }

            let shard_requests = shard_to_op
                .into_iter()
                .map(move |(replica_set, operation)| {
                    replica_set.update_with_consistency(operation, wait, ordering)
                });
            future::join_all(shard_requests).await
        };

        let with_error = results.iter().filter(|result| result.is_err()).count();

        // one request per shard
        let result_len = results.len();

        if with_error > 0 {
            let first_err = results.into_iter().find(|result| result.is_err()).unwrap();
            // inconsistent if only a subset of the requests fail - one request per shard.
            if with_error < result_len {
                first_err.map_err(|err| {
                    // compute final status code based on the first error
                    // e.g. a partially successful batch update failing because of bad input is a client error
                    CollectionError::InconsistentShardFailure {
                        shards_total: result_len as u32, // report only the number of shards that took part in the update
                        shards_failed: with_error as u32,
                        first_err: Box::new(err),
                    }
                })
            } else {
                // all requests per shard failed - propagate first error (assume there are all the same)
                first_err
            }
        } else {
            // At least one result is always present.
            results.pop().unwrap()
        }
    }

    pub async fn scroll_by(
        &self,
        request: ScrollRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selection: &ShardSelectorInternal,
    ) -> CollectionResult<ScrollResult> {
        let default_request = ScrollRequestInternal::default();

        let offset = request.offset;
        let limit = request
            .limit
            .unwrap_or_else(|| default_request.limit.unwrap());
        let with_payload_interface = request
            .with_payload
            .clone()
            .unwrap_or_else(|| default_request.with_payload.clone().unwrap());
        let with_vector = request.with_vector;

        let mut order_by = request.order_by.map(OrderBy::from);

        // Handle case of order_by
        if let Some(order_by) = &mut order_by {
            // Validate we have a range index for the order_by key
            let has_range_index_for_order_by_field = self
                .payload_index_schema
                .read()
                .schema
                .get(order_by.key.as_str())
                .is_some_and(|field| field.has_range_index());

            if !has_range_index_for_order_by_field {
                return Err(CollectionError::bad_request(format!(
                    "No range index for `order_by` key: {}. Please create one to use `order_by`. Integer and float payloads can have range indexes, see https://qdrant.tech/documentation/concepts/indexing/#payload-index.",
                    order_by.key.as_str()
                )));
            }

            // Fetch offset id to get offset value
            order_by.value_offset = if let Some(offset) = offset {
                let with_payload_interface =
                    WithPayloadInterface::Fields(vec![order_by.key.clone()]);
                let with_payload = WithPayload::from(&with_payload_interface);

                let retrieve_request = Arc::new(PointRequestInternal {
                    ids: vec![offset],
                    with_payload: Some(with_payload_interface),
                    with_vector: false.into(),
                });

                let offset_point: Vec<_> = {
                    let shards_holder = self.shards_holder.read().await;
                    let target_shards = shards_holder.select_shards(shard_selection)?;
                    let scroll_futures = target_shards.into_iter().map(|(shard, _shard_key)| {
                        let retrieve_request = retrieve_request.clone();
                        shard.retrieve(
                            retrieve_request,
                            &with_payload,
                            &with_vector,
                            read_consistency,
                            false,
                        )
                    });

                    future::try_join_all(scroll_futures).await?
                };

                // Extract number value from payload
                offset_point
                    .iter()
                    .flatten()
                    .next()
                    .and_then(|offset_point| offset_point.payload.as_ref())
                    .map(|payload| payload.get_value(&order_by.key))
                    .and_then(|multi_value| multi_value.values().iter().find_map(|v| v.as_f64()))
            } else {
                None
            };
        };

        if limit == 0 {
            return Err(CollectionError::BadRequest {
                description: "Limit cannot be 0".to_string(),
            });
        }

        // Needed to return next page offset.
        let limit = limit + 1;
        let retrieved_points: Vec<_> = {
            let shards_holder = self.shards_holder.read().await;
            let target_shards = shards_holder.select_shards(shard_selection)?;
            let scroll_futures = target_shards.into_iter().map(|(shard, shard_key)| {
                let shard_key = shard_key.cloned();
                shard
                    .scroll_by(
                        offset,
                        limit,
                        &with_payload_interface,
                        &with_vector,
                        request.filter.as_ref(),
                        read_consistency,
                        shard_selection.is_shard_id(),
                        order_by.as_ref(),
                    )
                    .and_then(move |mut records| async move {
                        if shard_key.is_none() {
                            return Ok(records);
                        }
                        for point in &mut records {
                            point.shard_key = shard_key.clone();
                        }
                        Ok(records)
                    })
            });

            future::try_join_all(scroll_futures).await?
        };

        let retrieved_iter = retrieved_points.into_iter().flatten();
        let mut points = match order_by {
            None => retrieved_iter
                .sorted_unstable_by_key(|point| point.id)
                .collect_vec(),
            Some(order_by) => {
                let default_value = match order_by.direction() {
                    Direction::Asc => std::f64::MAX,
                    Direction::Desc => std::f64::MIN,
                };

                // Extract order_by value from payload
                let retrieved_iter = retrieved_iter.map(|point| {
                    let value = point
                        .payload
                        .as_ref()
                        .and_then(|payload| {
                            payload
                                .get_value(&order_by.key)
                                .values()
                                .first()
                                .and_then(|v| v.as_f64())
                        })
                        .unwrap_or(default_value);
                    (value, point)
                });
                match order_by.direction() {
                    Direction::Asc => retrieved_iter
                        .sorted_unstable_by_key(|(value, point)| (OrderedFloat(*value), point.id))
                        .map(|(_, record)| record)
                        .collect_vec(),
                    Direction::Desc => retrieved_iter
                        .sorted_unstable_by_key(|(value, point)| (OrderedFloat(*value), point.id))
                        .rev()
                        .map(|(_, record)| record)
                        .collect_vec(),
                }
            }
        }
        .into_iter()
        .take(limit)
        .collect_vec();

        let next_page_offset = if points.len() < limit {
            // This was the last page
            None
        } else {
            // remove extra point, it would be a first point of the next page
            Some(points.pop().unwrap().id)
        };
        Ok(ScrollResult {
            points,
            next_page_offset,
        })
    }

    pub async fn count(
        &self,
        request: CountRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selection: &ShardSelectorInternal,
    ) -> CollectionResult<CountResult> {
        let shards_holder = self.shards_holder.read().await;
        let shards = shards_holder.select_shards(shard_selection)?;

        let request = Arc::new(request);
        let mut requests: futures::stream::FuturesUnordered<_> = shards
            .into_iter()
            // `count` requests received through internal gRPC *always* have `shard_selection`
            .map(|(shard, _shard_key)| {
                shard.count(
                    request.clone(),
                    read_consistency,
                    shard_selection.is_shard_id(),
                )
            })
            .collect();

        let mut count = 0;

        while let Some(response) = requests.try_next().await? {
            count += response.count;
        }

        Ok(CountResult { count })
    }

    pub async fn retrieve(
        &self,
        request: PointRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selection: &ShardSelectorInternal,
    ) -> CollectionResult<Vec<Record>> {
        let with_payload_interface = request
            .with_payload
            .as_ref()
            .unwrap_or(&WithPayloadInterface::Bool(false));
        let with_payload = WithPayload::from(with_payload_interface);
        let request = Arc::new(request);
        let all_shard_collection_results = {
            let shard_holder = self.shards_holder.read().await;
            let target_shards = shard_holder.select_shards(shard_selection)?;
            let retrieve_futures = target_shards.into_iter().map(|(shard, shard_key)| {
                let shard_key = shard_key.cloned();
                shard
                    .retrieve(
                        request.clone(),
                        &with_payload,
                        &request.with_vector,
                        read_consistency,
                        shard_selection.is_shard_id(),
                    )
                    .and_then(move |mut records| async move {
                        if shard_key.is_none() {
                            return Ok(records);
                        }
                        for point in &mut records {
                            point.shard_key = shard_key.clone();
                        }
                        Ok(records)
                    })
            });
            future::try_join_all(retrieve_futures).await?
        };
        let points = all_shard_collection_results.into_iter().flatten().collect();
        Ok(points)
    }
}
