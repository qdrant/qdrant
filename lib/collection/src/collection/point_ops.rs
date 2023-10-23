use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::{future, StreamExt as _, TryFutureExt, TryStreamExt as _};
use itertools::Itertools;
use segment::data_types::order_by::{Direction, OrderBy, INTERNAL_KEY_OF_ORDER_BY_VALUE};
use segment::types::{PayloadContainer, ShardKey, WithPayload, WithPayloadInterface};
use validator::Validate as _;

use super::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::point_ops::WriteOrdering;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::*;
use crate::operations::{CollectionUpdateOperations, OperationWithClockTag};
use crate::shards::shard::ShardId;

impl Collection {
    /// Apply collection update operation to all local shards.
    /// Return None if there are no local shards
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn update_all_local(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<Option<UpdateResult>> {
        let update_lock = self.updates_lock.clone().read_owned().await;
        let shard_holder = self.shards_holder.clone().read_owned().await;

        let results = tokio::task::spawn(async move {
            let _update_lock = update_lock;

            // `ShardReplicaSet::update_local` is *not* cancel safe, so we *have to* execute *all*
            // `update_local` requests to completion.
            //
            // Note that `futures::try_join_all`/`TryStreamExt::try_collect` *cancel* pending
            // requests if any of them returns an error, so we *have to* use
            // `futures::join_all`/`TryStreamExt::collect` instead!

            let local_updates: FuturesUnordered<_> = shard_holder
                .all_shards()
                .map(|shard| {
                    // The operation *can't* have a clock tag!
                    //
                    // We update *all* shards with a single operation, but each shard has it's own clock,
                    // so it's *impossible* to assign any single clock tag to this operation.
                    shard.update_local(OperationWithClockTag::from(operation.clone()), wait)
                })
                .collect();

            let results: Vec<_> = local_updates.collect().await;

            results
        })
        .await?;

        let mut result = None;

        for collection_result in results {
            let update_result = collection_result?;

            if result.is_none() && update_result.is_some() {
                result = update_result;
            }
        }

        Ok(result)
    }

    /// Handle collection updates from peers.
    ///
    /// Shard transfer aware.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn update_from_peer(
        &self,
        operation: OperationWithClockTag,
        shard_selection: ShardId,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        let update_lock = self.updates_lock.clone().read_owned().await;
        let shard_holder = self.shards_holder.clone().read_owned().await;

        let result = tokio::task::spawn(async move {
            let _update_lock = update_lock;

            let Some(shard) = shard_holder.get_shard(&shard_selection) else {
                return Ok(None);
            };

            match ordering {
                WriteOrdering::Weak => shard.update_local(operation, wait).await,
                WriteOrdering::Medium | WriteOrdering::Strong => {
                    if let Some(clock_tag) = operation.clock_tag {
                        log::warn!(
                            "Received update operation forwarded from another peer with {ordering:?} \
                             with non-`None` clock tag {clock_tag:?} (operation: {:#?})",
                             operation.operation,
                        );
                    }

                    shard
                        .update_with_consistency(operation.operation, wait, ordering)
                        .await
                        .map(Some)
                }
            }
        })
        .await??;

        if let Some(result) = result {
            Ok(result)
        } else {
            Err(CollectionError::service_error(format!(
                "No target shard {shard_selection} found for update"
            )))
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn update_from_client(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
        shard_keys_selection: Option<ShardKey>,
    ) -> CollectionResult<UpdateResult> {
        operation.validate()?;

        let update_lock = self.updates_lock.clone().read_owned().await;
        let shard_holder = self.shards_holder.clone().read_owned().await;

        let mut results = tokio::task::spawn(async move {
            let _update_lock = update_lock;

            let updates: FuturesUnordered<_> = shard_holder
                .split_by_shard(operation, &shard_keys_selection)?
                .into_iter()
                .map(move |(shard, operation)| {
                    shard.update_with_consistency(operation, wait, ordering)
                })
                .collect();

            let results: Vec<_> = updates.collect().await;

            CollectionResult::Ok(results)
        })
        .await??;

        if results.is_empty() {
            return Err(CollectionError::bad_request(
                "Empty update request".to_string(),
            ));
        }

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

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn update_from_client_simple(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        self.update_from_client(operation, wait, ordering, None)
            .await
    }

    pub async fn scroll_by(
        &self,
        request: ScrollRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selection: &ShardSelectorInternal,
    ) -> CollectionResult<ScrollResult> {
        let default_request = ScrollRequestInternal::default();

        let id_offset = request.offset;
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

            // Try to get offset value by fetching id offset point
            if order_by.value_offset.is_none() {
                order_by.value_offset = match id_offset {
                    Some(id_offset) => {
                        self.fetch_offset_value(
                            order_by,
                            id_offset,
                            shard_selection,
                            &with_vector,
                            read_consistency,
                        )
                        .await?
                    }
                    None => None,
                }
            }
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
                        id_offset,
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

        let retrieved_iter = retrieved_points.into_iter();

        let mut points = match order_by {
            None => retrieved_iter
                .flatten()
                .sorted_unstable_by_key(|point| point.id)
                .take(limit)
                .collect_vec(),
            Some(order_by) => {
                let default_value = match order_by.direction() {
                    Direction::Asc => std::f64::MAX,
                    Direction::Desc => std::f64::MIN,
                };

                let remove_order_value_from_payload = |record: &mut Record| {
                    record
                        .payload
                        .as_mut()
                        .and_then(|payload| {
                            payload
                                .0
                                .remove(INTERNAL_KEY_OF_ORDER_BY_VALUE)
                                .and_then(|v| v.as_f64())
                        })
                        .unwrap_or(default_value)
                };

                retrieved_iter
                    // Extract and remove order value from payload
                    .map(|vec| {
                        vec.into_iter().map(|mut record| {
                            let value = remove_order_value_from_payload(&mut record);
                            (value, record)
                        })
                    })
                    // Get top results
                    .kmerge_by(|(value_a, record_a), (value_b, record_b)| {
                        let key_a = (value_a, record_a.id);
                        let key_b = (value_b, record_b.id);

                        match order_by.direction() {
                            Direction::Asc => key_a <= key_b,
                            Direction::Desc => key_a >= key_b,
                        }
                    })
                    .take(limit)
                    .map(|(_, record)| record)
                    .collect_vec()
            }
        };

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

    async fn fetch_offset_value(
        &self,
        order_by: &mut OrderBy,
        id_offset: segment::types::ExtendedPointId,
        shard_selection: &ShardSelectorInternal,
        with_vector: &segment::types::WithVector,
        read_consistency: Option<ReadConsistency>,
    ) -> Result<Option<f64>, CollectionError> {
        let with_payload_interface = WithPayloadInterface::Fields(vec![order_by.key.clone()]);
        let with_payload = WithPayload::from(&with_payload_interface);
        let retrieve_request = Arc::new(PointRequestInternal {
            ids: vec![id_offset],
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
                    with_vector,
                    read_consistency,
                    false,
                )
            });

            future::try_join_all(scroll_futures).await?
        };
        Ok(offset_point
            .iter()
            .flatten()
            .next()
            .and_then(|offset_point| offset_point.payload.as_ref())
            .map(|payload| payload.get_value(&order_by.key))
            .and_then(|multi_value| multi_value.values().iter().find_map(|v| v.as_f64())))
    }
}
