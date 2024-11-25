use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::stream::FuturesUnordered;
use futures::{future, StreamExt as _, TryFutureExt, TryStreamExt as _};
use itertools::Itertools;
use segment::data_types::order_by::{Direction, OrderBy};
use segment::types::{ShardKey, WithPayload, WithPayloadInterface};

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

            let Some(shard) = shard_holder.get_shard(shard_selection) else {
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
                        .update_with_consistency(operation.operation, wait, ordering, false)
                        .await
                        .map(Some)
                }
            }
        })
        .await??;

        if let Some(result) = result {
            Ok(result)
        } else {
            // Special error type needed to handle creation of partial shards
            // In all other scenarios, equivalent to `service_error`
            Err(CollectionError::pre_condition_failed(format!(
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
        let update_lock = self.updates_lock.clone().read_owned().await;
        let shard_holder = self.shards_holder.clone().read_owned().await;

        let mut results = tokio::task::spawn(async move {
            let _update_lock = update_lock;

            let updates = FuturesUnordered::new();
            let operations = shard_holder.split_by_shard(operation, &shard_keys_selection)?;

            for (shard, operation) in operations {
                let operation = shard_holder.split_by_mode(shard.shard_id, operation);

                updates.push(async move {
                    let mut result = UpdateResult {
                        operation_id: None,
                        status: UpdateStatus::Acknowledged,
                        clock_tag: None,
                    };

                    for operation in operation.update_all {
                        result = shard
                            .update_with_consistency(operation, wait, ordering, false)
                            .await?;
                    }

                    for operation in operation.update_only_existing {
                        let res = shard
                            .update_with_consistency(operation, wait, ordering, true)
                            .await;

                        if let Err(err) = &res {
                            if err.is_missing_point() {
                                continue;
                            }
                        }

                        result = res?;
                    }

                    CollectionResult::Ok(result)
                });
            }

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
        timeout: Option<Duration>,
    ) -> CollectionResult<ScrollResult> {
        let default_request = ScrollRequestInternal::default();

        let id_offset = request.offset;
        let mut limit = request
            .limit
            .unwrap_or_else(|| default_request.limit.unwrap());
        let with_payload_interface = request
            .with_payload
            .clone()
            .unwrap_or_else(|| default_request.with_payload.clone().unwrap());
        let with_vector = request.with_vector;

        let order_by = request.order_by.map(OrderBy::from);

        // Validate user did not try to use an id offset with order_by
        if order_by.is_some() && id_offset.is_some() {
            return Err(CollectionError::bad_input("Cannot use an `offset` when using `order_by`. The alternative for paging is to use `order_by.start_from` and a filter to exclude the IDs that you've already seen for the `order_by.start_from` value".to_string()));
        };

        if limit == 0 {
            return Err(CollectionError::BadRequest {
                description: "Limit cannot be 0".to_string(),
            });
        }

        // `order_by` does not support offset
        if order_by.is_none() {
            // Needed to return next page offset.
            limit = limit.saturating_add(1);
        };

        let local_only = shard_selection.is_shard_id();

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
                        local_only,
                        order_by.as_ref(),
                        timeout,
                    )
                    .and_then(move |mut records| async move {
                        if shard_key.is_none() {
                            return Ok(records);
                        }
                        for point in &mut records {
                            point.shard_key.clone_from(&shard_key);
                        }
                        Ok(records)
                    })
            });
            future::try_join_all(scroll_futures).await?
        };

        let retrieved_iter = retrieved_points.into_iter();

        let mut points = match &order_by {
            None => retrieved_iter
                .flatten()
                .sorted_unstable_by_key(|point| point.id)
                // Add each point only once, deduplicate point IDs
                .dedup_by(|a, b| a.id == b.id)
                .take(limit)
                .map(api::rest::Record::from)
                .collect_vec(),
            Some(order_by) => {
                retrieved_iter
                    // Extract and remove order value from payload
                    .map(|records| {
                        // TODO(1.11): read value only from record.order_value, remove & cleanup this part
                        records.into_iter().map(|mut record| {
                            let value;
                            if local_only {
                                value = record.order_value.unwrap_or_else(|| {
                                    order_by.get_order_value_from_payload(record.payload.as_ref())
                                });
                            } else {
                                value = if let Some(order_value) = record.order_value {
                                    order_by
                                        .remove_order_value_from_payload(record.payload.as_mut());
                                    order_value
                                } else {
                                    order_by
                                        .remove_order_value_from_payload(record.payload.as_mut())
                                };
                                if !with_payload_interface.is_required() {
                                    // Use None instead of empty hashmap
                                    record.payload = None;
                                }
                            };
                            (value, record)
                        })
                    })
                    // Get top results
                    .kmerge_by(|(value_a, record_a), (value_b, record_b)| {
                        match order_by.direction() {
                            Direction::Asc => (value_a, record_a.id) < (value_b, record_b.id),
                            Direction::Desc => (value_a, record_a.id) > (value_b, record_b.id),
                        }
                    })
                    // Only keep the point with the most "valuable" order value
                    .dedup_by(|(_, record_a), (_, record_b)| record_a.id == record_b.id)
                    .map(|(_, record)| api::rest::Record::from(record))
                    .take(limit)
                    .collect_vec()
            }
        };

        let next_page_offset = if points.len() < limit || order_by.is_some() {
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
        timeout: Option<Duration>,
        hw_measurement_acc: &HwMeasurementAcc,
    ) -> CollectionResult<CountResult> {
        let shards_holder = self.shards_holder.read().await;
        let shards = shards_holder.select_shards(shard_selection)?;

        let request = Arc::new(request);

        let mut requests: FuturesUnordered<_> = shards
            .into_iter()
            // `count` requests received through internal gRPC *always* have `shard_selection`
            .map(|(shard, _shard_key)| {
                shard.count(
                    Arc::clone(&request),
                    read_consistency,
                    timeout,
                    shard_selection.is_shard_id(),
                    hw_measurement_acc,
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
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<RecordInternal>> {
        let with_payload_interface = request
            .with_payload
            .as_ref()
            .unwrap_or(&WithPayloadInterface::Bool(false));
        let with_payload = WithPayload::from(with_payload_interface);
        let ids_len = request.ids.len();
        let request = Arc::new(request);

        let shard_holder = self.shards_holder.read().await;
        let target_shards = shard_holder.select_shards(shard_selection)?;
        let mut all_shard_collection_requests = target_shards
            .into_iter()
            .map(|(shard, shard_key)| {
                // Explicitly borrow `request` and `with_payload`, so we can use them in `async move`
                // block below without unnecessarily cloning anything
                let request = &request;
                let with_payload = &with_payload;

                async move {
                    let mut records = shard
                        .retrieve(
                            request.clone(),
                            with_payload,
                            &request.with_vector,
                            read_consistency,
                            timeout,
                            shard_selection.is_shard_id(),
                        )
                        .await?;

                    if shard_key.is_none() {
                        return Ok(records);
                    }

                    for point in &mut records {
                        point.shard_key.clone_from(&shard_key.cloned());
                    }

                    CollectionResult::Ok(records)
                }
            })
            .collect::<FuturesUnordered<_>>();

        // pre-allocate hashmap with capped capacity to protect from malevolent input
        let mut covered_point_ids = HashMap::with_capacity(ids_len.min(1024));
        while let Some(response) = all_shard_collection_requests.try_next().await? {
            for point in response {
                // Add each point only once, deduplicate point IDs
                covered_point_ids.insert(point.id, point);
            }
        }

        // Collect points in the same order as they were requested
        let points = request
            .ids
            .iter()
            .filter_map(|id| covered_point_ids.remove(id))
            .collect();

        Ok(points)
    }

    pub async fn cleanup_local_shard(&self, shard_id: ShardId) -> CollectionResult<UpdateResult> {
        self.shards_holder
            .read()
            .await
            .cleanup_local_shard(shard_id)
            .await
    }
}
