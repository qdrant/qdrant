use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::{Either, Itertools};
use ordered_float::OrderedFloat;
use segment::types::{
    Direction, ExtendedPointId, Filter, OrderBy, PayloadContainer, ScoredPoint, WithPayload,
    WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;
use tokio::sync::oneshot;

use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::common::stopping_guard::StoppingGuard;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CoreSearchRequestBatch,
    CountRequestInternal, CountResult, PointRequestInternal, QueryEnum, Record, UpdateResult,
    UpdateStatus,
};
use crate::operations::CollectionUpdateOperations;
use crate::optimizers_builder::DEFAULT_INDEXING_THRESHOLD_KB;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::update_handler::{OperationData, UpdateSignal};

impl LocalShard {
    async fn do_search(
        &self,
        core_request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let (collection_params, indexing_threshold_kb, full_scan_threshold_kb) = {
            let collection_config = self.collection_config.read().await;
            (
                collection_config.params.clone(),
                collection_config
                    .optimizer_config
                    .indexing_threshold
                    .unwrap_or(DEFAULT_INDEXING_THRESHOLD_KB),
                collection_config.hnsw_config.full_scan_threshold,
            )
        };

        // check vector names existing
        for req in &core_request.searches {
            collection_params.get_distance(req.query.get_vector_name())?;
        }

        let is_stopped = StoppingGuard::new();

        let search_request = SegmentsSearcher::search(
            Arc::clone(&self.segments),
            Arc::clone(&core_request),
            search_runtime_handle,
            true,
            is_stopped.get_is_stopped(),
            indexing_threshold_kb.max(full_scan_threshold_kb),
        );

        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);

        let res = tokio::time::timeout(timeout, search_request)
            .await
            .map_err(|_| {
                log::debug!("Search timeout reached: {} seconds", timeout.as_secs());
                // StoppingGuard takes care of setting is_stopped to true
                CollectionError::timeout(timeout.as_secs() as usize, "Search")
            })??;

        let top_results = res
            .into_iter()
            .zip(core_request.searches.iter())
            .map(|(vector_res, req)| {
                let vector_name = req.query.get_vector_name();
                let distance = collection_params.get_distance(vector_name).unwrap();
                let processed_res = vector_res.into_iter().map(|mut scored_point| {
                    match req.query {
                        QueryEnum::Nearest(_) => {
                            scored_point.score = distance.postprocess_score(scored_point.score);
                        }
                        // Don't post-process if we are dealing with custom scoring
                        QueryEnum::RecommendBestScore(_)
                        | QueryEnum::Discover(_)
                        | QueryEnum::Context(_) => {}
                    };
                    scored_point
                });

                if let Some(threshold) = req.score_threshold {
                    processed_res
                        .take_while(|scored_point| {
                            distance.check_threshold(scored_point.score, threshold)
                        })
                        .collect()
                } else {
                    processed_res.collect()
                }
            })
            .collect();
        Ok(top_results)
    }
}
#[async_trait]
impl ShardOperation for LocalShard {
    /// Imply interior mutability.
    /// Performs update operation on this collection asynchronously.
    /// Explicitly waits for result to be updated.
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let (callback_sender, callback_receiver) = if wait {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let operation_id = {
            let update_sender = self.update_sender.load();
            let channel_permit = update_sender.reserve().await?;
            let mut wal_lock = self.wal.lock();
            let operation_id = wal_lock.write(&operation)?;
            channel_permit.send(UpdateSignal::Operation(OperationData {
                op_num: operation_id,
                operation,
                sender: callback_sender,
                wait,
            }));
            operation_id
        };

        if let Some(receiver) = callback_receiver {
            let _res = receiver.await??;
            Ok(UpdateResult {
                operation_id: Some(operation_id),
                status: UpdateStatus::Completed,
            })
        } else {
            Ok(UpdateResult {
                operation_id: Some(operation_id),
                status: UpdateStatus::Acknowledged,
            })
        }
    }

    async fn scroll_by(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
        order_by: Option<&OrderBy>,
    ) -> CollectionResult<Vec<Record>> {
        // ToDo: Make faster points selection with a set
        let segments = self.segments();
        let read_handles: Vec<_> = {
            let segments_guard = segments.read();
            segments_guard
                .iter()
                .map(|(_, segment)| {
                    let segment = segment.clone();
                    let filter = filter.cloned();

                    match order_by {
                        None => search_runtime_handle.spawn_blocking(move || {
                            Ok(segment.get().read().read_filtered(
                                offset,
                                Some(limit),
                                filter.as_ref(),
                            ))
                        }),
                        Some(order_by) => {
                            let order_by = order_by.clone();

                            search_runtime_handle.spawn_blocking(move || {
                                segment.get().read().read_ordered_filtered(
                                    Some(limit),
                                    filter.as_ref(),
                                    &order_by,
                                )
                            })
                        }
                    }
                })
                .collect()
        };

        let all_points = try_join_all(read_handles).await?;

        let with_order_by_payload: WithPayload;

        match order_by {
            None => {
                let point_ids = all_points
                    .into_iter()
                    .flatten()
                    .flatten()
                    .sorted()
                    .dedup()
                    .take(limit)
                    .collect_vec();
                let with_payload = WithPayload::from(with_payload_interface);

                let mut points =
                    SegmentsSearcher::retrieve(segments, &point_ids, &with_payload, with_vector)?;

                points.sort_by_key(|point| point.id);

                Ok(points)
            }
            Some(order_by) => {
                let preliminary_ids = all_points.into_iter().flatten().flatten().collect_vec();
                with_order_by_payload = (&WithPayloadInterface::Fields(vec![order_by.key.clone()])).into();

                // Fetch values to add to the internal `ordered_by` field of each record
                let points_with_order_key = SegmentsSearcher::retrieve(
                    segments,
                    &preliminary_ids,
                    &with_order_by_payload,
                    &false.into(),
                )?;

                let direction = order_by.direction.unwrap_or_default();
                let default_value = match direction {
                    Direction::Asc => f64::INFINITY,
                    Direction::Desc => f64::NEG_INFINITY,
                };

                let sorted_iter = points_with_order_key
                    .into_iter()
                    .map(|mut record| {
                        // Extract number value from payload
                        let order_value = record
                            .payload
                            .as_ref()
                            .map(|payload| payload.get_value(&order_by.key))
                            .and_then(|multi_value| {
                                multi_value.values().iter().find_map(|v| v.as_f64())
                            })
                            .unwrap_or(default_value);
                        record.payload = None;
                        record.ordered_by = Some(OrderedFloat(order_value));
                        record
                    })
                    .sorted_unstable_by_key(|record| (record.ordered_by, record.id));

                let sorted = match direction {
                    Direction::Asc => Either::Left(sorted_iter),
                    Direction::Desc => Either::Right(sorted_iter.rev()),
                }
                .dedup()
                .collect_vec();

                // Find whether we have an offset position to cut from
                let offset_position = offset
                    .and_then(|offset| sorted.iter().find_position(|record| record.id == offset))
                    .map(|(position, _)| position);

                let top_records = match offset_position {
                    None => Either::Left(sorted.into_iter()),
                    Some(position) => Either::Right(sorted.into_iter().skip(position)),
                }
                .take(limit)
                .collect_vec();

                let with_payload = WithPayload::from(with_payload_interface);

                // Fetch with the actual requested payload and vector
                let points =
                    if with_payload == with_order_by_payload && !with_vector.is_enabled() {
                        top_records
                    } else if with_payload.enable || with_vector.is_enabled() {
                        let point_ids = top_records.iter().map(|record| record.id).collect_vec();
                        let mut points = SegmentsSearcher::retrieve(
                            segments,
                            &point_ids,
                            &with_payload,
                            with_vector,
                        )?;
                        points
                            .iter_mut()
                            .zip(top_records)
                            .for_each(|(point, record)| {
                                point.ordered_by = record.ordered_by;
                            });
                        points
                    } else {
                        top_records
                            .into_iter()
                            .map(|record| Record {
                                id: record.id,
                                payload: None,
                                vector: None,
                                shard_key: record.shard_key,
                                ordered_by: record.ordered_by,
                            })
                            .collect_vec()
                    };

                Ok(points)
            }
        }
    }

    /// Collect overview information about the shard
    async fn info(&self) -> CollectionResult<CollectionInfo> {
        Ok(self.local_shard_info().await.into())
    }

    async fn core_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        self.do_search(request, search_runtime_handle, timeout)
            .await
    }

    async fn count(&self, request: Arc<CountRequestInternal>) -> CollectionResult<CountResult> {
        let total_count = if request.exact {
            let all_points = self.read_filtered(request.filter.as_ref())?;
            all_points.len()
        } else {
            self.estimate_cardinality(request.filter.as_ref())?.exp
        };
        Ok(CountResult { count: total_count })
    }

    async fn retrieve(
        &self,
        request: Arc<PointRequestInternal>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
    ) -> CollectionResult<Vec<Record>> {
        SegmentsSearcher::retrieve(self.segments(), &request.ids, with_payload, with_vector)
    }
}
