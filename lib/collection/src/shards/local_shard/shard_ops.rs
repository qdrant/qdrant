use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::Itertools;
use segment::data_types::order_by::{Direction, OrderBy};
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;
use tokio::sync::oneshot;

use crate::collection_manager::holders::segment_holder::LockedSegment;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::common::stopping_guard::StoppingGuard;
use crate::operations::query_enum::QueryEnum;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CoreSearchRequestBatch,
    CountRequestInternal, CountResult, PointRequestInternal, Record, UpdateResult, UpdateStatus,
};
use crate::operations::OperationWithClockTag;
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
        let is_stopped_guard = StoppingGuard::new();

        let (query_context, collection_params) = {
            let collection_config = self.collection_config.read().await;

            let query_context_opt = SegmentsSearcher::prepare_query_context(
                self.segments.clone(),
                &core_request,
                &collection_config,
                &is_stopped_guard,
            )
            .await?;

            let Some(query_context) = query_context_opt else {
                // No segments to search
                return Ok(vec![]);
            };

            (query_context, collection_config.params.clone())
        };

        let search_request = SegmentsSearcher::search(
            Arc::clone(&self.segments),
            Arc::clone(&core_request),
            search_runtime_handle,
            true,
            query_context,
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

    async fn scroll_by_id(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Record>> {
        let segments = self.segments();

        let (non_appendable, appendable) = segments.read().split_segments();

        let read_filtered = |segment: LockedSegment| {
            let filter = filter.cloned();

            search_runtime_handle.spawn_blocking(move || {
                segment
                    .get()
                    .read()
                    .read_filtered(offset, Some(limit), filter.as_ref())
            })
        };

        let non_appendable = try_join_all(non_appendable.into_iter().map(read_filtered)).await?;
        let appendable = try_join_all(appendable.into_iter().map(read_filtered)).await?;

        let point_ids = non_appendable
            .into_iter()
            .chain(appendable)
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

    async fn scroll_by_field(
        &self,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
        order_by: &OrderBy,
    ) -> CollectionResult<Vec<Record>> {
        let segments = self.segments();

        let (non_appendable, appendable) = segments.read().split_segments();

        let read_ordered_filtered = |segment: LockedSegment| {
            let filter = filter.cloned();
            let order_by = order_by.clone();

            search_runtime_handle.spawn_blocking(move || {
                segment
                    .get()
                    .read()
                    .read_ordered_filtered(Some(limit), filter.as_ref(), &order_by)
            })
        };

        let non_appendable =
            try_join_all(non_appendable.into_iter().map(read_ordered_filtered)).await?;
        let appendable = try_join_all(appendable.into_iter().map(read_ordered_filtered)).await?;

        let all_reads = non_appendable
            .into_iter()
            .chain(appendable)
            .collect::<Result<Vec<_>, _>>()?;

        let top_records = all_reads
            .into_iter()
            .kmerge_by(|a, b| match order_by.direction() {
                Direction::Asc => a <= b,
                Direction::Desc => a >= b,
            })
            .dedup()
            .take(limit)
            .collect_vec();

        let with_payload = WithPayload::from(with_payload_interface);

        let point_ids = top_records.iter().map(|(_, id)| *id).collect_vec();

        // Fetch with the requested vector and payload
        let mut records =
            SegmentsSearcher::retrieve(segments, &point_ids, &with_payload, with_vector)?;

        // Add order_by value to the payload. It will be removed in the next step, after crossing the shard boundary.
        records
            .iter_mut()
            .zip(top_records)
            .for_each(|(record, (value, _))| {
                let new_payload =
                    OrderBy::insert_order_value_in_payload(record.payload.take(), value);

                record.payload = Some(new_payload);
            });

        Ok(records)
    }

    /// Ensures that the `available disk space` is at least >= `wal_capacity_mb`.
    ///
    /// This function checks the available disk space on the file system where the specified path is located.
    /// It verifies that there is at least enough free space to meet the WAL buffer size configured for the collection.
    /// Introducing this check helps prevent service disruptions from unexpected disk space exhaustion.
    ///
    /// # Returns
    /// A result indicating success (`Ok(())`) if there is sufficient disk space,
    /// or a `CollectionError::service_error` if the disk space is insufficient or cannot be retrieved.
    ///
    /// # Errors
    /// This function returns an error if:
    /// - The disk space retrieval fails, detailing the failure reason.
    /// - The available space is less than the configured WAL buffer size, specifying both the available and required space.
    async fn ensure_sufficient_disk_space(&self) -> CollectionResult<()> {
        // Offload the synchronous I/O operation to a blocking thread
        let path = self.path.clone();
        let disk_free_space_bytes: u64 =
            tokio::task::spawn_blocking(move || fs4::available_space(path.as_path()))
                .await
                .map_err(|e| {
                    CollectionError::service_error(format!("Failed to join async task: {}", e))
                })?
                .map_err(|err| {
                    CollectionError::service_error(format!(
                        "Failed to get free space for path: {} due to: {}",
                        self.path.as_path().display(),
                        err
                    ))
                })?;
        let disk_buffer_bytes = self
            .collection_config
            .read()
            .await
            .wal_config
            .wal_capacity_mb
            * 1024
            * 1024;

        if disk_free_space_bytes < disk_buffer_bytes.try_into().unwrap_or_default() {
            return Err(CollectionError::service_error(
                "No space left on device: WAL buffer size exceeds available disk space".to_string(),
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl ShardOperation for LocalShard {
    /// Imply interior mutability.
    /// Performs update operation on this collection asynchronously.
    /// Explicitly waits for result to be updated.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn update(
        &self,
        mut operation: OperationWithClockTag,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        // `LocalShard::update` only has a single cancel safe `await`, WAL operations are blocking,
        // and update is applied by a separate task, so, surprisingly, this method is cancel safe. :D

        let (callback_sender, callback_receiver) = if wait {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        self.ensure_sufficient_disk_space().await?;

        let operation_id = {
            let update_sender = self.update_sender.load();
            let channel_permit = update_sender.reserve().await?;

            // It is *critical* to hold `_wal_lock` while sending operation to the update handler!
            //
            // TODO: Refactor `lock_and_write`, so this is less terrible? :/
            let (operation_id, _wal_lock) = match self.wal.lock_and_write(&mut operation).await {
                Ok(id_and_lock) => id_and_lock,

                Err(crate::wal::WalError::ClockRejected) => {
                    // Propagate clock rejection to operation sender
                    return Ok(UpdateResult {
                        operation_id: None,
                        status: UpdateStatus::ClockRejected,
                        clock_tag: operation.clock_tag,
                    });
                }

                Err(err) => return Err(err.into()),
            };

            channel_permit.send(UpdateSignal::Operation(OperationData {
                op_num: operation_id,
                operation: operation.operation,
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
                clock_tag: operation.clock_tag,
            })
        } else {
            Ok(UpdateResult {
                operation_id: Some(operation_id),
                status: UpdateStatus::Acknowledged,
                clock_tag: operation.clock_tag,
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
        match order_by {
            None => {
                self.scroll_by_id(
                    offset,
                    limit,
                    with_payload_interface,
                    with_vector,
                    filter,
                    search_runtime_handle,
                )
                .await
            }
            Some(order_by) => {
                self.scroll_by_field(
                    limit,
                    with_payload_interface,
                    with_vector,
                    filter,
                    search_runtime_handle,
                    order_by,
                )
                .await
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
