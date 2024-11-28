use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::data_types::order_by::OrderBy;
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::time::error::Elapsed;

use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CoreSearchRequestBatch,
    CountRequestInternal, CountResult, PointRequestInternal, RecordInternal, UpdateResult,
    UpdateStatus,
};
use crate::operations::universal_query::planned_query::PlannedQuery;
use crate::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};
use crate::operations::OperationWithClockTag;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::update_handler::{OperationData, UpdateSignal};

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
        // Check write rate limiter before proceeding
        self.check_write_rate_limiter()?;

        // `LocalShard::update` only has a single cancel safe `await`, WAL operations are blocking,
        // and update is applied by a separate task, so, surprisingly, this method is cancel safe. :D

        let (callback_sender, callback_receiver) = if wait {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        if self
            .disk_usage_watcher
            .is_disk_full()
            .await?
            .unwrap_or(false)
        {
            return Err(CollectionError::service_error(
                "No space left on device: WAL buffer size exceeds available disk space".to_string(),
            ));
        }

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
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<RecordInternal>> {
        // Check read rate limiter before proceeding
        self.check_read_rate_limiter()?;
        match order_by {
            None => {
                self.scroll_by_id(
                    offset,
                    limit,
                    with_payload_interface,
                    with_vector,
                    filter,
                    search_runtime_handle,
                    timeout,
                )
                .await
            }
            Some(order_by) => {
                let (mut records, values) = self
                    .scroll_by_field(
                        limit,
                        with_payload_interface,
                        with_vector,
                        filter,
                        search_runtime_handle,
                        order_by,
                        timeout,
                    )
                    .await?;

                records.iter_mut().zip(values).for_each(|(record, value)| {
                    // TODO(1.11): stop inserting the value in the payload, only use the order_value
                    // Add order_by value to the payload. It will be removed in the next step, after crossing the shard boundary.
                    let new_payload =
                        OrderBy::insert_order_value_in_payload(record.payload.take(), value);

                    record.payload = Some(new_payload);
                    record.order_value = Some(value);
                });

                Ok(records)
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
        hw_measurement_acc: &HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        // Check read rate limiter before proceeding
        self.check_read_rate_limiter()?;
        self.do_search(request, search_runtime_handle, timeout, hw_measurement_acc)
            .await
    }

    async fn count(
        &self,
        request: Arc<CountRequestInternal>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        _hw_measurement_acc: &HwMeasurementAcc, // TODO: measure hardware when counting
    ) -> CollectionResult<CountResult> {
        // Check read rate limiter before proceeding
        self.check_read_rate_limiter()?;
        let total_count = if request.exact {
            let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);
            let all_points = tokio::time::timeout(
                timeout,
                self.read_filtered(request.filter.as_ref(), search_runtime_handle),
            )
            .await
            .map_err(|_: Elapsed| {
                CollectionError::timeout(timeout.as_secs() as usize, "count")
            })??;
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
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<RecordInternal>> {
        // Check read rate limiter before proceeding
        self.check_read_rate_limiter()?;
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);
        let records_map = tokio::time::timeout(
            timeout,
            SegmentsSearcher::retrieve(
                self.segments.clone(),
                &request.ids,
                with_payload,
                with_vector,
                search_runtime_handle,
            ),
        )
        .await
        .map_err(|_: Elapsed| CollectionError::timeout(timeout.as_secs() as usize, "retrieve"))??;

        let ordered_records = request
            .ids
            .iter()
            .filter_map(|point| records_map.get(point).cloned())
            .collect();

        Ok(ordered_records)
    }

    async fn query_batch(
        &self,
        requests: Arc<Vec<ShardQueryRequest>>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: &HwMeasurementAcc,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        // Check read rate limiter before proceeding
        self.check_read_rate_limiter()?;
        let planned_query = PlannedQuery::try_from(requests.as_ref().to_owned())?;

        self.do_planned_query(
            planned_query,
            search_runtime_handle,
            timeout,
            hw_measurement_acc,
        )
        .await
    }

    async fn facet(
        &self,
        request: Arc<FacetParams>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<FacetResponse> {
        // Check read rate limiter before proceeding
        self.check_read_rate_limiter()?;
        let hits = if request.exact {
            self.exact_facet(request, search_runtime_handle, timeout)
                .await?
        } else {
            self.approx_facet(request, search_runtime_handle, timeout)
                .await?
        };
        Ok(FacetResponse { hits })
    }
}
