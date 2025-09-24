use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::data_types::order_by::OrderBy;
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use shard::retrieve::record_internal::RecordInternal;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio::time::error::Elapsed;

use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::OperationWithClockTag;
use crate::operations::generalizer::Generalizer;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CoreSearchRequestBatch,
    CountRequestInternal, CountResult, PointRequestInternal, ScrollRequestInternal, UpdateResult,
    UpdateStatus,
};
use crate::operations::universal_query::planned_query::PlannedQuery;
use crate::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};
use crate::operations::verification::operation_rate_cost::{BASE_COST, filter_rate_cost};
use crate::profiling::interface::log_request_to_collector;
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
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<UpdateResult> {
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

                Err(shard::wal::WalError::ClockRejected) => {
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
                hw_measurements: hw_measurement_acc.clone(),
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

    /// This call is rate limited by the read rate limiter.
    async fn scroll_by(
        &self,
        request: Arc<ScrollRequestInternal>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        let ScrollRequestInternal {
            offset,
            limit,
            filter,
            with_payload,
            with_vector,
            order_by,
        } = request.as_ref();

        let default_with_payload = ScrollRequestInternal::default_with_payload();

        // Validate user did not try to use an id offset with order_by
        if order_by.is_some() && offset.is_some() {
            return Err(CollectionError::bad_input("Cannot use an `offset` when using `order_by`. The alternative for paging is to use `order_by.start_from` and a filter to exclude the IDs that you've already seen for the `order_by.start_from` value".to_string()));
        };

        // Check read rate limiter before proceeding
        self.check_read_rate_limiter(&hw_measurement_acc, "scroll_by", || {
            let mut cost = BASE_COST;
            if let Some(filter) = &filter {
                cost += filter_rate_cost(filter);
            }
            cost
        })?;
        let start_time = Instant::now();

        let limit = limit.unwrap_or(ScrollRequestInternal::default_limit());
        let order_by = order_by.clone().map(OrderBy::from);

        let result = match order_by {
            None => {
                self.internal_scroll_by_id(
                    *offset,
                    limit,
                    with_payload.as_ref().unwrap_or(&default_with_payload),
                    with_vector,
                    filter.as_ref(),
                    search_runtime_handle,
                    timeout,
                    hw_measurement_acc,
                )
                .await?
            }
            Some(order_by) => {
                self.internal_scroll_by_field(
                    limit,
                    with_payload.as_ref().unwrap_or(&default_with_payload),
                    with_vector,
                    filter.as_ref(),
                    search_runtime_handle,
                    &order_by,
                    timeout,
                    hw_measurement_acc,
                )
                .await?
            }
        };

        let elapsed = start_time.elapsed();
        log_request_to_collector(&self.collection_name, elapsed, || request);
        Ok(result)
    }

    async fn local_scroll_by_id(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        self.internal_scroll_by_id(
            offset,
            limit,
            with_payload_interface,
            with_vector,
            filter,
            search_runtime_handle,
            timeout,
            hw_measurement_acc,
        )
        .await
    }

    /// Collect overview information about the shard
    async fn info(&self) -> CollectionResult<CollectionInfo> {
        Ok(CollectionInfo::from(self.local_shard_info().await))
    }

    /// This call is rate limited by the read rate limiter.
    async fn core_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        // Check read rate limiter before proceeding
        self.check_read_rate_limiter(&hw_measurement_acc, "core_search", || {
            request.searches.iter().map(|s| s.search_rate_cost()).sum()
        })?;
        self.do_search(request, search_runtime_handle, timeout, hw_measurement_acc)
            .await
    }

    /// This call is rate limited by the read rate limiter.
    async fn count(
        &self,
        request: Arc<CountRequestInternal>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<CountResult> {
        // Check read rate limiter before proceeding
        self.check_read_rate_limiter(&hw_measurement_acc, "count", || {
            let mut cost = BASE_COST;
            if let Some(filter) = &request.filter {
                cost += filter_rate_cost(filter);
            }
            cost
        })?;
        let start_time = Instant::now();
        let total_count = if request.exact {
            let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);
            let all_points = tokio::time::timeout(
                timeout,
                self.read_filtered(
                    request.filter.as_ref(),
                    search_runtime_handle,
                    hw_measurement_acc,
                ),
            )
            .await
            .map_err(|_: Elapsed| {
                CollectionError::timeout(timeout.as_secs() as usize, "count")
            })??;
            all_points.len()
        } else {
            self.estimate_cardinality(request.filter.as_ref(), &hw_measurement_acc)
                .await?
                .exp
        };
        let elapsed = start_time.elapsed();
        log_request_to_collector(&self.collection_name, elapsed, || request);
        Ok(CountResult { count: total_count })
    }

    /// This call is rate limited by the read rate limiter.
    async fn retrieve(
        &self,
        request: Arc<PointRequestInternal>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        // Check read rate limiter before proceeding
        self.check_read_rate_limiter(&hw_measurement_acc, "retrieve", || request.ids.len())?;
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);

        let start_time = Instant::now();
        let records_map = tokio::time::timeout(
            timeout,
            SegmentsSearcher::retrieve(
                self.segments.clone(),
                &request.ids,
                with_payload,
                with_vector,
                search_runtime_handle,
                hw_measurement_acc,
            ),
        )
        .await
        .map_err(|_: Elapsed| CollectionError::timeout(timeout.as_secs() as usize, "retrieve"))??;

        let ordered_records = request
            .ids
            .iter()
            .filter_map(|point| records_map.get(point).cloned())
            .collect();

        let elapsed = start_time.elapsed();
        log_request_to_collector(&self.collection_name, elapsed, || request);

        Ok(ordered_records)
    }

    /// This call is rate limited by the read rate limiter.
    async fn query_batch(
        &self,
        requests: Arc<Vec<ShardQueryRequest>>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        let start_time = Instant::now();
        let planned_query = PlannedQuery::try_from(requests.as_ref().to_owned())?;

        // Check read rate limiter before proceeding
        self.check_read_rate_limiter(&hw_measurement_acc, "query_batch", || {
            planned_query
                .searches
                .iter()
                .map(|s| s.search_rate_cost())
                .chain(planned_query.scrolls.iter().map(|s| s.scroll_rate_cost()))
                .sum()
        })?;

        let result = self
            .do_planned_query(
                planned_query,
                search_runtime_handle,
                timeout,
                hw_measurement_acc,
            )
            .await;

        let elapsed = start_time.elapsed();
        log_request_to_collector(&self.collection_name, elapsed, || requests.remove_details());

        result
    }

    /// This call is rate limited by the read rate limiter.
    async fn facet(
        &self,
        request: Arc<FacetParams>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<FacetResponse> {
        // Check read rate limiter before proceeding
        self.check_read_rate_limiter(&hw_measurement_acc, "facet", || {
            let mut cost = BASE_COST;
            if let Some(filter) = &request.filter {
                cost += filter_rate_cost(filter);
            }
            cost
        })?;

        let start_time = Instant::now();
        let hits = if request.exact {
            self.exact_facet(
                request.clone(),
                search_runtime_handle,
                timeout,
                hw_measurement_acc,
            )
            .await?
        } else {
            self.approx_facet(
                request.clone(),
                search_runtime_handle,
                timeout,
                hw_measurement_acc,
            )
            .await?
        };
        let elapsed = start_time.elapsed();
        log_request_to_collector(&self.collection_name, elapsed, || request);
        Ok(FacetResponse { hits })
    }
}
