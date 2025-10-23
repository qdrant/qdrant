use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use futures::future::try_join_all;
use itertools::Itertools as _;
use rand::distr::weighted::WeightedIndex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use segment::data_types::order_by::{Direction, OrderBy};
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use shard::retrieve::record_internal::RecordInternal;
use tokio::runtime::Handle;

use super::LocalShard;
use crate::collection_manager::holders::segment_holder::LockedSegment;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::common::stopping_guard::StoppingGuard;
use crate::operations::types::{
    CollectionError, CollectionResult, QueryScrollRequestInternal, ScrollOrder,
};

impl LocalShard {
    /// Basic parallel batching, it is conveniently used for the universal query API.
    pub(super) async fn query_scroll_batch(
        &self,
        batch: Arc<Vec<QueryScrollRequestInternal>>,
        search_runtime_handle: &Handle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        if batch.is_empty() {
            return Ok(vec![]);
        }

        let scrolls = batch.iter().map(|request| {
            self.query_scroll(
                request,
                search_runtime_handle,
                Some(timeout),
                hw_measurement_acc.clone(),
            )
        });

        // execute all the scrolls concurrently
        let all_scroll_results = try_join_all(scrolls);
        tokio::time::timeout(timeout, all_scroll_results)
            .await
            .map_err(|_| {
                log::debug!(
                    "Query scroll timeout reached: {} seconds",
                    timeout.as_secs()
                );
                CollectionError::timeout(timeout.as_secs() as usize, "Query scroll")
            })?
    }

    /// Scroll a single page, to be used for the universal query API only.
    async fn query_scroll(
        &self,
        request: &QueryScrollRequestInternal,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let QueryScrollRequestInternal {
            limit,
            with_vector,
            filter,
            scroll_order,
            with_payload,
        } = request;

        let limit = *limit;

        let offset_id = None;

        let record_results = match scroll_order {
            ScrollOrder::ById => {
                self.scroll_by_id(
                    offset_id,
                    limit,
                    with_payload,
                    with_vector,
                    filter.as_ref(),
                    search_runtime_handle,
                    timeout,
                    hw_measurement_acc,
                )
                .await?
            }
            ScrollOrder::ByField(order_by) => {
                self.scroll_by_field(
                    limit,
                    with_payload,
                    with_vector,
                    filter.as_ref(),
                    search_runtime_handle,
                    order_by,
                    timeout,
                    hw_measurement_acc,
                )
                .await?
            }
            ScrollOrder::Random => {
                self.scroll_randomly(
                    limit,
                    with_payload,
                    with_vector,
                    filter.as_ref(),
                    search_runtime_handle,
                    timeout,
                    hw_measurement_acc,
                )
                .await?
            }
        };

        let point_results = record_results
            .into_iter()
            .map(|record| ScoredPoint {
                id: record.id,
                version: 0,
                score: 1.0,
                payload: record.payload,
                vector: record.vector,
                shard_key: record.shard_key,
                order_value: record.order_value,
            })
            .collect();

        Ok(point_results)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn scroll_by_id(
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
        let start = Instant::now();
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);
        let stopping_guard = StoppingGuard::new();
        let segments = self.segments.clone();

        let update_operation_lock = self.update_operation_lock.read().await;
        let (non_appendable, appendable) = segments.read().split_segments();

        let read_filtered = |segment: LockedSegment, hw_counter: HardwareCounterCell| {
            let filter = filter.cloned();
            let is_stopped = stopping_guard.get_is_stopped();
            search_runtime_handle.spawn_blocking(move || {
                segment.get().read().read_filtered(
                    offset,
                    Some(limit),
                    filter.as_ref(),
                    &is_stopped,
                    &hw_counter,
                )
            })
        };

        let hw_counter = hw_measurement_acc.get_counter_cell();
        let all_reads = tokio::time::timeout(
            timeout,
            try_join_all(
                non_appendable
                    .into_iter()
                    .chain(appendable)
                    .map(|segment| read_filtered(segment, hw_counter.fork())),
            ),
        )
        .await
        .map_err(|_| CollectionError::timeout(timeout.as_secs() as usize, "scroll_by_id"))??;

        let point_ids = all_reads
            .into_iter()
            .flatten()
            .sorted()
            .dedup()
            .take(limit)
            .collect_vec();

        let with_payload = WithPayload::from(with_payload_interface);
        // update timeout
        let timeout = timeout.saturating_sub(start.elapsed());
        let mut records_map = tokio::time::timeout(
            timeout,
            SegmentsSearcher::retrieve(
                segments,
                &point_ids,
                &with_payload,
                with_vector,
                search_runtime_handle,
                hw_measurement_acc,
            ),
        )
        .await
        .map_err(|_| CollectionError::timeout(timeout.as_secs() as usize, "retrieve"))??;

        drop(update_operation_lock);

        let ordered_records = point_ids
            .iter()
            // Use remove to avoid cloning, we take each point ID only once
            .filter_map(|point_id| records_map.remove(point_id))
            .collect();

        Ok(ordered_records)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn scroll_by_field(
        &self,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
        order_by: &OrderBy,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        let start = Instant::now();
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);
        let stopping_guard = StoppingGuard::new();
        let segments = self.segments.clone();

        let update_operation_lock = self.update_operation_lock.read().await;
        let (non_appendable, appendable) = segments.read().split_segments();

        let read_ordered_filtered = |segment: LockedSegment, hw_counter: &HardwareCounterCell| {
            let is_stopped = stopping_guard.get_is_stopped();
            let filter = filter.cloned();
            let order_by = order_by.clone();

            let hw_counter = hw_counter.fork();
            search_runtime_handle.spawn_blocking(move || {
                segment.get().read().read_ordered_filtered(
                    Some(limit),
                    filter.as_ref(),
                    &order_by,
                    &is_stopped,
                    &hw_counter,
                )
            })
        };

        let hw_counter = hw_measurement_acc.get_counter_cell();

        let all_reads = tokio::time::timeout(
            timeout,
            try_join_all(
                non_appendable
                    .into_iter()
                    .chain(appendable)
                    .map(|segment| read_ordered_filtered(segment, &hw_counter)),
            ),
        )
        .await
        .map_err(|_| CollectionError::timeout(timeout.as_secs() as usize, "scroll_by_field"))??;

        let all_reads = all_reads.into_iter().collect::<Result<Vec<_>, _>>()?;

        let (values, point_ids): (Vec<_>, Vec<_>) = all_reads
            .into_iter()
            .kmerge_by(|a, b| match order_by.direction() {
                Direction::Asc => a <= b,
                Direction::Desc => a >= b,
            })
            .dedup()
            .take(limit)
            .unzip();

        let with_payload = WithPayload::from(with_payload_interface);

        // update timeout
        let timeout = timeout.saturating_sub(start.elapsed());

        // Fetch with the requested vector and payload
        let records_map = tokio::time::timeout(
            timeout,
            SegmentsSearcher::retrieve(
                segments,
                &point_ids,
                &with_payload,
                with_vector,
                search_runtime_handle,
                hw_measurement_acc,
            ),
        )
        .await
        .map_err(|_| CollectionError::timeout(timeout.as_secs() as usize, "retrieve"))??;

        drop(update_operation_lock);

        let ordered_records = point_ids
            .iter()
            .zip(values)
            .filter_map(|(point_id, value)| {
                let mut record = records_map.get(point_id).cloned()?;
                record.order_value = Some(value);
                Some(record)
            })
            .collect();

        Ok(ordered_records)
    }

    #[allow(clippy::too_many_arguments)]
    async fn scroll_randomly(
        &self,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        let start = Instant::now();
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);
        let stopping_guard = StoppingGuard::new();
        let segments = self.segments.clone();

        let update_operation_lock = self.update_operation_lock.read().await;
        let (non_appendable, appendable) = segments.read().split_segments();

        let read_filtered = |segment: LockedSegment, hw_counter: &HardwareCounterCell| {
            let is_stopped = stopping_guard.get_is_stopped();
            let filter = filter.cloned();

            let hw_counter = hw_counter.fork();
            search_runtime_handle.spawn_blocking(move || {
                let get_segment = segment.get();
                let read_segment = get_segment.read();

                (
                    read_segment.available_point_count(),
                    read_segment.read_random_filtered(
                        limit,
                        filter.as_ref(),
                        &is_stopped,
                        &hw_counter,
                    ),
                )
            })
        };

        let hw_counter = hw_measurement_acc.get_counter_cell();

        let all_reads = tokio::time::timeout(
            timeout,
            try_join_all(
                non_appendable
                    .into_iter()
                    .chain(appendable)
                    .map(|segment| read_filtered(segment, &hw_counter)),
            ),
        )
        .await
        .map_err(|_| CollectionError::timeout(timeout.as_secs() as usize, "scroll_randomly"))??;

        let (availability, mut segments_reads): (Vec<_>, Vec<_>) = all_reads.into_iter().unzip();

        // Shortcut if all segments are empty
        if availability.iter().all(|&count| count == 0) {
            return Ok(Vec::new());
        }
        // Select points in a weighted fashion from each segment, depending on how many points each segment has.
        let distribution = WeightedIndex::new(availability).map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to create weighted index for random scroll: {err:?}"
            ))
        })?;

        let mut rng = StdRng::from_os_rng();
        let mut random_points = HashSet::with_capacity(limit);

        // Randomly sample points in two stages
        //
        // 1. This loop iterates <= LIMIT times, and either breaks early if we
        // have enough points, or if some of the segments are exhausted.
        //
        // 2. If the segments are exhausted, we will fill up the rest of the
        // points from other segments. In total, the complexity is guaranteed to
        // be O(limit).
        while random_points.len() < limit {
            let segment_offset = rng.sample(&distribution);
            let points = segments_reads.get_mut(segment_offset).unwrap();
            if let Some(point) = points.pop() {
                random_points.insert(point);
            } else {
                // It seems that some segments are empty early,
                // so distribution does not make sense anymore.
                // This is only possible if segments size < limit.
                break;
            }
        }

        // If we still need more points, we will get them from the rest of the segments.
        // This is a rare case, as it seems we don't have enough points in individual segments.
        // Therefore, we can ignore "proper" distribution, as it won't be accurate anyway.
        if random_points.len() < limit {
            let rest_points = segments_reads.into_iter().flatten();
            for point in rest_points {
                random_points.insert(point);
                if random_points.len() >= limit {
                    break;
                }
            }
        }

        let selected_points: Vec<_> = random_points.into_iter().collect();

        let with_payload = WithPayload::from(with_payload_interface);
        // update timeout
        let timeout = timeout.saturating_sub(start.elapsed());
        let records_map = tokio::time::timeout(
            timeout,
            SegmentsSearcher::retrieve(
                segments,
                &selected_points,
                &with_payload,
                with_vector,
                search_runtime_handle,
                hw_measurement_acc,
            ),
        )
        .await
        .map_err(|_| CollectionError::timeout(timeout.as_secs() as usize, "retrieve"))??;

        drop(update_operation_lock);

        Ok(records_map.into_values().collect())
    }
}
