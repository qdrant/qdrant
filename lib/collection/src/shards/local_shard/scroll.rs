use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::{AHashMap, AHashSet};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use futures::future::try_join_all;
use itertools::Itertools as _;
use rand::RngExt;
use rand::distr::weighted::WeightedIndex;
use rand::rngs::StdRng;
use segment::common::operation_error::OperationResult;
use segment::data_types::order_by::{Direction, OrderBy};
use segment::types::{
    Condition, ExtendedPointId, Filter, HasIdCondition, ScoredPoint, WithPayload,
    WithPayloadInterface, WithVector,
};
use shard::common::stopping_guard::StoppingGuard;
use shard::operations::point_ops::PointStructRawPersisted;
use shard::retrieve::record_internal::RecordInternal;
use tokio_util::task::AbortOnDropHandle;

use super::LocalShard;
use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
use crate::collection_manager::provenance::Routes;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::types::{
    CollectionError, CollectionResult, QueryScrollRequestInternal, ScrollOrder,
};

impl LocalShard {
    /// Basic parallel batching, it is conveniently used for the universal query API.
    /// `routes` restricts the scroll to the segments that produced the
    /// candidates of a rescore stage, each one filtered to its own ids. It
    /// applies to a single-request batch, whose filter is exactly the `has_id`
    /// over the union of the routes.
    pub(super) async fn query_scroll_batch(
        &self,
        batch: Arc<Vec<QueryScrollRequestInternal>>,
        search_runtime_handle: &AdaptiveSearchHandle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
        routes: Option<&Routes>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        if batch.is_empty() {
            return Ok(vec![]);
        }
        debug_assert!(
            routes.is_none() || batch.len() == 1,
            "routes address the candidates of a single request",
        );

        let scrolls = batch.iter().map(|request| {
            self.query_scroll(
                request,
                search_runtime_handle,
                timeout,
                hw_measurement_acc.clone(),
                routes,
            )
        });

        // execute all the scrolls concurrently
        let all_scroll_results = try_join_all(scrolls);
        tokio::time::timeout(timeout, all_scroll_results)
            .await
            .map_err(|_| {
                log::debug!("Query scroll timeout reached: {timeout:?}");
                CollectionError::timeout(timeout, "Query scroll")
            })?
    }

    /// Scroll a single page, to be used for the universal query API only.
    async fn query_scroll(
        &self,
        request: &QueryScrollRequestInternal,
        search_runtime_handle: &AdaptiveSearchHandle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
        routes: Option<&Routes>,
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
                // Scrolling by id is only a prefetch source, never a rescore
                // stage, so there is nothing to route it with.
                debug_assert!(routes.is_none());
                self.internal_scroll_by_id(
                    offset_id,
                    limit,
                    with_payload,
                    with_vector,
                    filter.as_ref(),
                    search_runtime_handle,
                    timeout,
                    hw_measurement_acc,
                    DeferredBehavior::VisibleOnly,
                )
                .await?
            }
            ScrollOrder::ByField(order_by) => {
                self.internal_scroll_by_field(
                    limit,
                    with_payload,
                    with_vector,
                    filter.as_ref(),
                    routes,
                    search_runtime_handle,
                    order_by,
                    timeout,
                    hw_measurement_acc,
                    DeferredBehavior::VisibleOnly,
                )
                .await?
            }
            ScrollOrder::Random => {
                self.scroll_randomly(
                    limit,
                    with_payload,
                    with_vector,
                    filter.as_ref(),
                    routes,
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
    pub async fn internal_scroll_by_id(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &AdaptiveSearchHandle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
        deferred_behavior: DeferredBehavior,
    ) -> CollectionResult<Vec<RecordInternal>> {
        let start = Instant::now();
        let stopping_guard = StoppingGuard::new();
        let update_operation_lock = self.update_operation_lock.read().await;
        let segments = self.segments.clone();
        let (non_appendable, appendable) = {
            let Some(segments_guard) = segments.try_read_for(timeout) else {
                return Err(CollectionError::timeout(timeout, "internal_scroll_by_id"));
            };
            segments_guard.split_segments()
        };
        let read_filtered = |segment: LockedSegment, hw_counter: HardwareCounterCell| {
            let filter = filter.cloned();
            let is_stopped = stopping_guard.get_is_stopped();
            let cpu_utilization = hw_counter.cpu_utilization();
            let task = search_runtime_handle.spawn_blocking(move || -> OperationResult<_> {
                let work = || {
                    segment.get().read().read_filtered(
                        offset,
                        Some(limit),
                        filter.as_ref(),
                        &is_stopped,
                        &hw_counter,
                        deferred_behavior,
                    )
                };
                match cpu_utilization {
                    Some(cu) => cu.measure(work),
                    None => work(),
                }
            });
            AbortOnDropHandle::new(task)
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
        .map_err(|_| CollectionError::timeout(timeout, "scroll_by_id"))??;

        let point_ids = all_reads
            .into_iter()
            .process_results(|iter| iter.flatten().sorted().dedup().take(limit).collect_vec())?;

        let with_payload = WithPayload::from(with_payload_interface);
        // update timeout
        let timeout = timeout.saturating_sub(start.elapsed());
        let mut records_map = self
            .scroll_records(
                &point_ids,
                &with_payload,
                with_vector,
                search_runtime_handle,
                timeout,
                hw_measurement_acc,
                deferred_behavior,
            )
            .await?;

        drop(update_operation_lock);

        let ordered_records = point_ids
            .iter()
            // Use remove to avoid cloning, we take each point ID only once
            .filter_map(|point_id| records_map.remove(point_id))
            .collect();

        Ok(ordered_records)
    }

    /// Byte-blob analogue of [`Self::internal_scroll_by_id`]: reads points as
    /// storage-native raw vector bytes ([`PointStructRawPersisted`]) instead of
    /// decoded records, avoiding a lossy quantization round-trip during shard
    /// transfer. Point-id selection is identical to the decoded twin.
    #[allow(clippy::too_many_arguments)]
    pub async fn internal_scroll_by_id_raw(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &AdaptiveSearchHandle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
        deferred_behavior: DeferredBehavior,
    ) -> CollectionResult<Vec<PointStructRawPersisted>> {
        let start = Instant::now();
        let stopping_guard = StoppingGuard::new();
        let update_operation_lock = self.update_operation_lock.read().await;
        let segments = self.segments.clone();
        let (non_appendable, appendable) = {
            let Some(segments_guard) = segments.try_read_for(timeout) else {
                return Err(CollectionError::timeout(
                    timeout,
                    "internal_scroll_by_id_raw",
                ));
            };
            segments_guard.split_segments()
        };
        let read_filtered = |segment: LockedSegment, hw_counter: HardwareCounterCell| {
            let filter = filter.cloned();
            let is_stopped = stopping_guard.get_is_stopped();
            let cpu_utilization = hw_counter.cpu_utilization();
            let task = search_runtime_handle.spawn_blocking(move || -> OperationResult<_> {
                let work = || {
                    segment.get().read().read_filtered(
                        offset,
                        Some(limit),
                        filter.as_ref(),
                        &is_stopped,
                        &hw_counter,
                        deferred_behavior,
                    )
                };
                match cpu_utilization {
                    Some(cu) => cu.measure(work),
                    None => work(),
                }
            });
            AbortOnDropHandle::new(task)
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
        .map_err(|_| CollectionError::timeout(timeout, "scroll_by_id_raw"))??;

        let point_ids = all_reads
            .into_iter()
            .process_results(|iter| iter.flatten().sorted().dedup().take(limit).collect_vec())?;

        // update timeout
        let timeout = timeout.saturating_sub(start.elapsed());
        let mut records_map = tokio::time::timeout(
            timeout,
            SegmentsSearcher::retrieve_raw(
                segments,
                &point_ids,
                with_vector,
                search_runtime_handle,
                timeout,
                hw_measurement_acc,
                deferred_behavior,
            ),
        )
        .await
        .map_err(|_| CollectionError::timeout(timeout, "retrieve_raw"))??;

        drop(update_operation_lock);

        let ordered_records = point_ids
            .iter()
            // Use remove to avoid cloning, we take each point ID only once
            .filter_map(|point_id| records_map.remove(point_id))
            .map(PointStructRawPersisted::from)
            .collect();

        Ok(ordered_records)
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    pub async fn internal_scroll_by_field(
        &self,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        routes: Option<&Routes>,
        search_runtime_handle: &AdaptiveSearchHandle,
        order_by: &OrderBy,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
        deferred_behavior: DeferredBehavior,
    ) -> CollectionResult<Vec<RecordInternal>> {
        let start = Instant::now();
        let stopping_guard = StoppingGuard::new();
        let segments = self.segments.clone();

        let update_operation_lock = self.update_operation_lock.read().await;
        let segments_to_read = {
            let Some(segments_guard) = segments.try_read_for(timeout) else {
                return Err(CollectionError::timeout(
                    timeout,
                    "internal_scroll_by_field",
                ));
            };
            segments_to_read(&segments_guard, filter, routes)
        };

        let read_ordered_filtered =
            |(segment, filter): (LockedSegment, Option<Filter>),
             hw_counter: &HardwareCounterCell| {
                let is_stopped = stopping_guard.get_is_stopped();
                let order_by = order_by.clone();

                let hw_counter = hw_counter.fork();
                let cpu_utilization = hw_counter.cpu_utilization();
                let task = search_runtime_handle.spawn_blocking(move || {
                    let work = || {
                        segment.get().read().read_ordered_filtered(
                            Some(limit),
                            filter.as_ref(),
                            &order_by,
                            &is_stopped,
                            &hw_counter,
                            deferred_behavior,
                        )
                    };
                    match cpu_utilization {
                        Some(cu) => cu.measure(work),
                        None => work(),
                    }
                });
                AbortOnDropHandle::new(task)
            };

        let hw_counter = hw_measurement_acc.get_counter_cell();

        let all_reads = tokio::time::timeout(
            timeout,
            try_join_all(
                segments_to_read
                    .into_iter()
                    .map(|segment| read_ordered_filtered(segment, &hw_counter)),
            ),
        )
        .await
        .map_err(|_| CollectionError::timeout(timeout, "scroll_by_field"))??;

        let (values, point_ids): (Vec<_>, Vec<_>) =
            itertools::process_results(all_reads, |iter| {
                iter.kmerge_by(|a, b| match order_by.direction() {
                    Direction::Asc => a <= b,
                    Direction::Desc => a >= b,
                })
                .dedup()
                .take(limit)
                .unzip()
            })?;

        let with_payload = WithPayload::from(with_payload_interface);

        // update timeout
        let timeout = timeout.saturating_sub(start.elapsed());

        let records_map = self
            .scroll_records(
                &point_ids,
                &with_payload,
                with_vector,
                search_runtime_handle,
                timeout,
                hw_measurement_acc,
                deferred_behavior,
            )
            .await?;

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
    #[allow(clippy::too_many_arguments)]
    async fn scroll_randomly(
        &self,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        routes: Option<&Routes>,
        search_runtime_handle: &AdaptiveSearchHandle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        let start = Instant::now();
        let stopping_guard = StoppingGuard::new();
        let segments = self.segments.clone();

        let update_operation_lock = self.update_operation_lock.read().await;
        let segments_to_read = {
            let Some(segments_guard) = segments.try_read_for(timeout) else {
                return Err(CollectionError::timeout(timeout, "scroll_randomly"));
            };
            segments_to_read(&segments_guard, filter, routes)
        };

        let read_filtered = |(segment, filter): (LockedSegment, Option<Filter>),
                             hw_counter: &HardwareCounterCell| {
            let is_stopped = stopping_guard.get_is_stopped();

            let hw_counter = hw_counter.fork();
            let cpu_utilization = hw_counter.cpu_utilization();
            let task = search_runtime_handle.spawn_blocking(move || -> OperationResult<_> {
                let work = || -> OperationResult<_> {
                    let get_segment = segment.get();
                    let read_segment = get_segment.read();

                    Ok((
                        read_segment.available_point_count_without_deferred(),
                        read_segment.read_random_filtered(
                            limit,
                            filter.as_ref(),
                            &is_stopped,
                            &hw_counter,
                        )?,
                    ))
                };
                match cpu_utilization {
                    Some(cu) => cu.measure(work),
                    None => work(),
                }
            });
            AbortOnDropHandle::new(task)
        };

        let hw_counter = hw_measurement_acc.get_counter_cell();

        let all_reads = tokio::time::timeout(
            timeout,
            try_join_all(
                segments_to_read
                    .into_iter()
                    .map(|segment| read_filtered(segment, &hw_counter)),
            ),
        )
        .await
        .map_err(|_| CollectionError::timeout(timeout, "scroll_randomly"))??;

        let (availability, mut segments_reads): (Vec<_>, Vec<_>) =
            all_reads.into_iter().process_results(|iter| iter.unzip())?;

        // Shortcut if all segments are empty
        if availability.iter().all(|&count| count == 0) {
            return Ok(Vec::new());
        }
        // Cap HashSet capacity at filter-aware candidates in `segments_reads` (not segment sizes).
        // Unbounded client `limit` would otherwise abort via `handle_alloc_error`.
        let candidate_count: usize = segments_reads.iter().map(|points| points.len()).sum();

        // Select points in a weighted fashion from each segment, depending on how many points each segment has.
        let distribution = WeightedIndex::new(availability).map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to create weighted index for random scroll: {err:?}"
            ))
        })?;

        let mut rng = rand::make_rng::<StdRng>();
        let mut random_points = HashSet::with_capacity(limit.min(candidate_count));

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
        let records_map = self
            .scroll_records(
                &selected_points,
                &with_payload,
                with_vector,
                search_runtime_handle,
                timeout,
                hw_measurement_acc,
                DeferredBehavior::VisibleOnly,
            )
            .await?;

        drop(update_operation_lock);

        Ok(records_map.into_values().collect())
    }

    /// Records for scrolled ids; skips retrieval when neither payload nor vectors are requested.
    #[allow(clippy::too_many_arguments)]
    async fn scroll_records(
        &self,
        point_ids: &[ExtendedPointId],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        search_runtime_handle: &AdaptiveSearchHandle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
        deferred_behavior: DeferredBehavior,
    ) -> CollectionResult<AHashMap<ExtendedPointId, RecordInternal>> {
        if !with_payload.enable && !with_vector.is_enabled() {
            return Ok(point_ids
                .iter()
                .map(|&id| (id, RecordInternal::new_empty(id)))
                .collect());
        }

        tokio::time::timeout(
            timeout,
            SegmentsSearcher::retrieve(
                self.segments.clone(),
                point_ids,
                with_payload,
                with_vector,
                search_runtime_handle,
                timeout,
                hw_measurement_acc,
                deferred_behavior,
            ),
        )
        .await
        .map_err(|_| CollectionError::timeout(timeout, "retrieve"))?
    }
}

/// The segments to read, non-appendable first, and the filter each one gets.
///
/// Without routes that is every segment with the request's own filter. With
/// them it is only the segments that produced the candidates, each narrowed to
/// its own ids — the request's filter is the `has_id` over all of them, so
/// this only narrows it. A route to a segment the holder no longer has (a
/// finished optimization) falls back to reading every segment.
fn segments_to_read(
    holder: &SegmentHolder,
    filter: Option<&Filter>,
    routes: Option<&Routes>,
) -> Vec<(LockedSegment, Option<Filter>)> {
    let unrouted = || {
        holder
            .non_appendable_then_appendable_segments()
            .map(|segment| (segment, filter.cloned()))
            .collect::<Vec<_>>()
    };
    let Some(routes) = routes else {
        return unrouted();
    };

    let routed: Vec<_> = holder
        .non_appendable_then_appendable_segments_with_ids()
        .filter_map(|(segment_id, segment)| {
            let ids = routes.get(&segment_id)?;
            let own_ids = Filter::new_must(Condition::HasId(HasIdCondition::from(
                ids.iter().copied().collect::<AHashSet<_>>(),
            )));
            Some((segment, Some(own_ids)))
        })
        .collect();

    if routed.len() == routes.len() {
        routed
    } else {
        unrouted()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::build_test_holder;

    fn has_id(ids: impl IntoIterator<Item = u64>) -> Filter {
        Filter::new_must(Condition::HasId(HasIdCondition::from(
            ids.into_iter()
                .map(ExtendedPointId::from)
                .collect::<AHashSet<_>>(),
        )))
    }

    #[test]
    fn routes_narrow_the_filter_per_segment() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let holder = build_test_holder(dir.path());
        let holder = holder.read();
        let segment_ids = holder.segment_ids();
        assert_eq!(segment_ids.len(), 2);
        let candidates = has_id([1, 2, 3, 4]);

        let unrouted = segments_to_read(&holder, Some(&candidates), None);
        assert_eq!(unrouted.len(), 2);
        assert!(
            unrouted
                .iter()
                .all(|(_, f)| f.as_ref() == Some(&candidates))
        );

        let routes = Routes::from_iter([(segment_ids[0], vec![ExtendedPointId::from(1)])]);
        let routed = segments_to_read(&holder, Some(&candidates), Some(&routes));
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].1, Some(has_id([1])));

        // A route to a segment that is gone takes the whole read back to every
        // segment, with the full candidate list.
        let stale = Routes::from_iter([
            (segment_ids[0], vec![ExtendedPointId::from(1)]),
            (segment_ids[1] + 1000, vec![ExtendedPointId::from(2)]),
        ]);
        let fallback = segments_to_read(&holder, Some(&candidates), Some(&stale));
        assert_eq!(fallback.len(), 2);
        assert!(
            fallback
                .iter()
                .all(|(_, f)| f.as_ref() == Some(&candidates))
        );
    }
}
