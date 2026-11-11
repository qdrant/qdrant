use std::collections::BTreeSet;
use std::time::Duration;

use ahash::AHashMap;
use common::ambient;
use common::ambient::hw;
use common::types::DeferredBehavior;
use itertools::Itertools;
use segment::data_types::segment_record::SegmentRecordRaw;
use segment::types::{Filter, PointIdType, WithPayload, WithVector};
use shard::common::stopping_guard::StoppingGuard;
use shard::retrieve::record_internal::RecordInternal;
use shard::retrieve::retrieve_blocking::{retrieve_blocking, retrieve_raw_blocking};
use shard::segment_holder::locked::LockedSegmentHolder;
use tokio_util::task::AbortOnDropHandle;

use super::SegmentsSearcher;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::types::{CollectionError, CollectionResult};

impl SegmentsSearcher {
    /// Retrieve records for the given points ids from the segments
    /// - if payload is enabled, payload will be fetched
    /// - if vector is enabled, vector will be fetched
    ///
    /// The points ids can contain duplicates, the records will be fetched only once
    ///
    /// If an id is not found in the segments, it won't be included in the output.
    #[allow(clippy::too_many_arguments)]
    pub async fn retrieve(
        segments: LockedSegmentHolder,
        points: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        runtime_handle: &AdaptiveSearchHandle,
        timeout: Duration,
        deferred_behavior: DeferredBehavior,
    ) -> CollectionResult<AHashMap<PointIdType, RecordInternal>> {
        let stopping_guard = StoppingGuard::new();
        let points = runtime_handle.spawn_blocking({
            let segments = segments.clone();
            let points = points.to_vec();
            let with_payload = with_payload.clone();
            let with_vector = with_vector.clone();
            let is_stopped = stopping_guard.get_is_stopped();
            let handoff = ambient::current();
            // TODO create one Task per segment level retrieve
            move || {
                let _scope = handoff.enter_guard();
                retrieve_blocking(
                    segments,
                    &points,
                    &with_payload,
                    &with_vector,
                    timeout,
                    &is_stopped,
                    deferred_behavior,
                )
            }
        });
        Ok(AbortOnDropHandle::new(points).await??)
    }

    /// Byte-blob analogue of [`Self::retrieve`]: returns vectors as
    /// storage-native bytes ([`SegmentRecordRaw`]), avoiding a lossy
    /// quantization round-trip when relocating points during shard transfer.
    pub async fn retrieve_raw(
        segments: LockedSegmentHolder,
        points: &[PointIdType],
        with_vector: &WithVector,
        runtime_handle: &AdaptiveSearchHandle,
        timeout: Duration,
        deferred_behavior: DeferredBehavior,
    ) -> CollectionResult<AHashMap<PointIdType, SegmentRecordRaw>> {
        let stopping_guard = StoppingGuard::new();
        let points = runtime_handle.spawn_blocking({
            let segments = segments.clone();
            let points = points.to_vec();
            let with_vector = with_vector.clone();
            let is_stopped = stopping_guard.get_is_stopped();
            let handoff = ambient::current();
            move || {
                let _scope = handoff.enter_guard();
                retrieve_raw_blocking(
                    segments,
                    &points,
                    &with_vector,
                    timeout,
                    &is_stopped,
                    deferred_behavior,
                )
            }
        });
        Ok(AbortOnDropHandle::new(points).await??)
    }

    pub async fn read_filtered(
        segments: LockedSegmentHolder,
        filter: Option<&Filter>,
        runtime_handle: &AdaptiveSearchHandle,
        timeout: Option<Duration>,
        deferred_behavior: DeferredBehavior,
    ) -> CollectionResult<BTreeSet<PointIdType>> {
        let stopping_guard = StoppingGuard::new();
        // cloning filter spawning task
        let filter = filter.cloned();
        let handoff = ambient::current();
        let points = runtime_handle.spawn_blocking(move || {
            let is_stopped = stopping_guard.get_is_stopped();

            // Collect the segments first so we don't lock the segment holder during the operations.
            let segments: Vec<_> = {
                match timeout {
                    None => Ok(segments.read()),
                    Some(t) => segments
                        .try_read_for(t)
                        .ok_or_else(|| CollectionError::timeout(t, "read_filtered")),
                }?
                .non_appendable_then_appendable_segments()
                .collect()
            };

            let _scope = handoff.enter_guard();

            let work = || -> CollectionResult<_> {
                let all_points: BTreeSet<_> = segments
                    .into_iter()
                    .map(|segment| {
                        segment.get().read().read_filtered(
                            None,
                            None,
                            filter.as_ref(),
                            &is_stopped,
                            deferred_behavior,
                        )
                    })
                    .process_results(|iter| iter.flatten().collect())?;
                Ok(all_points)
            };

            hw::cpu_utilization().measure(work)
        });
        AbortOnDropHandle::new(points).await?
    }
}
