use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use ahash::AHashMap;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::DeferredBehavior;
use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::entry::ReadSegmentEntry;
use segment::types::{PointIdType, SeqNumberType, WithPayload, WithVector};

use crate::retrieve::record_internal::RecordInternal;
use crate::segment_holder::SegmentHolder;
use crate::segment_holder::locked::LockedSegmentHolder;

#[allow(clippy::too_many_arguments)]
pub fn retrieve_blocking(
    segments: LockedSegmentHolder,
    points: &[PointIdType],
    with_payload: &WithPayload,
    with_vector: &WithVector,
    timeout: Duration,
    is_stopped: &AtomicBool,
    hw_measurement_acc: HwMeasurementAcc,
    deferred_behavior: DeferredBehavior,
) -> OperationResult<AHashMap<PointIdType, RecordInternal>> {
    // Snapshot the segments (non-appendable first, then appendable) under a bounded read lock, then
    // run the version-dedup over that snapshot. The holder lock is released before retrieval.
    let segments: Vec<_> = {
        let Some(holder_guard) = segments.try_read_for(timeout) else {
            return Err(OperationError::timeout(timeout, "retrieve"));
        };
        holder_guard
            .non_appendable_then_appendable_segments()
            .map(|segment| segment.get_read_arc())
            .collect()
    };

    retrieve_over(
        segments,
        points,
        with_payload,
        with_vector,
        is_stopped,
        hw_measurement_acc,
        deferred_behavior,
    )
}

/// Retrieve records over an explicit, pre-collected snapshot of read handles.
///
/// Holds the same cross-segment version-dedup as [`retrieve_blocking`], but operates on a caller
/// owned set of segments rather than a [`LockedSegmentHolder`], so it can be shared by callers that
/// keep their own snapshot (e.g. `EdgeReadView` and the read-only follower shard).
///
/// Generic over the segment type `R` so callers with a homogeneous snapshot (e.g. a follower's
/// `ReadOnlySegment<S>`) are monomorphized; the heterogeneous leader instantiates `R = dyn
/// ReadSegmentEntry`.
pub fn retrieve_over<R: ReadSegmentEntry + ?Sized>(
    segments: Vec<Arc<RwLock<R>>>,
    points: &[PointIdType],
    with_payload: &WithPayload,
    with_vector: &WithVector,
    is_stopped: &AtomicBool,
    hw_measurement_acc: HwMeasurementAcc,
    deferred_behavior: DeferredBehavior,
) -> OperationResult<AHashMap<PointIdType, RecordInternal>> {
    let mut point_version: AHashMap<PointIdType, SeqNumberType> = Default::default();
    let mut point_records: AHashMap<PointIdType, RecordInternal> = Default::default();

    let hw_counter = hw_measurement_acc.get_counter_cell();

    SegmentHolder::read_points_over(
        segments,
        points,
        is_stopped,
        deferred_behavior,
        |ids, segment| {
            let mut newer_version_points: Vec<_> = Vec::with_capacity(ids.len());

            let mut applied = 0;

            for &id in ids {
                let version = segment.point_version(id).ok_or_else(|| {
                    OperationError::service_error(format!("No version for point {id}"))
                })?;

                // If we already have the latest point version, keep that and continue
                let version_entry = point_version.entry(id);
                if matches!(&version_entry, Entry::Occupied(entry) if *entry.get() >= version) {
                    applied += 1;
                    continue;
                }
                newer_version_points.push(id);
                *version_entry.or_default() = version;
            }

            for (id, record) in segment.retrieve(
                &newer_version_points,
                with_payload,
                with_vector,
                &hw_counter,
                is_stopped,
                deferred_behavior,
            )? {
                // We expect all points to be found since we already checked their versions
                point_records.insert(id, RecordInternal::from(record));
                applied += 1;
            }

            Ok(applied)
        },
    )?;

    Ok(point_records)
}
