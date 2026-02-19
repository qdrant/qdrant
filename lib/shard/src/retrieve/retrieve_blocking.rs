use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use ahash::AHashMap;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::types::{PointIdType, SeqNumberType, WithPayload, WithVector};

use crate::retrieve::record_internal::RecordInternal;
use crate::segment_holder::SegmentHolder;
use crate::segment_holder::locked::LockedSegmentHolder;

pub fn retrieve_blocking(
    segments: LockedSegmentHolder,
    points: &[PointIdType],
    with_payload: &WithPayload,
    with_vector: &WithVector,
    timeout: Duration,
    is_stopped: &AtomicBool,
    hw_measurement_acc: HwMeasurementAcc,
) -> OperationResult<AHashMap<PointIdType, RecordInternal>> {
    let mut point_version: AHashMap<PointIdType, SeqNumberType> = Default::default();
    let mut point_records: AHashMap<PointIdType, RecordInternal> = Default::default();

    let hw_counter = hw_measurement_acc.get_counter_cell();

    SegmentHolder::read_points_locked(&segments, points, is_stopped, timeout, |ids, segment| {
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
        )? {
            // We expect all points to be found since we already checked their versions
            point_records.insert(id, RecordInternal::from(record));
            applied += 1;
        }

        Ok(applied)
    })?;

    Ok(point_records)
}
