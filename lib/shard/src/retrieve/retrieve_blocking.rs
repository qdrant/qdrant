use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use ahash::AHashMap;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::VectorStructInternal;
use segment::types::{PointIdType, SeqNumberType, WithPayload, WithVector};

use crate::retrieve::record_internal::RecordInternal;
use crate::segment_holder::LockedSegmentHolder;

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

    let mut point_vectors: AHashMap<PointIdType, NamedVectors> = Default::default();

    let hw_counter = hw_measurement_acc.get_counter_cell();

    let Some(segments_guard) = segments.try_read_for(timeout) else {
        return Err(OperationError::timeout(timeout, "retrieve points"));
    };

    segments_guard.read_points_many(points, is_stopped, |ids, segment| {
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

        match with_vector {
            WithVector::Bool(true) => segment.all_vectors_many(
                &newer_version_points,
                &hw_counter,
                |point_id, vector_name, vector| {
                    point_vectors
                        .entry(point_id)
                        .or_default()
                        .insert(vector_name, vector);
                },
            )?,
            WithVector::Bool(false) => {}
            WithVector::Selector(selector) => {
                for vector_name in selector {
                    segment.vectors(
                        vector_name,
                        &newer_version_points,
                        &hw_counter,
                        |point_id, vector| {
                            point_vectors
                                .entry(point_id)
                                .or_default()
                                .insert(vector_name.to_owned(), vector);
                        },
                    )?;
                }
            }
        };

        for id in newer_version_points {
            point_records.insert(
                id,
                RecordInternal {
                    id,
                    payload: if with_payload.enable {
                        if let Some(selector) = &with_payload.payload_selector {
                            Some(selector.process(segment.payload(id, &hw_counter)?))
                        } else {
                            Some(segment.payload(id, &hw_counter)?)
                        }
                    } else {
                        None
                    },
                    vector: point_vectors.remove(&id).map(VectorStructInternal::from),
                    shard_key: None,
                    order_value: None,
                },
            );
            applied += 1;
        }

        Ok(applied)
    })?;

    Ok(point_records)
}
