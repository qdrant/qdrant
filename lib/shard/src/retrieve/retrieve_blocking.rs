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

    let hw_counter = hw_measurement_acc.get_counter_cell();

    let Some(segments_guard) = segments.try_read_for(timeout) else {
        return Err(OperationError::timeout(timeout, "retrieve points"));
    };

    segments_guard.read_points_many(points, is_stopped, |ids, segment| {
        let vectors: Vec<_> = match with_vector {
            WithVector::Bool(true) => segment
                .all_vectors_many(ids, &hw_counter)?
                .into_iter()
                .map(VectorStructInternal::from)
                .map(Some)
                .collect(),
            WithVector::Bool(false) => vec![None; ids.len()],
            WithVector::Selector(selector) => {
                let mut vectors = vec![NamedVectors::default(); ids.len()];

                for vector_name in selector {
                    let fetched_vectors = segment.vectors(vector_name, ids, &hw_counter)?;
                    for (i, vector_opt) in fetched_vectors.into_iter().enumerate() {
                        if let Some(vector) = vector_opt {
                            vectors[i].insert(vector_name.clone(), vector);
                        }
                    }
                }

                vectors
                    .into_iter()
                    .map(VectorStructInternal::from)
                    .map(Some)
                    .collect()
            }
        };

        let mut applied = 0;

        for (&id, vector) in ids.iter().zip(vectors) {
            let version = segment.point_version(id).ok_or_else(|| {
                OperationError::service_error(format!("No version for point {id}"))
            })?;

            // If we already have the latest point version, keep that and continue
            let version_entry = point_version.entry(id);
            if matches!(&version_entry, Entry::Occupied(entry) if *entry.get() >= version) {
                applied += 1;
                continue;
            }

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
                    vector,
                    shard_key: None,
                    order_value: None,
                },
            );
            *version_entry.or_default() = version;

            applied += 1;
        }

        Ok(applied)
    })?;

    Ok(point_records)
}
