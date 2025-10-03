use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicBool;

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
    is_stopped: &AtomicBool,
    hw_measurement_acc: HwMeasurementAcc,
) -> OperationResult<AHashMap<PointIdType, RecordInternal>> {
    let mut point_version: AHashMap<PointIdType, SeqNumberType> = Default::default();
    let mut point_records: AHashMap<PointIdType, RecordInternal> = Default::default();

    let hw_counter = hw_measurement_acc.get_counter_cell();

    segments
        .read()
        .read_points(points, is_stopped, |id, segment| {
            let version = segment.point_version(id).ok_or_else(|| {
                OperationError::service_error(format!("No version for point {id}"))
            })?;

            // If we already have the latest point version, keep that and continue
            let version_entry = point_version.entry(id);
            if matches!(&version_entry, Entry::Occupied(entry) if *entry.get() >= version) {
                return Ok(true);
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
                    vector: {
                        match with_vector {
                            WithVector::Bool(true) => {
                                let vectors = segment.all_vectors(id, &hw_counter)?;
                                Some(VectorStructInternal::from(vectors))
                            }
                            WithVector::Bool(false) => None,
                            WithVector::Selector(vector_names) => {
                                let mut selected_vectors = NamedVectors::default();
                                for vector_name in vector_names {
                                    if let Some(vector) =
                                        segment.vector(vector_name, id, &hw_counter)?
                                    {
                                        selected_vectors.insert(vector_name.clone(), vector);
                                    }
                                }
                                Some(VectorStructInternal::from(selected_vectors))
                            }
                        }
                    },
                    shard_key: None,
                    order_value: None,
                },
            );
            *version_entry.or_default() = version;

            Ok(true)
        })?;

    Ok(point_records)
}
