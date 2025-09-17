use std::collections::BTreeSet;
use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::common::operation_error::OperationError;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::VectorStructInternal;
use segment::types::{Filter, PointIdType, SeqNumberType, WithPayload, WithVector};
use shard::segment_holder::LockedSegmentHolder;
use tokio::runtime::Handle;

use crate::common::stopping_guard::StoppingGuard;
use crate::operations::types::{CollectionResult, RecordInternal};

#[derive(Default)]
pub struct SegmentsRetriever;

impl SegmentsRetriever {
    /// Retrieve records for the given points ids from the segments
    /// - if payload is enabled, payload will be fetched
    /// - if vector is enabled, vector will be fetched
    ///
    /// The points ids can contain duplicates, the records will be fetched only once
    ///
    /// If an id is not found in the segments, it won't be included in the output.
    pub async fn retrieve(
        segments: LockedSegmentHolder,
        points: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        runtime_handle: &Handle,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<AHashMap<PointIdType, RecordInternal>> {
        let stopping_guard = StoppingGuard::new();
        runtime_handle
            .spawn_blocking({
                let segments = segments.clone();
                let points = points.to_vec();
                let with_payload = with_payload.clone();
                let with_vector = with_vector.clone();
                let is_stopped = stopping_guard.get_is_stopped();
                // TODO create one Task per segment level retrieve
                move || {
                    Self::retrieve_blocking(
                        segments,
                        &points,
                        &with_payload,
                        &with_vector,
                        &is_stopped,
                        hw_measurement_acc,
                    )
                }
            })
            .await?
    }

    pub fn retrieve_blocking(
        segments: LockedSegmentHolder,
        points: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        is_stopped: &AtomicBool,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<AHashMap<PointIdType, RecordInternal>> {
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

    pub async fn read_filtered(
        segments: LockedSegmentHolder,
        filter: Option<&Filter>,
        runtime_handle: &Handle,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<BTreeSet<PointIdType>> {
        let stopping_guard = StoppingGuard::new();
        let filter = filter.cloned();
        runtime_handle
            .spawn_blocking(move || {
                let is_stopped = stopping_guard.get_is_stopped();
                let segments = segments.read();
                let hw_counter = hw_measurement_acc.get_counter_cell();
                let all_points: BTreeSet<_> = segments
                    .non_appendable_then_appendable_segments()
                    .flat_map(|segment| {
                        segment.get().read().read_filtered(
                            None,
                            None,
                            filter.as_ref(),
                            &is_stopped,
                            &hw_counter,
                        )
                    })
                    .collect();
                Ok(all_points)
            })
            .await?
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::build_test_holder;

    #[test]
    fn test_retrieve() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segment_holder = build_test_holder(dir.path());
        let records = SegmentsRetriever::retrieve_blocking(
            Arc::new(segment_holder),
            &[1.into(), 2.into(), 3.into()],
            &WithPayload::from(true),
            &true.into(),
            &AtomicBool::new(false),
            HwMeasurementAcc::new(),
        )
        .unwrap();
        assert_eq!(records.len(), 3);
    }
}
