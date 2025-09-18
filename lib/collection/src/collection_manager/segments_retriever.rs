use std::collections::BTreeSet;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::common::stopping_guard::StoppingGuard;
use crate::operations::types::{CollectionResult, RecordInternal, RecordInternalVersioned};
use ahash::AHashMap;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use segment::common::operation_error::OperationError;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::VectorStructInternal;
use segment::types::{Filter, PointIdType, WithPayload, WithVector};
use shard::locked_segment::LockedSegment;
use shard::segment_holder::LockedSegmentHolder;
use tokio::runtime::Handle;

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

        let points = Arc::new(points.to_vec());
        let with_payload = Arc::new(with_payload.clone());
        let with_vector = Arc::new(with_vector.clone());

        let retrieves: Vec<_> = {
            let segments_lock = segments.read();
            let segments = segments_lock.non_appendable_then_appendable_segments();

            segments
                .map(|segment| {
                    runtime_handle.spawn_blocking({
                        let points = points.clone();
                        let hw_counter = hw_measurement_acc.get_counter_cell();
                        let is_stopped = stopping_guard.get_is_stopped();
                        let with_payload = with_payload.clone();
                        let with_vector = with_vector.clone();
                        move || {
                            // Retrieve points with versions from the segment
                            SegmentsRetriever::retrieve_blocking(
                                segment,
                                &points,
                                &with_payload,
                                &with_vector,
                                &is_stopped,
                                hw_counter,
                            )
                        }
                    })
                })
                .collect()
        };

        let mut versioned_results: AHashMap<PointIdType, RecordInternalVersioned> = AHashMap::new();

        let mut retrieve_results_per_segment_res = FuturesUnordered::new();

        for retrieve in retrieves {
            retrieve_results_per_segment_res.push(retrieve);
        }

        while let Some(retrieve_result) = retrieve_results_per_segment_res.try_next().await? {
            let retrieve_result = retrieve_result?;
            for (point_id, record) in retrieve_result {
                let existing = versioned_results.entry(point_id);
                match existing {
                    Entry::Vacant(v) => {
                        v.insert(record);
                    }
                    Entry::Occupied(mut occupied) => {
                        // If we already have the latest point version, keep that and continue
                        if occupied.get().version < record.version {
                            occupied.insert(record);
                        }
                    }
                }
            }
        }

        Ok(versioned_results
            .into_iter()
            .map(|(k, v)| (k, v.record))
            .collect())
    }

    pub fn retrieve_blocking(
        segment: LockedSegment,
        points: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        is_stopped: &AtomicBool,
        hw_counter: HardwareCounterCell,
    ) -> CollectionResult<AHashMap<PointIdType, RecordInternalVersioned>> {
        let mut point_records: AHashMap<PointIdType, RecordInternalVersioned> = Default::default();

        let segment_arc = segment.get();
        let read_segment = segment_arc.read();

        let points = points
            .iter()
            .cloned()
            .check_stop(|| is_stopped.load(Ordering::Relaxed))
            .filter(|id| read_segment.has_point(*id));

        for id in points {
            let version = read_segment.point_version(id).ok_or_else(|| {
                OperationError::service_error(format!("No version for point {id}"))
            })?;

            let record = RecordInternal {
                id,
                payload: if with_payload.enable {
                    if let Some(selector) = &with_payload.payload_selector {
                        Some(selector.process(read_segment.payload(id, &hw_counter)?))
                    } else {
                        Some(read_segment.payload(id, &hw_counter)?)
                    }
                } else {
                    None
                },
                vector: {
                    match with_vector {
                        WithVector::Bool(true) => {
                            let vectors = read_segment.all_vectors(id, &hw_counter)?;
                            Some(VectorStructInternal::from(vectors))
                        }
                        WithVector::Bool(false) => None,
                        WithVector::Selector(vector_names) => {
                            let mut selected_vectors = NamedVectors::default();
                            for vector_name in vector_names {
                                if let Some(vector) =
                                    read_segment.vector(vector_name, id, &hw_counter)?
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
            };

            point_records.insert(id, RecordInternalVersioned { record, version });
        }
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

    #[tokio::test]
    async fn test_retrieve() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segment_holder = build_test_holder(dir.path());

        let records = SegmentsRetriever::retrieve(
            Arc::new(segment_holder),
            &[1.into(), 2.into(), 3.into()],
            &WithPayload::from(true),
            &true.into(),
            &Handle::current(),
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();
        assert_eq!(records.len(), 3);
    }
}
