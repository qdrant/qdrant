use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use parking_lot::RwLock;
use segment::types::SeqNumberType;
use shard::update::*;

use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::operations::CollectionUpdateOperations;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::update_tracker::UpdateTracker;

/// Implementation of the update operation
#[derive(Default)]
pub struct CollectionUpdater {}

impl CollectionUpdater {
    fn handle_update_result(
        segments: &RwLock<SegmentHolder>,
        op_num: SeqNumberType,
        operation_result: &CollectionResult<usize>,
    ) {
        match operation_result {
            Ok(_) => {
                if !segments.read().failed_operation.is_empty() {
                    // If this operation failed before, remove it because it got fixed now
                    segments.write().failed_operation.remove(&op_num);
                }
            }
            Err(collection_error) => {
                if collection_error.is_transient() {
                    let mut write_segments = segments.write();
                    write_segments.failed_operation.insert(op_num);
                    log::error!("Update operation failed: {collection_error}")
                } else {
                    log::warn!("Update operation declined: {collection_error}")
                }
            }
        }
    }

    pub fn update(
        segments: &RwLock<SegmentHolder>,
        op_num: SeqNumberType,
        operation: CollectionUpdateOperations,
        update_operation_lock: Arc<tokio::sync::RwLock<()>>,
        update_tracker: UpdateTracker,
        hw_counter: &HardwareCounterCell,
    ) -> CollectionResult<usize> {
        // Use block_in_place here to avoid blocking the current async executor
        let operation_result = tokio::task::block_in_place(|| {
            // Allow only one update at a time, ensure no data races between segments.
            // let _update_lock = self.update_lock.lock().unwrap();

            let _update_operation_lock = update_operation_lock.blocking_write();
            let _update_guard = update_tracker.update();

            match operation {
                CollectionUpdateOperations::PointOperation(point_operation) => {
                    process_point_operation(segments, op_num, point_operation, hw_counter)
                }
                CollectionUpdateOperations::VectorOperation(vector_operation) => {
                    process_vector_operation(segments, op_num, vector_operation, hw_counter)
                }
                CollectionUpdateOperations::PayloadOperation(payload_operation) => {
                    process_payload_operation(segments, op_num, payload_operation, hw_counter)
                }
                CollectionUpdateOperations::FieldIndexOperation(index_operation) => {
                    process_field_index_operation(segments, op_num, &index_operation, hw_counter)
                }
            }
        });

        let operation_result = operation_result.map_err(CollectionError::from);
        CollectionUpdater::handle_update_result(segments, op_num, &operation_result);
        operation_result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use itertools::Itertools;
    use parking_lot::RwLockUpgradableReadGuard;
    use segment::data_types::vectors::{
        DEFAULT_VECTOR_NAME, VectorStructInternal, only_default_vector,
    };
    use segment::entry::entry_point::SegmentEntry;
    use segment::json_path::JsonPath;
    use segment::payload_json;
    use segment::types::PayloadSchemaType::Keyword;
    use segment::types::{Payload, PayloadContainer, PayloadFieldSchema, WithPayload};
    use serde_json::json;
    use shard::retrieve::retrieve_blocking::retrieve_blocking;
    use shard::update::upsert_points;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{
        build_segment_1, build_segment_2, build_test_holder,
    };
    use crate::collection_manager::holders::segment_holder::LockedSegment::Original;
    use crate::operations::payload_ops::{DeletePayloadOp, PayloadOps, SetPayloadOp};
    use crate::operations::point_ops::{
        PointOperations, PointStructPersisted, VectorStructPersisted,
    };

    #[test]
    fn test_sync_ops() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segments = build_test_holder(dir.path());

        let vec11 = only_default_vector(&[1.0, 1.0, 1.0, 1.0]);
        let vec12 = only_default_vector(&[1.0, 1.0, 1.0, 0.0]);
        let vec13 = only_default_vector(&[1.0, 0.0, 1.0, 1.0]);

        let points = vec![
            PointStructPersisted {
                id: 11.into(),
                vector: VectorStructPersisted::from(VectorStructInternal::from(vec11)),
                payload: None,
            },
            PointStructPersisted {
                id: 12.into(),
                vector: VectorStructPersisted::from(VectorStructInternal::from(vec12)),
                payload: None,
            },
            PointStructPersisted {
                id: 13.into(),
                vector: VectorStructPersisted::from(VectorStructInternal::from(vec13)),
                payload: Some(payload_json! { "color": "red" }),
            },
            PointStructPersisted {
                id: 14.into(),
                vector: VectorStructPersisted::Single(vec![0., 0., 0., 0.]),
                payload: None,
            },
            PointStructPersisted {
                id: 500.into(),
                vector: VectorStructPersisted::Single(vec![2., 0., 2., 0.]),
                payload: None,
            },
        ];

        let hw_counter = HardwareCounterCell::new();

        let (num_deleted, num_new, num_updated) = sync_points(
            &segments.read(),
            100,
            Some(10.into()),
            None,
            &points,
            &hw_counter,
        )
        .unwrap();

        assert_eq!(num_deleted, 1); // delete point 15
        assert_eq!(num_new, 1); // insert point 500
        assert_eq!(num_updated, 2); // upsert point 13 and 14 as it has updated data
        // points 11 and 12 are not updated as they are same as before
    }

    #[test]
    fn test_point_ops() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let is_stopped = AtomicBool::new(false);

        let segments = build_test_holder(dir.path());
        let points = vec![
            PointStructPersisted {
                id: 1.into(),
                vector: VectorStructPersisted::Single(vec![2., 2., 2., 2.]),
                payload: None,
            },
            PointStructPersisted {
                id: 500.into(),
                vector: VectorStructPersisted::Single(vec![2., 0., 2., 0.]),
                payload: None,
            },
        ];

        let hw_counter = HardwareCounterCell::new();

        let res = upsert_points(&segments.read(), 100, &points, &hw_counter);
        assert!(matches!(res, Ok(1)));

        let segments = Arc::new(segments);
        let records = retrieve_blocking(
            segments.clone(),
            &[1.into(), 2.into(), 500.into()],
            &WithPayload::from(true),
            &true.into(),
            &is_stopped,
            HwMeasurementAcc::new(),
        )
        .unwrap()
        .into_values()
        .collect_vec();

        assert_eq!(records.len(), 3);

        for record in records {
            let v = record.vector.unwrap();

            let v1 = vec![2., 2., 2., 2.];
            if record.id == 1.into() {
                assert_eq!(v.get(DEFAULT_VECTOR_NAME), Some((&v1).into()))
            }
            let v2 = vec![2., 0., 2., 0.];
            if record.id == 500.into() {
                assert_eq!(v.get(DEFAULT_VECTOR_NAME), Some((&v2).into()))
            }
        }

        process_point_operation(
            &segments,
            101,
            PointOperations::DeletePoints {
                ids: vec![500.into()],
            },
            &hw_counter,
        )
        .unwrap();

        let records = retrieve_blocking(
            segments,
            &[1.into(), 2.into(), 500.into()],
            &WithPayload::from(true),
            &true.into(),
            &is_stopped,
            HwMeasurementAcc::new(),
        )
        .unwrap()
        .into_values()
        .collect_vec();

        for record in records {
            assert!(record.vector.is_some());
            assert_ne!(record.id, 500.into());
        }
    }

    #[test]
    fn test_payload_ops() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segments = build_test_holder(dir.path());
        let payload: Payload = serde_json::from_str(r#"{"color":"red"}"#).unwrap();
        let is_stopped = AtomicBool::new(false);

        let points = vec![1.into(), 2.into(), 3.into()];

        let hw_counter = HardwareCounterCell::new();

        process_payload_operation(
            &segments,
            100,
            PayloadOps::SetPayload(SetPayloadOp {
                payload,
                points: Some(points.clone()),
                filter: None,
                key: None,
            }),
            &hw_counter,
        )
        .unwrap();

        let segments = Arc::new(segments);
        let res = retrieve_blocking(
            segments.clone(),
            &points,
            &WithPayload::from(true),
            &false.into(),
            &is_stopped,
            HwMeasurementAcc::new(),
        )
        .unwrap()
        .into_values()
        .collect_vec();

        assert_eq!(res.len(), 3);

        match res.first() {
            None => panic!(),
            Some(r) => match &r.payload {
                None => panic!("No payload assigned"),
                Some(payload) => {
                    assert!(payload.contains_key("color"))
                }
            },
        };

        // Test payload delete
        process_payload_operation(
            &segments,
            101,
            PayloadOps::DeletePayload(DeletePayloadOp {
                points: Some(vec![3.into()]),
                keys: vec!["color".parse().unwrap(), "empty".parse().unwrap()],
                filter: None,
            }),
            &hw_counter,
        )
        .unwrap();

        let res = retrieve_blocking(
            segments.clone(),
            &[3.into()],
            &WithPayload::from(true),
            &false.into(),
            &is_stopped,
            HwMeasurementAcc::new(),
        )
        .unwrap()
        .into_values()
        .collect_vec();

        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));

        // Test clear payload

        let res = retrieve_blocking(
            segments.clone(),
            &[2.into()],
            &WithPayload::from(true),
            &false.into(),
            &is_stopped,
            HwMeasurementAcc::new(),
        )
        .unwrap()
        .into_values()
        .collect_vec();

        assert_eq!(res.len(), 1);
        assert!(res[0].payload.as_ref().unwrap().contains_key("color"));

        process_payload_operation(
            &segments,
            102,
            PayloadOps::ClearPayload {
                points: vec![2.into()],
            },
            &hw_counter,
        )
        .unwrap();
        let res = retrieve_blocking(
            segments,
            &[2.into()],
            &WithPayload::from(true),
            &false.into(),
            &is_stopped,
            HwMeasurementAcc::new(),
        )
        .unwrap()
        .into_values()
        .collect_vec();

        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));
    }

    #[test]
    fn test_nested_payload_update_with_index() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let path = dir.path();

        let meta_key_path = JsonPath::new("meta");
        let nested_key_path: JsonPath = JsonPath::new("meta.color");

        let hw_counter = HardwareCounterCell::new();

        let mut segment1 = build_segment_1(path);
        segment1
            .create_field_index(
                100,
                &nested_key_path,
                Some(&PayloadFieldSchema::FieldType(Keyword)),
                &hw_counter,
            )
            .unwrap();

        let mut segment2 = build_segment_2(path);
        segment2
            .create_field_index(
                101,
                &nested_key_path,
                Some(&PayloadFieldSchema::FieldType(Keyword)),
                &hw_counter,
            )
            .unwrap();

        let mut holder = SegmentHolder::default();
        let segment_ids = vec![holder.add_new(segment1), holder.add_new(segment2)];

        let segments_guard = RwLock::new(holder);
        let segments = Arc::new(segments_guard);

        // payload with nested structure
        let payload: Payload = serde_json::from_str(r#"{"color":"red"}"#).unwrap();
        let is_stopped = AtomicBool::new(false);

        // update points from segment 2
        let points = vec![11.into(), 12.into(), 13.into()];

        process_payload_operation(
            &segments,
            102,
            PayloadOps::SetPayload(SetPayloadOp {
                payload,
                points: Some(points.clone()),
                filter: None,
                key: Some(meta_key_path.clone()),
            }),
            &hw_counter,
        )
        .unwrap();

        let res = retrieve_blocking(
            segments.clone(),
            &points,
            &WithPayload::from(true),
            &false.into(),
            &is_stopped,
            HwMeasurementAcc::new(),
        )
        .unwrap()
        .into_values()
        .collect_vec();

        assert_eq!(res.len(), 3);

        match res.first() {
            None => panic!(),
            Some(r) => match &r.payload {
                None => panic!("No payload assigned"),
                Some(actual_payload) => {
                    let expect_value = json!({"color":"red"});
                    assert_eq!(
                        actual_payload.get_value(&meta_key_path).first().unwrap(),
                        &&expect_value
                    )
                }
            },
        };

        // segment 2 is marked as not appendable to trigger COW mechanism
        let upgradable = segments.upgradable_read();
        let segments = RwLockUpgradableReadGuard::upgrade(upgradable).remove(&segment_ids);
        match segments.get(segment_ids[1]) {
            Some(Original(segment)) => {
                let mut guard = segment.write();
                guard.appendable_flag = false;
            }
            x => panic!("Unexpected segment type: {x:?}"),
        };

        let mut holder = SegmentHolder::default();
        for segment in segments {
            holder.add_new(segment);
        }

        let segments_guard = RwLock::new(holder);
        let segments = Arc::new(segments_guard);

        // update points nested values
        let payload: Payload = serde_json::from_str(r#"{ "color":"blue"}"#).unwrap();

        process_payload_operation(
            &segments,
            103,
            PayloadOps::SetPayload(SetPayloadOp {
                payload,
                points: Some(points.clone()),
                filter: None,
                key: Some(meta_key_path.clone()),
            }),
            &hw_counter,
        )
        .unwrap();

        let res = retrieve_blocking(
            segments,
            &points,
            &WithPayload::from(true),
            &false.into(),
            &is_stopped,
            HwMeasurementAcc::new(),
        )
        .unwrap()
        .into_values()
        .collect_vec();

        assert_eq!(res.len(), 3);

        match res.first() {
            None => panic!(),
            Some(r) => match &r.payload {
                None => panic!("No payload assigned"),
                Some(actual_payload) => {
                    let expect_value = json!({"color":"blue"});
                    assert_eq!(
                        actual_payload.get_value(&meta_key_path).first().unwrap(),
                        &&expect_value
                    )
                }
            },
        };
    }
}
