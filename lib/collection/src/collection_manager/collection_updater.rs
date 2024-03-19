use parking_lot::RwLock;
use segment::types::SeqNumberType;

use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::collection_manager::segments_updater::*;
use crate::operations::types::CollectionResult;
use crate::operations::CollectionUpdateOperations;

/// Implementation of the update operation
#[derive(Default)]
pub struct CollectionUpdater {}

impl CollectionUpdater {
    pub fn new() -> Self {
        Self {}
    }

    fn handle_update_result(
        segments: &RwLock<SegmentHolder>,
        op_num: SeqNumberType,
        operation_result: &CollectionResult<usize>,
    ) {
        match operation_result {
            Ok(_) => {
                if !segments.read().failed_operation.is_empty() {
                    let mut write_segments = segments.write();
                    if write_segments.failed_operation.contains(&op_num) {
                        // Failed operation successfully fixed
                        write_segments.failed_operation.remove(&op_num);
                    }
                }
            }
            Err(collection_error) => {
                if collection_error.is_transient() {
                    let mut write_segments = segments.write();
                    write_segments.failed_operation.insert(op_num);
                    log::error!("Update operation failed: {}", collection_error)
                } else {
                    log::warn!("Update operation declined: {}", collection_error)
                }
            }
        }
    }

    pub fn update(
        segments: &RwLock<SegmentHolder>,
        op_num: SeqNumberType,
        operation: CollectionUpdateOperations,
    ) -> CollectionResult<usize> {
        // Allow only one update at a time, ensure no data races between segments.
        // let _lock = self.update_lock.lock().unwrap();
        let operation_result = match operation {
            CollectionUpdateOperations::PointOperation(point_operation) => {
                process_point_operation(segments, op_num, point_operation)
            }
            CollectionUpdateOperations::VectorOperation(vector_operation) => {
                process_vector_operation(segments, op_num, vector_operation)
            }
            CollectionUpdateOperations::PayloadOperation(payload_operation) => {
                process_payload_operation(segments, op_num, payload_operation)
            }
            CollectionUpdateOperations::FieldIndexOperation(index_operation) => {
                process_field_index_operation(segments, op_num, &index_operation)
            }
        };

        CollectionUpdater::handle_update_result(segments, op_num, &operation_result);

        operation_result
    }
}

#[cfg(test)]
mod tests {
    use segment::data_types::vectors::{only_default_vector, VectorStruct, DEFAULT_VECTOR_NAME};
    use segment::types::{Payload, WithPayload};
    use serde_json::json;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::build_test_holder;
    use crate::collection_manager::segments_searcher::SegmentsSearcher;
    use crate::collection_manager::segments_updater::upsert_points;
    use crate::operations::payload_ops::{DeletePayloadOp, PayloadOps, SetPayloadOp};
    use crate::operations::point_ops::{PointOperations, PointStruct};

    #[test]
    fn test_sync_ops() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segments = build_test_holder(dir.path());

        let vec11 = only_default_vector(&[1.0, 1.0, 1.0, 1.0]);
        let vec12 = only_default_vector(&[1.0, 1.0, 1.0, 0.0]);
        let vec13 = only_default_vector(&[1.0, 0.0, 1.0, 1.0]);

        let points = vec![
            PointStruct {
                id: 11.into(),
                vector: VectorStruct::from(vec11).into(),
                payload: None,
            },
            PointStruct {
                id: 12.into(),
                vector: VectorStruct::from(vec12).into(),
                payload: None,
            },
            PointStruct {
                id: 13.into(),
                vector: VectorStruct::from(vec13).into(),
                payload: Some(json!({ "color": "red" }).into()),
            },
            PointStruct {
                id: 14.into(),
                vector: VectorStruct::from(vec![0., 0., 0., 0.]).into(),
                payload: None,
            },
            PointStruct {
                id: 500.into(),
                vector: VectorStruct::from(vec![2., 0., 2., 0.]).into(),
                payload: None,
            },
        ];

        let (num_deleted, num_new, num_updated) =
            sync_points(&segments.read(), 100, Some(10.into()), None, &points).unwrap();

        assert_eq!(num_deleted, 1); // delete point 15
        assert_eq!(num_new, 1); // insert point 500
        assert_eq!(num_updated, 2); // upsert point 13 and 14 as it has updated data
                                    // points 11 and 12 are not updated as they are same as before
    }

    #[test]
    fn test_point_ops() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segments = build_test_holder(dir.path());
        let points = vec![
            PointStruct {
                id: 1.into(),
                vector: VectorStruct::from(vec![2., 2., 2., 2.]).into(),
                payload: None,
            },
            PointStruct {
                id: 500.into(),
                vector: VectorStruct::from(vec![2., 0., 2., 0.]).into(),
                payload: None,
            },
        ];

        let res = upsert_points(&segments.read(), 100, &points);
        assert!(matches!(res, Ok(1)));

        let records = SegmentsSearcher::retrieve(
            &segments,
            &[1.into(), 2.into(), 500.into()],
            &WithPayload::from(true),
            &true.into(),
        )
        .unwrap();

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
        )
        .unwrap();

        let records = SegmentsSearcher::retrieve(
            &segments,
            &[1.into(), 2.into(), 500.into()],
            &WithPayload::from(true),
            &true.into(),
        )
        .unwrap();

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

        let points = vec![1.into(), 2.into(), 3.into()];

        process_payload_operation(
            &segments,
            100,
            PayloadOps::SetPayload(SetPayloadOp {
                payload,
                points: Some(points.clone()),
                filter: None,
                key: None,
            }),
        )
        .unwrap();

        let res =
            SegmentsSearcher::retrieve(&segments, &points, &WithPayload::from(true), &false.into())
                .unwrap();

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
        )
        .unwrap();

        let res = SegmentsSearcher::retrieve(
            &segments,
            &[3.into()],
            &WithPayload::from(true),
            &false.into(),
        )
        .unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));

        // Test clear payload

        let res = SegmentsSearcher::retrieve(
            &segments,
            &[2.into()],
            &WithPayload::from(true),
            &false.into(),
        )
        .unwrap();
        assert_eq!(res.len(), 1);
        assert!(res[0].payload.as_ref().unwrap().contains_key("color"));

        process_payload_operation(
            &segments,
            102,
            PayloadOps::ClearPayload {
                points: vec![2.into()],
            },
        )
        .unwrap();
        let res = SegmentsSearcher::retrieve(
            &segments,
            &[2.into()],
            &WithPayload::from(true),
            &false.into(),
        )
        .unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));
    }
}
