use parking_lot::RwLock;
use segment::types::SeqNumberType;

use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::collection_manager::segments_updater::*;
use crate::operations::types::{CollectionError, CollectionResult};
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
            Err(collection_error) => match collection_error {
                CollectionError::ServiceError { error } => {
                    let mut write_segments = segments.write();
                    write_segments.failed_operation.insert(op_num);
                    log::error!("Update operation failed: {}", error)
                }
                _ => {
                    log::warn!("Update operation declined: {}", collection_error)
                }
            },
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
    use segment::types::{Payload, WithPayload};
    use tempdir::TempDir;

    use super::*;
    use crate::collection_manager::fixtures::build_test_holder;
    use crate::collection_manager::segments_searcher::SegmentsSearcher;
    use crate::collection_manager::segments_updater::upsert_points;
    use crate::operations::payload_ops::{DeletePayload, PayloadOps, SetPayload};
    use crate::operations::point_ops::PointOperations;

    #[tokio::test]
    async fn test_point_ops() {
        let dir = TempDir::new("segment_dir").unwrap();

        let segments = build_test_holder(dir.path());
        let points = vec![1.into(), 500.into()];

        let vectors = vec![vec![2., 2., 2., 2.], vec![2., 0., 2., 0.]];

        let res = upsert_points(&segments, 100, &points, &vectors, &None);
        assert!(matches!(res, Ok(1)));

        let records = SegmentsSearcher::retrieve(
            &segments,
            &[1.into(), 2.into(), 500.into()],
            &WithPayload::from(true),
            true,
        )
        .await
        .unwrap();

        assert_eq!(records.len(), 3);

        for record in records {
            let v = record.vector.unwrap();

            if record.id == 1.into() {
                assert_eq!(&v, &vec![2., 2., 2., 2.])
            }
            if record.id == 500.into() {
                assert_eq!(&v, &vec![2., 0., 2., 0.])
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
            true,
        )
        .await
        .unwrap();

        for record in records {
            let _v = record.vector.unwrap();
            assert_ne!(record.id, 500.into());
        }
    }

    #[tokio::test]
    async fn test_payload_ops() {
        let dir = TempDir::new("segment_dir").unwrap();
        let segments = build_test_holder(dir.path());

        let payload: Payload = serde_json::from_str(r#"{"color":"red"}"#).unwrap();

        let points = vec![1.into(), 2.into(), 3.into()];

        process_payload_operation(
            &segments,
            100,
            PayloadOps::SetPayload(SetPayload {
                payload,
                points: points.clone(),
            }),
        )
        .unwrap();

        let res = SegmentsSearcher::retrieve(&segments, &points, &WithPayload::from(true), false)
            .await
            .unwrap();

        assert_eq!(res.len(), 3);

        match res.get(0) {
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
            PayloadOps::DeletePayload(DeletePayload {
                points: vec![3.into()],
                keys: vec!["color".to_string(), "empty".to_string()],
            }),
        )
        .unwrap();

        let res =
            SegmentsSearcher::retrieve(&segments, &[3.into()], &WithPayload::from(true), false)
                .await
                .unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));

        // Test clear payload

        let res =
            SegmentsSearcher::retrieve(&segments, &[2.into()], &WithPayload::from(true), false)
                .await
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
        let res =
            SegmentsSearcher::retrieve(&segments, &[2.into()], &WithPayload::from(true), false)
                .await
                .unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));
    }
}
