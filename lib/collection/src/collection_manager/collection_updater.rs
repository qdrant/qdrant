use std::thread;
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
                log::debug!("handle_update_result trying to get RWLock in read for SegmentHolder {}", thread::current().name().unwrap());
                let lock = segments.read();
                log::debug!("handle_update_result got read lock on SegmentHolder! {}", thread::current().name().unwrap());
                if !lock.failed_operation.is_empty() {
                    log::debug!("handle_update_result trying to get RWLock in write for SegmentHolder {}", thread::current().name().unwrap());
                    let mut write_segments = segments.write();
                    log::debug!("handle_update_result got RWLock in write for SegmentHolder {}", thread::current().name().unwrap());
                    if write_segments.failed_operation.contains(&op_num) {
                        // Failed operation successfully fixed
                        write_segments.failed_operation.remove(&op_num);
                    }
                    log::debug!("handle_update_result release RWLock in write for SegmentHolder {}", thread::current().name().unwrap());
                }
                log::debug!("handle_update_result release read lock on SegmentHolder! {}", thread::current().name().unwrap());
            }
            Err(collection_error) => {
                log::debug!("handle_update_result - error {:?}", collection_error);
                match collection_error {
                    CollectionError::ServiceError { error } => {
                        let mut write_segments = segments.write();
                        write_segments.failed_operation.insert(op_num);
                        log::error!("Update operation failed: {}", error)
                    }
                    _ => {
                        log::warn!("Update operation declined: {}", collection_error)
                    }
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
                process_payload_operation(segments, op_num, &payload_operation)
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
    use tempdir::TempDir;

    use segment::types::{PayloadInterface, PayloadKeyType, PayloadVariant, WithPayload};

    use crate::collection_manager::collection_managers::CollectionSearcher;
    use crate::collection_manager::fixtures::build_test_holder;
    use crate::collection_manager::segments_updater::upsert_points;
    use crate::collection_manager::simple_collection_searcher::SimpleCollectionSearcher;

    use super::*;
    use crate::operations::payload_ops::{DeletePayload, PayloadOps, SetPayload};
    use crate::operations::point_ops::PointOperations;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_point_ops() {
        let dir = TempDir::new("segment_dir").unwrap();

        let segments = build_test_holder(dir.path());
        let searcher = SimpleCollectionSearcher::new();

        let points = vec![1, 500];

        let vectors = vec![vec![2., 2., 2., 2.], vec![2., 0., 2., 0.]];

        let res = upsert_points(&segments, 100, &points, &vectors, &None);
        assert!(matches!(res, Ok(1)));

        let records = searcher
            .retrieve(&segments, &[1, 2, 500], &WithPayload::from(true), true)
            .await
            .unwrap();

        assert_eq!(records.len(), 3);

        for record in records {
            let v = record.vector.unwrap();

            if record.id == 1 {
                assert_eq!(&v, &vec![2., 2., 2., 2.])
            }
            if record.id == 500 {
                assert_eq!(&v, &vec![2., 0., 2., 0.])
            }
        }

        process_point_operation(
            &segments,
            101,
            PointOperations::DeletePoints { ids: vec![500] },
        )
        .unwrap();

        let records = searcher
            .retrieve(&segments, &[1, 2, 500], &WithPayload::from(true), true)
            .await
            .unwrap();

        for record in records {
            let _v = record.vector.unwrap();
            assert_ne!(record.id, 500);
        }
    }

    #[tokio::test]
    async fn test_payload_ops() {
        let dir = TempDir::new("segment_dir").unwrap();
        let segments = build_test_holder(dir.path());
        let searcher = SimpleCollectionSearcher::new();

        let mut payload: HashMap<PayloadKeyType, PayloadInterface> = Default::default();

        payload.insert(
            "color".to_string(),
            PayloadInterface::KeywordShortcut(PayloadVariant::Value("red".to_string())),
        );

        let points = vec![1, 2, 3];

        process_payload_operation(
            &segments,
            100,
            &PayloadOps::SetPayload(SetPayload {
                payload,
                points: points.clone(),
            }),
        )
        .unwrap();

        let res = searcher
            .retrieve(&segments, &points, &WithPayload::from(true), false)
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
            &PayloadOps::DeletePayload(DeletePayload {
                points: vec![3],
                keys: vec!["color".to_string(), "empty".to_string()],
            }),
        )
        .unwrap();

        let res = searcher
            .retrieve(&segments, &[3], &WithPayload::from(true), false)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));

        // Test clear payload

        let res = searcher
            .retrieve(&segments, &[2], &WithPayload::from(true), false)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert!(res[0].payload.as_ref().unwrap().contains_key("color"));

        process_payload_operation(
            &segments,
            102,
            &PayloadOps::ClearPayload { points: vec![2] },
        )
        .unwrap();
        let res = searcher
            .retrieve(&segments, &[2], &WithPayload::from(true), false)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));
    }
}
