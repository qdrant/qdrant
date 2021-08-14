use std::collections::{HashMap, HashSet};

use segment::types::{PayloadInterface, PayloadKeyType, PointIdType, SeqNumberType};

use crate::collection_manager::collection_managers::CollectionUpdater;
use crate::collection_manager::holders::segment_holder::LockedSegmentHolder;
use crate::collection_manager::segments_updater::SegmentsUpdater;
use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::{PointInsertOperations, PointOperations};
use crate::operations::types::{CollectionError, CollectionResult, VectorType};
use crate::operations::{CollectionUpdateOperations, FieldIndexOperations};

pub struct SimpleCollectionUpdater {
    segments: LockedSegmentHolder,
    // update_lock: Mutex<bool>,
}

impl SimpleCollectionUpdater {
    pub fn new(segments: LockedSegmentHolder) -> Self {
        SimpleCollectionUpdater {
            segments,
            // update_lock: Mutex::new(false),
        }
    }

    /// Checks point id in each segment, update point if found.
    /// All not found points are inserted into random segment.
    /// Returns: number of updated points.
    fn upsert_points(
        &self,
        op_num: SeqNumberType,
        ids: &[PointIdType],
        vectors: &[VectorType],
        payloads: &Option<Vec<Option<HashMap<PayloadKeyType, PayloadInterface>>>>,
    ) -> CollectionResult<usize> {
        if ids.len() != vectors.len() {
            return Err(CollectionError::BadInput {
                description: format!(
                    "Amount of ids ({}) and vectors ({}) does not match",
                    ids.len(),
                    vectors.len()
                ),
            });
        }

        match payloads {
            None => {}
            Some(payload_vector) => {
                if payload_vector.len() != ids.len() {
                    return Err(CollectionError::BadInput {
                        description: format!(
                            "Amount of ids ({}) and payloads ({}) does not match",
                            ids.len(),
                            payload_vector.len()
                        ),
                    });
                }
            }
        }

        let mut updated_points: HashSet<PointIdType> = Default::default();
        let points_map: HashMap<PointIdType, &VectorType> =
            ids.iter().cloned().zip(vectors).collect();

        let segments = self.segments.read();

        // Get points, which presence in segments with higher version
        segments.read_points(ids, |id, segment| {
            if segment.version() > op_num {
                updated_points.insert(id);
            }
            Ok(true)
        })?;

        // Update points in writable segments
        let res = segments.apply_points_to_appendable(op_num, ids, |id, write_segment| {
            updated_points.insert(id);
            write_segment.upsert_point(op_num, id, points_map[&id])
        })?;

        // Insert new points, which was not updated.
        let new_point_ids = ids.iter().cloned().filter(|x| !updated_points.contains(x));

        {
            let default_write_segment =
                segments
                    .random_appendable_segment()
                    .ok_or(CollectionError::ServiceError {
                        error: "No segments exists, expected at least one".to_string(),
                    })?;

            let segment_arc = default_write_segment.get();
            let mut write_segment = segment_arc.write();
            for point_id in new_point_ids {
                write_segment.upsert_point(op_num, point_id, points_map[&point_id])?;
            }
        }

        if let Some(payload_vector) = payloads {
            for (point_id, payload) in ids.iter().zip(payload_vector.iter()) {
                if payload.is_some() {
                    SegmentsUpdater::set_payload(
                        &segments,
                        op_num,
                        payload.as_ref().unwrap(),
                        &[*point_id],
                    )?;
                }
            }
        }

        Ok(res)
    }

    pub fn process_point_operation(
        &self,
        op_num: SeqNumberType,
        point_operation: PointOperations,
    ) -> CollectionResult<usize> {
        match point_operation {
            PointOperations::DeletePoints { ids, .. } => {
                SegmentsUpdater::delete_points(&self.segments.read(), op_num, &ids)
            }
            PointOperations::UpsertPoints(operation) => {
                let (ids, vectors, payloads) = match operation {
                    PointInsertOperations::BatchPoints {
                        ids,
                        vectors,
                        payloads,
                        ..
                    } => (ids, vectors, payloads),
                    PointInsertOperations::PointsList(points) => {
                        let mut ids = vec![];
                        let mut vectors = vec![];
                        let mut payloads = vec![];
                        for point in points {
                            ids.push(point.id);
                            vectors.push(point.vector);
                            payloads.push(point.payload)
                        }
                        (ids, vectors, Some(payloads))
                    }
                };
                let res = self.upsert_points(op_num, &ids, &vectors, &payloads)?;
                Ok(res)
            }
        }
    }

    pub fn process_payload_operation(
        &self,
        op_num: SeqNumberType,
        payload_operation: &PayloadOps,
    ) -> CollectionResult<usize> {
        match payload_operation {
            PayloadOps::SetPayload {
                payload, points, ..
            } => SegmentsUpdater::set_payload(&self.segments.read(), op_num, payload, points),
            PayloadOps::DeletePayload { keys, points, .. } => {
                SegmentsUpdater::delete_payload(&self.segments.read(), op_num, points, keys)
            }
            PayloadOps::ClearPayload { points, .. } => {
                SegmentsUpdater::clear_payload(&self.segments.read(), op_num, points)
            }
        }
    }

    pub fn process_field_index_operation(
        &self,
        op_num: SeqNumberType,
        field_index_operation: &FieldIndexOperations,
    ) -> CollectionResult<usize> {
        match field_index_operation {
            FieldIndexOperations::CreateIndex(field_name) => {
                SegmentsUpdater::create_field_index(&self.segments.read(), op_num, field_name)
            }
            FieldIndexOperations::DeleteIndex(field_name) => {
                SegmentsUpdater::delete_field_index(&self.segments.read(), op_num, field_name)
            }
        }
    }
}

impl CollectionUpdater for SimpleCollectionUpdater {
    fn update(
        &self,
        op_num: SeqNumberType,
        operation: CollectionUpdateOperations,
    ) -> CollectionResult<usize> {
        // Allow only one update at a time, ensure no data races between segments.
        // let _lock = self.update_lock.lock().unwrap();
        match operation {
            CollectionUpdateOperations::PointOperation(point_operation) => {
                self.process_point_operation(op_num, point_operation)
            }
            CollectionUpdateOperations::PayloadOperation(payload_operation) => {
                self.process_payload_operation(op_num, &payload_operation)
            }
            CollectionUpdateOperations::FieldIndexOperation(index_operation) => {
                self.process_field_index_operation(op_num, &index_operation)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::collection_manager::collection_managers::CollectionSearcher;
    use crate::collection_manager::fixtures::build_searcher;
    use segment::types::PayloadVariant;

    use super::*;

    #[tokio::test]
    async fn test_point_ops() {
        let dir = TempDir::new("segment_dir").unwrap();

        let searcher = build_searcher(dir.path()).await;

        let updater = SimpleCollectionUpdater {
            segments: searcher.segments.clone(),
        };
        let points = vec![1, 500];

        let vectors = vec![vec![2., 2., 2., 2.], vec![2., 0., 2., 0.]];

        let res = updater.upsert_points(100, &points, &vectors, &None);
        assert!(matches!(res, Ok(1)));

        let records = searcher.retrieve(&[1, 2, 500], true, true).await.unwrap();

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

        updater
            .process_point_operation(101, PointOperations::DeletePoints { ids: vec![500] })
            .unwrap();

        let records = searcher.retrieve(&[1, 2, 500], true, true).await.unwrap();

        for record in records {
            let _v = record.vector.unwrap();
            assert_ne!(record.id, 500);
        }
    }

    #[tokio::test]
    async fn test_payload_ops() {
        let dir = TempDir::new("segment_dir").unwrap();
        let searcher = build_searcher(dir.path()).await;

        let updater = SimpleCollectionUpdater {
            segments: searcher.segments.clone(),
        };

        let mut payload: HashMap<PayloadKeyType, PayloadInterface> = Default::default();

        payload.insert(
            "color".to_string(),
            PayloadInterface::KeywordShortcut(PayloadVariant::Value("red".to_string())),
        );

        let points = vec![1, 2, 3];

        updater
            .process_payload_operation(
                100,
                &PayloadOps::SetPayload {
                    payload,
                    points: points.clone(),
                },
            )
            .unwrap();

        let res = searcher.retrieve(&points, true, false).await.unwrap();

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
        updater
            .process_payload_operation(
                101,
                &PayloadOps::DeletePayload {
                    points: vec![3],
                    keys: vec!["color".to_string(), "empty".to_string()],
                },
            )
            .unwrap();

        let res = searcher.retrieve(&[3], true, false).await.unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));

        // Test clear payload

        let res = searcher.retrieve(&[2], true, false).await.unwrap();
        assert_eq!(res.len(), 1);
        assert!(res[0].payload.as_ref().unwrap().contains_key("color"));

        updater
            .process_payload_operation(102, &PayloadOps::ClearPayload { points: vec![2] })
            .unwrap();
        let res = searcher.retrieve(&[2], true, false).await.unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));
    }
}
