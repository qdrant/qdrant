use std::sync::Mutex;
use crate::segment_manager::holders::segment_holder::{LockedSegmentHolder};
use crate::segment_manager::segment_managers::SegmentUpdater;
use crate::operations::{CollectionUpdateOperations, FieldIndexOperations};
use crate::collection::{CollectionResult, CollectionError};
use segment::types::{SeqNumberType, PointIdType, PayloadKeyType};
use std::collections::{HashSet, HashMap};
use crate::operations::types::VectorType;

use crate::operations::point_ops::{PointOperations, PointInsertOperations};
use crate::operations::payload_ops::{PayloadOps, PayloadInterface};

pub struct SimpleSegmentUpdater {
    segments: LockedSegmentHolder,
    update_lock: Mutex<bool>,
}

impl SimpleSegmentUpdater {
    pub fn new(segments: LockedSegmentHolder) -> Self {
        SimpleSegmentUpdater {
            segments,
            update_lock: Mutex::new(false),
        }
    }

    fn check_unprocessed_points(points: &Vec<PointIdType>, processed: &HashSet<PointIdType>) -> CollectionResult<usize> {
        let missed_point = points
            .iter()
            .cloned()
            .filter(|p| !processed.contains(p))
            .next();
        match missed_point {
            None => Ok(processed.len()),
            Some(missed_point) => Err(CollectionError::NotFound { missed_point_id: missed_point }),
        }
    }

    /// Tries to delete points from all segments, returns number of actually deleted points
    fn delete_points(&self, op_num: SeqNumberType, ids: &Vec<PointIdType>) -> CollectionResult<usize> {
        let res = self.segments.read()
            .apply_points(op_num, ids, |id, write_segment|
                write_segment.delete_point(op_num, id),
            )?;
        Ok(res)
    }


    /// Checks point id in each segment, update point if found.
    /// All not found points are inserted into random segment.
    /// Returns: number of updated points.
    fn upsert_points(&self,
                     op_num: SeqNumberType,
                     ids: &Vec<PointIdType>,
                     vectors: &Vec<VectorType>,
                     payloads: &Option<Vec<Option<HashMap<PayloadKeyType, PayloadInterface>>>>,
    ) -> CollectionResult<usize> {
        if ids.len() != vectors.len() {
            return Err(CollectionError::BadInput {
                description: format!("Amount of ids ({}) and vectors ({}) does not match", ids.len(), vectors.len())
            });
        }

        match payloads {
            None => {}
            Some(payload_vector) => if payload_vector.len() != ids.len() {
                return Err(CollectionError::BadInput {
                    description: format!("Amount of ids ({}) and payloads ({}) does not match", ids.len(), payload_vector.len())
                });
            }
        }

        let mut updated_points: HashSet<PointIdType> = Default::default();
        let points_map: HashMap<PointIdType, &VectorType> = ids.iter().cloned().zip(vectors).collect();

        let segments = self.segments.read();

        // Get points, which presence in segments with higher version
        self.segments.read().read_points(ids, |id, segment| {
            if segment.version() > op_num {
                updated_points.insert(id);
            }
            Ok(true)
        })?;

        // Update points in writable segments
        let res = segments.apply_points_to_appendable(
            op_num,
            ids,
            |id, write_segment| {
                updated_points.insert(id);
                write_segment.upsert_point(op_num, id, points_map[&id])
            })?;

        // Insert new points, which was not updated.
        let new_point_ids = ids
            .iter()
            .cloned()
            .filter(|x| !updated_points.contains(x));

        {
            let default_write_segment = segments.random_appendable_segment()
                .ok_or(CollectionError::ServiceError { error: "No segments exists, expected at least one".to_string() })?;

            let segment_arc = default_write_segment.get();
            let mut write_segment = segment_arc.write();
            for point_id in new_point_ids {
                write_segment.upsert_point(op_num, point_id, points_map[&point_id])?;
            }
        }

        match payloads {
            Some(payload_vector) => {
                for (point_id, payload) in ids.iter().zip(payload_vector.iter()) {
                    if payload.is_some() {
                        self.set_payload(op_num, payload.as_ref().unwrap(), &vec![*point_id])?;
                    }
                }
            }
            _ => {}
        }

        Ok(res)
    }

    fn set_payload(
        &self,
        op_num: SeqNumberType,
        payload: &HashMap<PayloadKeyType, PayloadInterface>,
        points: &Vec<PointIdType>,
    ) -> CollectionResult<usize> {
        let mut updated_points: HashSet<PointIdType> = Default::default();

        let res = self.segments.read().apply_points_to_appendable(
            op_num,
            points,
            |id, write_segment| {
                updated_points.insert(id);
                let mut res = true;
                for (key, payload) in payload {
                    res = write_segment.set_payload(op_num, id, key, payload.to_payload())? && res;
                }
                Ok(res)
            })?;

        SimpleSegmentUpdater::check_unprocessed_points(points, &updated_points)?;
        Ok(res)
    }

    fn delete_payload(
        &self,
        op_num: SeqNumberType,
        points: &Vec<PointIdType>,
        keys: &Vec<PayloadKeyType>,
    ) -> CollectionResult<usize> {
        let mut updated_points: HashSet<PointIdType> = Default::default();

        let res = self.segments
            .read()
            .apply_points_to_appendable(
                op_num,
                points,
                |id, write_segment| {
                    updated_points.insert(id);
                    let mut res = true;
                    for key in keys {
                        res = write_segment.delete_payload(op_num, id, key)? && res;
                    }
                    Ok(res)
                })?;

        SimpleSegmentUpdater::check_unprocessed_points(points, &updated_points)?;
        Ok(res)
    }

    fn clear_payload(
        &self,
        op_num: SeqNumberType,
        points: &Vec<PointIdType>,
    ) -> CollectionResult<usize> {
        let mut updated_points: HashSet<PointIdType> = Default::default();
        let res = self.segments
            .read()
            .apply_points_to_appendable(
                op_num,
                points,
                |id, write_segment| {
                    updated_points.insert(id);
                    write_segment.clear_payload(op_num, id)
                })?;

        SimpleSegmentUpdater::check_unprocessed_points(points, &updated_points)?;
        Ok(res)
    }

    fn create_field_index(&self, op_num: SeqNumberType, field_name: &PayloadKeyType) -> CollectionResult<usize> {
        let res = self.segments
            .read()
            .apply_segments(op_num, |write_segment| {
                write_segment.create_field_index(op_num, field_name)
            })?;
        Ok(res)
    }

    fn delete_field_index(&self, op_num: SeqNumberType, field_name: &PayloadKeyType) -> CollectionResult<usize> {
        let res = self.segments
            .read()
            .apply_segments(op_num, |write_segment| {
                write_segment.delete_field_index(op_num, field_name)
            })?;
        Ok(res)
    }

    pub fn process_point_operation(&self, op_num: SeqNumberType, point_operation: PointOperations) -> CollectionResult<usize> {
        match point_operation {
            PointOperations::DeletePoints { ids, .. } => self.delete_points(op_num, &ids),
            PointOperations::UpsertPoints(operation) => {
                let (ids, vectors, payloads) = match operation {
                    PointInsertOperations::BatchPoints { ids, vectors, payloads, .. } => {
                        (ids, vectors, payloads)
                    }
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


    pub fn process_payload_operation(&self, op_num: SeqNumberType, payload_operation: &PayloadOps) -> CollectionResult<usize> {
        match payload_operation {
            PayloadOps::SetPayload {
                payload,
                points,
                ..
            } => self.set_payload(op_num, payload, points),
            PayloadOps::DeletePayload {
                keys,
                points,
                ..
            } => self.delete_payload(op_num, points, keys),
            PayloadOps::ClearPayload {
                points, ..
            } => self.clear_payload(op_num, points),
        }
    }

    pub fn process_field_index_operation(&self, op_num: SeqNumberType, field_index_operation: &FieldIndexOperations) -> CollectionResult<usize> {
        match field_index_operation {
            FieldIndexOperations::CreateIndex(field_name) => self.create_field_index(op_num, field_name),
            FieldIndexOperations::DeleteIndex(field_name) => self.delete_field_index(op_num, field_name),
        }
    }
}


impl SegmentUpdater for SimpleSegmentUpdater {
    fn update(&self, op_num: SeqNumberType, operation: CollectionUpdateOperations) -> CollectionResult<usize> {
        // Allow only one update at a time, ensure no data races between segments.
        // let _lock = self.update_lock.lock().unwrap();
        match operation {
            CollectionUpdateOperations::PointOperation(point_operation) => self.process_point_operation(op_num, point_operation),
            CollectionUpdateOperations::PayloadOperation(payload_operation) => self.process_payload_operation(op_num, &payload_operation),
            CollectionUpdateOperations::FieldIndexOperation(index_operation) => self.process_field_index_operation(op_num, &index_operation),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_manager::fixtures::{build_searcher};
    use crate::segment_manager::segment_managers::SegmentSearcher;
    use crate::operations::payload_ops::PayloadVariant;
    use tempdir::TempDir;

    #[test]
    fn test_point_ops() {
        let dir = TempDir::new("segment_dir").unwrap();

        let (_rt, searcher) = build_searcher(dir.path());

        let updater = SimpleSegmentUpdater {
            segments: searcher.segments.clone(),
            update_lock: Mutex::new(false),
        };
        let points = vec![1, 500];

        let vectors = vec![
            vec![2., 2., 2., 2.],
            vec![2., 0., 2., 0.],
        ];

        let res = updater.upsert_points(
            100,
            &points,
            &vectors,
            &None,
        );

        match res {
            Ok(updated) => assert_eq!(updated, 1),
            Err(_) => assert!(false),
        };

        let records = searcher.retrieve(&vec![1, 2, 500], true, true).unwrap();

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

        updater.delete_points(101, &vec![500]).unwrap();

        let records = searcher.retrieve(&vec![1, 2, 500], true, true).unwrap();

        for record in records {
            let _v = record.vector.unwrap();

            if record.id == 500 {
                assert!(false)
            }
        }
    }

    #[test]
    fn test_payload_ops() {
        let dir = TempDir::new("segment_dir").unwrap();
        let (_rt, searcher) = build_searcher(dir.path());

        let updater = SimpleSegmentUpdater {
            segments: searcher.segments.clone(),
            update_lock: Mutex::new(false),
        };

        let mut payload: HashMap<PayloadKeyType, PayloadInterface> = Default::default();

        payload.insert(
            "color".to_string(),
            PayloadInterface::Keyword(PayloadVariant::Value("red".to_string())),
        );

        let points = vec![1, 2, 3];

        updater.process_payload_operation(100, &PayloadOps::SetPayload {
            payload,
            points: points.clone(),
        }).unwrap();

        let res = searcher.retrieve(&points, true, false).unwrap();

        assert_eq!(res.len(), 3);

        match res.get(0) {
            None => assert!(false),
            Some(r) => match &r.payload {
                None => assert!(false, "No payload assigned"),
                Some(payload) => {
                    assert!(payload.contains_key("color"))
                }
            },
        };

        // Test payload delete

        updater.delete_payload(101, &vec![3], &vec!["color".to_string(), "empty".to_string()]).unwrap();
        let res = searcher.retrieve(&vec![3], true, false).unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));

        // Test clear payload

        let res = searcher.retrieve(&vec![2], true, false).unwrap();
        assert_eq!(res.len(), 1);
        assert!(res[0].payload.as_ref().unwrap().contains_key("color"));

        updater.clear_payload(102, &vec![2]).unwrap();
        let res = searcher.retrieve(&vec![2], true, false).unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"))
    }
}
