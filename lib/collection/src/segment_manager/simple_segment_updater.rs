use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;

use segment::types::{
    PayloadInterface, PayloadKeyType, PayloadKeyTypeRef, PointIdType, SeqNumberType,
};

use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::{PointInsertOperations, PointOperations};
use crate::operations::types::{CollectionError, CollectionResult, VectorType};
use crate::operations::{CollectionUpdateOperations, FieldIndexOperations};
use crate::segment_manager::holders::segment_holder::SegmentHolder;
use crate::segment_manager::segment_managers::SegmentUpdater;

pub struct SimpleSegmentUpdater {}

impl SegmentUpdater for SimpleSegmentUpdater {
    fn update(
        segments: &RwLock<SegmentHolder>,
        op_num: SeqNumberType,
        operation: CollectionUpdateOperations,
    ) -> CollectionResult<usize> {
        // Allow only one update at a time, ensure no data races between segments.
        // let _lock = self.update_lock.lock().unwrap();
        match operation {
            CollectionUpdateOperations::PointOperation(point_operation) => {
                process_point_operation(segments, op_num, point_operation)
            }
            CollectionUpdateOperations::PayloadOperation(payload_operation) => {
                process_payload_operation(segments, op_num, &payload_operation)
            }
            CollectionUpdateOperations::FieldIndexOperation(index_operation) => {
                process_field_index_operation(segments, op_num, &index_operation)
            }
        }
    }
}

/// Checks point id in each segment, update point if found.
/// All not found points are inserted into random segment.
/// Returns: number of updated points.
fn upsert_points(
    segments: &RwLock<SegmentHolder>,
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
    let points_map: HashMap<PointIdType, &VectorType> = ids.iter().cloned().zip(vectors).collect();

    let segment_holder = segments.read();

    // Get points, which presence in segments with higher version
    segment_holder.read_points(ids, |id, segment| {
        if segment.version() > op_num {
            updated_points.insert(id);
        }
        Ok(true)
    })?;

    // Update points in writable segments
    let res = segment_holder.apply_points_to_appendable(op_num, ids, |id, write_segment| {
        updated_points.insert(id);
        write_segment.upsert_point(op_num, id, points_map[&id])
    })?;

    // Insert new points, which was not updated.
    let new_point_ids = ids.iter().cloned().filter(|x| !updated_points.contains(x));

    {
        let default_write_segment =
            segment_holder
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
                set_payload(segments, op_num, payload.as_ref().unwrap(), &[*point_id])?;
            }
        }
    }

    Ok(res)
}

fn check_unprocessed_points(
    points: &[PointIdType],
    processed: &HashSet<PointIdType>,
) -> CollectionResult<usize> {
    let missed_point = points.iter().cloned().find(|p| !processed.contains(p));
    match missed_point {
        None => Ok(processed.len()),
        Some(missed_point) => Err(CollectionError::NotFound {
            missed_point_id: missed_point,
        }),
    }
}

/// Tries to delete points from all segments, returns number of actually deleted points
fn delete_points(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    ids: &[PointIdType],
) -> CollectionResult<usize> {
    let res = segments
        .read()
        .apply_points(op_num, ids, |id, write_segment| {
            write_segment.delete_point(op_num, id)
        })?;
    Ok(res)
}

fn process_payload_operation(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    payload_operation: &PayloadOps,
) -> CollectionResult<usize> {
    match payload_operation {
        PayloadOps::SetPayload {
            payload, points, ..
        } => set_payload(segments, op_num, payload, points),
        PayloadOps::DeletePayload { keys, points, .. } => {
            delete_payload(segments, op_num, points, keys)
        }
        PayloadOps::ClearPayload { points, .. } => clear_payload(segments, op_num, points),
    }
}

pub(crate) fn process_field_index_operation(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    field_index_operation: &FieldIndexOperations,
) -> CollectionResult<usize> {
    match field_index_operation {
        FieldIndexOperations::CreateIndex(field_name) => {
            create_field_index(segments, op_num, field_name)
        }
        FieldIndexOperations::DeleteIndex(field_name) => {
            delete_field_index(segments, op_num, field_name)
        }
    }
}

pub(crate) fn process_point_operation(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    point_operation: PointOperations,
) -> CollectionResult<usize> {
    match point_operation {
        PointOperations::DeletePoints { ids, .. } => delete_points(segments, op_num, &ids),
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
            let res = upsert_points(segments, op_num, &ids, &vectors, &payloads)?;
            Ok(res)
        }
    }
}

fn set_payload(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    payload: &HashMap<PayloadKeyType, PayloadInterface>,
    points: &[PointIdType],
) -> CollectionResult<usize> {
    let mut updated_points: HashSet<PointIdType> = Default::default();

    let res = segments
        .read()
        .apply_points_to_appendable(op_num, points, |id, write_segment| {
            updated_points.insert(id);
            let mut res = true;
            for (key, payload) in payload {
                res = write_segment.set_payload(op_num, id, key, payload.into())? && res;
            }
            Ok(res)
        })?;

    check_unprocessed_points(points, &updated_points)?;
    Ok(res)
}

fn delete_payload(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    points: &[PointIdType],
    keys: &[PayloadKeyType],
) -> CollectionResult<usize> {
    let mut updated_points: HashSet<PointIdType> = Default::default();

    let res = segments
        .read()
        .apply_points_to_appendable(op_num, points, |id, write_segment| {
            updated_points.insert(id);
            let mut res = true;
            for key in keys {
                res = write_segment.delete_payload(op_num, id, key)? && res;
            }
            Ok(res)
        })?;

    check_unprocessed_points(points, &updated_points)?;
    Ok(res)
}

fn clear_payload(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    points: &[PointIdType],
) -> CollectionResult<usize> {
    let mut updated_points: HashSet<PointIdType> = Default::default();
    let res = segments
        .read()
        .apply_points_to_appendable(op_num, points, |id, write_segment| {
            updated_points.insert(id);
            write_segment.clear_payload(op_num, id)
        })?;

    check_unprocessed_points(points, &updated_points)?;
    Ok(res)
}

fn create_field_index(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    field_name: PayloadKeyTypeRef,
) -> CollectionResult<usize> {
    let res = segments.read().apply_segments(op_num, |write_segment| {
        write_segment.create_field_index(op_num, field_name)
    })?;
    Ok(res)
}

fn delete_field_index(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    field_name: PayloadKeyTypeRef,
) -> CollectionResult<usize> {
    let res = segments.read().apply_segments(op_num, |write_segment| {
        write_segment.delete_field_index(op_num, field_name)
    })?;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use segment::types::PayloadVariant;

    use crate::segment_manager::fixtures::build_test_holder;
    use crate::segment_manager::segment_managers::SegmentSearcher;
    use crate::segment_manager::simple_segment_searcher::SimpleSegmentSearcher;

    use super::*;

    #[tokio::test]
    async fn test_point_ops() {
        let dir = TempDir::new("segment_dir").unwrap();

        let segment_holder = build_test_holder(dir.path());

        let points = vec![1, 500];

        let vectors = vec![vec![2., 2., 2., 2.], vec![2., 0., 2., 0.]];

        let res = upsert_points(&segment_holder, 100, &points, &vectors, &None);
        assert!(matches!(res, Ok(1)));

        let records = SimpleSegmentSearcher::retrieve(&segment_holder, &[1, 2, 500], true, true)
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

        delete_points(&segment_holder, 101, &[500]).unwrap();

        let records = SimpleSegmentSearcher::retrieve(&segment_holder, &[1, 2, 500], true, true)
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
        let segment_holder = build_test_holder(dir.path());

        let mut payload: HashMap<PayloadKeyType, PayloadInterface> = Default::default();

        payload.insert(
            "color".to_string(),
            PayloadInterface::KeywordShortcut(PayloadVariant::Value("red".to_string())),
        );

        let points = vec![1, 2, 3];

        process_payload_operation(
            &segment_holder,
            100,
            &PayloadOps::SetPayload {
                payload,
                points: points.clone(),
            },
        )
        .unwrap();

        let res = SimpleSegmentSearcher::retrieve(&segment_holder, &points, true, false)
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

        delete_payload(
            &segment_holder,
            101,
            &[3],
            &["color".to_string(), "empty".to_string()],
        )
        .unwrap();
        let res = SimpleSegmentSearcher::retrieve(&segment_holder, &[3], true, false)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));

        // Test clear payload

        let res = SimpleSegmentSearcher::retrieve(&segment_holder, &[2], true, false)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert!(res[0].payload.as_ref().unwrap().contains_key("color"));

        clear_payload(&segment_holder, 102, &[2]).unwrap();
        let res = SimpleSegmentSearcher::retrieve(&segment_holder, &[2], true, false)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].payload.as_ref().unwrap().contains_key("color"));
    }
}
