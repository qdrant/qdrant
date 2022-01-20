use std::collections::{HashMap, HashSet};

use parking_lot::{RwLock, RwLockWriteGuard};

use segment::types::{
    PayloadInterface, PayloadKeyType, PayloadKeyTypeRef, PointIdType, SeqNumberType,
    VectorElementType,
};

use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::{PointInsertOperations, PointOperations};
use crate::operations::types::{CollectionError, CollectionResult, VectorType};
use crate::operations::FieldIndexOperations;
use itertools::Itertools;
use segment::entry::entry_point::{OperationResult, SegmentEntry};

/// A collection of functions for updating points and payloads stored in segments

pub(crate) fn check_unprocessed_points(
    points: &[PointIdType],
    processed: &HashSet<PointIdType>,
) -> CollectionResult<usize> {
    let unprocessed_points = points
        .iter()
        .cloned()
        .filter(|p| !processed.contains(p))
        .collect_vec();
    let missed_point = unprocessed_points.iter().cloned().next();

    // ToDo: check pre-existing points

    match missed_point {
        None => Ok(processed.len()),
        Some(missed_point) => Err(CollectionError::NotFound {
            missed_point_id: missed_point,
        }),
    }
}

/// Tries to delete points from all segments, returns number of actually deleted points
pub(crate) fn delete_points(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    ids: &[PointIdType],
) -> CollectionResult<usize> {
    let res = segments.apply_points(ids, |id, _idx, write_segment| {
        write_segment.delete_point(op_num, id)
    })?;
    Ok(res)
}

pub(crate) fn set_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &HashMap<PayloadKeyType, PayloadInterface>,
    points: &[PointIdType],
) -> CollectionResult<usize> {
    let updated_points =
        segments.apply_points_to_appendable(op_num, points, |id, write_segment| {
            let mut res = true;
            for (key, payload) in payload {
                res = write_segment.set_payload(op_num, id, key, payload.into())? && res;
            }
            Ok(res)
        })?;

    check_unprocessed_points(points, &updated_points)?;
    Ok(updated_points.len())
}

pub(crate) fn delete_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: &[PointIdType],
    keys: &[PayloadKeyType],
) -> CollectionResult<usize> {
    let updated_points =
        segments.apply_points_to_appendable(op_num, points, |id, write_segment| {
            let mut res = true;
            for key in keys {
                res = write_segment.delete_payload(op_num, id, key)? && res;
            }
            Ok(res)
        })?;

    check_unprocessed_points(points, &updated_points)?;
    Ok(updated_points.len())
}

pub(crate) fn clear_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: &[PointIdType],
) -> CollectionResult<usize> {
    let updated_points =
        segments.apply_points_to_appendable(op_num, points, |id, write_segment| {
            write_segment.clear_payload(op_num, id)
        })?;

    check_unprocessed_points(points, &updated_points)?;
    Ok(updated_points.len())
}

pub(crate) fn create_field_index(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    field_name: PayloadKeyTypeRef,
) -> CollectionResult<usize> {
    let res = segments
        .apply_segments(|write_segment| write_segment.create_field_index(op_num, field_name))?;
    Ok(res)
}

pub(crate) fn delete_field_index(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    field_name: PayloadKeyTypeRef,
) -> CollectionResult<usize> {
    let res = segments
        .apply_segments(|write_segment| write_segment.delete_field_index(op_num, field_name))?;
    Ok(res)
}

fn upsert_with_payload(
    segment: &mut RwLockWriteGuard<dyn SegmentEntry>,
    op_num: SeqNumberType,
    point_id: PointIdType,
    vector: &[VectorElementType],
    payload: Option<&HashMap<PayloadKeyType, PayloadInterface>>,
) -> OperationResult<bool> {
    let mut res = segment.upsert_point(op_num, point_id, vector)?;
    if let Some(full_payload) = payload {
        for (key, payload_value) in full_payload {
            res &= segment.set_payload(op_num, point_id, key, payload_value.into())?;
        }
    }
    Ok(res)
}

/// Checks point id in each segment, update point if found.
/// All not found points are inserted into random segment.
/// Returns: number of updated points.
pub(crate) fn upsert_points(
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

    let vectors_map: HashMap<PointIdType, &VectorType> = ids.iter().cloned().zip(vectors).collect();
    let payloads_map: HashMap<PointIdType, &HashMap<PayloadKeyType, PayloadInterface>> =
        match payloads {
            None => Default::default(),
            Some(payloads_vector) => ids
                .iter()
                .clone()
                .zip(payloads_vector)
                .filter_map(|(id, payload)| {
                    payload.as_ref().map(|payload_values| (*id, payload_values))
                })
                .collect(),
        };

    let segments = segments.read();
    // Update points in writable segments
    let updated_points =
        segments.apply_points_to_appendable(op_num, ids, |id, write_segment| {
            upsert_with_payload(
                write_segment,
                op_num,
                id,
                vectors_map[&id],
                payloads_map.get(&id).cloned(),
            )
        })?;

    let mut res = updated_points.len();
    // Insert new points, which was not updated or existed
    let new_point_ids = ids
        .iter()
        .cloned()
        .filter(|x| !(updated_points.contains(x)));

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
            res += upsert_with_payload(
                &mut write_segment,
                op_num,
                point_id,
                vectors_map[&point_id],
                payloads_map.get(&point_id).cloned(),
            )? as usize;
        }
    };

    Ok(res)
}

pub(crate) fn process_point_operation(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    point_operation: PointOperations,
) -> CollectionResult<usize> {
    match point_operation {
        PointOperations::DeletePoints { ids, .. } => delete_points(&segments.read(), op_num, &ids),
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

pub(crate) fn process_payload_operation(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    payload_operation: &PayloadOps,
) -> CollectionResult<usize> {
    match payload_operation {
        PayloadOps::SetPayload(sp) => {
            set_payload(&segments.read(), op_num, &sp.payload, &sp.points)
        }
        PayloadOps::DeletePayload(dp) => {
            delete_payload(&segments.read(), op_num, &dp.points, &dp.keys)
        }
        PayloadOps::ClearPayload { points, .. } => clear_payload(&segments.read(), op_num, points),
    }
}

pub(crate) fn process_field_index_operation(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    field_index_operation: &FieldIndexOperations,
) -> CollectionResult<usize> {
    match field_index_operation {
        FieldIndexOperations::CreateIndex(field_name) => {
            create_field_index(&segments.read(), op_num, field_name)
        }
        FieldIndexOperations::DeleteIndex(field_name) => {
            delete_field_index(&segments.read(), op_num, field_name)
        }
    }
}
