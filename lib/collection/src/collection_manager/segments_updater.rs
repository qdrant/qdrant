//! A collection of functions for updating points and payloads stored in segments

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;

use itertools::iproduct;
use parking_lot::{RwLock, RwLockWriteGuard};
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{BatchVectorStructInternal, VectorStructInternal};
use segment::entry::entry_point::SegmentEntry;
use segment::json_path::JsonPath;
use segment::types::{
    Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef, PointIdType,
    SeqNumberType,
};

use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::vector_ops::{PointVectorsPersisted, VectorOperations};
use crate::operations::FieldIndexOperations;

pub(crate) fn check_unprocessed_points(
    points: &[PointIdType],
    processed: &HashSet<PointIdType>,
) -> CollectionResult<usize> {
    let first_missed_point = points.iter().copied().find(|p| !processed.contains(p));

    match first_missed_point {
        None => Ok(processed.len()),
        Some(missed_point_id) => Err(CollectionError::PointNotFound { missed_point_id }),
    }
}

/// Tries to delete points from all segments, returns number of actually deleted points
pub(crate) fn delete_points(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    ids: &[PointIdType],
) -> CollectionResult<usize> {
    let mut total_deleted_points = 0;

    for batch in ids.chunks(VECTOR_OP_BATCH_SIZE) {
        let deleted_points = segments.apply_points(
            batch,
            |_| (),
            |id, _idx, write_segment, ()| write_segment.delete_point(op_num, id),
        )?;

        total_deleted_points += deleted_points;
    }

    Ok(total_deleted_points)
}

/// Update the specified named vectors of a point, keeping unspecified vectors intact.
pub(crate) fn update_vectors(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: Vec<PointVectorsPersisted>,
) -> CollectionResult<usize> {
    // Build a map of vectors to update per point, merge updates on same point ID
    let mut points_map: HashMap<PointIdType, NamedVectors> = HashMap::new();
    for point in points {
        let PointVectorsPersisted { id, vector } = point;
        let named_vector = NamedVectors::from(vector);

        let entry = points_map.entry(id).or_default();
        entry.merge(named_vector);
    }

    let ids: Vec<PointIdType> = points_map.keys().copied().collect();

    let mut total_updated_points = 0;
    for batch in ids.chunks(VECTOR_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| {
                let vectors = points_map[&id].clone();
                write_segment.update_vectors(op_num, id, vectors)
            },
            |id, owned_vectors, _| {
                for (vector_name, vector_ref) in points_map[&id].iter() {
                    owned_vectors.insert(vector_name.to_string(), vector_ref.to_owned());
                }
            },
            |_| false,
        )?;
        check_unprocessed_points(batch, &updated_points)?;
        total_updated_points += updated_points.len();
    }

    Ok(total_updated_points)
}

const VECTOR_OP_BATCH_SIZE: usize = 512;

/// Delete the given named vectors for the given points, keeping other vectors intact.
pub(crate) fn delete_vectors(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: &[PointIdType],
    vector_names: &[String],
) -> CollectionResult<usize> {
    let mut total_deleted_points = 0;

    for batch in points.chunks(VECTOR_OP_BATCH_SIZE) {
        let deleted_points = segments.apply_points(
            batch,
            |_| (),
            |id, _idx, write_segment, ()| {
                let mut res = true;
                for name in vector_names {
                    res &= write_segment.delete_vector(op_num, id, name)?;
                }
                Ok(res)
            },
        )?;

        total_deleted_points += deleted_points;
    }

    Ok(total_deleted_points)
}

/// Delete the given named vectors for points matching the given filter, keeping other vectors intact.
pub(crate) fn delete_vectors_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
    vector_names: &[String],
) -> CollectionResult<usize> {
    let affected_points = points_by_filter(segments, filter)?;
    delete_vectors(segments, op_num, &affected_points, vector_names)
}

/// Batch size when modifying payload.
const PAYLOAD_OP_BATCH_SIZE: usize = 512;

pub(crate) fn overwrite_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    points: &[PointIdType],
) -> CollectionResult<usize> {
    let mut total_updated_points = 0;

    for batch in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| write_segment.set_full_payload(op_num, id, payload),
            |_, _, old_payload| {
                *old_payload = payload.clone();
            },
            |segment| segment.get_indexed_fields().is_empty(),
        )?;

        total_updated_points += updated_points.len();
        check_unprocessed_points(batch, &updated_points)?;
    }

    Ok(total_updated_points)
}

pub(crate) fn overwrite_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    filter: &Filter,
) -> CollectionResult<usize> {
    let affected_points = points_by_filter(segments, filter)?;
    overwrite_payload(segments, op_num, payload, &affected_points)
}

pub(crate) fn set_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    points: &[PointIdType],
    key: &Option<JsonPath>,
) -> CollectionResult<usize> {
    let mut total_updated_points = 0;

    for chunk in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            chunk,
            |id, write_segment| write_segment.set_payload(op_num, id, payload, key),
            |_, _, old_payload| match key {
                Some(key) => old_payload.merge_by_key(payload, key),
                None => old_payload.merge(payload),
            },
            |segment| {
                segment.get_indexed_fields().keys().all(|indexed_path| {
                    !indexed_path.is_affected_by_value_set(&payload.0, key.as_ref())
                })
            },
        )?;

        check_unprocessed_points(chunk, &updated_points)?;
        total_updated_points += updated_points.len();
    }

    Ok(total_updated_points)
}

fn points_by_filter(
    segments: &SegmentHolder,
    filter: &Filter,
) -> CollectionResult<Vec<PointIdType>> {
    let mut affected_points: Vec<PointIdType> = Vec::new();
    // we don’t want to cancel this filtered read
    let is_stopped = AtomicBool::new(false);
    segments.for_each_segment(|s| {
        let points = s.read_filtered(None, None, Some(filter), &is_stopped);
        affected_points.extend_from_slice(points.as_slice());
        Ok(true)
    })?;
    Ok(affected_points)
}

pub(crate) fn set_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    filter: &Filter,
    key: &Option<JsonPath>,
) -> CollectionResult<usize> {
    let affected_points = points_by_filter(segments, filter)?;
    set_payload(segments, op_num, payload, &affected_points, key)
}

pub(crate) fn delete_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: &[PointIdType],
    keys: &[PayloadKeyType],
) -> CollectionResult<usize> {
    let mut total_deleted_points = 0;

    for batch in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| {
                let mut res = true;
                for key in keys {
                    res &= write_segment.delete_payload(op_num, id, key)?;
                }
                Ok(res)
            },
            |_, _, payload| {
                for key in keys {
                    payload.remove(key);
                }
            },
            |segment| {
                iproduct!(segment.get_indexed_fields().keys(), keys).all(
                    |(indexed_path, path_to_delete)| {
                        !indexed_path.is_affected_by_value_remove(path_to_delete)
                    },
                )
            },
        )?;

        check_unprocessed_points(batch, &updated_points)?;
        total_deleted_points += updated_points.len();
    }

    Ok(total_deleted_points)
}

pub(crate) fn delete_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
    keys: &[PayloadKeyType],
) -> CollectionResult<usize> {
    let affected_points = points_by_filter(segments, filter)?;
    delete_payload(segments, op_num, &affected_points, keys)
}

pub(crate) fn clear_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: &[PointIdType],
) -> CollectionResult<usize> {
    let mut total_updated_points = 0;

    for batch in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| write_segment.clear_payload(op_num, id),
            |_, _, payload| payload.0.clear(),
            |segment| segment.get_indexed_fields().is_empty(),
        )?;
        check_unprocessed_points(batch, &updated_points)?;
        total_updated_points += updated_points.len();
    }

    Ok(total_updated_points)
}

/// Clear Payloads from all segments matching the given filter
pub(crate) fn clear_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
) -> CollectionResult<usize> {
    let points_to_clear = points_by_filter(segments, filter)?;

    let mut total_updated_points = 0;

    for batch in points_to_clear.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| write_segment.clear_payload(op_num, id),
            |_, _, payload| payload.0.clear(),
            |segment| segment.get_indexed_fields().is_empty(),
        )?;
        total_updated_points += updated_points.len();
    }

    Ok(total_updated_points)
}

pub(crate) fn create_field_index(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    field_name: PayloadKeyTypeRef,
    field_schema: Option<&PayloadFieldSchema>,
) -> CollectionResult<usize> {
    segments
        .apply_segments(|write_segment| {
            let Some((schema, index)) =
                write_segment.build_field_index(op_num, field_name, field_schema)?
            else {
                return Ok(false);
            };

            write_segment.with_upgraded(|segment| {
                segment.apply_field_index(op_num, field_name.to_owned(), schema, index)
            })
        })
        .map_err(Into::into)
}

pub(crate) fn delete_field_index(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    field_name: PayloadKeyTypeRef,
) -> CollectionResult<usize> {
    segments
        .apply_segments(|write_segment| {
            write_segment.with_upgraded(|segment| segment.delete_field_index(op_num, field_name))
        })
        .map_err(Into::into)
}

/// Upsert to a point ID with the specified vectors and payload in the given segment.
///
/// Returns
/// - Ok(true) if the operation was successful and point replaced existing value
/// - Ok(false) if the operation was successful and point was inserted
/// - Err if the operation failed
fn upsert_with_payload(
    segment: &mut RwLockWriteGuard<dyn SegmentEntry>,
    op_num: SeqNumberType,
    point_id: PointIdType,
    vectors: NamedVectors,
    payload: Option<&Payload>,
) -> OperationResult<bool> {
    let mut res = segment.upsert_point(op_num, point_id, vectors)?;
    if let Some(full_payload) = payload {
        res &= segment.set_full_payload(op_num, point_id, full_payload)?;
    }
    Ok(res)
}

/// Sync points within a given [from_id; to_id) range.
///
/// 1. Retrieve existing points for a range
/// 2. Remove points, which are not present in the sync operation
/// 3. Retrieve overlapping points, detect which one of them are changed
/// 4. Select new points
/// 5. Upsert points which differ from the stored ones
///
/// Returns:
///     (number of deleted points, number of new points, number of updated points)
pub(crate) fn sync_points(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    from_id: Option<PointIdType>,
    to_id: Option<PointIdType>,
    points: &[PointStructPersisted],
) -> CollectionResult<(usize, usize, usize)> {
    let id_to_point: HashMap<PointIdType, _> = points.iter().map(|p| (p.id, p)).collect();
    let sync_points: HashSet<_> = points.iter().map(|p| p.id).collect();
    // 1. Retrieve existing points for a range
    let stored_point_ids: HashSet<_> = segments
        .iter()
        .flat_map(|(_, segment)| segment.get().read().read_range(from_id, to_id))
        .collect();
    // 2. Remove points, which are not present in the sync operation
    let points_to_remove: Vec<_> = stored_point_ids.difference(&sync_points).copied().collect();
    let deleted = delete_points(segments, op_num, points_to_remove.as_slice())?;
    // 3. Retrieve overlapping points, detect which one of them are changed
    let existing_point_ids: Vec<_> = stored_point_ids
        .intersection(&sync_points)
        .copied()
        .collect();

    let mut points_to_update: Vec<_> = Vec::new();
    // we don’t want to cancel this filtered read
    let is_stopped = AtomicBool::new(false);
    let _num_updated =
        segments.read_points(existing_point_ids.as_slice(), &is_stopped, |id, segment| {
            let all_vectors = match segment.all_vectors(id) {
                Ok(v) => v,
                Err(OperationError::InconsistentStorage { .. }) => NamedVectors::default(),
                Err(e) => return Err(e),
            };
            let payload = segment.payload(id)?;
            let point = id_to_point.get(&id).unwrap();
            if point.get_vectors() != all_vectors {
                points_to_update.push(*point);
                Ok(true)
            } else {
                let payload_match = match point.payload {
                    Some(ref p) => p == &payload,
                    None => Payload::default() == payload,
                };
                if !payload_match {
                    points_to_update.push(*point);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        })?;

    // 4. Select new points
    let num_updated = points_to_update.len();
    let mut num_new = 0;
    sync_points.difference(&stored_point_ids).for_each(|id| {
        num_new += 1;
        points_to_update.push(*id_to_point.get(id).unwrap());
    });

    // 5. Upsert points which differ from the stored ones
    let num_replaced = upsert_points(segments, op_num, points_to_update)?;
    debug_assert!(num_replaced <= num_updated, "number of replaced points cannot be greater than points to update ({num_replaced} <= {num_updated})");

    Ok((deleted, num_new, num_updated))
}

/// Checks point id in each segment, update point if found.
/// All not found points are inserted into random segment.
/// Returns: number of updated points.
pub(crate) fn upsert_points<'a, T>(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: T,
) -> CollectionResult<usize>
where
    T: IntoIterator<Item = &'a PointStructPersisted>,
{
    let points_map: HashMap<PointIdType, _> = points.into_iter().map(|p| (p.id, p)).collect();
    let ids: Vec<PointIdType> = points_map.keys().copied().collect();

    // Update points in writable segments
    let updated_points = segments.apply_points_with_conditional_move(
        op_num,
        &ids,
        |id, write_segment| {
            let point = points_map[&id];
            upsert_with_payload(
                write_segment,
                op_num,
                id,
                point.get_vectors(),
                point.payload.as_ref(),
            )
        },
        |id, vectors, old_payload| {
            let point = points_map[&id];
            for (name, vec) in point.get_vectors() {
                vectors.insert(name.to_string(), vec.to_owned());
            }
            if let Some(payload) = &point.payload {
                *old_payload = payload.clone();
            }
        },
        |_| false,
    )?;

    let mut res = updated_points.len();
    // Insert new points, which was not updated or existed
    let new_point_ids = ids.iter().copied().filter(|x| !updated_points.contains(x));

    {
        let default_write_segment = segments.smallest_appendable_segment().ok_or_else(|| {
            CollectionError::service_error("No appendable segments exists, expected at least one")
        })?;

        let segment_arc = default_write_segment.get();
        let mut write_segment = segment_arc.write();
        for point_id in new_point_ids {
            let point = points_map[&point_id];
            res += usize::from(upsert_with_payload(
                &mut write_segment,
                op_num,
                point_id,
                point.get_vectors(),
                point.payload.as_ref(),
            )?);
        }
        RwLockWriteGuard::unlock_fair(write_segment);
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
            let points: Vec<_> = match operation {
                PointInsertOperationsInternal::PointsBatch(batch) => {
                    let batch_vectors = BatchVectorStructInternal::from(batch.vectors);
                    let all_vectors = batch_vectors.into_all_vectors(batch.ids.len());
                    let vectors_iter = batch.ids.into_iter().zip(all_vectors);
                    match batch.payloads {
                        None => vectors_iter
                            .map(|(id, vectors)| PointStructPersisted {
                                id,
                                vector: VectorStructInternal::from(vectors).into(),
                                payload: None,
                            })
                            .collect(),
                        Some(payloads) => vectors_iter
                            .zip(payloads)
                            .map(|((id, vectors), payload)| PointStructPersisted {
                                id,
                                vector: VectorStructInternal::from(vectors).into(),
                                payload,
                            })
                            .collect(),
                    }
                }
                PointInsertOperationsInternal::PointsList(points) => points,
            };
            let res = upsert_points(&segments.read(), op_num, points.iter())?;
            Ok(res)
        }
        PointOperations::DeletePointsByFilter(filter) => {
            delete_points_by_filter(&segments.read(), op_num, &filter)
        }
        PointOperations::SyncPoints(operation) => {
            let (deleted, new, updated) = sync_points(
                &segments.read(),
                op_num,
                operation.from_id,
                operation.to_id,
                &operation.points,
            )?;
            Ok(deleted + new + updated)
        }
    }
}

pub(crate) fn process_vector_operation(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    vector_operation: VectorOperations,
) -> CollectionResult<usize> {
    match vector_operation {
        VectorOperations::UpdateVectors(operation) => {
            update_vectors(&segments.read(), op_num, operation.points)
        }
        VectorOperations::DeleteVectors(ids, vector_names) => {
            delete_vectors(&segments.read(), op_num, &ids.points, &vector_names)
        }
        VectorOperations::DeleteVectorsByFilter(filter, vector_names) => {
            delete_vectors_by_filter(&segments.read(), op_num, &filter, &vector_names)
        }
    }
}

pub(crate) fn process_payload_operation(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    payload_operation: PayloadOps,
) -> CollectionResult<usize> {
    match payload_operation {
        PayloadOps::SetPayload(sp) => {
            let payload: Payload = sp.payload;
            if let Some(points) = sp.points {
                set_payload(&segments.read(), op_num, &payload, &points, &sp.key)
            } else if let Some(filter) = sp.filter {
                set_payload_by_filter(&segments.read(), op_num, &payload, &filter, &sp.key)
            } else {
                Err(CollectionError::BadRequest {
                    description: "No points or filter specified".to_string(),
                })
            }
        }
        PayloadOps::DeletePayload(dp) => {
            if let Some(points) = dp.points {
                delete_payload(&segments.read(), op_num, &points, &dp.keys)
            } else if let Some(filter) = dp.filter {
                delete_payload_by_filter(&segments.read(), op_num, &filter, &dp.keys)
            } else {
                Err(CollectionError::BadRequest {
                    description: "No points or filter specified".to_string(),
                })
            }
        }
        PayloadOps::ClearPayload { ref points, .. } => {
            clear_payload(&segments.read(), op_num, points)
        }
        PayloadOps::ClearPayloadByFilter(ref filter) => {
            clear_payload_by_filter(&segments.read(), op_num, filter)
        }
        PayloadOps::OverwritePayload(sp) => {
            let payload: Payload = sp.payload;
            if let Some(points) = sp.points {
                overwrite_payload(&segments.read(), op_num, &payload, &points)
            } else if let Some(filter) = sp.filter {
                overwrite_payload_by_filter(&segments.read(), op_num, &payload, &filter)
            } else {
                Err(CollectionError::BadRequest {
                    description: "No points or filter specified".to_string(),
                })
            }
        }
    }
}

pub(crate) fn process_field_index_operation(
    segments: &RwLock<SegmentHolder>,
    op_num: SeqNumberType,
    field_index_operation: &FieldIndexOperations,
) -> CollectionResult<usize> {
    match field_index_operation {
        FieldIndexOperations::CreateIndex(index_data) => create_field_index(
            &segments.read(),
            op_num,
            &index_data.field_name,
            index_data.field_schema.as_ref(),
        ),
        FieldIndexOperations::DeleteIndex(field_name) => {
            delete_field_index(&segments.read(), op_num, field_name)
        }
    }
}

/// Max amount of points to delete in a batched deletion iteration.
const DELETION_BATCH_SIZE: usize = 512;

/// Deletes points from all segments matching the given filter
pub(crate) fn delete_points_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
) -> CollectionResult<usize> {
    let mut total_deleted = 0;
    // we don’t want to cancel this filtered read
    let is_stopped = AtomicBool::new(false);
    let mut points_to_delete: HashMap<_, _> = segments
        .iter()
        .map(|(segment_id, segment)| {
            (
                *segment_id,
                segment
                    .get()
                    .read()
                    .read_filtered(None, None, Some(filter), &is_stopped),
            )
        })
        .collect();

    segments.apply_segments_batched(|s, segment_id| {
        let Some(curr_points) = points_to_delete.get_mut(&segment_id) else {
            return Ok(false);
        };
        if curr_points.is_empty() {
            return Ok(false);
        }

        let mut deleted_in_batch = 0;
        while let Some(point_id) = curr_points.pop() {
            if s.delete_point(op_num, point_id)? {
                total_deleted += 1;
                deleted_in_batch += 1;
            }

            if deleted_in_batch >= DELETION_BATCH_SIZE {
                break;
            }
        }

        Ok(true)
    })?;

    Ok(total_deleted)
}
