//! A collection of functions for updating points and payloads stored in segments

use std::sync::atomic::AtomicBool;

use ahash::{AHashMap, AHashSet};
use common::counter::hardware_counter::HardwareCounterCell;
use parking_lot::RwLockWriteGuard;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::build_index_result::BuildFieldIndexResult;
use segment::data_types::named_vectors::NamedVectors;
use segment::entry::entry_point::SegmentEntry;
use segment::json_path::JsonPath;
use segment::types::{
    Condition, Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef, PointIdType,
    SeqNumberType, VectorNameBuf, WithPayload, WithVector,
};

use crate::operations::FieldIndexOperations;
use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::{
    ConditionalInsertOperationInternal, PointOperations, PointStructPersisted, UpdateMode,
};
use crate::operations::vector_ops::{PointVectorsPersisted, UpdateVectorsOp, VectorOperations};
use crate::segment_holder::SegmentHolder;

pub fn process_point_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    point_operation: PointOperations,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    match point_operation {
        PointOperations::UpsertPoints(operation) => {
            let points = operation.into_point_vec();
            let res = upsert_points(segments, op_num, points.iter(), hw_counter)?;
            Ok(res)
        }
        PointOperations::UpsertPointsConditional(operation) => {
            conditional_upsert(segments, op_num, operation, hw_counter)
        }
        PointOperations::DeletePoints { ids } => delete_points(segments, op_num, &ids, hw_counter),
        PointOperations::DeletePointsByFilter(filter) => {
            delete_points_by_filter(segments, op_num, &filter, hw_counter)
        }
        PointOperations::SyncPoints(operation) => {
            let (deleted, new, updated) = sync_points(
                segments,
                op_num,
                operation.from_id,
                operation.to_id,
                &operation.points,
                hw_counter,
            )?;
            Ok(deleted + new + updated)
        }
    }
}

#[cfg(feature = "staging")]
pub fn process_staging_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    operation: crate::operations::staging::StagingOperations,
) -> OperationResult<usize> {
    match operation {
        crate::operations::staging::StagingOperations::Delay(delay_op) => {
            delay_op.execute();
        }
    }

    // This operation doesn't directly affect segment/point versions, so we bump it here
    segments.bump_max_segment_version_overwrite(op_num);

    Ok(0)
}

pub fn process_vector_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    vector_operation: VectorOperations,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    match vector_operation {
        VectorOperations::UpdateVectors(update_vectors) => {
            update_vectors_conditional(segments, op_num, update_vectors, hw_counter)
        }
        VectorOperations::DeleteVectors(ids, vector_names) => {
            delete_vectors(segments, op_num, &ids.points, &vector_names, hw_counter)
        }
        VectorOperations::DeleteVectorsByFilter(filter, vector_names) => {
            delete_vectors_by_filter(segments, op_num, &filter, &vector_names, hw_counter)
        }
    }
}

pub fn process_payload_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload_operation: PayloadOps,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    match payload_operation {
        PayloadOps::SetPayload(sp) => {
            let payload: Payload = sp.payload;
            if let Some(points) = sp.points {
                set_payload(segments, op_num, &payload, &points, &sp.key, hw_counter)
            } else if let Some(filter) = sp.filter {
                set_payload_by_filter(segments, op_num, &payload, &filter, &sp.key, hw_counter)
            } else {
                // TODO: BadRequest (prev) vs BadInput (current)!?
                Err(OperationError::ValidationError {
                    description: "No points or filter specified".to_string(),
                })
            }
        }
        PayloadOps::DeletePayload(dp) => {
            if let Some(points) = dp.points {
                delete_payload(segments, op_num, &points, &dp.keys, hw_counter)
            } else if let Some(filter) = dp.filter {
                delete_payload_by_filter(segments, op_num, &filter, &dp.keys, hw_counter)
            } else {
                // TODO: BadRequest (prev) vs BadInput (current)!?
                Err(OperationError::ValidationError {
                    description: "No points or filter specified".to_string(),
                })
            }
        }
        PayloadOps::ClearPayload { ref points, .. } => {
            clear_payload(segments, op_num, points, hw_counter)
        }
        PayloadOps::ClearPayloadByFilter(ref filter) => {
            clear_payload_by_filter(segments, op_num, filter, hw_counter)
        }
        PayloadOps::OverwritePayload(sp) => {
            let payload: Payload = sp.payload;
            if let Some(points) = sp.points {
                overwrite_payload(segments, op_num, &payload, &points, hw_counter)
            } else if let Some(filter) = sp.filter {
                overwrite_payload_by_filter(segments, op_num, &payload, &filter, hw_counter)
            } else {
                // TODO: BadRequest (prev) vs BadInput (current)!?
                Err(OperationError::ValidationError {
                    description: "No points or filter specified".to_string(),
                })
            }
        }
    }
}

pub fn process_field_index_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    field_index_operation: &FieldIndexOperations,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    match field_index_operation {
        FieldIndexOperations::CreateIndex(index_data) => create_field_index(
            segments,
            op_num,
            &index_data.field_name,
            index_data.field_schema.as_ref(),
            hw_counter,
        ),
        FieldIndexOperations::DeleteIndex(field_name) => {
            delete_field_index(segments, op_num, field_name)
        }
    }
}

/// Do not insert more than this number of points in a single update operation chunk
/// This is needed to avoid locking segments for too long, so that
/// parallel read operations are not starved.
const UPDATE_OP_CHUNK_SIZE: usize = 32;

/// Checks point id in each segment, update point if found.
/// All not found points are inserted into random segment.
/// Returns: number of updated points.
pub fn upsert_points<'a, T>(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: T,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize>
where
    T: IntoIterator<Item = &'a PointStructPersisted>,
{
    let points_map: AHashMap<PointIdType, _> = points.into_iter().map(|p| (p.id, p)).collect();
    let ids: Vec<PointIdType> = points_map.keys().copied().collect();

    let mut res = 0;

    for ids_chunk in ids.chunks(UPDATE_OP_CHUNK_SIZE) {
        // Update points in writable segments
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            ids_chunk,
            |id, write_segment| {
                let point = points_map[&id];
                upsert_with_payload(
                    write_segment,
                    op_num,
                    id,
                    point.get_vectors(),
                    point.payload.as_ref(),
                    hw_counter,
                )
            },
            |id, vectors, old_payload| {
                let point = points_map[&id];
                for (name, vec) in point.get_vectors() {
                    vectors.insert(name.into(), vec.to_owned());
                }
                if let Some(payload) = &point.payload {
                    *old_payload = payload.clone();
                }
            },
            hw_counter,
        )?;

        res += updated_points.len();
        // Insert new points, which was not updated or existed
        let new_point_ids = ids_chunk
            .iter()
            .copied()
            .filter(|x| !updated_points.contains(x));

        {
            let default_write_segment =
                segments.smallest_appendable_segment().ok_or_else(|| {
                    OperationError::service_error(
                        "No appendable segments exist, expected at least one",
                    )
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
                    hw_counter,
                )?);
            }
            RwLockWriteGuard::unlock_fair(write_segment);
        };
    }

    Ok(res)
}

pub fn conditional_upsert(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    operation: ConditionalInsertOperationInternal,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    // Find points, which do exist, but don't match the condition.
    // Exclude those points from the upsert operation.

    let ConditionalInsertOperationInternal {
        mut points_op,
        condition,
        update_mode,
    } = operation;

    let point_ids = points_op.point_ids();
    let update_mode = update_mode.unwrap_or_default();

    match update_mode {
        UpdateMode::Upsert => {
            // Default behavior: insert new points, update existing points that match the condition
            let points_to_exclude =
                select_excluded_by_filter_ids(segments, point_ids, condition, hw_counter)?;
            points_op.retain_point_ids(|idx| !points_to_exclude.contains(idx));
        }
        UpdateMode::InsertOnly => {
            // Only insert new points, skip all existing points entirely
            let existing_points = segments.select_existing_points(point_ids);
            points_op.retain_point_ids(|idx| !existing_points.contains(idx));
        }
        UpdateMode::UpdateOnly => {
            // Only update existing points that match the condition, don't insert new points
            let points_to_exclude =
                select_excluded_by_filter_ids(segments, point_ids.clone(), condition, hw_counter)?;
            let existing_points = segments.select_existing_points(point_ids);
            // Keep only points that exist AND are not excluded by the condition
            points_op.retain_point_ids(|idx| {
                existing_points.contains(idx) && !points_to_exclude.contains(idx)
            });
        }
    }

    let points = points_op.into_point_vec();
    let upserted_points = upsert_points(segments, op_num, points.iter(), hw_counter)?;

    if upserted_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(upserted_points)
}

/// Upsert to a point ID with the specified vectors and payload in the given segment.
///
/// If the payload is None, the existing payload will be cleared.
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
    hw_counter: &HardwareCounterCell,
) -> OperationResult<bool> {
    let mut res = segment.upsert_point(op_num, point_id, vectors, hw_counter)?;
    if let Some(full_payload) = payload {
        res &= segment.set_full_payload(op_num, point_id, full_payload, hw_counter)?;
    } else {
        res &= segment.clear_payload(op_num, point_id, hw_counter)?;
    }
    debug_assert!(
        segment.has_point(point_id),
        "the point {point_id} should be present immediately after the upsert"
    );
    Ok(res)
}

/// Max amount of points to delete in a batched deletion iteration
const DELETION_BATCH_SIZE: usize = 512;

/// Tries to delete points from all segments, returns number of actually deleted points
pub fn delete_points(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    ids: &[PointIdType],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_deleted_points = 0;

    for batch in ids.chunks(DELETION_BATCH_SIZE) {
        let deleted_points = segments.apply_points(batch, |id, _idx, write_segment| {
            write_segment.delete_point(op_num, id, hw_counter)
        })?;

        total_deleted_points += deleted_points;
    }

    Ok(total_deleted_points)
}

/// Deletes points from all segments matching the given filter
pub fn delete_points_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_deleted = 0;
    // we don’t want to cancel this filtered read
    let is_stopped = AtomicBool::new(false);
    let mut points_to_delete: AHashMap<_, _> = segments
        .iter()
        .map(|(segment_id, segment)| {
            (
                segment_id,
                segment.get().read().read_filtered(
                    None,
                    None,
                    Some(filter),
                    &is_stopped,
                    hw_counter,
                ),
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
            if s.delete_point(op_num, point_id, hw_counter)? {
                total_deleted += 1;
                deleted_in_batch += 1;
            }

            if deleted_in_batch >= DELETION_BATCH_SIZE {
                break;
            }
        }

        Ok(true)
    })?;

    if total_deleted == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(total_deleted)
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
pub fn sync_points(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    from_id: Option<PointIdType>,
    to_id: Option<PointIdType>,
    points: &[PointStructPersisted],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<(usize, usize, usize)> {
    let id_to_point: AHashMap<PointIdType, _> = points.iter().map(|p| (p.id, p)).collect();
    let sync_points: AHashSet<_> = points.iter().map(|p| p.id).collect();
    // 1. Retrieve existing points for a range
    let stored_point_ids: AHashSet<_> = segments
        .iter()
        .flat_map(|(_, segment)| segment.get().read().read_range(from_id, to_id))
        .collect();
    // 2. Remove points, which are not present in the sync operation
    let points_to_remove: Vec<_> = stored_point_ids.difference(&sync_points).copied().collect();
    let deleted = delete_points(segments, op_num, points_to_remove.as_slice(), hw_counter)?;
    // 3. Retrieve overlapping points, detect which one of them are changed
    let existing_point_ids: Vec<_> = stored_point_ids
        .intersection(&sync_points)
        .copied()
        .collect();

    let mut points_to_update: Vec<_> = Vec::new();
    // we don’t want to cancel this filtered read
    let is_stopped = AtomicBool::new(false);
    let _num_updated = segments.read_points(
        existing_point_ids.as_slice(),
        &is_stopped,
        |ids, segment| {
            let with_vector = WithVector::Bool(true);
            let with_payload = WithPayload::from(true);
            // Since we retrieve points, which we already know exist, we expect all of them to be found
            let stored_records =
                segment.retrieve(ids, &with_payload, &with_vector, hw_counter, &is_stopped)?;
            let mut updated = 0;

            for (id, stored_record) in stored_records {
                let point = id_to_point.get(&id).unwrap();
                if !point.is_equal_to(&stored_record) {
                    points_to_update.push(*point);
                    updated += 1;
                }
            }

            Ok(updated)
        },
    )?;

    // 4. Select new points
    let num_updated = points_to_update.len();
    let mut num_new = 0;
    sync_points.difference(&stored_point_ids).for_each(|id| {
        num_new += 1;
        points_to_update.push(*id_to_point.get(id).unwrap());
    });

    // 5. Upsert points which differ from the stored ones
    let num_replaced = upsert_points(segments, op_num, points_to_update, hw_counter)?;
    debug_assert!(
        num_replaced <= num_updated,
        "number of replaced points cannot be greater than points to update ({num_replaced} <= {num_updated})",
    );

    Ok((deleted, num_new, num_updated))
}

/// Batch size when modifying vector
const VECTOR_OP_BATCH_SIZE: usize = 32;

pub fn update_vectors_conditional(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: UpdateVectorsOp,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let UpdateVectorsOp {
        mut points,
        update_filter,
    } = points;

    let Some(filter_condition) = update_filter else {
        return update_vectors(segments, op_num, points, hw_counter);
    };

    let point_ids: Vec<_> = points.iter().map(|point| point.id).collect();

    let points_to_exclude =
        select_excluded_by_filter_ids(segments, point_ids, filter_condition, hw_counter)?;

    points.retain(|p| !points_to_exclude.contains(&p.id));
    update_vectors(segments, op_num, points, hw_counter)
}

/// Update the specified named vectors of a point, keeping unspecified vectors intact.
fn update_vectors(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: Vec<PointVectorsPersisted>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    // Build a map of vectors to update per point, merge updates on same point ID
    let mut points_map: AHashMap<PointIdType, NamedVectors> = AHashMap::new();
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
                write_segment.update_vectors(op_num, id, vectors, hw_counter)
            },
            |id, owned_vectors, _| {
                for (vector_name, vector_ref) in points_map[&id].iter() {
                    owned_vectors.insert(vector_name.to_owned(), vector_ref.to_owned());
                }
            },
            hw_counter,
        )?;
        check_unprocessed_points(batch, &updated_points)?;
        total_updated_points += updated_points.len();
    }

    Ok(total_updated_points)
}

/// Delete the given named vectors for the given points, keeping other vectors intact.
pub fn delete_vectors(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: &[PointIdType],
    vector_names: &[VectorNameBuf],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_deleted_points = 0;

    for batch in points.chunks(VECTOR_OP_BATCH_SIZE) {
        let modified_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| {
                let mut res = true;
                for name in vector_names {
                    res &= write_segment.delete_vector(op_num, id, name)?;
                }
                Ok(res)
            },
            |_, owned_vectors, _| {
                for name in vector_names {
                    owned_vectors.remove_ref(name);
                }
            },
            hw_counter,
        )?;
        check_unprocessed_points(batch, &modified_points)?;
        total_deleted_points += modified_points.len();
    }

    Ok(total_deleted_points)
}

/// Delete the given named vectors for points matching the given filter, keeping other vectors intact.
pub fn delete_vectors_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
    vector_names: &[VectorNameBuf],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let affected_points = points_by_filter(segments, filter, hw_counter)?;
    let vectors_deleted =
        delete_vectors(segments, op_num, &affected_points, vector_names, hw_counter)?;

    if vectors_deleted == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(vectors_deleted)
}

/// Batch size when modifying payload
const PAYLOAD_OP_BATCH_SIZE: usize = 32;

pub fn set_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    points: &[PointIdType],
    key: &Option<JsonPath>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_updated_points = 0;

    for chunk in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            chunk,
            |id, write_segment| write_segment.set_payload(op_num, id, payload, key, hw_counter),
            |_, _, old_payload| match key {
                Some(key) => old_payload.merge_by_key(payload, key),
                None => old_payload.merge(payload),
            },
            hw_counter,
        )?;

        check_unprocessed_points(chunk, &updated_points)?;
        total_updated_points += updated_points.len();
    }

    Ok(total_updated_points)
}

pub fn set_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    filter: &Filter,
    key: &Option<JsonPath>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let affected_points = points_by_filter(segments, filter, hw_counter)?;
    let points_updated = set_payload(segments, op_num, payload, &affected_points, key, hw_counter)?;

    if points_updated == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(points_updated)
}

pub fn delete_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: &[PointIdType],
    keys: &[PayloadKeyType],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_deleted_points = 0;

    for batch in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| {
                let mut res = true;
                for key in keys {
                    res &= write_segment.delete_payload(op_num, id, key, hw_counter)?;
                }
                Ok(res)
            },
            |_, _, payload| {
                for key in keys {
                    payload.remove(key);
                }
            },
            hw_counter,
        )?;

        check_unprocessed_points(batch, &updated_points)?;
        total_deleted_points += updated_points.len();
    }

    Ok(total_deleted_points)
}

pub fn delete_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
    keys: &[PayloadKeyType],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let affected_points = points_by_filter(segments, filter, hw_counter)?;
    let points_updated = delete_payload(segments, op_num, &affected_points, keys, hw_counter)?;

    if points_updated == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(points_updated)
}

pub fn clear_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: &[PointIdType],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_updated_points = 0;

    for batch in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| write_segment.clear_payload(op_num, id, hw_counter),
            |_, _, payload| payload.0.clear(),
            hw_counter,
        )?;
        check_unprocessed_points(batch, &updated_points)?;
        total_updated_points += updated_points.len();
    }

    Ok(total_updated_points)
}

/// Clear Payloads from all segments matching the given filter
pub fn clear_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    filter: &Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let points_to_clear = points_by_filter(segments, filter, hw_counter)?;
    let points_cleared = clear_payload(segments, op_num, &points_to_clear, hw_counter)?;

    if points_cleared == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(points_cleared)
}

pub fn overwrite_payload(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    points: &[PointIdType],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_updated_points = 0;

    for batch in points.chunks(PAYLOAD_OP_BATCH_SIZE) {
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            batch,
            |id, write_segment| write_segment.set_full_payload(op_num, id, payload, hw_counter),
            |_, _, old_payload| {
                *old_payload = payload.clone();
            },
            hw_counter,
        )?;

        total_updated_points += updated_points.len();
        check_unprocessed_points(batch, &updated_points)?;
    }

    Ok(total_updated_points)
}

pub fn overwrite_payload_by_filter(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    payload: &Payload,
    filter: &Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let affected_points = points_by_filter(segments, filter, hw_counter)?;
    let points_updated =
        overwrite_payload(segments, op_num, payload, &affected_points, hw_counter)?;

    if points_updated == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(points_updated)
}

pub fn create_field_index(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    field_name: PayloadKeyTypeRef,
    field_schema: Option<&PayloadFieldSchema>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let Some(field_schema) = field_schema else {
        return Err(OperationError::TypeInferenceError {
            field_name: field_name.to_owned(),
        });
    };

    segments.apply_segments(|write_segment| {
        write_segment.with_upgraded(|segment| {
            segment.delete_field_index_if_incompatible(op_num, field_name, field_schema)
        })?;

        let (schema, indexes) =
            match write_segment.build_field_index(op_num, field_name, field_schema, hw_counter)? {
                BuildFieldIndexResult::SkippedByVersion => {
                    return Ok(false);
                }
                BuildFieldIndexResult::AlreadyExists => {
                    return Ok(false);
                }
                BuildFieldIndexResult::IncompatibleSchema => {
                    // This is a service error, as we should have just removed the old index
                    // So it should not be possible to get this error
                    return Err(OperationError::service_error(format!(
                        "Incompatible schema for field index on field {field_name}",
                    )));
                }
                BuildFieldIndexResult::Built { schema, indexes } => (schema, indexes),
            };

        write_segment.with_upgraded(|segment| {
            segment.apply_field_index(op_num, field_name.to_owned(), schema, indexes)
        })
    })
}

pub fn delete_field_index(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    field_name: PayloadKeyTypeRef,
) -> OperationResult<usize> {
    segments.apply_segments(|write_segment| {
        write_segment.with_upgraded(|segment| segment.delete_field_index(op_num, field_name))
    })
}

fn select_excluded_by_filter_ids(
    segments: &SegmentHolder,
    point_ids: impl IntoIterator<Item = PointIdType>,
    filter: Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<AHashSet<PointIdType>> {
    // Filter for points that doesn't match the condition, and have matching
    let non_match_filter =
        Filter::new_must_not(Condition::Filter(filter)).with_point_ids(point_ids);

    Ok(points_by_filter(segments, &non_match_filter, hw_counter)?
        .into_iter()
        .collect())
}

fn points_by_filter(
    segments: &SegmentHolder,
    filter: &Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Vec<PointIdType>> {
    let mut affected_points: Vec<PointIdType> = Vec::new();
    // we don’t want to cancel this filtered read
    let is_stopped = AtomicBool::new(false);
    segments.for_each_segment(|s| {
        let points = s.read_filtered(None, None, Some(filter), &is_stopped, hw_counter);
        affected_points.extend_from_slice(points.as_slice());
        Ok(true)
    })?;
    Ok(affected_points)
}

fn check_unprocessed_points(
    points: &[PointIdType],
    processed: &AHashSet<PointIdType>,
) -> OperationResult<usize> {
    let first_missed_point = points.iter().copied().find(|p| !processed.contains(p));

    match first_missed_point {
        None => Ok(processed.len()),
        Some(missed_point_id) => Err(OperationError::PointIdError { missed_point_id }),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common::counter::hardware_counter::HardwareCounterCell;
    use parking_lot::RwLock;
    use segment::types::{Condition, FieldCondition, Filter, Match, MatchValue, ValueVariants};
    use tempfile::Builder;

    use crate::fixtures::{build_segment_1, build_segment_2};
    use crate::segment_holder::SegmentHolder;
    use crate::update::delete_points_by_filter;

    #[test]
    fn test_delete_by_filter_version_bump() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segment1 = build_segment_1(dir.path());
        let segment2 = build_segment_2(dir.path());

        let hw_counter = HardwareCounterCell::new();

        let mut holder = SegmentHolder::default();

        let _sid1 = holder.add_new(segment1);
        let _sid2 = holder.add_new(segment2);

        const DELETE_OP_NUM: u64 = 16;

        assert!(
            holder
                .iter()
                .all(|i| i.1.get().read().version() < DELETE_OP_NUM)
        );

        let old_version = holder
            .flush_all(true, false)
            .expect("Failed to flush test segment holder");

        let segments = Arc::new(RwLock::new(holder));

        // A filter that matches no points.
        let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
            "color".parse().unwrap(),
            Match::Value(MatchValue {
                value: ValueVariants::String("white".to_string()),
            }),
        )));

        let deleted_count =
            delete_points_by_filter(&segments.read(), DELETE_OP_NUM, &filter, &hw_counter).unwrap();
        assert_eq!(deleted_count, 0);

        let new_version = segments
            .read()
            .flush_all(true, false)
            .expect("Failed to flush test segment holder");

        // Flushing again inrceases by 1 and is now equal to `DELETE_OP_NUM` as we want to acknowledge the empty
        // delete operation in WAL.
        assert_eq!(old_version + 1, new_version);
        assert_eq!(new_version, DELETE_OP_NUM);
    }
}
