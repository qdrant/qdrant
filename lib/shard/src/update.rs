//! A collection of functions for updating points and payloads stored in segments

use std::sync::atomic::AtomicBool;

use ahash::{AHashMap, AHashSet};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
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

use crate::operations::payload_ops::{DeletePayloadOp, PayloadOps, SetPayloadOp};
use crate::operations::point_ops::{
    ConditionalInsertOperationInternal, PointInsertOperationsInternal, PointOperations,
    PointStructPersisted, PointStructRawPersisted, UpdateMode,
};
use crate::operations::vector_ops::{PointVectorsPersisted, UpdateVectorsOp, VectorOperations};
use crate::operations::{
    CreateVectorName, DeleteVectorName, FieldIndexOperations, VectorNameOperations,
};
use crate::segment_holder::locked::LockedSegmentHolder;
use crate::segment_holder::provisioning::SegmentProvisioning;
use crate::segment_holder::{SegmentHolder, SegmentId};

pub fn process_point_operation(
    segments: &LockedSegmentHolder,
    provisioning: Option<&SegmentProvisioning>,
    op_num: SeqNumberType,
    point_operation: PointOperations,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    match point_operation {
        PointOperations::UpsertPoints(operation) => {
            let points = operation.into_point_vec();
            segments.update_with_segment_provisioning(provisioning, |segments| {
                if points.is_empty() {
                    // An empty upsert (e.g. an emptied-out resolved conditional upsert)
                    // touches no segment; bump so WAL can acknowledge it.
                    segments.bump_max_segment_version_overwrite(op_num);
                }
                upsert_points(segments, op_num, points.iter(), hw_counter)
            })
        }
        PointOperations::UpsertPointsConditional(operation) => {
            conditional_upsert(segments, provisioning, op_num, operation, hw_counter)
        }
        PointOperations::DeletePoints { ids } => {
            delete_points(&segments.read(), op_num, &ids, hw_counter)
        }
        PointOperations::DeletePointsByFilter(filter) => {
            delete_points_by_filter(&segments.read(), op_num, &filter, hw_counter)
        }
        PointOperations::SyncPoints(operation) => {
            segments.update_with_segment_provisioning(provisioning, |segments| {
                let (deleted, new, updated) = sync_points(
                    segments,
                    op_num,
                    operation.from_id,
                    operation.to_id,
                    &operation.points,
                    hw_counter,
                )?;
                Ok(deleted + new + updated)
            })
        }
        PointOperations::UpsertPointsRaw(points) => {
            segments.update_with_segment_provisioning(provisioning, |segments| {
                if points.is_empty() {
                    // An empty upsert touches no segment; bump so WAL can acknowledge it.
                    segments.bump_max_segment_version_overwrite(op_num);
                }
                upsert_points_raw(segments, op_num, points.iter(), hw_counter)
            })
        }
        PointOperations::SyncPointsRaw(operation) => {
            segments.update_with_segment_provisioning(provisioning, |segments| {
                let (deleted, new, updated) = sync_points_raw(
                    segments,
                    op_num,
                    operation.from_id,
                    operation.to_id,
                    &operation.points,
                    hw_counter,
                )?;
                Ok(deleted + new + updated)
            })
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
    segments: &LockedSegmentHolder,
    provisioning: Option<&SegmentProvisioning>,
    op_num: SeqNumberType,
    vector_operation: VectorOperations,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    match vector_operation {
        VectorOperations::UpdateVectors(update_vectors) => {
            update_vectors_conditional(segments, provisioning, op_num, update_vectors, hw_counter)
        }
        VectorOperations::DeleteVectors(ids, vector_names) => segments
            .update_with_segment_provisioning(provisioning, |segments| {
                delete_vectors(segments, op_num, &ids.points, &vector_names, hw_counter)
            }),
        VectorOperations::DeleteVectorsByFilter(filter, vector_names) => segments
            .update_with_segment_provisioning(provisioning, |segments| {
                delete_vectors_by_filter(segments, op_num, &filter, &vector_names, hw_counter)
            }),
    }
}

pub fn process_payload_operation(
    segments: &LockedSegmentHolder,
    provisioning: Option<&SegmentProvisioning>,
    op_num: SeqNumberType,
    payload_operation: PayloadOps,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    match payload_operation {
        PayloadOps::SetPayload(set_payload_op) => {
            let SetPayloadOp {
                payload,
                points,
                filter,
                key,
            } = set_payload_op;
            if let Some(points) = points {
                segments.update_with_segment_provisioning(provisioning, |segments| {
                    set_payload(segments, op_num, &payload, &points, &key, hw_counter)
                })
            } else if let Some(filter) = filter {
                segments.update_with_segment_provisioning(provisioning, |segments| {
                    set_payload_by_filter(segments, op_num, &payload, &filter, &key, hw_counter)
                })
            } else {
                // TODO: BadRequest (prev) vs BadInput (current)!?
                Err(OperationError::validation_error(
                    "No points or filter specified",
                ))
            }
        }
        PayloadOps::DeletePayload(delete_payload_op) => {
            let DeletePayloadOp {
                points,
                keys,
                filter,
            } = delete_payload_op;
            if let Some(points) = points {
                segments.update_with_segment_provisioning(provisioning, |segments| {
                    delete_payload(segments, op_num, &points, &keys, hw_counter)
                })
            } else if let Some(filter) = filter {
                segments.update_with_segment_provisioning(provisioning, |segments| {
                    delete_payload_by_filter(segments, op_num, &filter, &keys, hw_counter)
                })
            } else {
                // TODO: BadRequest (prev) vs BadInput (current)!?
                Err(OperationError::validation_error(
                    "No points or filter specified",
                ))
            }
        }
        PayloadOps::ClearPayload { points } => segments
            .update_with_segment_provisioning(provisioning, |segments| {
                clear_payload(segments, op_num, &points, hw_counter)
            }),
        PayloadOps::ClearPayloadByFilter(filter) => segments
            .update_with_segment_provisioning(provisioning, |segments| {
                clear_payload_by_filter(segments, op_num, &filter, hw_counter)
            }),
        PayloadOps::OverwritePayload(set_payload_op) => {
            let SetPayloadOp {
                payload,
                points,
                filter,
                key: _,
            } = set_payload_op;
            if let Some(points) = points {
                segments.update_with_segment_provisioning(provisioning, |segments| {
                    overwrite_payload(segments, op_num, &payload, &points, hw_counter)
                })
            } else if let Some(filter) = filter {
                segments.update_with_segment_provisioning(provisioning, |segments| {
                    overwrite_payload_by_filter(segments, op_num, &payload, &filter, hw_counter)
                })
            } else {
                // TODO: BadRequest (prev) vs BadInput (current)!?
                Err(OperationError::validation_error(
                    "No points or filter specified",
                ))
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
            |id, raw_vectors, updated_vectors, old_payload| {
                let point = points_map[&id];
                // Upsert replaces the whole point: old named vectors and
                // payload must not survive the move, matching the in-place
                // `upsert_with_payload` path (`replace_all_vectors` +
                // clear/set payload).
                raw_vectors.clear();
                *updated_vectors = point.get_vectors();
                *old_payload = point.payload.clone().unwrap_or_default();
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
            let default_write_segment = segments.smallest_appendable_segment()?;

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

/// Drop from `points_op` every point that the conditional-upsert
/// `update_mode` excludes, judged against current segment state.
///
/// This is the state-reading half of a conditional upsert, shared by the
/// apply path ([`conditional_upsert`]) and the submit-time resolution that
/// rewrites the operation to a plain upsert before it reaches the WAL
/// (`resolve::resolve_operation`).
pub(crate) fn retain_conditional_upsert_points(
    segments: &SegmentHolder,
    points_op: &mut PointInsertOperationsInternal,
    condition: Filter,
    update_mode: Option<UpdateMode>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<()> {
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

    Ok(())
}

pub fn conditional_upsert(
    segments: &LockedSegmentHolder,
    provisioning: Option<&SegmentProvisioning>,
    op_num: SeqNumberType,
    operation: ConditionalInsertOperationInternal,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let ConditionalInsertOperationInternal {
        mut points_op,
        condition,
        update_mode,
    } = operation;

    // The condition is resolved once, against pre-operation state; a provisioning re-run of the
    // upsert below must not re-evaluate it against partially applied points.
    {
        let segments_guard = segments.read();
        retain_conditional_upsert_points(
            &segments_guard,
            &mut points_op,
            condition,
            update_mode,
            hw_counter,
        )?;
    }

    let points = points_op.into_point_vec();
    segments.update_with_segment_provisioning(provisioning, |segments| {
        let upserted_points = upsert_points(segments, op_num, points.iter(), hw_counter)?;

        if upserted_points == 0 {
            // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
            // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
            segments.bump_max_segment_version_overwrite(op_num);
        }

        Ok(upserted_points)
    })
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
        segment.has_point(point_id, DeferredBehavior::WithDeferred),
        "the point {point_id} should be present immediately after the upsert"
    );
    Ok(res)
}

/// Max amount of points to delete in a batched deletion iteration
const DELETION_BATCH_SIZE: usize = 512;

/// Tries to delete points from all segments, returns number of actually deleted points.
///
/// Iterates all segments directly (rather than going through `apply_points`) to ensure
/// that ALL copies of a point are deleted, including old non-deferred copies in optimized
/// segments when the latest version is deferred in an appendable segment.
pub fn delete_points(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    ids: &[PointIdType],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let mut total_deleted_points = 0;

    for batch in ids.chunks(DELETION_BATCH_SIZE) {
        for (_segment_id, segment) in segments.iter() {
            let segment_arc = segment.get();
            let mut write_segment = segment_arc.write();
            for &id in batch {
                if write_segment.delete_point(op_num, id, hw_counter)? {
                    total_deleted_points += 1;
                }
            }
        }
    }

    if total_deleted_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
    }

    Ok(total_deleted_points)
}

/// Deferred points corner case for filtered operations.
///
/// When a point has multiple copies across segments, the old non-deferred copy may match
/// a filter while the newest deferred copy does not. In this case, the operation should NOT
/// be applied to the point — the old copy will be cleaned up during optimization deduplication.
///
/// Given per-segment filter match results, this function returns the set of point IDs that
/// should be excluded from the operation because a newer deferred version exists that wasn't
/// matched by the filter.
fn deferred_points_to_exclude_by_filter(
    segments: &SegmentHolder,
    per_segment_points: &AHashMap<SegmentId, Vec<PointIdType>>,
) -> AHashSet<PointIdType> {
    // Find the maximum version for each point across segments where the filter matched.
    let mut max_versions: AHashMap<PointIdType, Option<SeqNumberType>> = Default::default();
    for (segment_id, point_ids) in per_segment_points {
        let segment = segments.get(*segment_id).unwrap().get().read();
        for point_id in point_ids {
            let version = segment.point_version(*point_id);
            let entry = max_versions.entry(*point_id).or_insert(None);
            *entry = std::cmp::max(*entry, version);
        }
    }

    // Check if any deferred point has a newer version than the max matched version.
    // Such a point was not matched by the filter (its deferred version has different data),
    // so the operation should not be applied.
    let mut to_exclude = AHashSet::new();
    for (_segment_id, segment) in segments.iter() {
        let segment = segment.get().read();
        if !segment.has_deferred_points() {
            continue;
        }
        for (point_id, max_version) in &max_versions {
            if segment.has_point(*point_id, DeferredBehavior::WithDeferred)
                && segment.point_version(*point_id) > *max_version
                && segment.point_is_deferred(*point_id)
            {
                to_exclude.insert(*point_id);
            }
        }
    }

    to_exclude
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
    let mut has_deferred = false;
    let mut points_to_delete: AHashMap<_, _> = segments
        .iter()
        .map(|(segment_id, segment)| {
            let segment = segment.get().read();
            let point_ids = segment.read_filtered(
                None,
                None,
                Some(filter),
                &is_stopped,
                hw_counter,
                // Include also deferred points.
                DeferredBehavior::WithDeferred,
            )?;
            has_deferred |= segment.has_deferred_points();
            Ok((segment_id, point_ids))
        })
        .collect::<OperationResult<_>>()?;

    // Deferred points corner case.
    // If the latest version of a point is deferred and does not match the filter,
    // we need to skip deletion for all copies and let deduplication during optimization delete old points.
    if has_deferred {
        let points_to_keep = deferred_points_to_exclude_by_filter(segments, &points_to_delete);

        // Expand per-segment lists to include all segments that have each matched point,
        // so that ALL copies get deleted (not just the segment where the filter matched).
        let all_matched_points: AHashSet<PointIdType> = points_to_delete
            .values()
            .flat_map(|v| v.iter().copied())
            .collect();
        for (segment_id, segment) in segments.iter() {
            let segment = segment.get().read();
            let present: Vec<_> = all_matched_points
                .iter()
                .copied()
                .filter(|point_id| {
                    segment.has_point(*point_id, DeferredBehavior::WithDeferred)
                        && !points_to_keep.contains(point_id)
                })
                .collect();
            points_to_delete.insert(segment_id, present);
        }
    }

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
        DeferredBehavior::WithDeferred,
        |ids, segment| {
            let with_vector = WithVector::Bool(true);
            let with_payload = WithPayload::from(true);
            // Since we retrieve points, which we already know exist, we expect all of them to be found
            let stored_records = segment.retrieve(
                ids,
                &with_payload,
                &with_vector,
                hw_counter,
                &is_stopped,
                DeferredBehavior::WithDeferred,
            )?;
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

pub fn upsert_points_raw<'a, T>(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points: T,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize>
where
    T: IntoIterator<Item = &'a PointStructRawPersisted>,
{
    let points_map: AHashMap<PointIdType, _> = points.into_iter().map(|p| (p.id, p)).collect();
    let ids: Vec<PointIdType> = points_map.keys().copied().collect();

    let mut res = 0;

    for ids_chunk in ids.chunks(UPDATE_OP_CHUNK_SIZE) {
        // Replace points which are already present in some writable segment
        let updated_points = segments.apply_points_with_conditional_move(
            op_num,
            ids_chunk,
            |id, write_segment| {
                upsert_raw_with_payload(write_segment, op_num, points_map[&id], hw_counter)
            },
            |id, raw_vectors, _updated_vectors, payload| {
                // A raw upsert replaces the whole point: drop the old raw
                // vectors and payload, carry the incoming bytes verbatim.
                let point = points_map[&id];
                raw_vectors.clear();
                raw_vectors.extend(point.vectors.iter().cloned());
                *payload = point.payload.clone().unwrap_or_default();
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
            let default_write_segment = segments.smallest_appendable_segment()?;

            let segment_arc = default_write_segment.get();
            let mut write_segment = segment_arc.write();
            for point_id in new_point_ids {
                res += usize::from(upsert_raw_with_payload(
                    &mut write_segment,
                    op_num,
                    points_map[&point_id],
                    hw_counter,
                )?);
            }
            RwLockWriteGuard::unlock_fair(write_segment);
        };
    }

    Ok(res)
}

fn upsert_raw_with_payload(
    segment: &mut RwLockWriteGuard<dyn SegmentEntry>,
    op_num: SeqNumberType,
    point: &PointStructRawPersisted,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<bool> {
    let mut res = segment.upsert_point_raw(op_num, point.id, &point.vectors, hw_counter)?;
    if let Some(full_payload) = &point.payload {
        res &= segment.set_full_payload(op_num, point.id, full_payload, hw_counter)?;
    } else {
        res &= segment.clear_payload(op_num, point.id, hw_counter)?;
    }
    debug_assert!(
        segment.has_point(point.id, DeferredBehavior::WithDeferred),
        "the point {} should be present immediately after the upsert",
        point.id,
    );
    Ok(res)
}

pub fn sync_points_raw(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    from_id: Option<PointIdType>,
    to_id: Option<PointIdType>,
    points: &[PointStructRawPersisted],
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
        DeferredBehavior::WithDeferred,
        |ids, segment| {
            let with_vector = WithVector::Bool(true);
            let with_payload = WithPayload::from(true);
            // Since we retrieve points, which we already know exist, we expect all of them to be found
            let stored_records = segment.retrieve_raw(
                ids,
                &with_payload,
                &with_vector,
                hw_counter,
                &is_stopped,
                DeferredBehavior::WithDeferred,
            )?;
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
    let num_replaced = upsert_points_raw(segments, op_num, points_to_update, hw_counter)?;
    debug_assert!(
        num_replaced <= num_updated,
        "number of replaced points cannot be greater than points to update ({num_replaced} <= {num_updated})",
    );

    Ok((deleted, num_new, num_updated))
}

/// Batch size when modifying vector
const VECTOR_OP_BATCH_SIZE: usize = 32;

pub fn update_vectors_conditional(
    segments: &LockedSegmentHolder,
    provisioning: Option<&SegmentProvisioning>,
    op_num: SeqNumberType,
    points: UpdateVectorsOp,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    let UpdateVectorsOp {
        mut points,
        update_filter,
    } = points;

    // The filter is resolved once, against pre-operation state; a provisioning re-run of the
    // update below must not re-evaluate it against partially applied points.
    if let Some(filter_condition) = update_filter {
        let segments_guard = segments.read();
        let point_ids: Vec<_> = points.iter().map(|point| point.id).collect();

        let points_to_exclude = select_excluded_by_filter_ids(
            &segments_guard,
            point_ids,
            filter_condition,
            hw_counter,
        )?;

        points.retain(|p| !points_to_exclude.contains(&p.id));
    }

    // Build a map of vectors to update per point, merge updates on same point ID
    let mut points_map: AHashMap<PointIdType, NamedVectors> = AHashMap::new();
    for point in points {
        let PointVectorsPersisted { id, vector } = point;
        let named_vector = NamedVectors::from(vector);

        let entry = points_map.entry(id).or_default();
        entry.merge(named_vector);
    }

    segments.update_with_segment_provisioning(provisioning, |segments| {
        update_vectors(segments, op_num, &points_map, hw_counter)
    })
}

/// Update the specified named vectors of a point, keeping unspecified vectors intact.
fn update_vectors(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    points_map: &AHashMap<PointIdType, NamedVectors>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
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
            |id, _raw_vectors, updated_vectors, _| {
                for (vector_name, vector_ref) in points_map[&id].iter() {
                    updated_vectors.insert_ref(vector_name, vector_ref);
                }
            },
            hw_counter,
        )?;
        check_unprocessed_points(batch, &updated_points)?;
        total_updated_points += updated_points.len();
    }

    if total_updated_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
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
            |_, raw_vectors, _, _| {
                raw_vectors.retain(|(name, _)| !vector_names.contains(name));
            },
            hw_counter,
        )?;
        check_unprocessed_points(batch, &modified_points)?;
        total_deleted_points += modified_points.len();
    }

    if total_deleted_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
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
            |_, _, _, old_payload| match key {
                Some(key) => old_payload.merge_by_key(payload, key),
                None => old_payload.merge(payload),
            },
            hw_counter,
        )?;

        check_unprocessed_points(chunk, &updated_points)?;
        total_updated_points += updated_points.len();
    }

    if total_updated_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
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
            |_, _, _, payload| {
                for key in keys {
                    payload.remove(key);
                }
            },
            hw_counter,
        )?;

        check_unprocessed_points(batch, &updated_points)?;
        total_deleted_points += updated_points.len();
    }

    if total_deleted_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
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
            |_, _, _, payload| payload.0.clear(),
            hw_counter,
        )?;
        check_unprocessed_points(batch, &updated_points)?;
        total_updated_points += updated_points.len();
    }

    if total_updated_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
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
            |_, _, _, old_payload| {
                *old_payload = payload.clone();
            },
            hw_counter,
        )?;

        total_updated_points += updated_points.len();
        check_unprocessed_points(batch, &updated_points)?;
    }

    if total_updated_points == 0 {
        // In case we didn't hit any points, we suggest this op_num to the segment-holder to make WAL acknowledge this operation.
        // If we don't do this, startup might take up a lot of time in some scenarios because of recovering these no-op operations.
        segments.bump_max_segment_version_overwrite(op_num);
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

        // Pin the state the build will observe before the index config durably marks
        // the index as complete. The build reads the live id tracker and payload
        // storage, which may hold changes that are not flushed yet; if such a change
        // were lost in a crash, the durable index (and the config that WAL replay
        // trusts via `AlreadyBuilt`) would permanently disagree with the durable
        // payload. Flushing the whole segment first makes every crash outcome
        // consistent: either the config is not durable yet and replay rebuilds, or
        // config, index data, and observed state are durable together.
        //
        // Serialized with the flush pipeline: an extra flush run between another
        // flusher's capture and its execution corrupts storage (see
        // `SegmentHolder::with_flush_serialized`). Skipped for idempotent
        // ensure-calls (index already present with an identical schema), which must
        // stay cheap; those resolve to `AlreadyExists` in the build below.
        let already_indexed =
            write_segment.get_indexed_fields().get(field_name) == Some(field_schema);
        if !already_indexed {
            segments.with_flush_serialized(|| write_segment.flush(true))?;
        }

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

pub fn process_vector_name_operation(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    vector_name_operation: &VectorNameOperations,
) -> OperationResult<usize> {
    match vector_name_operation {
        VectorNameOperations::CreateVectorName(create_data) => {
            let CreateVectorName {
                vector_name,
                config,
            } = create_data;

            segments.apply_segments(|write_segment| {
                write_segment.with_upgraded(|segment| {
                    segment.create_vector_name(op_num, vector_name, config)
                })
            })
        }
        VectorNameOperations::DeleteVectorName(delete_data) => {
            let DeleteVectorName { vector_name } = delete_data;
            segments.apply_segments(|write_segment| {
                write_segment
                    .with_upgraded(|segment| segment.delete_vector_name(op_num, vector_name))
            })
        }
    }
}

pub(crate) fn select_excluded_by_filter_ids(
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

pub(crate) fn points_by_filter(
    segments: &SegmentHolder,
    filter: &Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Vec<PointIdType>> {
    // we don’t want to cancel this filtered read
    let is_stopped = AtomicBool::new(false);
    let mut has_deferred = false;
    let per_segment_points: AHashMap<SegmentId, Vec<PointIdType>> = segments
        .iter()
        .map(|(segment_id, segment)| {
            let segment = segment.get().read();
            let point_ids = segment.read_filtered(
                None,
                None,
                Some(filter),
                &is_stopped,
                hw_counter,
                // Read operation used for updates, so we must handle all points
                DeferredBehavior::WithDeferred,
            )?;
            has_deferred |= segment.has_deferred_points();
            Ok((segment_id, point_ids))
        })
        .collect::<OperationResult<_>>()?;

    let mut affected_points: Vec<PointIdType> = per_segment_points
        .values()
        .flat_map(|v| v.iter().copied())
        .collect();

    // Deferred points corner case: exclude points where the newest version is deferred
    // and wasn’t matched by the filter (only an old stale copy matched).
    if has_deferred {
        let to_exclude = deferred_points_to_exclude_by_filter(segments, &per_segment_points);
        if !to_exclude.is_empty() {
            affected_points.retain(|id| !to_exclude.contains(id));
        }
    }

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
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use common::counter::hardware_counter::HardwareCounterCell;
    use common::save_on_disk::SaveOnDisk;
    use parking_lot::RwLock;
    use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
    use segment::entry::ReadSegmentEntry as _;
    use segment::entry::entry_point::SegmentEntry as _;
    use segment::payload_json;
    use segment::types::{
        Condition, FieldCondition, Filter, Match, MatchValue, PayloadKeyType, PointIdType,
        ValueVariants,
    };
    use tempfile::Builder;

    use super::{
        PayloadOps, PointInsertOperationsInternal, PointOperations, PointStructPersisted,
        SetPayloadOp,
    };
    use crate::fixtures::{
        build_segment_1, build_segment_2, empty_segment, empty_segment_with_deferred,
    };
    use crate::operations::point_ops::PointStructRawPersisted;
    use crate::segment_holder::locked::LockedSegmentHolder;
    use crate::segment_holder::provisioning::SegmentProvisioning;
    use crate::segment_holder::{FlushMode, SegmentHolder};
    use crate::update::{
        clear_payload_by_filter, create_field_index, delete_payload_by_filter,
        delete_points_by_filter, delete_vectors_by_filter, overwrite_payload_by_filter,
        process_payload_operation, process_point_operation, set_payload, set_payload_by_filter,
        sync_points_raw, upsert_points_raw,
    };

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
            .flush_all(FlushMode::Sync, false)
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
            .flush_all(FlushMode::Sync, false)
            .expect("Failed to flush test segment holder");

        // Flushing again inrceases by 1 and is now equal to `DELETE_OP_NUM` as we want to acknowledge the empty
        // delete operation in WAL.
        assert_eq!(old_version + 1, new_version);
        assert_eq!(new_version, DELETE_OP_NUM);
    }

    fn retrieve_raw_record(
        holder: &SegmentHolder,
        segment_id: crate::segment_holder::SegmentId,
        point_id: u64,
    ) -> Option<segment::data_types::segment_record::SegmentRecordRaw> {
        let hw_counter = HardwareCounterCell::new();
        let is_stopped = std::sync::atomic::AtomicBool::new(false);
        let segment = holder.get(segment_id).unwrap().get();
        let segment = segment.read();
        segment
            .retrieve_raw(
                &[point_id.into()],
                &segment::types::WithPayload::from(true),
                &segment::types::WithVector::Bool(true),
                &hw_counter,
                &is_stopped,
                common::types::DeferredBehavior::WithDeferred,
            )
            .unwrap()
            .remove(&point_id.into())
    }

    #[test]
    fn test_upsert_points_raw_moves_point_from_non_appendable() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let mut non_appendable = build_segment_1(dir.path()); // points 1-5
        non_appendable.appendable_flag = false;
        let appendable = empty_segment(dir.path());

        let mut holder = SegmentHolder::default();
        let sid_non_app = holder.add_new(non_appendable);
        let sid_app = holder.add_new(appendable);

        let new_vector: Vec<f32> = vec![9.0, 8.0, 7.0, 6.0];
        let new_bytes: Vec<u8> = new_vector.iter().flat_map(|v| v.to_le_bytes()).collect();
        let payload: segment::types::Payload = payload_json! {"city": "Berlin"};

        let points = [
            PointStructRawPersisted {
                id: 1.into(),
                vectors: vec![(DEFAULT_VECTOR_NAME.to_owned(), new_bytes.clone())].into(),
                payload: Some(payload.clone()),
            },
            PointStructRawPersisted {
                id: 100.into(),
                vectors: vec![(DEFAULT_VECTOR_NAME.to_owned(), new_bytes.clone())].into(),
                payload: None,
            },
        ];

        let updated = upsert_points_raw(&holder, 100, points.iter(), &hw_counter).unwrap();
        assert_eq!(updated, 1);

        {
            let non_app = holder.get(sid_non_app).unwrap().get();
            let non_app = non_app.read();
            assert!(!non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred));
        }

        for point_id in [1, 100] {
            let record = retrieve_raw_record(&holder, sid_app, point_id)
                .unwrap_or_else(|| panic!("point {point_id} must be in the appendable segment"));
            let vectors = record.vectors.expect("vectors were requested");
            assert_eq!(
                vectors.to_vec(),
                vec![(DEFAULT_VECTOR_NAME.to_owned(), new_bytes.clone())],
                "raw bytes of point {point_id} must round-trip exactly",
            );
        }

        let record = retrieve_raw_record(&holder, sid_app, 1).unwrap();
        assert_eq!(record.payload, Some(payload));
        let record = retrieve_raw_record(&holder, sid_app, 100).unwrap();
        assert_eq!(record.payload.filter(|p| !p.is_empty()), None);
    }

    #[test]
    fn test_sync_points_raw() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let segment = build_segment_1(dir.path()); // points 1-5
        let mut holder = SegmentHolder::default();
        let sid = holder.add_new(segment);

        let point_2 = PointStructRawPersisted::from(retrieve_raw_record(&holder, sid, 2).unwrap());
        let point_2_version_before = holder
            .get(sid)
            .unwrap()
            .get()
            .read()
            .point_version(2.into());

        let mut point_3 =
            PointStructRawPersisted::from(retrieve_raw_record(&holder, sid, 3).unwrap());
        let changed_bytes: Vec<u8> = [9.0f32, 8.0, 7.0, 6.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        point_3.vectors = vec![(DEFAULT_VECTOR_NAME.to_owned(), changed_bytes.clone())].into();

        let point_100 = PointStructRawPersisted {
            id: 100.into(),
            vectors: vec![(DEFAULT_VECTOR_NAME.to_owned(), changed_bytes.clone())].into(),
            payload: None,
        };

        let (deleted, new, updated) = sync_points_raw(
            &holder,
            100,
            None,
            None,
            &[point_2, point_3, point_100],
            &hw_counter,
        )
        .unwrap();

        assert_eq!(deleted, 3, "points 1, 4 and 5 are not in the sync set");
        assert_eq!(new, 1, "point 100 is new");
        assert_eq!(updated, 1, "only point 3 has different bytes");

        {
            let segment = holder.get(sid).unwrap().get();
            let segment = segment.read();
            for point_id in [1, 4, 5] {
                assert!(
                    !segment.has_point(
                        point_id.into(),
                        common::types::DeferredBehavior::WithDeferred
                    ),
                    "point {point_id} must be deleted",
                );
            }
            assert_eq!(segment.point_version(2.into()), point_2_version_before);
            assert_eq!(segment.point_version(3.into()), Some(100));
        }

        let record = retrieve_raw_record(&holder, sid, 3).unwrap();
        assert_eq!(
            record.vectors.unwrap().to_vec(),
            vec![(DEFAULT_VECTOR_NAME.to_owned(), changed_bytes)],
        );
    }

    /// Helper: creates a non-appendable segment with a single point at the given version and city payload.
    fn build_non_appendable_with_city(
        path: &std::path::Path,
        point_id: u64,
        version: u64,
        city: &str,
    ) -> segment::segment::Segment {
        let hw_counter = HardwareCounterCell::new();
        let mut seg = empty_segment(path);
        seg.upsert_point(
            version,
            point_id.into(),
            only_default_vector(&[1.0, 0.0, 0.0, 0.0]),
            &hw_counter,
        )
        .unwrap();
        let payload: segment::types::Payload = payload_json! {"city": city.to_owned()};
        seg.set_payload(version, point_id.into(), &payload, &None, &hw_counter)
            .unwrap();
        seg.appendable_flag = false;
        seg
    }

    /// Helper: creates an appendable segment with deferred threshold 0 (all points deferred),
    /// containing a single point with the given city payload.
    fn build_deferred_with_city(
        path: &std::path::Path,
        point_id: u64,
        version: u64,
        city: &str,
    ) -> segment::segment::Segment {
        let hw_counter = HardwareCounterCell::new();
        // threshold 0 => every point is deferred
        let mut seg = empty_segment_with_deferred(path, 0);
        seg.upsert_point(
            version,
            point_id.into(),
            only_default_vector(&[1.0, 0.0, 0.0, 0.0]),
            &hw_counter,
        )
        .unwrap();
        let payload: segment::types::Payload = payload_json! {"city": city.to_owned()};
        seg.set_payload(version, point_id.into(), &payload, &None, &hw_counter)
            .unwrap();
        assert!(
            seg.point_is_deferred(point_id.into()),
            "Point {point_id} should be deferred"
        );
        seg
    }

    fn city_filter(city: &str) -> Filter {
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            "city".parse().unwrap(),
            Match::Value(MatchValue {
                value: ValueVariants::String(city.to_string()),
            }),
        )))
    }

    /// Delete by filter with deferred points corner case:
    ///   - Non-appendable segment: point 1 at version 1, city=Berlin
    ///   - Appendable+deferred segment: point 1 at version 2, city=Amsterdam
    ///   - Delete by filter on city=Amsterdam
    ///
    /// The deferred point (newest) matches the filter, so both copies must be deleted.
    #[test]
    fn test_delete_by_filter_deferred_filter_matches_deferred() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        let sid_non_app = holder.add_new(non_appendable);
        let sid_app = holder.add_new(appendable);

        let filter = city_filter("Amsterdam");
        let deleted = delete_points_by_filter(&holder, 10, &filter, &hw_counter).unwrap();

        // The deferred version matches the filter => both copies deleted.
        assert!(deleted > 0, "Should have deleted at least one copy");

        let non_app = holder.get(sid_non_app).unwrap().get();
        let non_app = non_app.read();
        let app = holder.get(sid_app).unwrap().get();
        let app = app.read();

        assert!(
            !app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Deferred copy should be deleted (matches filter)"
        );
        assert!(
            !non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Old copy should also be deleted (deferred version matched filter)"
        );
    }

    /// Delete by filter with deferred points corner case:
    ///   - Non-appendable segment: point 1 at version 1, city=Berlin
    ///   - Appendable+deferred segment: point 1 at version 2, city=Amsterdam
    ///   - Delete by filter on city=Berlin
    ///
    /// The old copy matches the filter, but the newest version is deferred and does NOT
    /// match city=Berlin. The delete must be skipped for all copies so that after
    /// optimization deduplication we're left with the Amsterdam version.
    #[test]
    fn test_delete_by_filter_deferred_filter_matches_old_copy() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        let sid_non_app = holder.add_new(non_appendable);
        let sid_app = holder.add_new(appendable);

        let filter = city_filter("Berlin");
        let _deleted = delete_points_by_filter(&holder, 10, &filter, &hw_counter).unwrap();

        let non_app = holder.get(sid_non_app).unwrap().get();
        let non_app = non_app.read();
        let app = holder.get(sid_app).unwrap().get();
        let app = app.read();

        // The deferred version (Amsterdam) does NOT match the filter (Berlin),
        // so both copies must be kept. Once the optimizer kicks in and deduplicates,
        // only the Amsterdam version will remain.
        assert!(
            app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Deferred copy must be kept (does not match filter, is newest)"
        );
        assert!(
            non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Old copy must be kept (deferred version is newer and does not match filter)"
        );
    }

    // --- set_payload_by_filter deferred tests ---

    /// Set payload by filter with deferred points:
    ///   - Non-appendable: point 1 v1, city=Berlin
    ///   - Deferred: point 1 v2, city=Amsterdam
    ///   - Filter: city=Amsterdam (matches deferred copy)
    ///
    /// The deferred point (newest) matches the filter, so the operation should be applied.
    #[test]
    fn test_set_payload_by_filter_deferred_filter_matches_deferred() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        holder.add_new(non_appendable);
        holder.add_new(appendable);

        let filter = city_filter("Amsterdam");
        let payload: segment::types::Payload = payload_json! {"color": "red"};
        let updated =
            set_payload_by_filter(&holder, 10, &payload, &filter, &None, &hw_counter).unwrap();

        assert!(updated > 0, "Should have updated at least one point");
    }

    /// Set payload by filter with deferred points:
    ///   - Non-appendable: point 1 v1, city=Berlin
    ///   - Deferred: point 1 v2, city=Amsterdam
    ///   - Filter: city=Berlin (matches old copy only)
    ///
    /// The deferred version does NOT match, so the operation must be skipped.
    #[test]
    fn test_set_payload_by_filter_deferred_filter_matches_old_copy() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        let sid_non_app = holder.add_new(non_appendable);
        let sid_app = holder.add_new(appendable);

        let filter = city_filter("Berlin");
        let payload: segment::types::Payload = payload_json! {"color": "red"};
        let updated =
            set_payload_by_filter(&holder, 10, &payload, &filter, &None, &hw_counter).unwrap();

        assert_eq!(
            updated, 0,
            "Operation should be skipped (deferred version does not match filter)"
        );

        let non_app = holder.get(sid_non_app).unwrap().get();
        let non_app = non_app.read();
        let app = holder.get(sid_app).unwrap().get();
        let app = app.read();

        assert!(
            non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Old copy must be kept"
        );
        assert!(
            app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Deferred copy must be kept"
        );
    }

    // --- delete_payload_by_filter deferred tests ---

    /// Delete payload by filter with deferred points:
    ///   - Non-appendable: point 1 v1, city=Berlin
    ///   - Deferred: point 1 v2, city=Amsterdam
    ///   - Filter: city=Amsterdam (matches deferred copy)
    ///
    /// The deferred point (newest) matches, so the payload key should be deleted.
    #[test]
    fn test_delete_payload_by_filter_deferred_filter_matches_deferred() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        holder.add_new(non_appendable);
        holder.add_new(appendable);

        let filter = city_filter("Amsterdam");
        let keys: Vec<PayloadKeyType> = vec!["city".parse().unwrap()];
        let updated = delete_payload_by_filter(&holder, 10, &filter, &keys, &hw_counter).unwrap();

        assert!(updated > 0, "Should have updated at least one point");
    }

    /// Delete payload by filter with deferred points:
    ///   - Non-appendable: point 1 v1, city=Berlin
    ///   - Deferred: point 1 v2, city=Amsterdam
    ///   - Filter: city=Berlin (matches old copy only)
    ///
    /// The deferred version does NOT match, so the operation must be skipped.
    #[test]
    fn test_delete_payload_by_filter_deferred_filter_matches_old_copy() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        let sid_non_app = holder.add_new(non_appendable);
        let sid_app = holder.add_new(appendable);

        let filter = city_filter("Berlin");
        let keys: Vec<PayloadKeyType> = vec!["city".parse().unwrap()];
        let updated = delete_payload_by_filter(&holder, 10, &filter, &keys, &hw_counter).unwrap();

        assert_eq!(
            updated, 0,
            "Operation should be skipped (deferred version does not match filter)"
        );

        let non_app = holder.get(sid_non_app).unwrap().get();
        let non_app = non_app.read();
        let app = holder.get(sid_app).unwrap().get();
        let app = app.read();

        assert!(
            non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Old copy must be kept"
        );
        assert!(
            app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Deferred copy must be kept"
        );
    }

    // --- clear_payload_by_filter deferred tests ---

    /// Clear payload by filter with deferred points:
    ///   - Non-appendable: point 1 v1, city=Berlin
    ///   - Deferred: point 1 v2, city=Amsterdam
    ///   - Filter: city=Amsterdam (matches deferred copy)
    ///
    /// The deferred point (newest) matches, so the payload should be cleared.
    #[test]
    fn test_clear_payload_by_filter_deferred_filter_matches_deferred() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        holder.add_new(non_appendable);
        holder.add_new(appendable);

        let filter = city_filter("Amsterdam");
        let updated = clear_payload_by_filter(&holder, 10, &filter, &hw_counter).unwrap();

        assert!(updated > 0, "Should have updated at least one point");
    }

    /// Clear payload by filter with deferred points:
    ///   - Non-appendable: point 1 v1, city=Berlin
    ///   - Deferred: point 1 v2, city=Amsterdam
    ///   - Filter: city=Berlin (matches old copy only)
    ///
    /// The deferred version does NOT match, so the operation must be skipped.
    #[test]
    fn test_clear_payload_by_filter_deferred_filter_matches_old_copy() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        let sid_non_app = holder.add_new(non_appendable);
        let sid_app = holder.add_new(appendable);

        let filter = city_filter("Berlin");
        let updated = clear_payload_by_filter(&holder, 10, &filter, &hw_counter).unwrap();

        assert_eq!(
            updated, 0,
            "Operation should be skipped (deferred version does not match filter)"
        );

        let non_app = holder.get(sid_non_app).unwrap().get();
        let non_app = non_app.read();
        let app = holder.get(sid_app).unwrap().get();
        let app = app.read();

        assert!(
            non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Old copy must be kept"
        );
        assert!(
            app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Deferred copy must be kept"
        );
    }

    // --- overwrite_payload_by_filter deferred tests ---

    /// Overwrite payload by filter with deferred points:
    ///   - Non-appendable: point 1 v1, city=Berlin
    ///   - Deferred: point 1 v2, city=Amsterdam
    ///   - Filter: city=Amsterdam (matches deferred copy)
    ///
    /// The deferred point (newest) matches, so the payload should be overwritten.
    #[test]
    fn test_overwrite_payload_by_filter_deferred_filter_matches_deferred() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        holder.add_new(non_appendable);
        holder.add_new(appendable);

        let filter = city_filter("Amsterdam");
        let payload: segment::types::Payload = payload_json! {"color": "red"};
        let updated =
            overwrite_payload_by_filter(&holder, 10, &payload, &filter, &hw_counter).unwrap();

        assert!(updated > 0, "Should have updated at least one point");
    }

    /// Overwrite payload by filter with deferred points:
    ///   - Non-appendable: point 1 v1, city=Berlin
    ///   - Deferred: point 1 v2, city=Amsterdam
    ///   - Filter: city=Berlin (matches old copy only)
    ///
    /// The deferred version does NOT match, so the operation must be skipped.
    #[test]
    fn test_overwrite_payload_by_filter_deferred_filter_matches_old_copy() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        let sid_non_app = holder.add_new(non_appendable);
        let sid_app = holder.add_new(appendable);

        let filter = city_filter("Berlin");
        let payload: segment::types::Payload = payload_json! {"color": "red"};
        let updated =
            overwrite_payload_by_filter(&holder, 10, &payload, &filter, &hw_counter).unwrap();

        assert_eq!(
            updated, 0,
            "Operation should be skipped (deferred version does not match filter)"
        );

        let non_app = holder.get(sid_non_app).unwrap().get();
        let non_app = non_app.read();
        let app = holder.get(sid_app).unwrap().get();
        let app = app.read();

        assert!(
            non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Old copy must be kept"
        );
        assert!(
            app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Deferred copy must be kept"
        );
    }

    // --- delete_vectors_by_filter deferred tests ---

    /// Delete vectors by filter with deferred points:
    ///   - Non-appendable: point 1 v1, city=Berlin
    ///   - Deferred: point 1 v2, city=Amsterdam
    ///   - Filter: city=Amsterdam (matches deferred copy)
    ///
    /// The deferred point (newest) matches, so the vector should be deleted.
    #[test]
    fn test_delete_vectors_by_filter_deferred_filter_matches_deferred() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        holder.add_new(non_appendable);
        holder.add_new(appendable);

        let filter = city_filter("Amsterdam");
        let vector_names = vec![DEFAULT_VECTOR_NAME.into()];
        let deleted =
            delete_vectors_by_filter(&holder, 10, &filter, &vector_names, &hw_counter).unwrap();

        assert!(deleted > 0, "Should have deleted at least one vector");
    }

    /// Delete vectors by filter with deferred points:
    ///   - Non-appendable: point 1 v1, city=Berlin
    ///   - Deferred: point 1 v2, city=Amsterdam
    ///   - Filter: city=Berlin (matches old copy only)
    ///
    /// The deferred version does NOT match, so the operation must be skipped.
    #[test]
    fn test_delete_vectors_by_filter_deferred_filter_matches_old_copy() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
        let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

        let mut holder = SegmentHolder::default();
        let sid_non_app = holder.add_new(non_appendable);
        let sid_app = holder.add_new(appendable);

        let filter = city_filter("Berlin");
        let vector_names = vec![DEFAULT_VECTOR_NAME.into()];
        let deleted =
            delete_vectors_by_filter(&holder, 10, &filter, &vector_names, &hw_counter).unwrap();

        assert_eq!(
            deleted, 0,
            "Operation should be skipped (deferred version does not match filter)"
        );

        let non_app = holder.get(sid_non_app).unwrap().get();
        let non_app = non_app.read();
        let app = holder.get(sid_app).unwrap().get();
        let app = app.read();

        assert!(
            non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Old copy must be kept"
        );
        assert!(
            app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
            "Deferred copy must be kept"
        );
    }

    /// Upsert of a point that lives in a non-appendable segment takes the
    /// CoW-move arm of `apply_points_with_conditional_move`. The moved record
    /// must match in-place upsert semantics (`upsert_with_payload`): named
    /// vectors and payload absent from the incoming point are dropped, not
    /// carried over from the old record.
    #[test]
    fn test_upsert_cow_move_replaces_whole_point() {
        use std::collections::HashMap;

        use common::types::DeferredBehavior;
        use segment::data_types::named_vectors::NamedVectors;
        use segment::entry::entry_point::SegmentEntry;
        use segment::segment_constructor::simple_segment_constructor::{
            VECTOR1_NAME, VECTOR2_NAME, build_segment_with_two_named_vecs,
        };
        use segment::types::{Distance, PointIdType};

        use crate::operations::point_ops::{
            PointStructPersisted, VectorPersisted, VectorStructPersisted,
        };
        use crate::update::upsert_points;

        const DIM: usize = 4;
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();
        let point_id: PointIdType = 7.into();

        // Old record: both named vectors plus a payload.
        let seed = |segment: &mut segment::segment::Segment| {
            segment
                .upsert_point(
                    100,
                    point_id,
                    NamedVectors::from_pairs([
                        (VECTOR1_NAME.to_owned(), vec![0.1, 0.2, 0.3, 0.4]),
                        (VECTOR2_NAME.to_owned(), vec![0.5, 0.6, 0.7, 0.8]),
                    ]),
                    &hw_counter,
                )
                .unwrap();
            segment
                .set_payload(
                    100,
                    point_id,
                    &payload_json! {"city": "Berlin"},
                    &None,
                    &hw_counter,
                )
                .unwrap();
        };

        // Incoming upsert: only `vector1`, no payload.
        let incoming = PointStructPersisted {
            id: point_id,
            vector: VectorStructPersisted::Named(HashMap::from([(
                VECTOR1_NAME.to_owned(),
                VectorPersisted::Dense(vec![1.0, 1.0, 1.0, 1.0]),
            )])),
            payload: None,
        };

        let check = |segment: &dyn SegmentEntry, path: &str| {
            assert!(segment.has_point(point_id, DeferredBehavior::WithDeferred));
            assert!(
                segment
                    .vector(VECTOR1_NAME, point_id, &hw_counter)
                    .unwrap()
                    .is_some(),
                "{path}: upserted vector must be present",
            );
            assert!(
                segment
                    .vector(VECTOR2_NAME, point_id, &hw_counter)
                    .unwrap()
                    .is_none(),
                "{path}: named vector absent from the upsert must be dropped",
            );
            assert!(
                segment.payload(point_id, &hw_counter).unwrap().is_empty(),
                "{path}: payload absent from the upsert must be cleared",
            );
        };

        // Oracle: in-place upsert into an appendable segment.
        let mut in_place =
            build_segment_with_two_named_vecs(dir.path(), DIM, DIM, Distance::Dot).unwrap();
        seed(&mut in_place);
        let mut holder = SegmentHolder::default();
        let sid = holder.add_new(in_place);
        upsert_points(&holder, 101, [&incoming], &hw_counter).unwrap();
        let segment = holder.get(sid).unwrap().get();
        check(&*segment.read(), "in-place");

        // CoW move: the point's segment is non-appendable, so the upsert must
        // move it into the appendable destination with the same semantics.
        let mut source =
            build_segment_with_two_named_vecs(dir.path(), DIM, DIM, Distance::Dot).unwrap();
        seed(&mut source);
        source.appendable_flag = false;
        let destination =
            build_segment_with_two_named_vecs(dir.path(), DIM, DIM, Distance::Dot).unwrap();
        let mut holder = SegmentHolder::default();
        holder.add_new(source);
        let sid = holder.add_new(destination);
        upsert_points(&holder, 101, [&incoming], &hw_counter).unwrap();
        let segment = holder.get(sid).unwrap().get();
        check(&*segment.read(), "CoW move");
    }

    /// Failure-direction regression test for the payload index durability fix: an
    /// index built while a payload change is still pending must not let the durable
    /// index config outrun the state the build observed. `create_field_index` flushes
    /// the segment (serialized with the flush pipeline) before building, so the
    /// pending change becomes durable together with the index: after a crash the
    /// reloaded segment sees the cleared payload, the re-applied `CreateFieldIndex`
    /// (WAL replay) stays a truthful no-op (`AlreadyBuilt`), and filtered reads agree
    /// with full scans.
    ///
    /// Without the pre-build flush, the reloaded segment resurrects the cleared
    /// payload row while the durable index has no posting for it, and the row stays
    /// invisible to filtered reads forever.
    #[test]
    fn create_field_index_pins_pending_payload_state() {
        use std::sync::atomic::AtomicBool;

        use common::types::DeferredBehavior;
        use segment::entry::{NonAppendableSegmentEntry as _, StorageSegmentEntry as _};
        use segment::segment_constructor::load_segment;
        use segment::types::{PayloadFieldSchema, PayloadSchemaType};
        use uuid::Uuid;

        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();
        let key: PayloadKeyType = "city".parse().unwrap();
        let schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
        let is_stopped = AtomicBool::new(false);

        let mut seg = empty_segment(dir.path());
        seg.upsert_point(
            1,
            0.into(),
            only_default_vector(&[1.0, 0.0, 0.0, 0.0]),
            &hw_counter,
        )
        .unwrap();
        let payload: segment::types::Payload = payload_json! {"city": "Berlin"};
        seg.set_payload(2, 0.into(), &payload, &None, &hw_counter)
            .unwrap();
        seg.flush(true).unwrap();

        // Pending payload clear: stays in memory until the next flush cycle.
        seg.clear_payload(3, 0.into(), &hw_counter).unwrap();
        let segment_path = seg.segment_path.clone();

        let mut holder = SegmentHolder::default();
        holder.add_new(seg);

        // The build observes the cleared row and must pin it durably before the
        // index config becomes durable.
        create_field_index(&holder, 4, &key, Some(&schema), &hw_counter).unwrap();

        // Simulated crash: dropped without any flush after the op.
        drop(holder);

        let mut segment = load_segment(&segment_path, Uuid::nil(), None, &AtomicBool::new(false))
            .expect("segment must load after simulated crash");

        // The pre-build flush persisted the pending clear together with the index.
        let reloaded = segment.payload(0.into(), &hw_counter).unwrap();
        assert!(
            !reloaded.0.contains_key("city"),
            "the payload state observed by the index build must be durable",
        );

        // WAL replay re-applies the CreateFieldIndex op; the config is truthful, so
        // `AlreadyBuilt` is a correct no-op.
        segment
            .create_field_index(4, &key, Some(&schema), &hw_counter)
            .unwrap();

        let hits = segment
            .read_filtered(
                None,
                None,
                Some(&city_filter("Berlin")),
                &is_stopped,
                &hw_counter,
                DeferredBehavior::VisibleOnly,
            )
            .unwrap();
        assert!(
            hits.is_empty(),
            "filtered reads must agree with payload storage: the row was cleared",
        );
    }

    /// Fixtures use dim-4 f32 vectors: 16 bytes per point.
    const TEST_POINT_SIZE_BYTES: usize = 16;

    #[test]
    fn test_capacity_error_when_all_appendable_at_cap() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let mut non_appendable = build_segment_1(dir.path()); // points 1-5
        non_appendable.appendable_flag = false;

        // Fill the only appendable segment up to the cap of 2 points.
        let mut appendable = empty_segment(dir.path());
        for point_id in [100u64, 101] {
            appendable
                .upsert_point(
                    10,
                    point_id.into(),
                    only_default_vector(&[1.0, 0.0, 1.0, 0.0]),
                    &hw_counter,
                )
                .unwrap();
        }

        let mut holder = SegmentHolder::default();
        holder.add_new(non_appendable);
        holder.add_new(appendable);
        holder.set_max_segment_size_bytes(NonZeroUsize::new(2 * TEST_POINT_SIZE_BYTES));

        // Setting payload on the points of the non-appendable segment needs to CoW-move them,
        // but no appendable segment is below the cap: the operation must fail with the
        // recoverable capacity error instead of growing the full destination further.
        let points: Vec<PointIdType> = (1..=5).map(Into::into).collect();
        let err = set_payload(
            &holder,
            100,
            &payload_json! {"town": "Amsterdam"},
            &points,
            &None,
            &hw_counter,
        )
        .expect_err("no appendable segment below the cap can accept the moved points");
        assert!(
            err.is_out_of_appendable_capacity(),
            "expected OutOfAppendableCapacity, got: {err}",
        );

        // The insert path reports the same error instead of writing past the cap.
        let err = holder
            .smallest_appendable_segment()
            .expect_err("even the smallest appendable segment is at the cap");
        assert!(
            err.is_out_of_appendable_capacity(),
            "expected OutOfAppendableCapacity, got: {err}",
        );
    }

    /// Build a holder with a non-appendable segment holding points 1-5 and one empty appendable
    /// segment, capped at two points worth of vector data, plus the matching provisioning.
    fn capped_holder_with_immutable_points(
        path: &std::path::Path,
    ) -> (LockedSegmentHolder, SegmentProvisioning) {
        let mut non_appendable = build_segment_1(path); // points 1-5
        non_appendable.appendable_flag = false;
        let appendable = empty_segment(path);
        let segment_config = appendable.segment_config.clone();

        let mut holder = SegmentHolder::default();
        holder.add_new(non_appendable);
        holder.add_new(appendable);
        holder.set_max_segment_size_bytes(NonZeroUsize::new(2 * TEST_POINT_SIZE_BYTES));

        let provisioning = SegmentProvisioning {
            segments_path: path.to_owned(),
            segment_config,
            payload_index_schema: Arc::new(
                SaveOnDisk::load_or_init_default(path.join("payload.schema")).unwrap(),
            ),
            deferred_internal_id: None,
        };

        (LockedSegmentHolder::new(holder), provisioning)
    }

    fn appendable_segment_sizes(segments: &LockedSegmentHolder) -> Vec<usize> {
        let segments = segments.read();
        segments
            .iter()
            .filter_map(|(_segment_id, segment)| {
                let segment = segment.get().read();
                segment
                    .is_appendable()
                    .then(|| segment.max_available_vectors_size_in_bytes().unwrap())
            })
            .collect()
    }

    #[test]
    fn test_cow_move_provisions_segments_at_capacity() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let (segments, provisioning) = capped_holder_with_immutable_points(dir.path());

        // Set payload on all 5 points: every one CoW-moves out of the non-appendable segment.
        // Only 2 fit into the existing appendable segment, so the update must provision fresh
        // segments for the remaining 3 instead of overgrowing the destination.
        let operation = PayloadOps::SetPayload(SetPayloadOp {
            payload: payload_json! {"town": "Amsterdam"},
            points: Some((1..=5).map(Into::into).collect()),
            filter: None,
            key: None,
        });
        let updated =
            process_payload_operation(&segments, Some(&provisioning), 100, operation, &hw_counter)
                .unwrap();
        assert_eq!(updated, 5);

        let sizes = appendable_segment_sizes(&segments);
        assert!(
            sizes.iter().all(|size| *size <= 2 * TEST_POINT_SIZE_BYTES),
            "no appendable segment may exceed the cap: {sizes:?}",
        );
        assert_eq!(
            sizes.iter().sum::<usize>(),
            5 * TEST_POINT_SIZE_BYTES,
            "all moved points must be accounted for: {sizes:?}",
        );
        assert!(
            sizes.len() >= 3,
            "the update must have provisioned fresh appendable segments: {sizes:?}",
        );
    }

    #[test]
    fn test_upsert_provisions_segments_at_capacity() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let appendable = empty_segment(dir.path());
        let segment_config = appendable.segment_config.clone();

        let mut holder = SegmentHolder::default();
        holder.add_new(appendable);
        holder.set_max_segment_size_bytes(NonZeroUsize::new(2 * TEST_POINT_SIZE_BYTES));

        let provisioning = SegmentProvisioning {
            segments_path: dir.path().to_owned(),
            segment_config,
            payload_index_schema: Arc::new(
                SaveOnDisk::load_or_init_default(dir.path().join("payload.schema")).unwrap(),
            ),
            deferred_internal_id: None,
        };
        let segments = LockedSegmentHolder::new(holder);

        // Insert 5 new points; only 2 fit under the cap in the initial appendable segment.
        let points: Vec<_> = (1..=5)
            .map(|id: u64| PointStructPersisted {
                id: id.into(),
                vector: crate::operations::point_ops::VectorStructPersisted::Single(vec![
                    1.0, 0.0, 1.0, 0.0,
                ]),
                payload: None,
            })
            .collect();
        let operation =
            PointOperations::UpsertPoints(PointInsertOperationsInternal::PointsList(points));
        process_point_operation(&segments, Some(&provisioning), 100, operation, &hw_counter)
            .unwrap();

        let sizes = appendable_segment_sizes(&segments);
        assert!(
            sizes.iter().all(|size| *size <= 2 * TEST_POINT_SIZE_BYTES),
            "no appendable segment may exceed the cap: {sizes:?}",
        );
        assert_eq!(
            sizes.iter().sum::<usize>(),
            5 * TEST_POINT_SIZE_BYTES,
            "all inserted points must be accounted for: {sizes:?}",
        );
    }

    #[test]
    fn test_capacity_error_without_provisioning() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let (segments, _provisioning) = capped_holder_with_immutable_points(dir.path());

        // Without provisioning, the same update must fail once the destination is full, and the
        // error must be recognizable so callers with provisioning can recover from it.
        let operation = PayloadOps::SetPayload(SetPayloadOp {
            payload: payload_json! {"town": "Amsterdam"},
            points: Some((1..=5).map(Into::into).collect()),
            filter: None,
            key: None,
        });
        let err = process_payload_operation(&segments, None, 100, operation, &hw_counter)
            .expect_err("the appendable segment cannot hold all 5 moved points");
        assert!(
            err.is_out_of_appendable_capacity(),
            "expected OutOfAppendableCapacity, got: {err}",
        );
    }
}
