//! Point syncs: replace a point range with the given set, plain and raw.

use std::sync::atomic::AtomicBool;

use ahash::{AHashMap, AHashSet};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use segment::common::operation_error::OperationResult;
use segment::data_types::segment_record::{SegmentRecord, SegmentRecordRaw};
use segment::entry::ReadSegmentEntry;
use segment::types::{PointIdType, SeqNumberType, WithPayload, WithVector};

use super::delete_points;
use super::upsert::{PointToUpsert, upsert_points_impl};
use crate::operations::point_ops::{PointStructPersisted, PointStructRawPersisted};
use crate::segment_holder::SegmentHolder;

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
    sync_points_impl(segments, op_num, from_id, to_id, points, hw_counter)
}

/// Same as [`sync_points`], but for points carrying raw vector bytes verbatim.
pub fn sync_points_raw(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    from_id: Option<PointIdType>,
    to_id: Option<PointIdType>,
    points: &[PointStructRawPersisted],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<(usize, usize, usize)> {
    sync_points_impl(segments, op_num, from_id, to_id, points, hw_counter)
}

/// A point struct that can be compared against its stored counterpart, to
/// skip upserting points whose stored data is already identical.
trait PointToSync: PointToUpsert {
    /// The form the stored counterpart is retrieved in.
    type StoredRecord;

    /// Retrieve the stored counterparts of the given ids from a segment,
    /// in the form [`Self::is_equal_to`] compares against.
    fn retrieve_stored(
        segment: &dyn ReadSegmentEntry,
        ids: &[PointIdType],
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
    ) -> OperationResult<AHashMap<PointIdType, Self::StoredRecord>>;

    /// Does this point carry exactly the stored data, so the upsert can be skipped?
    fn is_equal_to(&self, stored: &Self::StoredRecord) -> bool;
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
fn sync_points_impl<P>(
    segments: &SegmentHolder,
    op_num: SeqNumberType,
    from_id: Option<PointIdType>,
    to_id: Option<PointIdType>,
    points: &[P],
    hw_counter: &HardwareCounterCell,
) -> OperationResult<(usize, usize, usize)>
where
    P: PointToSync,
{
    let id_to_point: AHashMap<PointIdType, &P> = points.iter().map(|p| (p.id(), p)).collect();
    let sync_points: AHashSet<_> = points.iter().map(|p| p.id()).collect();
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

    let mut points_to_update: Vec<&P> = Vec::new();
    // we don’t want to cancel this filtered read
    let is_stopped = AtomicBool::new(false);
    let _num_updated = segments.read_points(
        existing_point_ids.as_slice(),
        &is_stopped,
        DeferredBehavior::WithDeferred,
        |ids, segment| {
            // Since we retrieve points, which we already know exist, we expect all of them to be found
            let stored_records = P::retrieve_stored(&**segment, ids, hw_counter, &is_stopped)?;
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
    let num_replaced = upsert_points_impl(segments, op_num, points_to_update, hw_counter)?;
    debug_assert!(
        num_replaced <= num_updated,
        "number of replaced points cannot be greater than points to update ({num_replaced} <= {num_updated})",
    );

    Ok((deleted, num_new, num_updated))
}

impl PointToSync for PointStructPersisted {
    type StoredRecord = SegmentRecord;

    fn retrieve_stored(
        segment: &dyn ReadSegmentEntry,
        ids: &[PointIdType],
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
    ) -> OperationResult<AHashMap<PointIdType, SegmentRecord>> {
        segment.retrieve(
            ids,
            &WithPayload::from(true),
            &WithVector::Bool(true),
            hw_counter,
            is_stopped,
            DeferredBehavior::WithDeferred,
        )
    }

    fn is_equal_to(&self, stored: &SegmentRecord) -> bool {
        PointStructPersisted::is_equal_to(self, stored)
    }
}

impl PointToSync for PointStructRawPersisted {
    type StoredRecord = SegmentRecordRaw;

    fn retrieve_stored(
        segment: &dyn ReadSegmentEntry,
        ids: &[PointIdType],
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
    ) -> OperationResult<AHashMap<PointIdType, SegmentRecordRaw>> {
        segment.retrieve_raw(
            ids,
            &WithPayload::from(true),
            &WithVector::Bool(true),
            hw_counter,
            is_stopped,
            DeferredBehavior::WithDeferred,
        )
    }

    fn is_equal_to(&self, stored: &SegmentRecordRaw) -> bool {
        PointStructRawPersisted::is_equal_to(self, stored)
    }
}
