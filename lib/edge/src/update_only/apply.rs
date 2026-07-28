//! Applying a folded batch: locate, resolve, materialize, append.
//!
//! The four stages are separate on purpose — each is one batched pass over the
//! whole point set, so the cost of a batch scales with the number of *points*
//! it touches, not with the number of operations in it.

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::fully_qualified_point::{FullyQualifiedPoint, StoredPoint};
use segment::types::{PointIdType, SeqNumberType};
use shard::operations::CollectionUpdateOperations;
use uuid::Uuid;

use crate::update_only::UpdateOnlyEdgeShard;
use crate::update_only::batch::UpdateBatchPlan;
use crate::update_only::holder::UpdateOnlySegmentHolder;

/// What a batch did, counted per point rather than per operation — a point
/// named by ten operations in one batch is one write, and counts once.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UpdateBatchOutcome {
    /// Points written: created, or rewritten into a fresh slot.
    pub stored: usize,
    /// Points removed.
    pub deleted: usize,
    /// Points already at or beyond the batch's version, left untouched. Makes
    /// a replayed batch a no-op, which is what lets the caller retry one after
    /// an ambiguous failure.
    pub skipped: usize,
    /// Points an operation named that no segment holds and the batch did not
    /// create — a payload update to a point that is not there.
    pub missing: usize,
}

/// Where a point currently lives, and at what version.
#[derive(Debug, Clone, Copy)]
struct PointLocation {
    segment: Uuid,
    internal_id: PointOffsetType,
    version: SeqNumberType,
    /// Whether the holding segment accepts appends. Only used to break a
    /// version tie — see [`locate_points`].
    appendable: bool,
}

impl PointLocation {
    /// Whether this copy of the point supersedes `other`.
    ///
    /// Version decides. On a tie the appendable segment wins, matching the
    /// read path's retrieval order (non-appendable first, appendable last, last
    /// max wins): a point being moved out of an immutable segment exists in
    /// both at the same version, and the appendable copy is the live one.
    fn supersedes(&self, other: &Self) -> bool {
        (self.version, self.appendable) > (other.version, other.appendable)
    }
}

impl<S: UniversalRead + 'static> UpdateOnlyEdgeShard<S> {
    /// Apply a batch of update operations, each paired with the operation
    /// number to record as its version.
    ///
    /// Operations are expected in ascending operation-number order — the order
    /// they were submitted in. Only operations that name their points are
    /// accepted; see [`UpdateBatchPlan::build`] for what is rejected and why.
    ///
    /// The batch is atomic in the sense that matters without a WAL: it is
    /// applied in full or the error is returned, and re-applying a batch that
    /// partially landed skips the points that already carry its version.
    pub fn apply_batch(
        &self,
        operations: impl IntoIterator<Item = (SeqNumberType, CollectionUpdateOperations)>,
    ) -> OperationResult<UpdateBatchOutcome> {
        let plan = UpdateBatchPlan::build(operations)?;
        if plan.is_empty() {
            return Ok(UpdateBatchOutcome::default());
        }

        let hw_counter = HardwareCounterCell::disposable();
        let segments = self.segments.read();

        // 1. Find which segment holds each touched point, and at what version.
        let locations = locate_points(&segments, &plan)?;

        // 2. Read the points that cannot be resolved from the batch alone.
        let mut stored = read_stored_points(&segments, &plan, &locations, &hw_counter)?;

        // 3. Fold each point's mutations onto what is stored.
        let mut outcome = UpdateBatchOutcome::default();
        let mut to_store: Vec<FullyQualifiedPoint> = Vec::new();
        let mut to_tombstone: AHashMap<Uuid, Vec<PointOffsetType>> = AHashMap::new();

        for (id, updates) in plan.into_point_updates() {
            let location = locations.get(&id).copied();

            // Already applied: the stored point is at or beyond this batch's
            // version, so re-applying would move it backwards.
            if location.is_some_and(|location| location.version >= updates.version()) {
                outcome.skipped += 1;
                continue;
            }

            match updates.materialize(id, stored.remove(&id))? {
                Some(point) => {
                    to_store.push(point);
                    outcome.stored += 1;
                }
                None if location.is_some() => outcome.deleted += 1,
                None => {
                    outcome.missing += 1;
                    continue;
                }
            }

            // Whatever happened to the point, the slot it used to occupy is
            // retired: a rewrite left its replacement elsewhere, a delete left
            // nothing.
            if let Some(location) = location {
                to_tombstone
                    .entry(location.segment)
                    .or_default()
                    .push(location.internal_id);
            }
        }

        // 4. Append the resolved points, then retire the slots they replaced.
        if !to_store.is_empty() {
            let write_target = segments.write_target()?;
            write_target.write().store_points(&to_store, &hw_counter)?;

            // The new slots must be durable before the tombstones that retire
            // the old ones: the reverse order can lose a point outright if the
            // process dies in between.
            write_target.read().flush()?;
        }

        for (uuid, internal_ids) in to_tombstone {
            let segment = segments.get(&uuid).ok_or_else(|| {
                OperationError::service_error(format!("Segment {uuid} disappeared mid-batch"))
            })?;
            segment.write().tombstone_points(&internal_ids)?;
            segment.read().flush()?;
        }

        Ok(outcome)
    }
}

/// Locate every point the batch touches, keeping the newest copy when more than
/// one segment holds the point — which happens while an optimization is moving
/// points between segments.
fn locate_points<S: UniversalRead + 'static>(
    segments: &UpdateOnlySegmentHolder<S>,
    plan: &UpdateBatchPlan,
) -> OperationResult<AHashMap<PointIdType, PointLocation>> {
    let ids: Vec<PointIdType> = plan.point_ids().collect();
    let mut locations: AHashMap<PointIdType, PointLocation> = AHashMap::new();

    for (uuid, segment) in segments.iter() {
        let segment = segment.read();
        let appendable = segment.is_appendable();
        let found = segment.with_update_view(|view| {
            let mut found_ids = Vec::new();
            let mut internal_ids = Vec::new();
            view.locate_points(ids.iter().copied(), |id, internal_id| {
                found_ids.push(id);
                internal_ids.push(internal_id);
            })?;

            let versions = view.point_versions(&internal_ids)?;
            OperationResult::Ok((found_ids, internal_ids, versions))
        })?;

        let (found_ids, internal_ids, versions) = found;
        for ((id, internal_id), version) in found_ids.into_iter().zip(internal_ids).zip(versions) {
            let location = PointLocation {
                segment: uuid,
                internal_id,
                version,
                appendable,
            };
            locations
                .entry(id)
                .and_modify(|current| {
                    if location.supersedes(current) {
                        *current = location;
                    }
                })
                .or_insert(location);
        }
    }

    Ok(locations)
}

/// Read the stored form of the points whose mutations need it, one batched pass
/// per segment.
fn read_stored_points<S: UniversalRead + 'static>(
    segments: &UpdateOnlySegmentHolder<S>,
    plan: &UpdateBatchPlan,
    locations: &AHashMap<PointIdType, PointLocation>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<AHashMap<PointIdType, StoredPoint>> {
    let mut by_segment: AHashMap<Uuid, Vec<(PointIdType, PointOffsetType)>> = AHashMap::new();
    for id in plan.point_ids_needing_stored_point() {
        // A point no segment holds has nothing to read; its mutations either
        // create it outright or resolve to nothing.
        if let Some(location) = locations.get(&id) {
            by_segment
                .entry(location.segment)
                .or_default()
                .push((id, location.internal_id));
        }
    }

    let mut stored = AHashMap::new();
    for (uuid, entries) in by_segment {
        let segment = segments.get(&uuid).ok_or_else(|| {
            OperationError::service_error(format!("Segment {uuid} disappeared mid-batch"))
        })?;
        let segment = segment.read();

        let internal_ids: Vec<PointOffsetType> = entries
            .iter()
            .map(|(_, internal_id)| *internal_id)
            .collect();
        let points =
            segment.with_update_view(|view| view.read_stored_points(&internal_ids, hw_counter))?;

        for ((id, _), point) in entries.into_iter().zip(points) {
            stored.insert(id, point);
        }
    }

    Ok(stored)
}
