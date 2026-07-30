//! Applying a folded batch: locate, resolve, materialize, append — each one
//! batched pass over the whole point set, so a batch's cost scales with the
//! points it touches, not the operations in it.

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use rayon::ThreadPool;
use rayon::prelude::*;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::fully_qualified_point::{FullyQualifiedPoint, StoredPoint};
use segment::types::{PointIdType, SeqNumberType};
use shard::operations::CollectionUpdateOperations;
use uuid::Uuid;

use crate::update_only::UpdateOnlyEdgeShard;
use crate::update_only::batch::UpdateBatchPlan;
use crate::update_only::holder::UpdateOnlySegmentHolder;
use crate::update_only::preview::{PointAction, PointPreview, resolve_batch};

/// What a batch did, counted per point rather than per operation: a point
/// named by ten operations counts once.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UpdateBatchOutcome {
    /// Points written: created, or rewritten into a fresh slot.
    pub stored: usize,
    /// Points removed.
    pub deleted: usize,
    /// Points already at or beyond the batch's version, left untouched — what
    /// makes a replayed batch a no-op.
    pub skipped: usize,
    /// Points an operation named that no segment holds and the batch did not
    /// create — a payload update to a point that is not there.
    pub missing: usize,
}

/// One copy of a point: where it lives, and at what version.
#[derive(Debug, Clone, Copy)]
pub(super) struct PointLocation {
    pub(super) segment: Uuid,
    pub(super) internal_id: PointOffsetType,
    pub(super) version: SeqNumberType,
    /// Whether the holding segment accepts appends; breaks a version tie.
    appendable: bool,
}

impl PointLocation {
    /// Whether this copy of the point supersedes `other`: the higher version
    /// wins, and on a tie the appendable copy is the live one (a point being
    /// moved between segments exists in both at the same version).
    fn supersedes(&self, other: &Self) -> bool {
        (self.version, self.appendable) > (other.version, other.appendable)
    }
}

/// Every copy of one point across the shard's segments.
pub(super) struct PointLocations {
    /// The live copy: its version decides whether the batch is already
    /// applied, and its slot is the one a resolve reads from.
    pub(super) newest: PointLocation,
    /// Every slot the point occupies, `newest`'s included. A rewrite or a
    /// delete retires them all — tombstoning only the newest slot would let
    /// an older duplicate (left by an interrupted move) outlive the point
    /// and, on a delete, resurrect it.
    pub(super) slots: Vec<(Uuid, PointOffsetType)>,
}

impl<S: UniversalRead + 'static> UpdateOnlyEdgeShard<S> {
    /// Apply a batch of update operations, each paired with the operation
    /// number to record as its version. Operations are expected in ascending
    /// operation-number order; see [`UpdateBatchPlan::build`] for what is
    /// rejected.
    ///
    /// Atomic in the sense that matters without a WAL: applied in full or the
    /// error is returned, and re-applying a batch that partially landed skips
    /// the points that already carry its version.
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

        // 1-3. Locate, read, materialize — the decision stage shared with
        // `preview_batch`, so a preview cannot drift from the real apply.
        let resolved = resolve_batch(&segments, plan, &self.pool)?;

        let mut outcome = UpdateBatchOutcome::default();
        let mut to_store: Vec<FullyQualifiedPoint> = Vec::new();
        let mut to_tombstone: AHashMap<Uuid, Vec<PointOffsetType>> = AHashMap::new();

        for point in resolved {
            let PointPreview {
                id: _,
                current: _,
                slots,
                action,
            } = point;

            match action {
                PointAction::Skip => {
                    outcome.skipped += 1;
                    continue;
                }
                PointAction::Missing => {
                    outcome.missing += 1;
                    continue;
                }
                PointAction::Store(point) => {
                    to_store.push(point);
                    outcome.stored += 1;
                }
                PointAction::Delete => outcome.deleted += 1,
            }

            // Whatever happened to the point, every slot it occupied — in any
            // segment — is retired: a rewrite left its replacement elsewhere,
            // a delete left nothing, and an older duplicate must not outlive
            // either.
            for (segment, internal_id) in slots {
                to_tombstone.entry(segment).or_default().push(internal_id);
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

/// Locate every point the batch touches: every slot it occupies, with the
/// newest copy marked, when more than one segment holds the point. Segments
/// are visited in parallel on `pool`.
pub(super) fn locate_points<S: UniversalRead + 'static>(
    segments: &UpdateOnlySegmentHolder<S>,
    plan: &UpdateBatchPlan,
    pool: &ThreadPool,
) -> OperationResult<AHashMap<PointIdType, PointLocations>> {
    let ids: Vec<PointIdType> = plan.point_ids().collect();

    let per_segment: Vec<Vec<(PointIdType, PointLocation)>> = pool.install(|| {
        segments
            .iter()
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|(uuid, segment)| {
                let segment = segment.read();
                let appendable = segment.is_appendable();

                let mut found_ids = Vec::new();
                let mut internal_ids = Vec::new();
                segment.locate_points(ids.iter().copied(), |id, internal_id| {
                    found_ids.push(id);
                    internal_ids.push(internal_id);
                })?;
                let versions = segment.point_versions(&internal_ids)?;

                let located = found_ids
                    .into_iter()
                    .zip(internal_ids)
                    .map(|(id, internal_id)| {
                        let location = PointLocation {
                            segment: uuid,
                            internal_id,
                            // A slot without a stored version is unwritten,
                            // which compares as version 0.
                            version: versions.get(&internal_id).copied().unwrap_or(0),
                            appendable,
                        };
                        (id, location)
                    })
                    .collect();
                Ok(located)
            })
            .collect::<OperationResult<Vec<_>>>()
    })?;

    let mut locations: AHashMap<PointIdType, PointLocations> = AHashMap::new();
    for (id, location) in per_segment.into_iter().flatten() {
        let slot = (location.segment, location.internal_id);
        locations
            .entry(id)
            .and_modify(|current| {
                current.slots.push(slot);
                if location.supersedes(&current.newest) {
                    current.newest = location;
                }
            })
            .or_insert_with(|| PointLocations {
                newest: location,
                slots: vec![slot],
            });
    }

    Ok(locations)
}

/// Read the stored form of the points whose mutations need it, one batched
/// pass per segment; segments are read in parallel on `pool`.
pub(super) fn read_stored_points<S: UniversalRead + 'static>(
    segments: &UpdateOnlySegmentHolder<S>,
    plan: &UpdateBatchPlan,
    locations: &AHashMap<PointIdType, PointLocations>,
    pool: &ThreadPool,
) -> OperationResult<AHashMap<PointIdType, StoredPoint>> {
    let mut by_segment: AHashMap<Uuid, Vec<(PointIdType, PointOffsetType)>> = AHashMap::new();
    for id in plan.point_ids_needing_stored_point() {
        // A point no segment holds has nothing to read; its mutations either
        // create it outright or resolve to nothing. Only the newest copy is
        // read — older duplicates are stale.
        if let Some(location) = locations.get(&id) {
            by_segment
                .entry(location.newest.segment)
                .or_default()
                .push((id, location.newest.internal_id));
        }
    }

    let per_segment: Vec<Vec<(PointIdType, StoredPoint)>> = pool.install(|| {
        by_segment
            .into_iter()
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|(uuid, entries)| {
                let segment = segments.get(&uuid).ok_or_else(|| {
                    OperationError::service_error(format!("Segment {uuid} disappeared mid-batch"))
                })?;
                let segment = segment.read();

                let internal_ids: Vec<PointOffsetType> = entries
                    .iter()
                    .map(|(_, internal_id)| *internal_id)
                    .collect();
                // Not shared with the caller's counter: `HardwareCounterCell`
                // is not `Sync`, and the writer's accounting is disposable.
                let hw_counter = HardwareCounterCell::disposable();
                let points = segment.read_stored_points(&internal_ids, &hw_counter)?;

                Ok(entries.into_iter().map(|(id, _)| id).zip(points).collect())
            })
            .collect::<OperationResult<Vec<_>>>()
    })?;

    let mut stored = AHashMap::new();
    for (id, point) in per_segment.into_iter().flatten() {
        stored.insert(id, point);
    }

    Ok(stored)
}
