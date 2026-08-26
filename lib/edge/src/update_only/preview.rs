//! Dry-run of a batch: the apply pipeline run up to — but not including — the
//! writes.
//!
//! [`preview_batch`] and [`apply_batch`] share one resolution stage
//! ([`resolve_batch`]), so a preview reports exactly what an apply would do.
//!
//! [`preview_batch`]: UpdateOnlyEdgeShard::preview_batch
//! [`apply_batch`]: UpdateOnlyEdgeShard::apply_batch

use common::types::PointOffsetType;
use common::universal_io::{UniversalAppend, UniversalRead};
use rayon::ThreadPool;
use segment::common::operation_error::OperationResult;
use segment::data_types::fully_qualified_point::FullyQualifiedPoint;
use segment::types::{PointIdType, SeqNumberType};
use shard::operations::CollectionUpdateOperations;
use uuid::Uuid;

use crate::update_only::UpdateOnlyEdgeShard;
use crate::update_only::apply::{locate_points, read_stored_points};
use crate::update_only::batch::UpdateBatchPlan;
use crate::update_only::holder::LookupSegmentHolder;

/// A resolved batch: one entry per touched point, in first-touched order.
pub struct UpdateBatchPreview {
    pub points: Vec<PointPreview>,
}

/// What the batch does to one point.
pub struct PointPreview {
    pub id: PointIdType,
    /// The newest stored copy of the point; `None` when no segment holds it.
    pub current: Option<PointCopy>,
    /// Every slot the point occupies across segments, the newest's included —
    /// all of them are tombstoned when the action stores or deletes the point.
    pub slots: Vec<(Uuid, PointOffsetType)>,
    pub action: PointAction,
}

/// One stored copy of a point: which segment holds it, in which slot, at what
/// version.
pub struct PointCopy {
    pub segment: Uuid,
    pub internal_id: PointOffsetType,
    pub version: SeqNumberType,
}

/// The write one point's folded mutations resolved to.
pub enum PointAction {
    /// The point is appended to the write target in this fully qualified
    /// form, and every slot in [`PointPreview::slots`] is tombstoned.
    /// Boxed: a resolved point is hundreds of bytes, the other variants none.
    Store(Box<FullyQualifiedPoint>),
    /// The point is removed: every slot is tombstoned, nothing is stored.
    Delete,
    /// Left untouched: the stored copy is already at or beyond the batch's
    /// version, so re-applying would move the point backwards.
    Skip,
    /// Left untouched: every operation naming the point was rejected by its
    /// update mode — an `insert_only` upsert of a point that is already
    /// there. Nothing is written and no slot is retired.
    Rejected,
    /// An operation that can only modify an existing point named one that no
    /// segment holds; there is nothing to write. An `update_only` upsert of a
    /// point that does not exist lands here.
    Missing,
}

/// Resolve a folded batch against the segments: locate every touched point,
/// read the ones whose mutations need the stored form, and materialize each
/// into its [`PointAction`]. Reads only — the single decision stage behind
/// both [`UpdateOnlyEdgeShard::preview_batch`] and
/// [`UpdateOnlyEdgeShard::apply_batch`].
pub(super) fn resolve_batch<S: UniversalRead + 'static>(
    segments: &LookupSegmentHolder<S>,
    plan: UpdateBatchPlan,
    pool: &ThreadPool,
) -> OperationResult<Vec<PointPreview>> {
    let locations = locate_points(segments, &plan, pool)?;
    let mut stored = read_stored_points(segments, &plan, &locations, pool)?;

    let mut points = Vec::with_capacity(plan.len());
    for (id, updates) in plan.into_point_updates() {
        let location = locations.get(&id);
        let current = location.map(|location| PointCopy {
            segment: location.newest.segment,
            internal_id: location.newest.internal_id,
            version: location.newest.version,
        });
        let slots = location
            .map(|location| location.slots.clone())
            .unwrap_or_default();

        let exists = current.is_some();
        // Already applied: the stored point is at or beyond this batch's
        // version, so re-applying would move it backwards.
        let already_applied = current
            .as_ref()
            .is_some_and(|current| current.version >= updates.version());

        let action = if already_applied {
            PointAction::Skip
        } else if !updates.applies_any(exists) {
            // Every operation was rejected by its update mode. A rejected
            // `update_only` upsert is the `Missing` case — an operation that
            // can only modify an existing point, naming one that is not there.
            if exists {
                PointAction::Rejected
            } else {
                PointAction::Missing
            }
        } else {
            match updates.materialize(id, exists, stored.remove(&id))? {
                Some(point) => PointAction::Store(Box::new(point)),
                None if exists => PointAction::Delete,
                None => PointAction::Missing,
            }
        };

        points.push(PointPreview {
            id,
            current,
            slots,
            action,
        });
    }

    Ok(points)
}

impl<S: UniversalAppend + 'static> UpdateOnlyEdgeShard<S> {
    /// Resolve a batch without writing anything: what
    /// [`apply_batch`](Self::apply_batch) would do, reported per point.
    ///
    /// Runs the same resolution code path as the real apply — the same
    /// operations are rejected, the same points read — so the report cannot
    /// drift from the apply's behavior.
    pub fn preview_batch(
        &self,
        operations: impl IntoIterator<Item = (SeqNumberType, CollectionUpdateOperations)>,
    ) -> OperationResult<UpdateBatchPreview> {
        let plan = UpdateBatchPlan::build(operations)?;
        if plan.is_empty() {
            return Ok(UpdateBatchPreview { points: Vec::new() });
        }

        let segments = self.segments.read();
        let points = resolve_batch(&segments, plan, &self.pool)?;
        Ok(UpdateBatchPreview { points })
    }
}
