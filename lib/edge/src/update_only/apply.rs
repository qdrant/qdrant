//! Applying a folded batch: locate, resolve, materialize, append — each one
//! batched pass over the whole point set, so a batch's cost scales with the
//! points it touches, not the operations in it.

use std::collections::HashMap;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{UniversalAppendFs, UniversalRead};
use rayon::ThreadPool;
use rayon::prelude::*;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::fully_qualified_point::{FullyQualifiedPoint, StoredPoint};
use segment::segment::update_only::UpdateOnlySegmentEnum;
use segment::types::{PointIdType, SeqNumberType};
use shard::operations::CollectionUpdateOperations;
use uuid::Uuid;

use crate::update_only::UpdateOnlyEdgeShard;
use crate::update_only::batch::UpdateBatchPlan;
use crate::update_only::holder::LookupSegmentHolder;
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
    /// Points every naming operation's update mode rejected, left untouched —
    /// how many ids of an `insert_only` upsert were already taken.
    pub rejected: usize,
    /// Points an operation named that no segment holds and the batch did not
    /// create — a payload update, or an `update_only` upsert, to a point that
    /// is not there.
    pub missing: usize,
    /// One record per touched point, in first-touched order: what happened
    /// to it, and which slots it vacated where.
    pub points: Vec<PointApplyRecord>,
}

/// How one point's slots changed under [`apply_batch`]: where its previous
/// copies were retired, distinguishing an overwrite from a fresh insert.
///
/// [`apply_batch`]: UpdateOnlyEdgeShard::apply_batch
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointApplyRecord {
    pub id: PointIdType,
    pub kind: PointApplyKind,
    /// Slots retired with a tombstone: every segment that held a copy of the
    /// point, except the write target's own slot when the point is stored.
    /// Empty for a stored point means a fresh insert, not an overwrite.
    pub tombstoned: Vec<(Uuid, PointOffsetType)>,
    /// The write-target slot a stored point left behind without a tombstone:
    /// appending the point records a mapping that supersedes it.
    pub superseded: Option<(Uuid, PointOffsetType)>,
}

/// The per-point action of [`PointApplyRecord`], mirroring the counts on
/// [`UpdateBatchOutcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointApplyKind {
    /// Appended to the write target — an overwrite when the record lists
    /// retired slots, a fresh insert otherwise.
    Stored,
    /// Removed from every slot it occupied.
    Deleted,
    /// Left untouched: already at or beyond the batch's version.
    Skipped,
    /// Left untouched: every operation naming it was rejected by its update
    /// mode.
    Rejected,
    /// Named by an operation, held by no segment, not created by the batch.
    Missing,
}

impl PointApplyKind {
    /// Whether the point's previous slots have to stop resolving: a point the
    /// batch rewrote or removed must not keep serving from where it used to
    /// sit. The kinds that write nothing leave every slot alone.
    fn retires_slots(self) -> bool {
        match self {
            Self::Stored | Self::Deleted => true,
            Self::Skipped | Self::Rejected | Self::Missing => false,
        }
    }
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

impl<Fs: UniversalAppendFs> UpdateOnlyEdgeShard<Fs> {
    /// Apply a batch of update operations, each paired with the operation
    /// number to record as its version. Operations are expected in ascending
    /// operation-number order; see [`UpdateBatchPlan::build`] for what is
    /// rejected.
    ///
    /// Atomic in the sense that matters without a WAL: applied in full or the
    /// error is returned, and re-applying a batch that partially landed skips
    /// the points that already carry its version.
    ///
    /// Consumes the writer and hands it back on success, ready for the next
    /// batch: the lookup half of every segment this one wrote to is
    /// live-reloaded, so the next batch resolves against what this one
    /// wrote. On error the writer is gone — its lookups may no longer
    /// describe the durable state, and resolving another batch against them
    /// can retire the wrong copy of a point. Recover by opening a fresh
    /// writer over the directory, which also retires whatever the failed
    /// batch left unpublished.
    pub fn apply_batch(
        mut self,
        operations: impl IntoIterator<Item = (SeqNumberType, CollectionUpdateOperations)>,
    ) -> OperationResult<(Self, UpdateBatchOutcome)> {
        const LOG_TARGET: &str = "apply_batch";

        let start = std::time::Instant::now();
        let plan = UpdateBatchPlan::build(operations)?;
        if plan.is_empty() {
            return Ok((self, UpdateBatchOutcome::default()));
        }

        let hw_counter = HardwareCounterCell::disposable();
        let segments = self.segments.read();

        // 1-3. Locate, read, materialize — the decision stage shared with
        // `preview_batch`, so a preview cannot drift from the real apply.
        let instant = std::time::Instant::now();
        let resolved = resolve_batch(&segments, plan, &self.pool)?;
        log::trace!(target: LOG_TARGET, "resolve_batch took {:?}", instant.elapsed());

        let mut outcome = UpdateBatchOutcome::default();
        let mut to_store: Vec<FullyQualifiedPoint> = Vec::new();
        let mut to_tombstone: AHashMap<Uuid, Vec<(PointIdType, PointOffsetType)>> = AHashMap::new();
        let write_target_uuid = segments.write_target_uuid();

        for point in resolved {
            let PointPreview {
                id,
                current: _,
                slots,
                action,
            } = point;

            let kind = match action {
                PointAction::Skip => {
                    outcome.skipped += 1;
                    PointApplyKind::Skipped
                }
                PointAction::Rejected => {
                    outcome.rejected += 1;
                    PointApplyKind::Rejected
                }
                PointAction::Missing => {
                    outcome.missing += 1;
                    PointApplyKind::Missing
                }
                PointAction::Store(point) => {
                    to_store.push(*point);
                    outcome.stored += 1;
                    PointApplyKind::Stored
                }
                PointAction::Delete => {
                    outcome.deleted += 1;
                    PointApplyKind::Deleted
                }
            };

            let mut record = PointApplyRecord {
                id,
                kind,
                tombstoned: Vec::new(),
                superseded: None,
            };

            if kind.retires_slots() {
                for (segment, internal_id) in slots {
                    // The copy a stored point leaves behind in the write target
                    // needs no retirement: appending the point records a mapping
                    // that supersedes its old slot, and retiring the id on top of
                    // that would take the new slot with it. Every other segment's
                    // copy does have to stop resolving — an older duplicate left
                    // by an interrupted move included — and a delete retires the
                    // point everywhere it sits.
                    if kind == PointApplyKind::Stored && Some(segment) == write_target_uuid {
                        record.superseded = Some((segment, internal_id));
                        continue;
                    }
                    to_tombstone
                        .entry(segment)
                        .or_default()
                        .push((id, internal_id));
                    record.tombstoned.push((segment, internal_id));
                }
            }

            outcome.points.push(record);
        }

        drop(segments);

        if to_store.is_empty() && to_tombstone.is_empty() {
            return Ok((self, outcome));
        }

        // 4. Append the resolved points, then retire the slots they replaced.
        // There is no flush step: the append-only components behind the
        // writers buffer nothing across calls, so a write is durable when it
        // returns.
        let mut written: Vec<Uuid> = Vec::new();
        let mut tombstone_start = None;
        if !to_store.is_empty() {
            let uuid = write_target_uuid.ok_or_else(|| {
                OperationError::service_error("No appendable segment exists, expected exactly one")
            })?;
            let writer = get_writer(&mut self.writers, uuid)?;

            let instant = std::time::Instant::now();
            writer
                .as_appendable_mut()
                .ok_or_else(|| {
                    OperationError::service_error(format!(
                        "Write target {uuid} was opened as delete-only, it cannot store points",
                    ))
                })?
                .store_points(&to_store, &hw_counter)?;
            log::trace!(target: LOG_TARGET, "store_points took: {:?}", instant.elapsed());

            // The write target's retirements happen after the store, since
            // every write is durable when it returns and the reverse order
            // can lose a point outright if the process dies in between.
            if let Some(points) = to_tombstone.remove(&uuid) {
                tombstone_start = Some(std::time::Instant::now());
                writer.tombstone_points(&points)?;
            }
            written.push(uuid);
        }

        let instant = tombstone_start.unwrap_or_else(std::time::Instant::now);
        for (uuid, points) in to_tombstone {
            get_writer(&mut self.writers, uuid)?.tombstone_points(&points)?;
            written.push(uuid);
        }
        log::trace!(target: LOG_TARGET, "tombstone_points took {:?}", instant.elapsed());

        let instant = std::time::Instant::now();
        self.reload_lookups(&written)?;
        log::trace!(target: LOG_TARGET, "reload_lookups took {:?}", instant.elapsed());

        log::trace!(target: LOG_TARGET, "total apply_batch took {:?}", start.elapsed());
        Ok((self, outcome))
    }

    /// Live-reload the lookup half of every segment in `written`, so the
    /// next batch resolves against what this one wrote. The writers stay
    /// open: their in-memory state advanced with each durable write, so a
    /// reloaded lookup and its held writer describe the same log.
    fn reload_lookups(&self, written: &[Uuid]) -> OperationResult<()> {
        let segments = self.segments.read();
        self.pool.install(|| {
            written.par_iter().try_for_each(|&uuid| {
                // Not shared across segments: `HardwareCounterCell` is not
                // `Sync`, and the writer's accounting is disposable.
                let hw_counter = HardwareCounterCell::disposable();
                segments
                    .get(uuid)?
                    .write()
                    .live_reload(&self.fs, &hw_counter)
            })
        })
    }
}

/// The held writer for segment `uuid`; an error when there is none, which can
/// only mean the shard's inventory changed under a batch in flight.
fn get_writer<Fs: UniversalAppendFs>(
    writers: &mut HashMap<Uuid, UpdateOnlySegmentEnum<Fs>>,
    uuid: Uuid,
) -> OperationResult<&mut UpdateOnlySegmentEnum<Fs>> {
    writers
        .get_mut(&uuid)
        .ok_or_else(|| OperationError::service_error(format!("No writer open for segment {uuid}")))
}

/// Locate every point the batch touches: every slot it occupies, with the
/// newest copy marked, when more than one segment holds the point. Segments
/// are visited in parallel on `pool`.
pub(super) fn locate_points<S: UniversalRead + 'static>(
    segments: &LookupSegmentHolder<S>,
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
                let appendable = segment.appendable;

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
    segments: &LookupSegmentHolder<S>,
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
                let segment = segments.get(uuid)?.read();

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
