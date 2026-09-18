//! The set of changes buffered by a proxy segment, and how to propagate them to a real segment.

use std::cmp::max;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;

use super::change::{DeletedPoints, ProxyIndexChange};
use super::index_changes::ProxyIndexChanges;
use super::vector_name_changes::{IntendedVector, ProxyVectorNameChanges};
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::entry::entry_point::NonAppendableSegmentEntry;

/// Changes a proxy segment buffers instead of applying them to the segment it wraps: point
/// deletes, payload index changes and vector name changes.
///
/// Each kind keeps the latest change per key (point, payload field or vector name), along with
/// the version of the operation that caused it. [`Self::propagate`] applies them to a real
/// segment through its regular version-gated operations, which is the single way buffered
/// changes reach a segment: the wrapped segment when a proxy is unwrapped, or the optimized
/// segment when an optimization finishes.
///
/// Changes of multiple proxies can be [merged](Self::merge) into one set to propagate them
/// together.
#[derive(Debug, Default)]
pub struct ProxyChanges {
    deleted_points: DeletedPoints,
    index_changes: ProxyIndexChanges,
    vector_name_changes: ProxyVectorNameChanges,
}

impl ProxyChanges {
    /// Points which should no longer be used from the wrapped segment.
    pub fn deleted_points(&self) -> &DeletedPoints {
        &self.deleted_points
    }

    /// Pending payload index changes, per field key.
    pub fn index_changes(&self) -> &ProxyIndexChanges {
        &self.index_changes
    }

    /// Pending vector name changes, per vector name.
    pub fn vector_name_changes(&self) -> &ProxyVectorNameChanges {
        &self.vector_name_changes
    }

    pub(super) fn deleted_points_mut(&mut self) -> &mut DeletedPoints {
        &mut self.deleted_points
    }

    pub(super) fn index_changes_mut(&mut self) -> &mut ProxyIndexChanges {
        &mut self.index_changes
    }

    pub(super) fn vector_name_changes_mut(&mut self) -> &mut ProxyVectorNameChanges {
        &mut self.vector_name_changes
    }

    pub fn is_empty(&self) -> bool {
        self.deleted_points.is_empty()
            && self.index_changes.is_empty()
            && self.vector_name_changes.is_empty()
    }

    pub fn clear(&mut self) {
        self.deleted_points.clear();
        self.index_changes.clear();
        self.vector_name_changes.clear();
    }

    /// Merge the changes of another proxy into this set.
    ///
    /// Where both touch the same key the newest change wins. For a point deleted through both,
    /// the highest of each version is kept.
    pub fn merge(&mut self, other: &Self) {
        for (point_id, versions) in &other.deleted_points {
            let entry = self.deleted_points.entry(*point_id).or_insert(*versions);
            entry.operation_version = max(entry.operation_version, versions.operation_version);
            entry.local_version = max(entry.local_version, versions.local_version);
        }
        self.index_changes.merge(&other.index_changes);
        self.vector_name_changes.merge(&other.vector_name_changes);
    }

    /// Drop every change that is present in `applied` unchanged.
    ///
    /// Propagating a set of changes to a segment and later propagating a newer snapshot of the
    /// same buffers would apply most changes twice. The second application is a version-gated
    /// no-op, but for a proxy that buffered many deletes it is not free. Excluding what the
    /// first pass applied leaves exactly the changes that arrived since, plus keys that were
    /// changed again with a newer version.
    pub fn exclude_applied(&mut self, applied: &Self) {
        self.deleted_points
            .retain(|point_id, versions| applied.deleted_points.get(point_id) != Some(versions));
        self.index_changes
            .retain(|field_name, change| applied.index_changes.get(field_name) != Some(change));
        self.vector_name_changes.retain(|vector_name, intent| {
            applied.vector_name_changes.get(vector_name) != Some(intent)
        });
    }

    /// Apply all buffered changes to `segment`.
    ///
    /// Changes are applied through the segment's regular version-gated operations, so applying a
    /// change the segment has already seen is a no-op. That makes propagating the same set twice
    /// safe, though see [`Self::exclude_applied`] to avoid the cost of doing so.
    ///
    /// Applies the changes in stages: vector name changes, then payload index changes, then
    /// point deletes. Point deletes bump the segment version and would gate out the segment-level
    /// changes if applied before them. A segment-level change may still be older than the
    /// segment, if the segment already applied a higher-versioned change of another kind in an
    /// earlier propagation pass; those are applied with the segment version instead so that they
    /// are not ignored.
    ///
    /// Fails with a cancellation error if `stopped` is set while propagating.
    pub fn propagate<S>(&self, segment: &mut S, stopped: &AtomicBool) -> OperationResult<()>
    where
        S: NonAppendableSegmentEntry + ?Sized,
    {
        // Internal operation, no need to measure hardware IO
        let hw_counter = HardwareCounterCell::disposable();

        // New named vectors must exist before indexes or points reference them
        for (vector_name, intent) in self.vector_name_changes.iter_ordered() {
            let op_num = max(intent.version(), segment.version());
            match intent {
                IntendedVector::Absent { .. } => {
                    segment.delete_vector_name(op_num, vector_name)?;
                }
                IntendedVector::Present {
                    config,
                    version: _,
                    supersedes_wrapped,
                } => {
                    if *supersedes_wrapped {
                        // `create_vector_name` is idempotent and would silently keep the
                        // segment's stale storage. Clear it first so the new schema actually
                        // takes effect.
                        segment.delete_vector_name(op_num, vector_name)?;
                    }
                    segment.create_vector_name(op_num, vector_name, config)?;
                }
            }
            check_process_stopped(stopped)?;
        }

        for (field_name, change) in self.index_changes.iter_ordered() {
            let op_num = max(change.version(), segment.version());
            match change {
                ProxyIndexChange::Create(schema, _) => {
                    segment.create_field_index(op_num, field_name, Some(schema), &hw_counter)?;
                }
                ProxyIndexChange::Delete(_) => {
                    segment.delete_field_index(op_num, field_name)?;
                }
                ProxyIndexChange::DeleteIfIncompatible(_, schema) => {
                    segment.delete_field_index_if_incompatible(op_num, field_name, schema)?;
                }
            }
            check_process_stopped(stopped)?;
        }

        for (point_id, versions) in &self.deleted_points {
            // Note:
            // The delete may have an older version than the point currently has in the segment.
            // Such deletes are ignored because the point in the segment is considered to be
            // newer. This is possible because different proxy segments can share state through a
            // common write segment.
            // See: <https://github.com/qdrant/qdrant/pull/7208>
            segment.delete_point(versions.operation_version, *point_id, &hw_counter)?;
            check_process_stopped(stopped)?;
        }

        Ok(())
    }
}
