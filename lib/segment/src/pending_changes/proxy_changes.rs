//! The set of changes buffered by a proxy segment, and how to propagate them to a real segment.

use std::cmp::max;
use std::sync::atomic::AtomicBool;

use itertools::Itertools as _;

use super::change::{DeletedPoints, PendingChange};
use super::index_changes::ProxyIndexChanges;
use super::vector_name_changes::ProxyVectorNameChanges;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::entry::entry_point::NonAppendableSegmentEntry;

/// Changes a proxy segment buffers instead of applying them to the segment it wraps: point
/// deletes, payload index changes and vector name changes.
///
/// Each kind keeps the latest change per key (point, payload field or vector name), along with
/// the version of the operation that caused it. [`Self::propagate`] applies them to a real
/// segment in operation version order, through its regular version-gated operations. It is the
/// single way buffered changes reach a segment: the wrapped segment when a proxy is unwrapped, or
/// the optimized segment when an optimization finishes.
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

    /// Iterate over all buffered changes, ordered by operation version.
    ///
    /// The three kinds are interleaved by the version of the operation that caused each change,
    /// so iterating yields them in the order the operations were originally applied to the proxy.
    /// Point deletes of one operation share its version, changes of different kinds never do as
    /// every operation is of one kind. Should they anyway, vector name changes come first, then
    /// payload index changes, then point deletes.
    pub fn iter_ordered(&self) -> impl Iterator<Item = PendingChange> + '_ {
        let vector_name_changes =
            self.vector_name_changes
                .iter_ordered()
                .map(|(vector_name, intent)| PendingChange::VectorNameChange {
                    vector_name: vector_name.clone(),
                    intent: intent.clone(),
                });
        let index_changes = self
            .index_changes
            .iter_ordered()
            .map(|(field_name, change)| PendingChange::IndexChange {
                field_name: field_name.clone(),
                change: change.clone(),
            });
        let deleted_points = self
            .deleted_points
            .iter()
            .sorted_unstable_by_key(|(_, versions)| versions.operation_version)
            .map(|(&point_id, &versions)| PendingChange::DeletePoint { point_id, versions });

        vector_name_changes
            .merge_by(index_changes, |a, b| a.version() <= b.version())
            .merge_by(deleted_points, |a, b| a.version() <= b.version())
    }

    /// Apply all buffered changes to `segment`, in operation version order.
    ///
    /// Every change is applied with the version of the operation that caused it, through the
    /// segment's regular version-gated operations, in the same order those operations originally
    /// arrived at the proxy (see [`Self::iter_ordered`]). Applying them in that order is what
    /// makes the plain versions work: the segment version only ever grows to the version of the
    /// change just applied, so no change of one kind is gated out by a later change of another
    /// kind, as it would be if the kinds were applied one after the other.
    ///
    /// Applying a change the segment has already seen is a no-op, so propagating the same set
    /// twice is safe, though see [`Self::exclude_applied`] to avoid the cost of doing so.
    ///
    /// The `supersedes_wrapped` flag of [`IntendedVector::Present`](super::IntendedVector::Present)
    /// is computed when the change is recorded, against the proxy's wrapped segment, not against
    /// `segment` as it is now. Propagating twice therefore relies on a name never being created
    /// twice with a different config without a delete in between: the second create would be an
    /// idempotent no-op, keeping the first config. Such creates are rejected before they reach a
    /// shard, see `add_vector_to_config` in the collection's vector name schema.
    ///
    /// Fails with a cancellation error if `stopped` is set while propagating.
    pub fn propagate<S>(&self, segment: &mut S, stopped: &AtomicBool) -> OperationResult<()>
    where
        S: NonAppendableSegmentEntry + ?Sized,
    {
        for change in self.iter_ordered() {
            super::apply_change(segment, &change)?;
            check_process_stopped(stopped)?;
        }
        Ok(())
    }
}
