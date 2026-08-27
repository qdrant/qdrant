pub mod segment_entry;
pub mod snapshot_entry;

#[cfg(test)]
mod tests;

use std::borrow::Cow;

use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use segment::common::operation_error::OperationResult;
use segment::pending_changes::PendingChanges;
pub use segment::pending_changes::{
    DeletedPoints, IntendedVector, ProxyDeletedPoint, ProxyIndexChange, ProxyIndexChanges,
    ProxyVectorNameChanges,
};
use segment::types::*;

use crate::locked_segment::LockedSegment;

/// This object is a wrapper around read-only segment.
///
/// It could be used to provide all read and write operations while wrapped segment is being optimized (i.e. not available for writing)
/// It writes all changed records into a temporary `write_segment` and keeps track on changed points
#[derive(Debug)]
pub struct ProxySegment {
    pub wrapped_segment: LockedSegment,
    /// Internal mask of deleted points, specific to the wrapped segment
    /// Present if the wrapped segment is a plain segment
    /// Used for faster deletion checks
    deleted_mask: Option<BitVec>,
    /// Pending point deletes, payload index changes and vector name changes buffered by this
    /// proxy, along with their persistence into the pending changes log file inside the wrapped
    /// segment's directory.
    pending_changes: PendingChanges,
    deleted_deferred_count: usize,
    wrapped_config: SegmentConfig,

    /// Version of the last change in this proxy, considering point deletes and payload index
    /// changes. Defaults to the version of the wrapped segment.
    version: SeqNumberType,
}

/// A freshly built [`ProxySegment`] whose `deleted_mask` has not been synced yet.
///
/// `deleted_mask` is a snapshot of the wrapped segment's deleted bitvec. That snapshot is only
/// valid once the wrapped segment is frozen under the segment-holder write lock: a proxy is built
/// while only a read/upgradable-read lock is held, so an upsert or delete can still land on the
/// not-yet-frozen wrapped segment afterwards. An upsert landing in that window extends the wrapped
/// segment's point count past the snapshot; the scored search path then treats every offset beyond
/// `deleted_mask` as deleted (`NotDeletedChecker` defaults out-of-range to deleted), silently
/// dropping a live point from filtered KNN even though scroll/count/retrieve still see it.
///
/// To make this impossible to get wrong, [`ProxySegment::new`] hands back this type rather than a
/// usable `ProxySegment`. The only way to obtain a `ProxySegment` is [`Self::finalize`], which
/// reads the mask exactly once — so it cannot be forgotten, nor done twice. Call `finalize` once
/// the holder write lock is held (wrapped segment frozen) and before the proxy goes live.
#[must_use = "an UnsyncedProxySegment must be turned into a ProxySegment via `.finalize()`"]
#[derive(Debug)]
pub struct UnsyncedProxySegment(ProxySegment);

impl UnsyncedProxySegment {
    /// Build a proxy wrapping `segment`.
    ///
    /// `deleted_mask` is deliberately left empty here: it snapshots the wrapped segment's deleted
    /// bitvec, but that snapshot is only valid once the wrapped segment is frozen under the
    /// segment-holder write lock (see the type-level docs). The mask is read exactly once, later,
    /// by [`Self::finalize`] — which is also the only way to turn this into a usable
    /// [`ProxySegment`], so the sync cannot be forgotten nor done twice.
    ///
    /// Opens the pending changes log for this proxy layer inside the wrapped segment's
    /// directory. Wrapping a segment that already is a proxy uses the next layer up, writing to a
    /// dedicated log file. If a log file for this layer already exists — left behind by a
    /// previous proxy that propagated its changes into the segment before unwrapping — it is
    /// adopted and appended to.
    pub fn new(segment: LockedSegment) -> OperationResult<Self> {
        let (wrapped_config, version, data_path) = {
            let read_segment = segment.get().read();
            (
                read_segment.config().clone(),
                read_segment.version(),
                read_segment.data_path(),
            )
        };

        // Each proxy layer writes its pending changes to a dedicated log file; wrapping another
        // proxy means this proxy is one layer further up
        let pending_changes_level = match &segment {
            LockedSegment::Original(_) => 0,
            LockedSegment::Proxy(proxy_segment) => {
                log::debug!("Double proxy segment creation");
                proxy_segment.read().pending_changes.level() + 1
            }
        };

        let pending_changes = PendingChanges::open(&data_path, pending_changes_level)?;

        Ok(UnsyncedProxySegment(ProxySegment {
            wrapped_segment: segment,
            // Synced only in `finalize`, once the wrapped segment is frozen.
            deleted_mask: None,
            pending_changes,
            deleted_deferred_count: 0,
            wrapped_config,
            version,
        }))
    }

    /// Sync `deleted_mask` from the now-frozen wrapped segment and return the usable proxy.
    ///
    /// Must be called once the segment-holder write lock is held, so the wrapped segment can no
    /// longer change and the mask covers its full, final point range. The fresh read also captures
    /// any deletes that raced in, closing the ghost direction too.
    pub fn finalize(mut self) -> ProxySegment {
        self.0.sync_deleted_mask();
        self.0
    }

    /// The wrapped (soon-to-be-frozen) segment. Exposed for invariant checks before finalizing.
    pub fn wrapped_segment(&self) -> &LockedSegment {
        &self.0.wrapped_segment
    }

    /// See [`ProxySegment::replicate_field_indexes`].
    pub fn replicate_field_indexes(
        &self,
        op_num: SeqNumberType,
        hw_counter: &HardwareCounterCell,
        segment_to_update: &LockedSegment,
    ) -> OperationResult<()> {
        self.0
            .replicate_field_indexes(op_num, hw_counter, segment_to_update)
    }
}

impl ProxySegment {
    /// Build a proxy wrapping `segment` and immediately sync its `deleted_mask`.
    ///
    /// Test-only convenience that collapses the two-phase [`UnsyncedProxySegment::new`] +
    /// [`UnsyncedProxySegment::finalize`] construction into one call. Production code must use the
    /// two-phase form so the mask is synced under the segment-holder write lock; see
    /// [`UnsyncedProxySegment`].
    #[cfg(feature = "testing")]
    pub fn new(segment: LockedSegment) -> Self {
        UnsyncedProxySegment::new(segment)
            .expect("failed to open proxy segment pending changes")
            .finalize()
    }

    /// Read the wrapped segment's deleted bitvec into `deleted_mask`.
    ///
    /// Only called from [`UnsyncedProxySegment::finalize`]; see that type for why the timing
    /// (after the wrapped segment is frozen) matters.
    fn sync_deleted_mask(&mut self) {
        match &self.wrapped_segment {
            LockedSegment::Original(raw_segment) => {
                self.deleted_mask = Some(raw_segment.read().get_deleted_points_bitvec());
            }
            LockedSegment::Proxy(_) => {
                // A double proxy has no own deleted bitvec to sync.
            }
        }
    }

    /// Ensure that write segment have same indexes as wrapped segment
    pub fn replicate_field_indexes(
        &self,
        op_num: SeqNumberType,
        hw_counter: &HardwareCounterCell,
        segment_to_update: &LockedSegment,
    ) -> OperationResult<()> {
        let existing_indexes = segment_to_update.get().read().get_indexed_fields();
        let expected_indexes = self.wrapped_segment.get().read().get_indexed_fields();

        // Add missing indexes
        for (expected_field, expected_schema) in &expected_indexes {
            let existing_schema = existing_indexes.get(expected_field);

            if existing_schema != Some(expected_schema) {
                if existing_schema.is_some() {
                    segment_to_update
                        .get()
                        .write()
                        .delete_field_index(op_num, expected_field)?;
                }
                segment_to_update.get().write().create_field_index(
                    op_num,
                    expected_field,
                    Some(expected_schema),
                    hw_counter,
                )?;
            }
        }

        // Remove extra indexes
        for existing_field in existing_indexes.keys() {
            if !expected_indexes.contains_key(existing_field) {
                segment_to_update
                    .get()
                    .write()
                    .delete_field_index(op_num, existing_field)?;
            }
        }

        Ok(())
    }

    /// Updates the deleted mask with the given point offset
    /// Ensures that the mask is resized if necessary and returns false
    /// if either the mask or the point offset is missing (mask is not applicable)
    fn set_deleted_offset(&mut self, point_offset: Option<PointOffsetType>) -> bool {
        match (&mut self.deleted_mask, point_offset) {
            (Some(deleted_mask), Some(point_offset)) => {
                if deleted_mask.len() <= point_offset as usize {
                    deleted_mask.resize(point_offset as usize + 1, false);
                }
                deleted_mask.set(point_offset as usize, true);
                true
            }
            _ => false,
        }
    }

    /// Build a filter that excludes the given deleted points. Accepts
    /// `Option<Cow<Filter>>` so that a filter already owned by the caller
    /// (e.g. from [`ProxyVectorNameChanges::redact_filter`]) is reused
    /// without an extra clone.
    fn add_deleted_points_condition_to_filter(
        filter: Option<Cow<'_, Filter>>,
        deleted_points: impl IntoIterator<Item = PointIdType>,
    ) -> Filter {
        let wrapper_condition = Condition::HasId(HasIdCondition::from_iter(deleted_points));
        match filter {
            None => Filter::new_must_not(wrapper_condition),
            Some(f) => {
                let mut new_filter = f.into_owned();
                let new_must_not = match new_filter.must_not {
                    None => Some(vec![wrapper_condition]),
                    Some(mut conditions) => {
                        conditions.push(wrapper_condition);
                        Some(conditions)
                    }
                };
                new_filter.must_not = new_must_not;
                new_filter
            }
        }
    }

    /// Propagate changes in this proxy to the wrapped segment
    ///
    /// This propagates:
    /// - delete (or moved) points
    /// - deleted payload indexes
    /// - created payload indexes
    ///
    /// This is required if making both the wrapped segment and the writable segment available in a
    /// shard holder at the same time. If the wrapped segment is thrown away, then this is not
    /// required.
    ///
    /// The pending changes log file is deliberately left in place: deleting it before the wrapped
    /// segment has flushed the propagated changes would not be crash safe. It is cleaned up on
    /// restart and when the segment directory is dropped, and a new proxy on the same segment
    /// adopts it. Replaying it is safe because all operations are version gated.
    pub fn propagate_to_wrapped(&mut self) -> OperationResult<()> {
        // Important: we must not keep a write lock on the wrapped segment for the duration of this
        // function to prevent a deadlock. The search functions conflict with it trying to take a
        // read lock on the wrapped segment as well while already holding the deleted points lock
        // (or others). Careful locking management is very important here. Instead we just take an
        // upgradable read lock, upgrading to a write lock on demand.
        // See: <https://github.com/qdrant/qdrant/pull/4206>
        let wrapped_segment = self.wrapped_segment.get();
        let mut wrapped_segment = wrapped_segment.upgradable_read();

        // Propagate index changes before point deletions
        // Point deletions bump the segment version, can cause index changes to be ignored
        // Lock ordering is important here and must match the flush function to prevent a deadlock
        {
            let op_num = wrapped_segment.version();
            if !self.pending_changes.index_changes().is_empty() {
                wrapped_segment.with_upgraded(|wrapped_segment| {
                    for (field_name, change) in self.pending_changes.index_changes().iter_ordered()
                    {
                        debug_assert!(
                            change.version() >= op_num,
                            "proxied index change should have newer version than segment",
                        );
                        match change {
                            ProxyIndexChange::Create(schema, version) => {
                                wrapped_segment.create_field_index(
                                    *version,
                                    field_name,
                                    Some(schema),
                                    &HardwareCounterCell::disposable(), // Internal operation
                                )?;
                            }
                            ProxyIndexChange::Delete(version) => {
                                wrapped_segment.delete_field_index(*version, field_name)?;
                            }
                            ProxyIndexChange::DeleteIfIncompatible(version, schema) => {
                                wrapped_segment.delete_field_index_if_incompatible(
                                    *version, field_name, schema,
                                )?;
                            }
                        }
                    }
                    OperationResult::Ok(())
                })?;
                self.pending_changes.clear_index_changes();
            }
        }

        // Propagate vector name changes (between index changes and point deletions)
        {
            if !self.pending_changes.vector_name_changes().is_empty() {
                wrapped_segment.with_upgraded(|wrapped_segment| {
                    for (vector_name, intent) in
                        self.pending_changes.vector_name_changes().iter_ordered()
                    {
                        match intent {
                            IntendedVector::Absent { version } => {
                                wrapped_segment.delete_vector_name(*version, vector_name)?;
                            }
                            IntendedVector::Present {
                                config,
                                version,
                                supersedes_wrapped,
                            } => {
                                if *supersedes_wrapped {
                                    // `create_vector_name_impl` is idempotent and would
                                    // silently keep the wrapped's stale storage. Clear it
                                    // first so the new schema actually takes effect.
                                    wrapped_segment.delete_vector_name(*version, vector_name)?;
                                }
                                wrapped_segment.create_vector_name(
                                    *version,
                                    vector_name,
                                    config,
                                )?;
                            }
                        }
                    }
                    OperationResult::Ok(())
                })?;
                self.pending_changes.clear_vector_name_changes();
            }
        }

        // Propagate deleted points
        // Lock ordering is important here and must match the flush function to prevent a deadlock
        {
            if !self.pending_changes.deleted_points().is_empty() {
                wrapped_segment.with_upgraded(|wrapped_segment| {
                    for (point_id, versions) in self.pending_changes.deleted_points().iter() {
                        // Note:
                        // Queued deletes may have an older version than what is currently in the
                        // wrapped segment. Such deletes are ignored because the point in the
                        // wrapped segment is considered to be newer. This is possible because
                        // different proxy segments can share state through a common write segment.
                        // See: <https://github.com/qdrant/qdrant/pull/7208>
                        wrapped_segment.delete_point(
                            versions.operation_version,
                            *point_id,
                            &HardwareCounterCell::disposable(), // Internal operation: no need to measure.
                        )?;
                    }
                    OperationResult::Ok(())
                })?;
                self.pending_changes.clear_deleted_points();
                self.deleted_deferred_count = 0;

                // Note: We do not clear the deleted mask here, as it provides
                // no performance advantage and does not affect the correctness of search.
                // Points are still marked as deleted in two places, which is fine
            }
        }

        Ok(())
    }

    pub fn get_deleted_points(&self) -> &DeletedPoints {
        self.pending_changes.deleted_points()
    }

    pub fn get_index_changes(&self) -> &ProxyIndexChanges {
        self.pending_changes.index_changes()
    }

    pub fn get_vector_name_changes(&self) -> &ProxyVectorNameChanges {
        self.pending_changes.vector_name_changes()
    }
}
