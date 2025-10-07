pub mod segment_entry;
pub mod snapshot_entry;

#[cfg(test)]
mod tests;

use ahash::AHashMap;
use bitvec::prelude::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use itertools::Itertools as _;
use segment::common::operation_error::OperationResult;
use segment::types::*;

use crate::locked_segment::LockedSegment;

pub type DeletedPoints = AHashMap<PointIdType, ProxyDeletedPoint>;

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
    changed_indexes: ProxyIndexChanges,
    /// Points which should no longer used from wrapped_segment
    /// May contain points which are not in wrapped_segment,
    /// because the set is shared among all proxy segments
    deleted_points: DeletedPoints,
    wrapped_config: SegmentConfig,

    /// Version of the last change in this proxy, considering point deletes and payload index
    /// changes. Defaults to the version of the wrapped segment.
    version: SeqNumberType,
}

impl ProxySegment {
    pub fn new(segment: LockedSegment) -> Self {
        let deleted_mask = match &segment {
            LockedSegment::Original(raw_segment) => {
                let raw_segment_guard = raw_segment.read();
                let already_deleted = raw_segment_guard.get_deleted_points_bitvec();
                Some(already_deleted)
            }
            LockedSegment::Proxy(_) => {
                log::debug!("Double proxy segment creation");
                None
            }
        };

        let (wrapped_config, version) = {
            let read_segment = segment.get().read();
            (read_segment.config().clone(), read_segment.version())
        };

        ProxySegment {
            wrapped_segment: segment,
            deleted_mask,
            changed_indexes: ProxyIndexChanges::default(),
            deleted_points: AHashMap::new(),
            wrapped_config,
            version,
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

    fn add_deleted_points_condition_to_filter(
        filter: Option<&Filter>,
        deleted_points: impl IntoIterator<Item = PointIdType>,
    ) -> Filter {
        #[allow(clippy::from_iter_instead_of_collect)]
        let wrapper_condition = Condition::HasId(HasIdCondition::from_iter(deleted_points));
        match filter {
            None => Filter::new_must_not(wrapper_condition),
            Some(f) => {
                let mut new_filter = f.clone();
                let must_not = new_filter.must_not;

                let new_must_not = match must_not {
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
            if !self.changed_indexes.is_empty() {
                wrapped_segment.with_upgraded(|wrapped_segment| {
                    for (field_name, change) in self.changed_indexes.iter_ordered() {
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
                self.changed_indexes.clear();
            }
        }

        // Propagate deleted points
        // Lock ordering is important here and must match the flush function to prevent a deadlock
        {
            if !self.deleted_points.is_empty() {
                wrapped_segment.with_upgraded(|wrapped_segment| {
                    for (point_id, versions) in self.deleted_points.iter() {
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
                self.deleted_points.clear();

                // Note: We do not clear the deleted mask here, as it provides
                // no performance advantage and does not affect the correctness of search.
                // Points are still marked as deleted in two places, which is fine
            }
        }

        Ok(())
    }

    pub fn get_deleted_points(&self) -> &DeletedPoints {
        &self.deleted_points
    }

    pub fn get_index_changes(&self) -> &ProxyIndexChanges {
        &self.changed_indexes
    }
}

/// Point persion information of points to delete from a wrapped proxy segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProxyDeletedPoint {
    /// Version the point had in the wrapped segment when the delete was scheduled.
    /// We use it to determine if some other proxy segment should move the point again with
    /// `move_if_exists` if it has newer point data.
    pub local_version: SeqNumberType,
    /// Version of the operation that caused the delete to be scheduled.
    /// We use it for the delete operations when propagating them to the wrapped or optimized
    /// segment.
    pub operation_version: SeqNumberType,
}

#[derive(Debug, Default)]
pub struct ProxyIndexChanges {
    changes: AHashMap<PayloadKeyType, ProxyIndexChange>,
}

impl ProxyIndexChanges {
    pub fn insert(&mut self, key: PayloadKeyType, change: ProxyIndexChange) {
        self.changes.insert(key, change);
    }

    pub fn remove(&mut self, key: &PayloadKeyType) {
        self.changes.remove(key);
    }

    pub fn len(&self) -> usize {
        self.changes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn clear(&mut self) {
        self.changes.clear();
    }

    /// Iterate over proxy index changes in order of version.
    ///
    /// Index changes must be applied in order because changes with an old version will silently be
    /// rejected.
    pub fn iter_ordered(&self) -> impl Iterator<Item = (&PayloadKeyType, &ProxyIndexChange)> {
        self.changes
            .iter()
            .sorted_by_key(|(_, change)| change.version())
    }

    /// Iterate over proxy index changes in arbitrary order.
    pub fn iter_unordered(&self) -> impl Iterator<Item = (&PayloadKeyType, &ProxyIndexChange)> {
        self.changes.iter()
    }

    pub fn merge(&mut self, other: &Self) {
        for (key, change) in &other.changes {
            self.changes.insert(key.clone(), change.clone());
        }
    }
}

#[derive(Debug, Clone)]
pub enum ProxyIndexChange {
    Create(PayloadFieldSchema, SeqNumberType),
    Delete(SeqNumberType),
    DeleteIfIncompatible(SeqNumberType, PayloadFieldSchema),
}

impl ProxyIndexChange {
    pub fn version(&self) -> SeqNumberType {
        match self {
            ProxyIndexChange::Create(_, version) => *version,
            ProxyIndexChange::Delete(version) => *version,
            ProxyIndexChange::DeleteIfIncompatible(version, _) => *version,
        }
    }
}
