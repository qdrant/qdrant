pub mod compressed;
pub mod id_tracker_base;
pub mod immutable_id_tracker;
pub mod in_memory_id_tracker;
mod memory_reporter;
pub mod mutable_id_tracker;
pub mod point_mappings;

use common::types::PointOffsetType;
pub use id_tracker_base::*;
use itertools::Itertools as _;

use crate::types::{ExtendedPointId, PointIdType};

/// Calling [`for_each_unique_point`] will yield this struct for each unique
/// point.
#[derive(Debug, Clone, Copy)]
pub struct MergedPointId {
    /// Unique external ID. If the same external ID is present in multiple
    /// trackers, the item with the highest version takes precedence.
    pub external_id: ExtendedPointId,
    /// An index within `id_trackers` iterator that points to the [`IdTracker`]
    /// that contains this point.
    pub tracker_index: usize,
    /// The internal ID of the point within the [`IdTracker`] that contains it.
    pub internal_id: PointOffsetType,
    /// The version of the point within the [`IdTracker`] that contains it.
    pub version: u64,
    /// Whether the point is marked deleted in the source tracker. Used by
    /// [`for_each_unique_point`] to skip yielding a point whose
    /// highest-versioned copy is a tombstone — without this filter,
    /// `segment_builder` would resurrect deletes that were applied to a source
    /// segment between when it was first picked for optimization and when the
    /// merge actually reads from it (e.g. via snapshot teardown's
    /// `propagate_to_wrapped`).
    pub is_deleted: bool,
}

/// Calls a closure for each unique point from multiple ID trackers.
///
/// Discards points that have no version (their flush was interrupted) and
/// points whose highest-versioned copy across the input trackers is marked
/// deleted (so deletes applied to a source between optimization start and
/// merge are not silently dropped).
pub fn for_each_unique_point<'a>(
    id_trackers: impl Iterator<Item = &'a (impl IdTracker + ?Sized + 'a)>,
    mut f: impl FnMut(MergedPointId),
) {
    let mut iter = id_trackers
        .enumerate()
        .map(|(segment_index, id_tracker)| {
            id_tracker.point_mappings().iter_from(None).filter_map(
                move |(external_id, internal_id)| {
                    let version = id_tracker.internal_version(internal_id);
                    let is_deleted = id_tracker.is_deleted_point(internal_id);
                    // a point without a version had an interrupted flush sequence and should be discarded
                    version.map(|version| MergedPointId {
                        external_id,
                        tracker_index: segment_index,
                        internal_id,
                        version,
                        is_deleted,
                    })
                },
            )
        })
        .kmerge_by(|a, b| a.external_id < b.external_id);

    let Some(mut best_item) = iter.next() else {
        return;
    };

    for item in iter {
        if best_item.external_id == item.external_id {
            if best_item.version < item.version {
                best_item = item;
            }
        } else {
            if !best_item.is_deleted {
                f(best_item);
            }
            best_item = item;
        }
    }
    if !best_item.is_deleted {
        f(best_item);
    }
}

impl From<&ExtendedPointId> for PointIdType {
    fn from(point_id: &ExtendedPointId) -> Self {
        match point_id {
            ExtendedPointId::NumId(idx) => PointIdType::NumId(*idx),
            ExtendedPointId::Uuid(uuid) => PointIdType::Uuid(*uuid),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, hash_map};

    use in_memory_id_tracker::InMemoryIdTracker;
    use rand::SeedableRng as _;
    use rand::rngs::StdRng;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_for_each_unique_point(#[values(0, 1, 5)] tracker_count: usize) {
        let mut rand = StdRng::seed_from_u64(42);

        let id_trackers = (0..tracker_count)
            .map(|_| InMemoryIdTracker::random(&mut rand, 1000, 500, 10))
            .collect_vec();

        let mut collisions = 0;

        // Naive HashMap-based implementation of for_each_unique_point.
        let mut expected = HashMap::<ExtendedPointId, MergedPointId>::new();
        for (tracker_index, id_tracker) in id_trackers.iter().enumerate() {
            for (external_id, internal_id) in id_tracker.point_mappings().iter_from(None) {
                let version = id_tracker.internal_version(internal_id).unwrap();
                let is_deleted = id_tracker.is_deleted_point(internal_id);
                let merged_point_id = MergedPointId {
                    external_id,
                    tracker_index,
                    internal_id,
                    version,
                    is_deleted,
                };
                match expected.entry(external_id) {
                    hash_map::Entry::Occupied(mut entry) => {
                        collisions += 1;
                        if entry.get().version < version {
                            entry.insert(merged_point_id);
                        }
                    }
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert(merged_point_id);
                    }
                }
            }
        }

        if tracker_count > 1 {
            // Ensure generated id_trackers have a lot of common points, so we
            // are testing the merge logic.
            assert!(collisions > 500);
        } else {
            // No collisions expected for a single or no id_trackers.
            assert_eq!(collisions, 0);
        }
        if tracker_count == 0 {
            assert!(expected.is_empty());
        }

        // `for_each_unique_point` skips winners whose source has them marked
        // deleted, so drop those from the expected set before comparing.
        expected.retain(|_, v| !v.is_deleted);

        for_each_unique_point(id_trackers.iter(), |merged_point_id| {
            let v = expected.remove(&merged_point_id.external_id).unwrap();
            assert_eq!(merged_point_id.tracker_index, v.tracker_index);
            assert_eq!(merged_point_id.internal_id, v.internal_id);
            assert_eq!(merged_point_id.version, v.version);
            assert!(!merged_point_id.is_deleted);
        });

        assert!(expected.is_empty());
    }
}
