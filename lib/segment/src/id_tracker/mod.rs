pub mod compressed;
pub mod id_tracker_base;
pub mod immutable_id_tracker;
pub mod in_memory_id_tracker;
pub mod mutable_id_tracker;
pub mod point_mappings;
pub mod simple_id_tracker;

use common::types::PointOffsetType;
pub use id_tracker_base::*;
use itertools::Itertools as _;

use crate::types::ExtendedPointId;

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
}

/// Calls a closure for each unique point from multiple ID trackers.
///
/// Discard points that have no version.
pub fn for_each_unique_point<'a>(
    id_trackers: impl Iterator<Item = &'a (impl IdTracker + ?Sized + 'a)>,
    mut f: impl FnMut(MergedPointId),
) {
    let mut iter = id_trackers
        .enumerate()
        .map(|(segment_index, id_tracker)| {
            id_tracker
                .iter_from(None)
                .filter_map(move |(external_id, internal_id)| {
                    let version = id_tracker.internal_version(internal_id);
                    // a point without a version had an interrupted flush sequence and should be discarded
                    version.map(|version| MergedPointId {
                        external_id,
                        tracker_index: segment_index,
                        internal_id,
                        version,
                    })
                })
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
            f(best_item);
            best_item = item;
        }
    }
    f(best_item);
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
            for (external_id, internal_id) in id_tracker.iter_from(None) {
                let version = id_tracker.internal_version(internal_id).unwrap();
                let merged_point_id = MergedPointId {
                    external_id,
                    tracker_index,
                    internal_id,
                    version,
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

        for_each_unique_point(id_trackers.iter(), |merged_point_id| {
            let v = expected.remove(&merged_point_id.external_id).unwrap();
            assert_eq!(merged_point_id.tracker_index, v.tracker_index);
            assert_eq!(merged_point_id.internal_id, v.internal_id);
            assert_eq!(merged_point_id.version, v.version);
        });

        assert!(expected.is_empty());
    }
}
