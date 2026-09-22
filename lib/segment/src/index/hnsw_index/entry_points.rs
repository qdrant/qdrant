use std::cmp::Ordering;
use std::collections::HashSet;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::PointOffsetType;
use rand::{Rng, RngExt};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq)]
pub struct EntryPoint {
    pub point_id: PointOffsetType,
    pub level: usize,
}

impl Eq for EntryPoint {}

impl PartialOrd for EntryPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EntryPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.level.cmp(&other.level)
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct EntryPoints {
    entry_points: Vec<EntryPoint>,
    extra_entry_points: FixedLengthPriorityQueue<EntryPoint>,
}

impl EntryPoints {
    pub fn new(extra_entry_points: usize) -> Self {
        EntryPoints {
            entry_points: vec![],
            extra_entry_points: FixedLengthPriorityQueue::new(extra_entry_points),
        }
    }
    /// Merge another graph's entry points into this one.
    ///
    /// The other graph's extra entry points are kept as well, deduplicated against
    /// `known` (point ids already offered, owned by the caller for the whole build):
    /// they are the only way to reach a payload block whose primary entry point does
    /// not satisfy the rest of a filter (`get_entry_point` falls back to them). The
    /// queue grows geometrically when full, so no sample is ever dropped, whatever
    /// the number of blocks or how many blocks a point belongs to.
    pub fn merge_from_other(
        &mut self,
        mut other: EntryPoints,
        known: &mut HashSet<PointOffsetType>,
    ) {
        self.entry_points.append(&mut other.entry_points);
        for entry in other.extra_entry_points.into_iter_sorted() {
            if known.insert(entry.point_id) {
                if self.extra_entry_points.is_full() {
                    self.grow_extra_entry_points();
                }
                self.extra_entry_points.push(entry);
            }
        }
    }

    /// Ids of every entry point currently held, to seed `known` for `merge_from_other`.
    pub fn point_ids(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.entry_points
            .iter()
            .chain(self.extra_entry_points.iter_unsorted())
            .map(|entry| entry.point_id)
    }

    fn grow_extra_entry_points(&mut self) {
        let capacity = self.extra_entry_points.capacity().saturating_mul(2).max(1);
        let old = std::mem::replace(
            &mut self.extra_entry_points,
            FixedLengthPriorityQueue::new(capacity),
        );
        for entry in old.into_iter_sorted() {
            self.extra_entry_points.push(entry);
        }
    }

    /// Replace the extra entry points with the given ones, with the given capacity.
    ///
    /// Payload-block graphs are single-level, so the extra entry points collected while
    /// linking are merely the first points of the block. Callers use this to install an
    /// evenly spaced sample instead, so a filter that combines the block condition with
    /// another condition finds an entry point whatever the insertion order.
    pub fn set_extra_entry_points(
        &mut self,
        capacity: usize,
        points: impl IntoIterator<Item = EntryPoint>,
    ) {
        let capacity = capacity.max(1);
        let mut queue = FixedLengthPriorityQueue::new(capacity);
        for point in points.into_iter().take(capacity) {
            queue.push(point);
        }
        self.extra_entry_points = queue;
    }

    pub fn new_point<F>(
        &mut self,
        new_point: PointOffsetType,
        level: usize,
        checker: F,
    ) -> Option<EntryPoint>
    where
        F: Fn(PointOffsetType) -> bool,
    {
        // there are 3 cases:
        // - There is proper entry point for a new point higher or same level - return the point
        // - The new point is higher than any alternative - return the next best thing
        // - There is no point and alternatives - return None

        for i in 0..self.entry_points.len() {
            let candidate = &self.entry_points[i];

            if !checker(candidate.point_id) {
                continue; // Checkpoint does not fulfil filtering conditions. Hence, does not "exists"
            }
            // Found checkpoint candidate
            return if candidate.level >= level {
                // The good checkpoint exists.
                // Return it, and also try to save given if required
                self.extra_entry_points.push(EntryPoint {
                    point_id: new_point,
                    level,
                });
                Some(*candidate)
            } else {
                // The current point is better than existing
                let entry = self.entry_points[i];
                self.entry_points[i] = EntryPoint {
                    point_id: new_point,
                    level,
                };
                self.extra_entry_points.push(entry);
                Some(entry)
            };
        }
        // No entry points found. Create a new one and return self
        let new_entry = EntryPoint {
            point_id: new_point,
            level,
        };
        self.entry_points.push(new_entry);
        None
    }

    /// Find the highest `EntryPoint` which satisfies filtering condition of `checker`
    pub fn get_entry_point<F>(&self, checker: F) -> Option<EntryPoint>
    where
        F: Fn(PointOffsetType) -> bool,
    {
        self.entry_points
            .iter()
            .find(|entry| checker(entry.point_id))
            .cloned()
            .or_else(|| {
                // Searching for at least some entry point
                self.extra_entry_points
                    .iter_unsorted()
                    .filter(|entry| checker(entry.point_id))
                    .cloned()
                    .max_by_key(|ep| ep.level)
            })
    }

    pub fn get_random_entry_point<F, R: Rng + ?Sized>(
        &self,
        rnd: &mut R,
        checker: F,
    ) -> Option<EntryPoint>
    where
        F: Fn(PointOffsetType) -> bool,
    {
        let filtered_entry_points: Vec<_> = self
            .entry_points
            .iter()
            .filter(|entry| checker(entry.point_id))
            .cloned()
            .collect();

        if !filtered_entry_points.is_empty() {
            let random_index = rnd.random_range(0..filtered_entry_points.len());
            return Some(filtered_entry_points[random_index]);
        }

        let filtered_extra_entry_points: Vec<_> = self
            .extra_entry_points
            .iter_unsorted()
            .filter(|entry| checker(entry.point_id))
            .cloned()
            .collect();

        if !filtered_extra_entry_points.is_empty() {
            let random_index = rnd.random_range(0..filtered_extra_entry_points.len());
            return Some(filtered_extra_entry_points[random_index]);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_extra_entry_points_keeps_capacity() {
        let mut points = EntryPoints::new(3);
        for i in 0..10 {
            points.new_point(i, 0, |_| true);
        }
        assert_eq!(points.extra_entry_points.len(), 3);
        points.set_extra_entry_points(
            3,
            (100..110).map(|point_id| EntryPoint { point_id, level: 0 }),
        );
        assert_eq!(points.extra_entry_points.len(), 3);
        assert!(points.get_entry_point(|p| p >= 100).is_some());
        assert!(points.get_entry_point(|p| (1..100).contains(&p)).is_none());
    }

    #[test]
    fn test_merge_keeps_block_samples_next_to_main_entries() {
        // Main graph queue already full of higher-level entries.
        let mut main = EntryPoints::new(3);
        for i in 0..4 {
            main.new_point(i, 3, |_| true);
        }
        assert!(main.extra_entry_points.is_full());
        let mut known: HashSet<PointOffsetType> = main.point_ids().collect();
        let mut block = EntryPoints::new(4);
        block.set_extra_entry_points(
            4,
            (100..104).map(|point_id| EntryPoint { point_id, level: 0 }),
        );
        main.merge_from_other(block.clone(), &mut known);
        // The queue grew instead of rejecting the level-0 samples.
        assert_eq!(main.extra_entry_points.len(), 7);
        // A filter only satisfied by block points still finds an entry point.
        assert!(main.get_entry_point(|p| p >= 100).is_some());
        // Merging the same block again adds nothing.
        main.merge_from_other(block, &mut known);
        assert_eq!(main.extra_entry_points.len(), 7);
    }

    #[test]
    fn test_merge_grows_for_many_blocks() {
        let mut main = EntryPoints::new(1);
        let mut known = HashSet::new();
        for b in 0..5u32 {
            let mut block = EntryPoints::new(3);
            block.set_extra_entry_points(
                3,
                (b * 10..b * 10 + 3).map(|point_id| EntryPoint { point_id, level: 0 }),
            );
            main.merge_from_other(block, &mut known);
        }
        assert_eq!(main.extra_entry_points.len(), 15);
        assert!(main.get_entry_point(|p| p == 42).is_some());
    }

    #[test]
    fn test_merge_keeps_extra_entry_points() {
        // Main graph with room for 8 extra entry points, block graph with 4.
        let mut main = EntryPoints::new(8);
        let mut block = EntryPoints::new(4);
        for i in 0..100 {
            block.new_point(i, (i % 7) as usize, |_| true);
        }
        assert_eq!(block.entry_points.len(), 1);
        assert_eq!(block.extra_entry_points.len(), 4);

        main.merge_from_other(block, &mut HashSet::new());

        assert_eq!(main.entry_points.len(), 1);
        assert_eq!(main.extra_entry_points.len(), 4);

        // A filter rejecting the block's primary entry point still finds an entry point.
        let primary = main.entry_points[0].point_id;
        assert!(main.get_entry_point(|p| p != primary).is_some());
        assert!(main.get_entry_point(|p| p == primary).is_some());
    }

    #[test]
    fn test_entry_points() {
        let mut points = EntryPoints::new(10);

        let mut rnd = rand::rng();

        for i in 0..1000 {
            let level = rnd.random_range(0..10000);
            points.new_point(i, level, |_x| true);
        }

        assert_eq!(points.entry_points.len(), 1);
        assert_eq!(points.extra_entry_points.len(), 10);

        assert!(points.entry_points[0].level > 1);

        for i in 1000..2000 {
            let level = rnd.random_range(0..10000);
            points.new_point(i, level, |x| x % 5 == i % 5);
        }

        assert_eq!(points.entry_points.len(), 5);
        assert_eq!(points.extra_entry_points.len(), 10);
    }
}
