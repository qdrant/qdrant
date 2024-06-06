use std::cmp::Ordering;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::PointOffsetType;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
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

    pub fn is_empty(&self) -> bool {
        self.entry_points.is_empty()
    }

    pub fn merge_from_other(&mut self, mut other: EntryPoints) {
        self.entry_points.append(&mut other.entry_points);
        // Do not merge `extra_entry_points` to prevent duplications
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
                Some(candidate.clone())
            } else {
                // The current point is better than existing
                let entry = self.entry_points[i].clone();
                self.entry_points[i] = EntryPoint {
                    point_id: new_point,
                    level,
                };
                self.extra_entry_points.push(entry.clone());
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
                    .iter()
                    .filter(|entry| checker(entry.point_id))
                    .cloned()
                    .max_by_key(|ep| ep.level)
            })
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn test_entry_points() {
        let mut points = EntryPoints::new(10);

        let mut rnd = rand::thread_rng();

        for i in 0..1000 {
            let level = rnd.gen_range(0..10000);
            points.new_point(i, level, |_x| true);
        }

        assert_eq!(points.entry_points.len(), 1);
        assert_eq!(points.extra_entry_points.len(), 10);

        assert!(points.entry_points[0].level > 1);

        for i in 1000..2000 {
            let level = rnd.gen_range(0..10000);
            points.new_point(i, level, |x| x % 5 == i % 5);
        }

        assert_eq!(points.entry_points.len(), 5);
        assert_eq!(points.extra_entry_points.len(), 10);
    }
}
