use serde::{Deserialize, Serialize};
use crate::types::PointOffsetType;
use std::cmp::Ordering;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::spaces::tools::FixedLengthPriorityQueue;

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
    extra_entry_points: FixedLengthPriorityQueue<EntryPoint>
}

impl EntryPoints {
    pub fn new(extra_entry_points: usize) -> Self {
        EntryPoints {
            entry_points: vec![],
            extra_entry_points: FixedLengthPriorityQueue::new(extra_entry_points)
        }
    }

    pub fn new_point(&mut self, new_point: PointOffsetType, level: usize, points_scorer: &FilteredScorer) -> EntryPoint {
        // there are 3 cases:
        // - There is proper entry point for a new point higher or same level - return the point
        // - The new point is higher than any alternative - return the next best thing
        // - There is no point and alternatives - return self

        for i in 0..self.entry_points.len() {
            let candidate = &self.entry_points[i];
            if points_scorer.check_point(candidate.point_id) {
                // Found checkpoint candidate
                return if candidate.level >= level {
                    // The good checkpoint exists.
                    // Return it, and also try to save given if required
                    self.extra_entry_points.push(EntryPoint {
                        point_id: new_point,
                        level
                    });
                    candidate.clone()
                } else {
                    // The current point is better than existing
                    let entry = self.entry_points[i].clone();
                    self.entry_points[i] = EntryPoint {
                        point_id: new_point,
                        level
                    };
                    self.extra_entry_points.push(entry.clone());
                    entry
                }
            }
        }
        // No entry points found. Create a new one and return self
        let new_entry = EntryPoint {
            point_id: new_point,
            level
        };
        self.entry_points.push(new_entry.clone());
        new_entry
    }

    pub fn get_entry_point(&self, points_scorer: &FilteredScorer) -> Option<EntryPoint> {
        self.entry_points.iter()
            .filter(|entry| points_scorer.check_point(entry.point_id))
            .cloned().next().or_else(|| {
            // Searching for at least some entry point
            self.extra_entry_points
                .iter()
                .filter(|entry| points_scorer.check_point(entry.point_id))
                .cloned()
                .next()
        })
    }
}