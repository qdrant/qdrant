use serde::{Deserialize, Serialize};
use crate::types::{PointOffsetType, ScoreType};
use crate::spaces::tools::FixedLengthPriorityQueue;
use std::cmp::{Ordering, max};
use std::path::{Path, PathBuf};
use crate::entry::entry_point::OperationResult;
use crate::common::file_operations::{read_bin, atomic_save_bin};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::hnsw_index::entry_points::{EntryPoints, EntryPoint};
use std::mem;


pub type LinkContainer = Vec<PointOffsetType>;
pub type LayersContainer = Vec<LinkContainer>;

pub const HNSW_GRAPH_FILE: &str = "graph.bin";

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GraphLayers {
    max_level: usize,
    m: usize,
    m0: usize,
    links_layers: Vec<LayersContainer>,
    entry_points: EntryPoints,
}

/// Object contains links between nodes for HNSW search
///
/// Assume all scores are similarities. Larger score = closer points
impl GraphLayers {
    pub fn new(
        num_points: usize, // Initial number of points in index
        m: usize, // Expected M for non-first layer
        m0: usize, // Expected M for first layer
        entry_points_num: usize, // Depends on number of points
    ) -> Self {
        let mut links_layers: Vec<LayersContainer> = vec![];

        for _i in 0..num_points {
            let mut links: LinkContainer = Vec::new();
            links.reserve(m0);
            links_layers.push(vec![links]);
        }

        GraphLayers {
            max_level: 0,
            m,
            m0,
            links_layers,
            entry_points: EntryPoints::new(entry_points_num),
        }
    }

    fn links(&self, point_id: PointOffsetType, level: usize) -> &LinkContainer {
        &self.links_layers[point_id as usize][level]
    }

    fn get_m(&self, level: usize) -> usize {
        return if level == 0 { self.m0 } else { self.m };
    }

    fn set_levels(&mut self, point_id: PointOffsetType, level: usize) {
        let mut point_layers = &mut self.links_layers[point_id as usize];
        while point_layers.len() <= level {
            let mut links = vec![];
            links.reserve(self.m);
            point_layers.push(links)
        }
        self.max_level = max(level, self.max_level);
    }

    /// Connect new point to links, so that links contains only closest points
    fn connect_new_point<F>(&mut self,
                            new_point_id: PointOffsetType,
                            target_point_id: PointOffsetType,
                            level: usize,
                            mut score_internal: F,
    )
        where F: FnMut(PointOffsetType, PointOffsetType) -> ScoreType
    {
        // ToDo: binary search here ? (most likely does not worth it)
        let level_m = self.get_m(level);
        let new_to_target = score_internal(target_point_id, new_point_id);
        let links = &mut self.links_layers[target_point_id as usize][level];

        let mut id_to_insert = links.len();
        for i in 0..links.len() {
            let target_to_link = score_internal(target_point_id, links[i]);
            if target_to_link < new_to_target {
                id_to_insert = i;
                break;
            }
        }

        if links.len() < level_m {
            links.insert(id_to_insert, new_point_id)
        } else {
            if id_to_insert != links.len() {
                links.pop();
                links.insert(id_to_insert, new_point_id)
            }
        }
    }

    /// Try to insert new point into old links.
    /// If there is a place - just push new link
    /// If no place - apply heuristic, insert if:
    ///     - new point is closer than at least some of existing points
    ///     - new point preserve diversity rule: https://github.com/nmslib/hnswlib/issues/99
    ///         - sim(new, target) > sim(new, existing)
    ///
    /// Assume scores = similarities
    fn connect_new_point_with_heuristic<F>(&mut self,
                                           new_point_id: PointOffsetType,
                                           target_point_id: PointOffsetType,
                                           level: usize,
                                           mut score_internal: F,
    )
        where F: FnMut(PointOffsetType, PointOffsetType) -> ScoreType
    {
        let level_m = self.get_m(level);

        let links = &mut self.links_layers[target_point_id as usize][level];
        let new_to_target = score_internal(target_point_id, new_point_id);

        let mut temp_links = LinkContainer::new();
        temp_links.reserve(level_m);

        let mut new_point_inserted = false;

        for link in links.iter().cloned() {
            let target_to_link = score_internal(target_point_id, link);

            if !new_point_inserted {
                if new_to_target > target_to_link {
                    // New point is better than current
                    temp_links.push(new_point_id);
                    new_point_inserted = true;
                }
            }
            let new_to_link = score_internal(new_point_id, link);

            if new_point_inserted {
                // Check that the current point is compatible with inserted
                // It should be closer to target than to a new point
                if target_to_link > new_to_link {
                    temp_links.push(link);
                }
            } else {
                // check that new point is compatible with current
                // If should be closer to target than to link
                if new_to_target < new_to_link {
                    // Not compatible, nothing to be changed, just quit
                    return;
                }
                temp_links.push(link);
            }
        }

        if new_point_inserted {
            mem::replace(links, temp_links);
        } else {
            if links.len() < level_m {
                links.push(new_point_id)
            }
        }
    }

    pub fn link_new_point(&mut self, point_id: PointOffsetType, level: usize, ef: usize, points_scorer: &FilteredScorer) {
        // Check if there is an suitable entry point
        //   - entry point level if higher or equel
        //   - it satisfies filters

        self.set_levels(point_id, level);

        let entry_point_opt = self.entry_points.new_point(point_id, level, points_scorer);
        match entry_point_opt {
            // New point is a new empty entry (for this filter, at least)
            // We can't do much here, so just quit
            None => {}

            // Entry point found.
            Some(entry_point) => {
                let mut entry_id = entry_point.point_id;
                if entry_point.level > level {
                    // The entry point is higher than a new point
                    // Let's find closest one on same level
                    // ToDo: search for closest point on `level`
                } else {
                    // New point is above existing point
                    // Let's start linking from the old point level
                }
            }
        }
    }

    pub fn get_path(path: &Path) -> PathBuf {
        path.join(HNSW_GRAPH_FILE)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        read_bin(path)
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        atomic_save_bin(path, self)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::VectorElementType;
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    #[test]
    fn test_connect_new_point() {
        let num_points = 10;
        let m = 6;

        // See illustration in docs
        let points: Vec<Vec<VectorElementType>> = vec![
            vec![21.79, 7.18],  // Target
            vec![20.58, 5.46],  // 1  B - yes
            vec![21.19, 4.51],  // 2  C
            vec![24.73, 8.24],  // 3  D - yes
            vec![24.55, 9.98],  // 4  E
            vec![26.11, 6.85],  // 5  F
            vec![17.64, 11.14], // 6  G - yes
            vec![14.97, 11.52], // 7  I
            vec![14.97, 9.60],  // 8  J
            vec![16.23, 14.32], // 9  H
            vec![12.69, 19.13], // 10 K
        ];

        let scorer = |a: PointOffsetType, b: PointOffsetType| {
            -(
                (points[a as usize][0] - points[b as usize][0]).powi(2) +
                    (points[a as usize][1] - points[b as usize][1]).powi(2)
            ).sqrt()
        };

        let mut insert_ids = (1..points.len() as PointOffsetType).collect_vec();

        for i in 0..10 {
            let mut graph_layers = GraphLayers::new(num_points, m, m, 1);
            insert_ids.shuffle(&mut thread_rng());
            for id in insert_ids.iter().cloned() {
                graph_layers.connect_new_point_with_heuristic(
                    id,
                    0,
                    0,
                    scorer,
                )
            }
            assert_eq!(graph_layers.links(0, 0), &vec![1, 3, 6]);
        }

        for i in 0..10 {
            let mut graph_layers = GraphLayers::new(num_points, m, m, 1);
            insert_ids.shuffle(&mut thread_rng());
            for id in insert_ids.iter().cloned() {
                graph_layers.connect_new_point(
                    id,
                    0,
                    0,
                    scorer,
                )
            }
            assert_eq!(graph_layers.links(0, 0), &vec![1, 2, 3, 4, 5, 6]);
        }
    }
}
