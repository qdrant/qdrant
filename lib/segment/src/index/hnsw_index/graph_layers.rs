use serde::{Deserialize, Serialize};
use crate::types::PointOffsetType;
use crate::spaces::tools::FixedLengthPriorityQueue;
use std::cmp::{Ordering, max};
use std::path::{Path, PathBuf};
use crate::entry::entry_point::OperationResult;
use crate::common::file_operations::{read_bin, atomic_save_bin};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::hnsw_index::entry_points::EntryPoints;


pub type LinkContainer = Vec<PointOffsetType>;
pub type LayersContainer = Vec<LinkContainer>;

pub const HNSW_GRAPH_FILE: &str = "graph.bin";

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GraphLayers {
    max_level: usize,
    min_m: usize,
    min_m0: usize,
    links_layers: Vec<LayersContainer>,
    entry_points: EntryPoints,
}


impl GraphLayers {
    pub fn new(
        num_points: usize, // Initial number of points in index
        min_m: usize, // Expected M for non-first layer
        min_m0: usize, // Expected M for first layer
        entry_points_num: usize, // Depends on number of points
    ) -> Self {
        let mut links_layers: Vec<LayersContainer> = vec![];

        for _i in 0..num_points {
            let mut links: LinkContainer = Vec::new();
            links.reserve(min_m0);
            links_layers.push(vec![links]);
        }

        GraphLayers {
            max_level: 0,
            min_m,
            min_m0,
            links_layers,
            entry_points: EntryPoints::new(entry_points_num)
        }
    }

    fn set_levels(&mut self, point_id: PointOffsetType, level: usize) {
        let mut point_layers = &mut self.links_layers[point_id as usize];
        while point_layers.len() <= level {
            let mut links = vec![];
            links.reserve(self.min_m);
            point_layers.push(links)
        }
        self.max_level = max(level, self.max_level);
    }

    pub fn link_new_point(&mut self, point_id: PointOffsetType, level: usize, ef: usize,  points_scorer: &FilteredScorer) {
        // Check if there is an suitable entry point
        //   - entry point level if higher or equel
        //   - it satisfies filters

        let entry_point = self.entry_points.new_point(point_id, level, points_scorer);

        if entry_point.point_id == point_id {
            // The point is a new empty entry (for this filter, at least)
        } else {
            if entry_point.level < level {
                // The point is above existing point
            } else {
                // The entry point is regular
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

    #[test]
    fn test_graph_layers() {}
}
