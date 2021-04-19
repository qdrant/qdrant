use serde::{Deserialize, Serialize};
use crate::types::PointOffsetType;
use crate::spaces::tools::FixedLengthPriorityQueue;
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use crate::entry::entry_point::OperationResult;
use crate::common::file_operations::{read_bin, atomic_save_bin};


pub type LinkContainer = Vec<PointOffsetType>;
pub type LayersContainer = Vec<LinkContainer>;

pub const HNSW_GRAPH_FILE: &str = "graph.bin";


#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
struct EntryPoint {
    point_id: PointOffsetType,
    level: usize
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
pub struct GraphLayers {
    max_level: usize,
    min_m: usize,
    min_m0: usize,
    links_layers: Vec<LayersContainer>,
}


impl GraphLayers {
    pub fn new(
        num_points: usize, // Initial number of points in index
        min_m: usize, // Expected M for non-first layer
        min_m0: usize, // Expected M for first layer
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
            links_layers
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
    fn test_graph_layers() {

    }
}
