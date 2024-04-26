use ahash::HashMap;
use sparse::common::types::DimId;

use crate::data_types::tiny_map;

pub struct QueryContext {
    /// Total amount of available points in the segment.
    available_point_count: usize,

    /// Parameter, which defines how big a plain segment can be to be considered
    /// small enough to be searched with `indexed_only` option.
    search_optimized_threshold_kb: usize,

    /// Statistics of the element frequency,
    /// collected over all segments.
    /// Required for processing sparse vector search with `idf-dot` similarity.
    #[allow(dead_code)]
    idf: tiny_map::TinyMap<String, HashMap<DimId, usize>>,
}

impl QueryContext {
    pub fn new(search_optimized_threshold_kb: usize) -> Self {
        Self {
            available_point_count: 0,
            search_optimized_threshold_kb,
            idf: tiny_map::TinyMap::new(),
        }
    }

    pub fn get_available_point_count(&self) -> usize {
        self.available_point_count
    }

    pub fn get_search_optimized_threshold_kb(&self) -> usize {
        self.search_optimized_threshold_kb
    }
}
