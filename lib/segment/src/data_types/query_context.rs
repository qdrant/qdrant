use std::collections::HashMap;

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

    pub fn add_available_point_count(&mut self, count: usize) {
        self.available_point_count += count;
    }

    /// Fill indices of sparse vectors, which are required for `idf-dot` similarity
    /// with zeros, so the statistics can be collected.
    pub fn init_idf(&mut self, vector_name: &str, indices: &[DimId]) {
        // ToDo: Would be nice to have an implementation of `entry` for `TinyMap`.
        let idf = if let Some(idf) = self.idf.get_mut(vector_name) {
            idf
        } else {
            self.idf.insert(vector_name.to_string(), HashMap::default());
            self.idf.get_mut(vector_name).unwrap()
        };

        for index in indices {
            idf.insert(*index, 0);
        }
    }

    pub fn get_mut_idf(&mut self) -> &mut tiny_map::TinyMap<String, HashMap<DimId, usize>> {
        &mut self.idf
    }
}
