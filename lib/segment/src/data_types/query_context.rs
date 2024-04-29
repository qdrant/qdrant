use std::collections::HashMap;

use sparse::common::types::{DimId, DimWeight};

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

    pub fn available_point_count(&self) -> usize {
        self.available_point_count
    }

    pub fn search_optimized_threshold_kb(&self) -> usize {
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

    pub fn mut_idf(&mut self) -> &mut tiny_map::TinyMap<String, HashMap<DimId, usize>> {
        &mut self.idf
    }

    pub fn get_vector_context(&self, vector_name: &str) -> VectorQueryContext {
        VectorQueryContext {
            available_point_count: self.available_point_count,
            search_optimized_threshold_kb: self.search_optimized_threshold_kb,
            idf: self.idf.get(vector_name),
        }
    }
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new(usize::MAX) // Search optimized threshold won't affect the search.
    }
}

/// Query context related to a specific vector
pub struct VectorQueryContext<'a> {
    /// Total amount of available points in the segment.
    available_point_count: usize,

    /// Parameter, which defines how big a plain segment can be to be considered
    /// small enough to be searched with `indexed_only` option.
    search_optimized_threshold_kb: usize,

    idf: Option<&'a HashMap<DimId, usize>>,
}

impl VectorQueryContext<'_> {
    pub fn available_point_count(&self) -> usize {
        self.available_point_count
    }

    pub fn search_optimized_threshold_kb(&self) -> usize {
        self.search_optimized_threshold_kb
    }

    /// Compute advanced formula for Inverse Document Frequency (IDF) according to wikipedia.
    /// This should account for corner cases when `df` and `n` are small or zero.
    #[inline]
    fn fancy_idf(n: DimWeight, df: DimWeight) -> DimWeight {
        ((n - df + 0.5) / (df + 0.5) + 1.).ln()
    }

    pub fn remap_idf_weights(&self, indices: &[DimId], weights: &mut [DimWeight]) {
        // Number of documents
        let n = self.available_point_count as DimWeight;
        for (weight, index) in weights.iter_mut().zip(indices) {
            // Document frequency
            let df = self
                .idf
                .and_then(|idf| idf.get(index))
                .copied()
                .unwrap_or(0);

            *weight *= Self::fancy_idf(n, df as DimWeight);
        }
    }

    pub fn is_require_idf(&self) -> bool {
        self.idf.is_some()
    }
}

impl Default for VectorQueryContext<'_> {
    fn default() -> Self {
        VectorQueryContext {
            available_point_count: 0,
            search_optimized_threshold_kb: usize::MAX,
            idf: None,
        }
    }
}
