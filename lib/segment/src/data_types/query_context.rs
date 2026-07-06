use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::cow::SimpleCow;
use common::types::ScoreType;
use sparse::common::types::{DimId, DimWeight};

use crate::data_types::tiny_map;
use crate::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use crate::types::{Filter, ScoredPoint, VectorName, VectorNameBuf};

#[derive(Debug, Default)]
pub struct QueryIdfStats {
    /// IDF statistics per corpus scope.
    ///
    /// Each batch request contributes its query terms to the scope matching
    /// its IDF corpus, so requests with different corpora get independently
    /// computed statistics. Typically holds a single (global) entry.
    pub scopes: Vec<IdfScopeStats>,
}

impl QueryIdfStats {
    pub fn scope(&self, corpus: Option<&Filter>) -> Option<&IdfScopeStats> {
        self.scopes
            .iter()
            .find(|scope| scope.corpus.as_ref() == corpus)
    }

    /// Remove and return the statistics scope for the given corpus.
    pub fn take_scope(&mut self, corpus: Option<&Filter>) -> Option<IdfScopeStats> {
        let index = self
            .scopes
            .iter()
            .position(|scope| scope.corpus.as_ref() == corpus)?;
        Some(self.scopes.swap_remove(index))
    }
}

/// Statistics of the element frequency, collected over all segments,
/// scoped to a single IDF corpus.
/// Required for processing sparse vector search with `idf-dot` similarity.
#[derive(Debug)]
pub struct IdfScopeStats {
    /// Filter defining the population the statistics are computed over.
    /// `None` — the whole collection (global statistics).
    pub corpus: Option<Filter>,

    /// Document frequency per dimension, per vector name.
    pub idf: tiny_map::TinyMap<VectorNameBuf, HashMap<DimId, usize>>,

    /// Number of documents (indexed vectors within the corpus) per vector name.
    pub indexed_vectors: tiny_map::TinyMap<VectorNameBuf, usize>,
}

#[derive(Debug)]
pub struct QueryContext {
    /// Total amount of available (and visible) points in the segment.
    available_point_count: usize,

    /// Parameter, which defines how big a plain segment can be to be considered
    /// small enough to be searched with `indexed_only` option.
    search_optimized_threshold_kb: usize,

    /// Defines if the search process was stopped.
    /// Is changed externally if API times out or cancelled.
    is_stopped: Arc<AtomicBool>,

    /// Statistics of the element frequency,
    /// collected over all segments.
    /// Required for processing sparse vector search with `idf-dot` similarity.
    idf_stats: QueryIdfStats,

    /// Structure to accumulate and report hardware usage.
    /// Holds reference to the shared drain, which is used to accumulate the values.
    hardware_usage_accumulator: HwMeasurementAcc,
}

impl QueryContext {
    pub fn new(
        search_optimized_threshold_kb: usize,
        hardware_usage_accumulator: HwMeasurementAcc,
    ) -> Self {
        Self {
            available_point_count: 0,
            search_optimized_threshold_kb,
            is_stopped: Arc::new(AtomicBool::new(false)),
            idf_stats: QueryIdfStats::default(),
            hardware_usage_accumulator,
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.is_stopped.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn with_is_stopped(mut self, flag: Arc<AtomicBool>) -> Self {
        self.is_stopped = flag;
        self
    }

    /// Shared stop flag handle, e.g. to check for cancellation while
    /// collecting IDF statistics.
    pub fn is_stopped_handle(&self) -> Arc<AtomicBool> {
        self.is_stopped.clone()
    }

    /// Returns the amount of available (and visible) points.
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
    ///
    /// `corpus` defines the population the statistics are computed over,
    /// `None` for the whole collection. Requests sharing a corpus share
    /// a statistics scope.
    pub fn init_idf(
        &mut self,
        vector_name: &VectorName,
        corpus: Option<&Filter>,
        indices: &[DimId],
    ) {
        let scope_index = self
            .idf_stats
            .scopes
            .iter()
            .position(|scope| scope.corpus.as_ref() == corpus)
            .unwrap_or_else(|| {
                self.idf_stats.scopes.push(IdfScopeStats {
                    corpus: corpus.cloned(),
                    idf: tiny_map::TinyMap::new(),
                    indexed_vectors: tiny_map::TinyMap::new(),
                });
                self.idf_stats.scopes.len() - 1
            });
        let scope = &mut self.idf_stats.scopes[scope_index];

        if scope.indexed_vectors.get(vector_name).is_none() {
            scope.indexed_vectors.insert(vector_name.to_owned(), 0);
        }

        // ToDo: Would be nice to have an implementation of `entry` for `TinyMap`.
        let idf = if let Some(idf) = scope.idf.get_mut(vector_name) {
            idf
        } else {
            scope.idf.insert(vector_name.to_owned(), HashMap::default());
            scope.idf.get_mut(vector_name).unwrap()
        };

        for index in indices {
            idf.insert(*index, 0);
        }
    }

    pub fn idf_stats(&self) -> &QueryIdfStats {
        &self.idf_stats
    }

    pub fn mut_idf_stats(&mut self) -> &mut QueryIdfStats {
        &mut self.idf_stats
    }

    pub fn get_segment_query_context(&self) -> SegmentQueryContext<'_> {
        SegmentQueryContext {
            query_context: self,
            deleted_points: None,
            hardware_counter: self.hardware_usage_accumulator.get_counter_cell(),
        }
    }

    pub fn hardware_usage_accumulator(&self) -> &HwMeasurementAcc {
        &self.hardware_usage_accumulator
    }
}

#[cfg(feature = "testing")]
impl Default for QueryContext {
    fn default() -> Self {
        Self::new(usize::MAX, HwMeasurementAcc::new()) // Search optimized threshold won't affect the search.
    }
}

/// Defines context of the search query on the segment level
#[derive(Debug)]
pub struct SegmentQueryContext<'a> {
    query_context: &'a QueryContext,
    deleted_points: Option<&'a BitSlice>,
    hardware_counter: HardwareCounterCell,
}

impl<'a> SegmentQueryContext<'a> {
    pub fn available_point_count(&self) -> usize {
        self.query_context.available_point_count()
    }

    /// Vector-level context for the given vector name and IDF corpus
    /// (`None` corpus — global statistics).
    pub fn get_vector_context(
        &self,
        vector_name: &VectorName,
        idf_corpus: Option<&Filter>,
    ) -> VectorQueryContext<'_> {
        let idf_scope = self.query_context.idf_stats.scope(idf_corpus);
        VectorQueryContext {
            search_optimized_threshold_kb: self.query_context.search_optimized_threshold_kb,
            is_stopped: Some(&self.query_context.is_stopped),
            idf: idf_scope.and_then(|scope| scope.idf.get(vector_name)),
            indexed_vectors: idf_scope
                .and_then(|scope| scope.indexed_vectors.get(vector_name))
                .copied(),
            deleted_points: self.deleted_points,
            hardware_counter: self.hardware_counter.fork(),
        }
    }

    pub fn with_deleted_points(mut self, deleted_points: &'a BitSlice) -> Self {
        self.deleted_points = Some(deleted_points);
        self
    }

    pub fn is_stopped(&self) -> bool {
        self.query_context.is_stopped()
    }

    pub fn fork(&self) -> Self {
        Self {
            query_context: self.query_context,
            deleted_points: self.deleted_points,
            hardware_counter: self.hardware_counter.fork(),
        }
    }
}

/// Query context related to a specific vector
#[derive(Debug)]
pub struct VectorQueryContext<'a> {
    /// Parameter, which defines how big a plain segment can be to be considered
    /// small enough to be searched with `indexed_only` option.
    search_optimized_threshold_kb: usize,

    is_stopped: Option<&'a AtomicBool>,

    idf: Option<&'a HashMap<DimId, usize>>,

    indexed_vectors: Option<usize>,

    deleted_points: Option<&'a BitSlice>,

    hardware_counter: HardwareCounterCell,
}

/// Compute advanced formula for Inverse Document Frequency (IDF) according to wikipedia.
/// This should account for corner cases when `df` and `n` are small or zero.
#[inline]
pub fn fancy_idf(n: DimWeight, df: DimWeight) -> DimWeight {
    ((n - df + 0.5) / (df + 0.5) + 1.).ln()
}

impl VectorQueryContext<'_> {
    pub fn hardware_counter(&self) -> HardwareCounterCell {
        self.hardware_counter.fork()
    }

    pub fn search_optimized_threshold_kb(&self) -> usize {
        self.search_optimized_threshold_kb
    }

    pub fn deleted_points(&self) -> Option<&BitSlice> {
        self.deleted_points
    }

    pub fn is_stopped(&self) -> SimpleCow<'_, AtomicBool> {
        self.is_stopped
            .map(SimpleCow::Borrowed)
            .unwrap_or_else(|| SimpleCow::Owned(AtomicBool::new(false)))
    }

    pub fn remap_idf_weights(&self, indices: &[DimId], weights: &mut [DimWeight]) {
        // Number of documents
        let Some(indexed_vectors) = self.indexed_vectors else {
            return;
        };

        let n = indexed_vectors as DimWeight;
        for (weight, index) in weights.iter_mut().zip(indices) {
            // Document frequency
            let df = self
                .idf
                .and_then(|idf| idf.get(index))
                .copied()
                .unwrap_or(0);

            *weight *= fancy_idf(n, df as DimWeight);
        }
    }

    pub fn is_require_idf(&self) -> bool {
        self.idf.is_some() && self.indexed_vectors.is_some()
    }
}

#[cfg(feature = "testing")]
impl Default for VectorQueryContext<'_> {
    fn default() -> Self {
        VectorQueryContext {
            search_optimized_threshold_kb: usize::MAX,
            is_stopped: None,
            idf: None,
            indexed_vectors: None,
            deleted_points: None,
            hardware_counter: HardwareCounterCell::new(),
        }
    }
}

pub struct FormulaContext {
    pub formula: ParsedFormula,
    pub prefetches_results: Vec<Vec<ScoredPoint>>,
    pub limit: usize,
    pub score_threshold: Option<ScoreType>,
    pub is_stopped: Arc<AtomicBool>,
}
