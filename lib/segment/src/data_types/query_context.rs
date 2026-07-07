use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::cow::SimpleCow;
use common::types::ScoreType;
use sparse::common::types::{DimId, DimWeight};

use crate::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use crate::types::{Filter, IdfSearchScope, ScoredPoint, SearchParams, VectorName, VectorNameBuf};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IdfStatsKey {
    Global {
        vector_name: VectorNameBuf,
    },
    PerFilter {
        vector_name: VectorNameBuf,
        filter: Filter,
    },
}

impl IdfStatsKey {
    pub fn global(vector_name: &VectorName) -> Self {
        Self::Global {
            vector_name: vector_name.to_owned(),
        }
    }

    pub fn per_filter(vector_name: &VectorName, filter: Filter) -> Self {
        Self::PerFilter {
            vector_name: vector_name.to_owned(),
            filter,
        }
    }

    pub fn for_search(
        vector_name: &VectorName,
        filter: Option<&Filter>,
        params: Option<&SearchParams>,
    ) -> Self {
        match params.and_then(|params| params.idf).unwrap_or_default() {
            IdfSearchScope::Global => Self::global(vector_name),
            IdfSearchScope::PerFilter => match filter {
                Some(filter) => Self::per_filter(vector_name, filter.clone()),
                None => Self::global(vector_name),
            },
        }
    }

    pub fn vector_name(&self) -> &VectorName {
        match self {
            Self::Global { vector_name } => vector_name,
            Self::PerFilter {
                vector_name,
                filter: _,
            } => vector_name,
        }
    }

    pub fn filter(&self) -> Option<&Filter> {
        match self {
            Self::Global { vector_name: _ } => None,
            Self::PerFilter {
                vector_name: _,
                filter,
            } => Some(filter),
        }
    }
}

#[derive(Debug, Default)]
pub struct IdfStats {
    /// Element frequency for sparse-vector query dimensions.
    pub idf: HashMap<DimId, usize>,

    /// Number of vectors in the IDF scope.
    pub indexed_vectors: usize,
}

#[derive(Debug, Default)]
pub struct QueryIdfStats {
    /// Statistics of the element frequency, collected over all segments.
    ///
    /// Required for processing sparse vector search with `idf-dot` similarity.
    /// Global IDF scopes are keyed by vector name. Per-filter IDF scopes are
    /// additionally keyed by the request filter.
    pub stats: HashMap<IdfStatsKey, IdfStats>,
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
    pub fn init_idf(&mut self, key: IdfStatsKey, indices: &[DimId]) {
        let stats = self.idf_stats.stats.entry(key).or_default();

        for index in indices {
            stats.idf.insert(*index, 0);
        }
    }

    pub fn mut_idf_stats(&mut self) -> &mut QueryIdfStats {
        &mut self.idf_stats
    }

    pub fn is_stopped_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.is_stopped)
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

    pub fn get_vector_context(&self, vector_name: &VectorName) -> VectorQueryContext<'_> {
        self.get_vector_context_for_key(&IdfStatsKey::global(vector_name))
    }

    pub fn get_vector_context_for_key(&self, key: &IdfStatsKey) -> VectorQueryContext<'_> {
        let stats = self.query_context.idf_stats.stats.get(key);
        VectorQueryContext {
            search_optimized_threshold_kb: self.query_context.search_optimized_threshold_kb,
            is_stopped: Some(&self.query_context.is_stopped),
            idf: stats.map(|stats| &stats.idf),
            indexed_vectors: stats.map(|stats| stats.indexed_vectors),
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

    /// Compute advanced formula for Inverse Document Frequency (IDF) according to wikipedia.
    /// This should account for corner cases when `df` and `n` are small or zero.
    #[inline]
    fn fancy_idf(n: DimWeight, df: DimWeight) -> DimWeight {
        ((n - df + 0.5) / (df + 0.5) + 1.).ln()
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

            *weight *= Self::fancy_idf(n, df as DimWeight);
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
