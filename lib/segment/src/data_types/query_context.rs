use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::cow::SimpleCow;
use common::types::ScoreType;
use sparse::common::types::{DimId, DimWeight};

use crate::data_types::tiny_map;
use crate::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use crate::types::{Filter, PayloadKeyType, ScoredPoint, VectorName, VectorNameBuf};

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
}

/// Statistics of the element frequency, collected over all segments,
/// scoped to a single IDF corpus.
/// Required for processing sparse vector search with `idf-dot` similarity.
#[derive(Debug)]
pub struct IdfScopeStats {
    /// Filter defining the population the statistics are computed over.
    /// `None` — the whole collection (global statistics).
    pub corpus: Option<Filter>,

    /// Document frequency per dimension, per vector name. Named for what it
    /// holds: IDF is derived from this and `indexed_vectors` by [`fancy_idf`].
    pub df: tiny_map::TinyMap<VectorNameBuf, HashMap<DimId, usize>>,

    /// Number of documents (indexed vectors within the corpus) per vector name.
    pub indexed_vectors: tiny_map::TinyMap<VectorNameBuf, usize>,
}

/// Corpus statistics for one text field, summed over every segment of one
/// local shard.
///
/// Sibling of [`QueryIdfStats`] rather than part of it. The two share the
/// carrier and the gather lifecycle, and nothing else: this one is keyed by
/// payload field and by term string, because a `TokenId` is local to the
/// segment that assigned it, and it is applied inside the BM25 term sum
/// instead of by scaling a query vector's weights.
#[derive(Debug)]
pub struct TextFieldStats {
    /// Document frequency per query term, seeded with the terms the query
    /// needs so each segment knows which ones to resolve and report.
    pub df: HashMap<String, usize>,

    /// Documents carrying this field: `N` in the IDF formula.
    pub documents: usize,

    /// Total tokens over those documents, the numerator of `avgdl`.
    ///
    /// `None` as soon as one contributing segment does not record document
    /// lengths: an average over part of the corpus is not the average, and
    /// silently scoring against one would make a document's rank depend on
    /// which segments happened to be built with scoring on.
    pub total_tokens: Option<u64>,
}

impl Default for TextFieldStats {
    /// Nothing seeded and nothing counted, with a total that is still a total:
    /// `None` is reserved for "some segment could not contribute one".
    fn default() -> Self {
        Self {
            df: HashMap::new(),
            documents: 0,
            total_tokens: Some(0),
        }
    }
}

impl TextFieldStats {
    /// Seeded with the query's terms, and nothing counted yet.
    pub fn seeded(terms: impl IntoIterator<Item = String>) -> Self {
        Self {
            df: terms.into_iter().map(|term| (term, 0)).collect(),
            ..Default::default()
        }
    }

    /// Fold in one segment's contribution. `total_tokens` of `None` means that
    /// segment records no lengths, which poisons the average for the whole
    /// corpus.
    pub fn add_segment(&mut self, documents: usize, total_tokens: Option<u64>) {
        self.documents += documents;
        self.total_tokens = match (self.total_tokens, total_tokens) {
            (Some(total), Some(segment_total)) => Some(total + segment_total),
            _ => None,
        };
    }
}

/// Advanced formula for Inverse Document Frequency (IDF) according to wikipedia.
/// This should account for corner cases when `df` and `n` are small or zero.
#[inline]
pub fn fancy_idf(n: DimWeight, df: DimWeight) -> DimWeight {
    ((n - df + 0.5) / (df + 0.5) + 1.).ln()
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

    /// Corpus statistics per text field, collected over all segments.
    /// Required for scoring a text query against a payload index.
    text_stats: AHashMap<PayloadKeyType, TextFieldStats>,

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
            text_stats: AHashMap::new(),
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
                    df: tiny_map::TinyMap::new(),
                    indexed_vectors: tiny_map::TinyMap::new(),
                });
                self.idf_stats.scopes.len() - 1
            });
        let scope = &mut self.idf_stats.scopes[scope_index];

        if scope.indexed_vectors.get(vector_name).is_none() {
            scope.indexed_vectors.insert(vector_name.to_owned(), 0);
        }

        // ToDo: Would be nice to have an implementation of `entry` for `TinyMap`.
        let df = if let Some(df) = scope.df.get_mut(vector_name) {
            df
        } else {
            scope.df.insert(vector_name.to_owned(), HashMap::default());
            scope.df.get_mut(vector_name).unwrap()
        };

        for index in indices {
            df.insert(*index, 0);
        }
    }

    /// Seed the terms a scored text query needs on `field`, so that every
    /// segment of this shard reports their document frequencies.
    ///
    /// **Terms must already be tokenized the way the index tokenizes**, since
    /// resolution is a bare vocabulary lookup. An untokenized term misses
    /// everywhere and keeps the `df` of zero seeded here, which is the largest
    /// IDF the formula produces.
    ///
    /// No corpus filter. A corpus-scoped text statistic has to intersect the
    /// posting lists with the corpus, which sparse does through its own index,
    /// and nothing can ask for one yet.
    pub fn init_text_stats(
        &mut self,
        field: &PayloadKeyType,
        terms: impl IntoIterator<Item = String>,
    ) {
        let stats = self.text_stats.entry(field.clone()).or_default();
        for term in terms {
            stats.df.entry(term).or_insert(0);
        }
    }

    pub fn idf_stats(&self) -> &QueryIdfStats {
        &self.idf_stats
    }

    pub fn mut_idf_stats(&mut self) -> &mut QueryIdfStats {
        &mut self.idf_stats
    }

    pub fn mut_text_stats(&mut self) -> &mut AHashMap<PayloadKeyType, TextFieldStats> {
        &mut self.text_stats
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
            idf: idf_scope.and_then(|scope| scope.df.get(vector_name)),
            indexed_vectors: idf_scope
                .and_then(|scope| scope.indexed_vectors.get(vector_name))
                .copied(),
            deleted_points: self.deleted_points,
            hardware_counter: self.hardware_counter.fork(),
        }
    }

    /// Corpus statistics for a scored text query on `field`, or `None` when
    /// nothing seeded them.
    pub fn get_text_context(&self, field: &PayloadKeyType) -> Option<TextQueryContext<'_>> {
        self.query_context
            .text_stats
            .get(field)
            .map(|stats| TextQueryContext { stats })
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

/// Corpus statistics as a scored text query consumes them: an IDF per term and
/// an average document length, both over the segments of one local shard.
/// Shard-local, like the sparse statistics beside them: the gather runs per
/// shard and nothing merges across shards.
///
/// The sparse side applies the same statistic by scaling query weights in
/// place, which only means anything when the query is itself a weight vector.
#[derive(Debug)]
pub struct TextQueryContext<'a> {
    stats: &'a TextFieldStats,
}

impl TextQueryContext<'_> {
    /// `N`: documents carrying the field, over this shard's segments.
    pub fn document_count(&self) -> usize {
        self.stats.documents
    }

    /// `df(t)`: documents holding the term, summed over every segment. Zero
    /// for a term nothing seeded or nothing holds.
    pub fn document_frequency(&self, term: &str) -> usize {
        self.stats.df.get(term).copied().unwrap_or(0)
    }

    /// `IDF(t)`. A term nothing reported keeps the `df` of zero it was seeded
    /// with, which is the *largest* value this formula produces, not the
    /// smallest. Such a term matches no document, so it contributes to no
    /// score, but a caller reading the number for its own purposes should know
    /// which end of the range it sits at.
    ///
    /// Clamped at zero at the other end. Posting lists keep deleted documents
    /// while the document count excludes them, so `df` can exceed `N` in a
    /// segment with many deletions, and the unclamped formula would then flip
    /// that term's sign.
    pub fn idf(&self, term: &str) -> DimWeight {
        let df = self.stats.df.get(term).copied().unwrap_or(0);
        fancy_idf(self.stats.documents as DimWeight, df as DimWeight).max(0.0)
    }

    /// `avgdl`, or `None` when the corpus holds no documents or some segment
    /// records no lengths. Computed once over the summed totals: an average of
    /// per-segment averages is a different, wrong number.
    pub fn avg_doc_len(&self) -> Option<DimWeight> {
        let total_tokens = self.stats.total_tokens?;
        (self.stats.documents > 0)
            .then(|| total_tokens as DimWeight / self.stats.documents as DimWeight)
    }
}

pub struct FormulaContext {
    pub formula: ParsedFormula,
    pub prefetches_results: Vec<Vec<ScoredPoint>>,
    pub limit: usize,
    pub score_threshold: Option<ScoreType>,
    pub is_stopped: Arc<AtomicBool>,
}
