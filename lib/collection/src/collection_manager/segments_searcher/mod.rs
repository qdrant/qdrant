//! Search and read operations over all segments of a local shard.

/// BM25 ranking over a text index: [`SegmentsSearcher::score_bm25`]
mod bm25;
/// Shard-wide statistics gathered before a search: [`SegmentsSearcher::prepare_query_context`]
mod query_context;
/// Formula rescoring: [`SegmentsSearcher::rescore_with_formula`]
mod rescore;
/// Point reads: [`SegmentsSearcher::retrieve`], [`SegmentsSearcher::read_filtered`]
mod retrieve;
/// Probabilistic per-segment limit
mod sampling;
/// Batch search across segments and merging of their results: [`SegmentsSearcher::search`]
mod search;
/// Batch search within one segment
mod segment_search;
#[cfg(test)]
mod shared_filter_tests;
#[cfg(test)]
mod tests;

use segment::types::ScoredPoint;

use crate::operations::types::CollectionResult;

type BatchOffset = usize;
type SegmentOffset = usize;

// batch -> point for one segment
type SegmentBatchSearchResult = Vec<Vec<ScoredPoint>>;
// Segment -> batch -> point
type BatchSearchResult = Vec<SegmentBatchSearchResult>;

// Result of batch search in one segment
type SegmentSearchExecutedResult = CollectionResult<(SegmentBatchSearchResult, Vec<bool>)>;

/// Simple implementation of segment manager
///  - rebuild segment for memory optimization purposes
#[derive(Default)]
pub struct SegmentsSearcher;
