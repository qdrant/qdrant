use common::types::ScoreType;
use segment::types::{Filter, SearchParams, WithPayloadInterface, WithVector};
use shard::query::ScoringQuery;

/// Universal query over an edge shard: a scoring query with optional prefetch stages.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryRequest {
    /// Sub-requests resolved first; their results form the candidate set the top-level
    /// `query` re-scores.
    pub prefetches: Vec<Prefetch>,
    /// How to score the candidates. `None` scrolls by id instead of scoring.
    pub query: Option<ScoringQuery>,
    /// Look only for points which satisfy these conditions.
    pub filter: Option<Filter>,
    /// Exclude results with a worse score than this.
    pub score_threshold: Option<ScoreType>,
    /// Max number of results to return.
    pub limit: usize,
    /// Offset of the first result to return. May be used to paginate results.
    /// Note: large offset values may cause performance issues.
    pub offset: usize,
    /// Search params for when there is no prefetch.
    pub params: Option<SearchParams>,
    /// Options for specifying which vectors to include into the response. Default is false.
    pub with_vector: WithVector,
    /// Select which payload to return with the response. Default is false.
    pub with_payload: WithPayloadInterface,
}

impl QueryRequest {
    pub fn new(limit: usize) -> Self {
        Self {
            prefetches: Vec::new(),
            query: None,
            filter: None,
            score_threshold: None,
            limit,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(false),
            with_payload: WithPayloadInterface::Bool(false),
        }
    }
}

/// One prefetch stage of a [`QueryRequest`]: produces the candidate set its parent re-scores.
/// Prefetches nest, forming a candidate-resolution tree evaluated leaves-first.
#[derive(Clone, Debug, PartialEq)]
pub struct Prefetch {
    /// Nested sub-prefetches resolved before this one.
    pub prefetches: Vec<Prefetch>,
    /// How to score this stage's candidates. `None` scrolls by id instead of scoring.
    pub query: Option<ScoringQuery>,
    /// Max number of candidates this stage passes to its parent.
    pub limit: usize,
    /// Additional search params.
    pub params: Option<SearchParams>,
    /// Look only for points which satisfy these conditions.
    pub filter: Option<Filter>,
    /// Exclude candidates with a worse score than this.
    pub score_threshold: Option<ScoreType>,
}

impl Prefetch {
    pub fn new(limit: usize) -> Self {
        Self {
            prefetches: Vec::new(),
            query: None,
            limit,
            params: None,
            filter: None,
            score_threshold: None,
        }
    }
}
