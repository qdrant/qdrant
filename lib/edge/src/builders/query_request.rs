//! Fluent builder for [`QueryRequest`].
//!
//! Builder fields mirror [`QueryRequest`] explicitly so adding a field
//! to the target struct forces a compile error here.

use common::types::ScoreType;
use segment::types::{Filter, SearchParams, WithPayloadInterface, WithVector};
use shard::query::ScoringQuery;

use crate::requests::query::{Prefetch, QueryRequest};

/// Fluent builder for [`QueryRequest`].
///
/// `limit` is required and passed through [`Self::new`]; every other field is
/// optional and falls back to the [`QueryRequest::new`] defaults.
#[derive(Clone, Debug)]
pub struct QueryRequestBuilder {
    prefetches: Vec<Prefetch>,
    query: Option<ScoringQuery>,
    filter: Option<Filter>,
    score_threshold: Option<ScoreType>,
    limit: usize,
    offset: usize,
    params: Option<SearchParams>,
    with_vector: WithVector,
    with_payload: WithPayloadInterface,
}

impl QueryRequestBuilder {
    pub fn new(limit: usize) -> Self {
        let QueryRequest {
            prefetches,
            query,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        } = QueryRequest::new(limit);
        Self {
            prefetches,
            query,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        }
    }

    /// Replaces the whole prefetch list; see [`Self::add_prefetch`] to append one stage.
    pub fn prefetches(mut self, prefetches: Vec<Prefetch>) -> Self {
        self.prefetches = prefetches;
        self
    }

    pub fn add_prefetch(mut self, prefetch: Prefetch) -> Self {
        self.prefetches.push(prefetch);
        self
    }

    pub fn query(mut self, query: ScoringQuery) -> Self {
        self.query = Some(query);
        self
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn score_threshold(mut self, score_threshold: ScoreType) -> Self {
        self.score_threshold = Some(score_threshold);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    pub fn params(mut self, params: SearchParams) -> Self {
        self.params = Some(params);
        self
    }

    pub fn with_vector(mut self, with_vector: WithVector) -> Self {
        self.with_vector = with_vector;
        self
    }

    pub fn with_payload(mut self, with_payload: WithPayloadInterface) -> Self {
        self.with_payload = with_payload;
        self
    }

    pub fn build(self) -> QueryRequest {
        // Exhaustively destructure Self and construct QueryRequest:
        // adding a field to either type forces a compile error here.
        let Self {
            prefetches,
            query,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        } = self;
        QueryRequest {
            prefetches,
            query,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        }
    }
}
