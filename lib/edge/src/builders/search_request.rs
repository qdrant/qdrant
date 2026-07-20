//! Fluent builder for [`SearchRequest`].
//!
//! Builder fields mirror [`SearchRequest`] explicitly so adding a field
//! to the target struct forces a compile error here.

use common::types::ScoreType;
use segment::types::{Filter, SearchParams, WithPayloadInterface, WithVector};
use shard::query::query_enum::QueryEnum;

use crate::requests::search::SearchRequest;

/// Fluent builder for [`SearchRequest`].
///
/// `query` and `limit` are required and passed through [`Self::new`]; every
/// other field is optional and falls back to the [`SearchRequest::new`] defaults.
#[derive(Clone, Debug)]
pub struct SearchRequestBuilder {
    query: QueryEnum,
    filter: Option<Filter>,
    params: Option<SearchParams>,
    limit: usize,
    offset: usize,
    with_payload: Option<WithPayloadInterface>,
    with_vector: Option<WithVector>,
    score_threshold: Option<ScoreType>,
}

impl SearchRequestBuilder {
    pub fn new(query: QueryEnum, limit: usize) -> Self {
        let SearchRequest {
            query,
            filter,
            params,
            limit,
            offset,
            with_payload,
            with_vector,
            score_threshold,
        } = SearchRequest::new(query, limit);
        Self {
            query,
            filter,
            params,
            limit,
            offset,
            with_payload,
            with_vector,
            score_threshold,
        }
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn params(mut self, params: SearchParams) -> Self {
        self.params = Some(params);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    pub fn with_payload(mut self, with_payload: WithPayloadInterface) -> Self {
        self.with_payload = Some(with_payload);
        self
    }

    pub fn with_vector(mut self, with_vector: WithVector) -> Self {
        self.with_vector = Some(with_vector);
        self
    }

    pub fn score_threshold(mut self, score_threshold: ScoreType) -> Self {
        self.score_threshold = Some(score_threshold);
        self
    }

    pub fn build(self) -> SearchRequest {
        // Exhaustively destructure Self and construct SearchRequest:
        // adding a field to either type forces a compile error here.
        let Self {
            query,
            filter,
            params,
            limit,
            offset,
            with_payload,
            with_vector,
            score_threshold,
        } = self;
        SearchRequest {
            query,
            filter,
            params,
            limit,
            offset,
            with_payload,
            with_vector,
            score_threshold,
        }
    }
}
