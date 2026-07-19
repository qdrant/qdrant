use common::types::ScoreType;
use segment::data_types::load_profile::LoadProfile;
use segment::types::{Filter, SearchParams, WithPayloadInterface, WithVector};
use shard::query::query_enum::QueryEnum;
use shard::search::CoreSearchRequest;

/// Single-query vector search over an edge shard.
///
/// DEPRECATED together with [`EdgeShardRead::search`](crate::EdgeShardRead::search): prefer
/// [`QueryRequest`](crate::QueryRequest) and [`EdgeShardRead::query`](crate::EdgeShardRead::query).
#[derive(Clone, Debug, PartialEq)]
pub struct SearchRequest {
    /// Every kind of query that can be performed on segment level.
    pub query: QueryEnum,
    /// Look only for points which satisfy these conditions.
    pub filter: Option<Filter>,
    /// Additional search params.
    pub params: Option<SearchParams>,
    /// Max number of results to return.
    pub limit: usize,
    /// Offset of the first result to return. May be used to paginate results.
    /// Note: large offset values may cause performance issues.
    pub offset: usize,
    /// Select which payload to return with the response. Default is false.
    pub with_payload: Option<WithPayloadInterface>,
    /// Options for specifying which vectors to include into the response. Default is false.
    pub with_vector: Option<WithVector>,
    /// Exclude results with a worse score than this.
    pub score_threshold: Option<ScoreType>,
}

impl SearchRequest {
    pub fn new(query: QueryEnum, limit: usize) -> Self {
        Self {
            query,
            filter: None,
            params: None,
            limit,
            offset: 0,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        }
    }

    /// Request-specific [`LoadProfile`] for opening a read-only shard to serve exactly
    /// this search: only the queried vector's components and the filter's field indexes
    /// keep their configured placement.
    pub fn load_profile(&self) -> LoadProfile {
        CoreSearchRequest::from(self.clone()).load_profile()
    }
}
