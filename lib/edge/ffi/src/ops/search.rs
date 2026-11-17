//! [`EdgeShard::search`] — plain nearest-neighbor search.

use segment::types::{
    Filter as SegmentFilter, SearchParams as SegmentSearchParams, WithPayloadInterface,
    WithVector as SegmentWithVector,
};
use shard::query::query_enum::QueryEnum;

use crate::EdgeShard;
use crate::error::Result;
use crate::filter::Filter;
use crate::ops::query::{Query, SearchParams};
use crate::types::{ScoredPoint, WithPayload, WithVector};

#[uniffi::export]
impl EdgeShard {
    /// Executes a single nearest-neighbor search against a dense vector
    /// field.
    ///
    /// Returns the top `request.limit` points scored against `request.query`,
    /// optionally filtered by `request.filter`.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`](crate::error::EdgeError) if the
    /// shard is unloaded, or
    /// [`EdgeError::OperationError`](crate::error::EdgeError) if the vector
    /// dimensionality does not match the configured vector field or the filter
    /// references a payload key without an index.
    pub fn search(&self, request: SearchRequest) -> Result<Vec<ScoredPoint>> {
        self.with_shard(|shard| {
            let points = shard.search(request.try_into()?)?;
            Ok(points.into_iter().map(ScoredPoint::from).collect())
        })
    }
}

// ── SearchRequest ───────────────────────────────────────────────────────────

/// A plain nearest-neighbor search request.
///
/// Use this for the common case of "find the K most similar points to this
/// vector, optionally filtered". For more advanced pipelines (fusion,
/// multi-stage retrieval) use [`QueryRequest`](crate::ops::query::QueryRequest).
#[derive(Clone, Debug, uniffi::Record)]
pub struct SearchRequest {
    /// Vector query describing what to search for.
    pub query: Query,
    /// Maximum number of results to return.
    pub limit: u64,
    /// Number of results to skip (for pagination).
    #[uniffi(default = None)]
    pub offset: Option<u64>,
    /// Optional filter applied to all candidates.
    #[uniffi(default = None)]
    pub filter: Option<Filter>,
    /// Search tuning parameters.
    #[uniffi(default = None)]
    pub params: Option<SearchParams>,
    /// Include vectors in the response.
    #[uniffi(default = None)]
    pub with_vector: Option<WithVector>,
    /// Include payload in the response.
    #[uniffi(default = None)]
    pub with_payload: Option<WithPayload>,
    /// Minimum score threshold; candidates scoring below are dropped.
    #[uniffi(default = None)]
    pub score_threshold: Option<f32>,
}

impl TryFrom<SearchRequest> for edge::SearchRequest {
    type Error = crate::error::EdgeError;

    fn try_from(r: SearchRequest) -> Result<Self, Self::Error> {
        let SearchRequest {
            query,
            limit,
            offset,
            filter,
            params,
            with_vector,
            with_payload,
            score_threshold,
        } = r;
        Ok(edge::SearchRequest {
            query: QueryEnum::try_from(query)?,
            limit: crate::error::bounded_limit("limit", limit)?,
            offset: crate::error::bounded_limit("offset", offset.unwrap_or(0))?,
            filter: filter.map(SegmentFilter::try_from).transpose()?,
            params: params.map(SegmentSearchParams::from),
            with_vector: with_vector.map(SegmentWithVector::from),
            with_payload: with_payload
                .map(WithPayloadInterface::try_from)
                .transpose()?,
            score_threshold,
        })
    }
}
