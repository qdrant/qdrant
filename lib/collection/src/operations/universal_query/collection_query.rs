use common::types::ScoreType;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::Vector;
use segment::types::{Filter, PointIdType, SearchParams, WithPayloadInterface, WithVector};
use segment::vector_storage::query::{ContextQuery, DiscoveryQuery, RecoQuery};

use super::shard_query::Fusion;

/// Internal representation of a query request, used to converge from REST and gRPC. This can have IDs referencing vectors.
pub struct CollectionQueryRequest {
    pub prefetches: Vec<CollectionPrefetch>,
    pub query: Option<Query>,
    pub using: String,
    pub filter: Option<Filter>,
    pub score_threshold: Option<ScoreType>,
    pub limit: usize,
    pub offset: usize,
    /// Search params for when there is no prefetch
    pub params: Option<SearchParams>,
    pub with_vector: WithVector,
    pub with_payload: WithPayloadInterface,
}

pub enum Query {
    /// Score points against some vector(s)
    Vector(VectorQuery),

    /// Reciprocal rank fusion
    Fusion(Fusion),

    /// Order by a payload field
    OrderBy(OrderBy),
}

pub enum VectorInput {
    Id(PointIdType),
    Vector(Vector),
}

pub enum VectorQuery {
    Nearest(VectorInput),
    RecommendBestScore(RecoQuery<VectorInput>),
    Discover(DiscoveryQuery<VectorInput>),
    Context(ContextQuery<VectorInput>),
}

pub struct CollectionPrefetch {
    pub prefetch: Vec<CollectionPrefetch>,
    pub query: Option<Query>,
    pub using: String,
    pub filter: Option<Filter>,
    pub score_threshold: Option<ScoreType>,
    pub limit: usize,
    /// Search params for when there is no prefetch
    pub params: Option<SearchParams>,
}
