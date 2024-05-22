use api::rest::{DenseVector, MultiDenseVector};
use schemars::JsonSchema;
use segment::common::utils::MaybeOneOrMany;
use segment::types::{Filter, SearchParams, WithPayloadInterface, WithVector};
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::SparseVector;

use super::shard_query::Fusion;
use crate::operations::types::{OrderByInterface, RecommendStrategy};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum VectorInput {
    Id(segment::types::PointIdType),
    DenseVector(DenseVector),
    SparseVector(SparseVector),
    MultiDenseVector(MultiDenseVector),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct QueryRequest {
    /// Sub-requests to perform first. If present, the query will be performed on the results of the prefetches.
    #[serde(with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<Prefetch>")]
    pub prefetch: Option<Vec<Prefetch>>,

    /// Query to perform. If missing, returns points ordered by their IDs.
    pub query: Option<QueryInterface>,

    /// Define which vector to use for querying. If missing, the default vector is used.
    pub using: Option<String>,

    /// Filter conditions - return only those points that satisfy the specified conditions.
    pub filter: Option<Filter>,

    /// Search params for when there is no prefetch
    pub params: Option<SearchParams>,

    /// Max number of points. Default is 10.
    pub limit: Option<usize>,

    /// Offset of the result. Skip this many points. Default is 0
    pub offset: Option<usize>,

    /// Options for specifying which vectors to include into the response. Default is false.
    pub with_vector: Option<WithVector>,

    /// Options for specifying which payload to include or not. Default is false.
    pub with_payload: Option<WithPayloadInterface>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum QueryInterface {
    Nearest(VectorInput),
    Query(Query),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Query {
    /// Find the nearest neighbors to this vector.
    Nearest(VectorInput),

    /// Use multiple positive and negative vectors to find the results.
    Recommend(RecommendInput),

    /// Search for nearest points, but constrain the search space with context
    Discover(DiscoverInput),

    /// Return points that live in positive areas.
    Context(ContextInput),

    /// Order the points by a payload field.
    OrderBy(OrderByInterface),

    /// Fuse the results of multiple prefetches.
    Fusion(Fusion),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Prefetch {
    /// Sub-requests to perform first. If present, the query will be performed on the results of the prefetches.
    #[serde(with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<Prefetch>")]
    pub prefetch: Option<Vec<Prefetch>>,

    /// Query to perform. If missing, returns points ordered by their IDs.
    pub query: Option<QueryInterface>,

    /// Define which vector to use for querying. If missing, the default vector is used.
    pub using: Option<String>,

    /// Filter conditions - return only those points that satisfy the specified conditions.
    pub filter: Option<Filter>,

    /// Search params for when there is no prefetch
    pub params: Option<SearchParams>,

    /// Max number of points. Default is 10.
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RecommendInput {
    /// Look for vectors closest to the vectors from these points
    pub positives: Option<Vec<VectorInput>>,

    /// Try to avoid vectors like the vector from these points
    pub negatives: Option<Vec<VectorInput>>,

    /// How to use the provided vectors to find the results
    pub strategy: Option<RecommendStrategy>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DiscoverInput {
    /// Use this as the primary search objective
    pub target: VectorInput,

    /// Search space will be constrained by these pairs of vectors
    #[serde(with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<ContextPair>")]
    pub context_pairs: Option<Vec<ContextPair>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ContextInput {
    /// Search space will be constrained by these pairs of vectors
    #[serde(with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<ContextPair>")]
    pub pairs: Option<Vec<ContextPair>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ContextPair {
    /// A positive vector
    pub positive: VectorInput,

    /// Repel from this vector
    pub negative: VectorInput,
}
