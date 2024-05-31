use std::collections::HashMap;

use common::types::ScoreType;
use schemars::JsonSchema;
use segment::common::utils::MaybeOneOrMany;
use segment::data_types::order_by::OrderBy;
use segment::json_path::JsonPath;
use segment::types::{Filter, SearchParams, ShardKey, WithPayloadInterface, WithVector};
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::SparseVector;
use validator::Validate;

/// Type for dense vector
pub type DenseVector = Vec<segment::data_types::vectors::VectorElementType>;

/// Type for multi dense vector
pub type MultiDenseVector = Vec<DenseVector>;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum Vector {
    Dense(DenseVector),
    Sparse(sparse::common::sparse_vector::SparseVector),
    MultiDense(MultiDenseVector),
}

/// Full vector data per point separator with single and multiple vector modes
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorStruct {
    Single(DenseVector),
    Multi(HashMap<String, Vector>),
}

impl VectorStruct {
    /// Check if this vector struct is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            VectorStruct::Single(vector) => vector.is_empty(),
            VectorStruct::Multi(vectors) => vectors.values().all(|v| match v {
                Vector::Dense(vector) => vector.is_empty(),
                Vector::Sparse(vector) => vector.indices.is_empty(),
                Vector::MultiDense(vector) => vector.is_empty(),
            }),
        }
    }

    /// TODO(colbert): remove this method and use `merge` from segment::VectorStruct
    pub fn merge(&mut self, other: Self) {
        match (self, other) {
            // If other is empty, merge nothing
            (_, VectorStruct::Multi(other)) if other.is_empty() => {}
            // Single overwrites single
            (VectorStruct::Single(this), VectorStruct::Single(other)) => {
                *this = other;
            }
            // If multi into single, convert this to multi and merge
            (this @ VectorStruct::Single(_), other @ VectorStruct::Multi(_)) => {
                let VectorStruct::Single(single) = this.clone() else {
                    unreachable!();
                };
                *this =
                    VectorStruct::Multi(HashMap::from([(String::new(), Vector::Dense(single))]));
                this.merge(other);
            }
            // Single into multi
            (VectorStruct::Multi(this), VectorStruct::Single(other)) => {
                this.insert(String::new(), Vector::Dense(other));
            }
            // Multi into multi
            (VectorStruct::Multi(this), VectorStruct::Multi(other)) => this.extend(other),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum BatchVectorStruct {
    Single(Vec<DenseVector>),
    Multi(HashMap<String, Vec<Vector>>),
}

#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema, PartialEq)]
#[serde(untagged)]
pub enum ShardKeySelector {
    ShardKey(ShardKey),
    ShardKeys(Vec<ShardKey>),
    // ToDo: select by pattern
}

/// Search result
#[derive(Serialize, JsonSchema, Clone, Debug)]
pub struct ScoredPoint {
    /// Point id
    pub id: segment::types::PointIdType,
    /// Point version
    pub version: segment::types::SeqNumberType,
    /// Points vector distance to the query vector
    pub score: common::types::ScoreType,
    /// Payload - values assigned to the point
    pub payload: Option<segment::types::Payload>,
    /// Vector of the point
    pub vector: Option<VectorStruct>,
    /// Shard Key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<segment::types::ShardKey>,
    /// Order-by value
    pub order_value: Option<segment::data_types::order_by::OrderValue>,
}

/// Point data
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct Record {
    /// Id of the point
    pub id: segment::types::PointIdType,
    /// Payload - values assigned to the point
    pub payload: Option<segment::types::Payload>,
    /// Vector of the point
    pub vector: Option<VectorStruct>,
    /// Shard Key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<segment::types::ShardKey>,
}

/// Vector data separator for named and unnamed modes
/// Unnamed mode:
///
/// {
///   "vector": [1.0, 2.0, 3.0]
/// }
///
/// or named mode:
///
/// {
///   "vector": {
///     "vector": [1.0, 2.0, 3.0],
///     "name": "image-embeddings"
///   }
/// }
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum NamedVectorStruct {
    Default(segment::data_types::vectors::DenseVector),
    Dense(segment::data_types::vectors::NamedVector),
    Sparse(segment::data_types::vectors::NamedSparseVector),
    // No support for multi-dense vectors in search
}

#[derive(Deserialize, Serialize, JsonSchema, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum OrderByInterface {
    Key(JsonPath),
    Struct(OrderBy),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum Fusion {
    /// Reciprocal rank fusion
    Rrf,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum VectorInput {
    DenseVector(DenseVector),
    SparseVector(SparseVector),
    MultiDenseVector(MultiDenseVector),
    Id(segment::types::PointIdType),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct QueryRequestInternal {
    /// Sub-requests to perform first. If present, the query will be performed on the results of the prefetch(es).
    #[validate]
    #[serde(default, with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<Prefetch>")]
    pub prefetch: Option<Vec<Prefetch>>,

    /// Query to perform. If missing, returns points ordered by their IDs.
    #[validate]
    pub query: Option<QueryInterface>,

    /// Define which vector name to use for querying. If missing, the default vector is used.
    pub using: Option<String>,

    /// Filter conditions - return only those points that satisfy the specified conditions.
    #[validate]
    pub filter: Option<Filter>,

    /// Search params for when there is no prefetch
    pub params: Option<SearchParams>,

    /// Return points with scores better than this threshold.
    pub score_threshold: Option<ScoreType>,

    /// Max number of points to return. Default is 10.
    pub limit: Option<usize>,

    /// Offset of the result. Skip this many points. Default is 0
    pub offset: Option<usize>,

    /// Options for specifying which vectors to include into the response. Default is false.
    pub with_vector: Option<WithVector>,

    /// Options for specifying which payload to include or not. Default is false.
    pub with_payload: Option<WithPayloadInterface>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct QueryRequest {
    #[validate]
    #[serde(flatten)]
    pub internal: QueryRequestInternal,
    pub shard_key: Option<ShardKeySelector>,
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

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct Prefetch {
    /// Sub-requests to perform first. If present, the query will be performed on the results of the prefetches.
    #[validate]
    #[serde(default, with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<Prefetch>")]
    pub prefetch: Option<Vec<Prefetch>>,

    /// Query to perform. If missing, returns points ordered by their IDs.
    #[validate]
    pub query: Option<QueryInterface>,

    /// Define which vector name to use for querying. If missing, the default vector is used.
    pub using: Option<String>,

    /// Filter conditions - return only those points that satisfy the specified conditions.
    #[validate]
    pub filter: Option<Filter>,

    /// Search params for when there is no prefetch
    pub params: Option<SearchParams>,

    /// Return points with scores better than this threshold.
    pub score_threshold: Option<ScoreType>,

    /// Max number of points to return. Default is 10.
    pub limit: Option<usize>,
}

/// How to use positive and negative examples to find the results, default is `average_vector`:
///
/// * `average_vector` - Average positive and negative vectors and create a single query
///   with the formula `query = avg_pos + avg_pos - avg_neg`. Then performs normal search.
///
/// * `best_score` - Uses custom search objective. Each candidate is compared against all
///   examples, its score is then chosen from the `max(max_pos_score, max_neg_score)`.
///   If the `max_neg_score` is chosen then it is squared and negated, otherwise it is just
///   the `max_pos_score`.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum RecommendStrategy {
    #[default]
    AverageVector,
    BestScore,
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

impl RecommendInput {
    pub fn iter(&self) -> impl Iterator<Item = &VectorInput> {
        self.positives
            .iter()
            .flatten()
            .chain(self.negatives.iter().flatten())
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct DiscoverInput {
    /// Use this as the primary search objective
    #[validate]
    pub target: VectorInput,

    /// Search space will be constrained by these pairs of vectors
    #[validate]
    #[serde(with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<ContextPair>")]
    pub context_pairs: Option<Vec<ContextPair>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct ContextInput {
    /// Search space will be constrained by these pairs of vectors
    #[validate]
    #[serde(with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<ContextPair>")]
    pub pairs: Option<Vec<ContextPair>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct ContextPair {
    /// A positive vector
    #[validate]
    pub positive: VectorInput,

    /// Repel from this vector
    #[validate]
    pub negative: VectorInput,
}
