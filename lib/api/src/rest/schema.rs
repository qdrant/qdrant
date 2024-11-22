use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use common::types::ScoreType;
use common::validation::validate_multi_vector;
use schemars::JsonSchema;
use segment::common::utils::MaybeOneOrMany;
use segment::data_types::order_by::OrderBy;
use segment::json_path::JsonPath;
use segment::types::{
    Filter, IntPayloadType, Payload, PointIdType, SearchParams, ShardKey, WithPayloadInterface,
    WithVector,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sparse::common::sparse_vector::SparseVector;
use validator::{Validate, ValidationErrors};

/// Type for dense vector
pub type DenseVector = Vec<segment::data_types::vectors::VectorElementType>;

/// Type for multi dense vector
pub type MultiDenseVector = Vec<DenseVector>;

/// Vector Data
/// Vectors can be described directly with values
/// Or specified with source "objects" for inference
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum Vector {
    Dense(DenseVector),
    Sparse(sparse::common::sparse_vector::SparseVector),
    MultiDense(MultiDenseVector),
    Document(Document),
    Image(Image),
    Object(InferenceObject),
}

/// Vector Data stored in Point
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorOutput {
    Dense(DenseVector),
    Sparse(sparse::common::sparse_vector::SparseVector),
    MultiDense(MultiDenseVector),
}

impl Validate for Vector {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Vector::Dense(_) => Ok(()),
            Vector::Sparse(v) => v.validate(),
            Vector::MultiDense(m) => validate_multi_vector(m),
            Vector::Document(_) => Ok(()),
            Vector::Image(_) => Ok(()),
            Vector::Object(_) => Ok(()),
        }
    }
}

fn vector_example() -> DenseVector {
    vec![0.875, 0.140625, 0.8976]
}

fn multi_dense_vector_example() -> MultiDenseVector {
    vec![
        vec![0.875, 0.140625, 0.1102],
        vec![0.758, 0.28126, 0.96871],
        vec![0.621, 0.421878, 0.9375],
    ]
}

fn named_vector_example() -> HashMap<String, Vector> {
    let mut map = HashMap::new();
    map.insert(
        "image-embeddings".to_string(),
        Vector::Dense(vec![0.873, 0.140625, 0.8976]),
    );
    map
}

/// Full vector data per point separator with single and multiple vector modes
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorStruct {
    #[schemars(example = "vector_example")]
    Single(DenseVector),
    #[schemars(example = "multi_dense_vector_example")]
    MultiDense(MultiDenseVector),
    #[schemars(example = "named_vector_example")]
    Named(HashMap<String, Vector>),
    Document(Document),
    Image(Image),
    Object(InferenceObject),
}

/// Vector data stored in Point
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorStructOutput {
    #[schemars(example = "vector_example")]
    Single(DenseVector),
    #[schemars(example = "multi_dense_vector_example")]
    MultiDense(MultiDenseVector),
    #[schemars(example = "named_vector_example")]
    Named(HashMap<String, VectorOutput>),
}

impl VectorStruct {
    /// Check if this vector struct is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            VectorStruct::Single(vector) => vector.is_empty(),
            VectorStruct::MultiDense(vector) => vector.is_empty(),
            VectorStruct::Named(vectors) => vectors.values().all(|v| match v {
                Vector::Dense(vector) => vector.is_empty(),
                Vector::Sparse(vector) => vector.indices.is_empty(),
                Vector::MultiDense(vector) => vector.is_empty(),
                Vector::Document(_) => false,
                Vector::Image(_) => false,
                Vector::Object(_) => false,
            }),
            VectorStruct::Document(_) => false,
            VectorStruct::Image(_) => false,
            VectorStruct::Object(_) => false,
        }
    }
}

impl Validate for VectorStruct {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            VectorStruct::Single(_) => Ok(()),
            VectorStruct::MultiDense(v) => validate_multi_vector(v),
            VectorStruct::Named(v) => common::validation::validate_iter(v.values()),
            VectorStruct::Document(_) => Ok(()),
            VectorStruct::Image(_) => Ok(()),
            VectorStruct::Object(_) => Ok(()),
        }
    }
}

#[derive(Clone, Default, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Options {
    /// Parameters for the model
    /// Values of the parameters are model-specific
    pub options: Option<HashMap<String, Value>>,
}

impl Hash for Options {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Order of keys in the map should not affect the hash
        if let Some(options) = &self.options {
            let mut keys: Vec<_> = options.keys().collect();
            keys.sort();
            for key in keys {
                key.hash(state);
                options.get(key).unwrap().hash(state);
            }
        }
    }
}

/// WARN: Work-in-progress, unimplemented
///
/// Text document for embedding. Requires inference infrastructure, unimplemented.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema, Hash, Validate)]
pub struct Document {
    /// Text of the document
    /// This field will be used as input for the embedding model
    #[schemars(example = "document_text_example")]
    pub text: String,
    /// Name of the model used to generate the vector
    /// List of available models depends on a provider
    #[validate(length(min = 1))]
    #[schemars(length(min = 1), example = "model_example")]
    pub model: String,
    #[serde(flatten)]
    pub options: Options,
}

/// WARN: Work-in-progress, unimplemented
///
/// Image object for embedding. Requires inference infrastructure, unimplemented.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema, Validate, Hash)]
pub struct Image {
    /// Image data: base64 encoded image or an URL
    #[schemars(example = "image_value_example")]
    pub image: Value,
    /// Name of the model used to generate the vector
    /// List of available models depends on a provider
    #[validate(length(min = 1))]
    #[schemars(length(min = 1), example = "image_model_example")]
    pub model: String,
    /// Parameters for the model
    /// Values of the parameters are model-specific
    #[serde(flatten)]
    pub options: Options,
}

/// WARN: Work-in-progress, unimplemented
///
/// Custom object for embedding. Requires inference infrastructure, unimplemented.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema, Hash, Validate)]
pub struct InferenceObject {
    /// Arbitrary data, used as input for the embedding model
    /// Used if the model requires more than one input or a custom input
    pub object: Value,
    /// Name of the model used to generate the vector
    /// List of available models depends on a provider
    #[validate(length(min = 1))]
    #[schemars(length(min = 1), example = "model_example")]
    pub model: String,
    /// Parameters for the model
    /// Values of the parameters are model-specific
    #[serde(flatten)]
    pub options: Options,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum BatchVectorStruct {
    Single(Vec<DenseVector>),
    MultiDense(Vec<MultiDenseVector>),
    Named(HashMap<String, Vec<Vector>>),
    Document(Vec<Document>),
    Image(Vec<Image>),
    Object(Vec<InferenceObject>),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct Batch {
    pub ids: Vec<PointIdType>,
    pub vectors: BatchVectorStruct,
    pub payloads: Option<Vec<Option<Payload>>>,
}

#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema, PartialEq)]
#[serde(untagged)]
pub enum ShardKeySelector {
    ShardKey(ShardKey),
    ShardKeys(Vec<ShardKey>),
    // ToDo: select by pattern
}

fn version_example() -> segment::types::SeqNumberType {
    3
}

fn score_example() -> common::types::ScoreType {
    0.75
}

fn document_text_example() -> String {
    "This is a document text".to_string()
}

fn model_example() -> String {
    "jinaai/jina-embeddings-v2-base-en".to_string()
}

fn image_value_example() -> String {
    "https://example.com/image.jpg".to_string()
}

fn image_model_example() -> String {
    "Qdrant/clip-ViT-B-32-vision".to_string()
}

/// Search result
#[derive(Serialize, JsonSchema, Clone, Debug)]
pub struct ScoredPoint {
    /// Point id
    pub id: PointIdType,
    /// Point version
    #[schemars(example = "version_example")]
    pub version: segment::types::SeqNumberType,
    /// Points vector distance to the query vector
    #[schemars(example = "score_example")]
    pub score: ScoreType,
    /// Payload - values assigned to the point
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<segment::types::Payload>,
    /// Vector of the point
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorStructOutput>,
    /// Shard Key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKey>,
    /// Order-by value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_value: Option<segment::data_types::order_by::OrderValue>,
}

/// Point data
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct Record {
    /// Id of the point
    pub id: segment::types::PointIdType,
    /// Payload - values assigned to the point
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<segment::types::Payload>,
    /// Vector of the point
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorStructOutput>,
    /// Shard Key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<segment::types::ShardKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_value: Option<segment::data_types::order_by::OrderValue>,
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

/// Fusion algorithm allows to combine results of multiple prefetches.
///
/// Available fusion algorithms:
///
/// * `rrf` - Reciprocal Rank Fusion
/// * `dbsf` - Distribution-Based Score Fusion
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Fusion {
    Rrf,
    Dbsf,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum VectorInput {
    DenseVector(DenseVector),
    SparseVector(SparseVector),
    MultiDenseVector(MultiDenseVector),
    Id(segment::types::PointIdType),
    Document(Document),
    Image(Image),
    Object(InferenceObject),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct QueryRequestInternal {
    /// Sub-requests to perform first. If present, the query will be performed on the results of the prefetch(es).
    #[validate(nested)]
    #[serde(default, with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<Prefetch>")]
    pub prefetch: Option<Vec<Prefetch>>,

    /// Query to perform. If missing without prefetches, returns points ordered by their IDs.
    #[validate(nested)]
    pub query: Option<QueryInterface>,

    /// Define which vector name to use for querying. If missing, the default vector is used.
    pub using: Option<String>,

    /// Filter conditions - return only those points that satisfy the specified conditions.
    #[validate(nested)]
    pub filter: Option<Filter>,

    /// Search params for when there is no prefetch
    #[validate(nested)]
    pub params: Option<SearchParams>,

    /// Return points with scores better than this threshold.
    pub score_threshold: Option<ScoreType>,

    /// Max number of points to return. Default is 10.
    #[validate(range(min = 1))]
    pub limit: Option<usize>,

    /// Offset of the result. Skip this many points. Default is 0
    pub offset: Option<usize>,

    /// Options for specifying which vectors to include into the response. Default is false.
    pub with_vector: Option<WithVector>,

    /// Options for specifying which payload to include or not. Default is false.
    pub with_payload: Option<WithPayloadInterface>,

    /// The location to use for IDs lookup, if not specified - use the current collection and the 'using' vector
    /// Note: the other collection vectors should have the same vector size as the 'using' vector in the current collection
    #[serde(default)]
    pub lookup_from: Option<LookupLocation>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct QueryRequest {
    #[validate(nested)]
    #[serde(flatten)]
    pub internal: QueryRequestInternal,
    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct QueryRequestBatch {
    #[validate(nested)]
    pub searches: Vec<QueryRequest>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct QueryResponse {
    pub points: Vec<ScoredPoint>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum QueryInterface {
    Nearest(VectorInput),
    Query(Query),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum Query {
    /// Find the nearest neighbors to this vector.
    Nearest(NearestQuery),

    /// Use multiple positive and negative vectors to find the results.
    Recommend(RecommendQuery),

    /// Search for nearest points, but constrain the search space with context
    Discover(DiscoverQuery),

    /// Return points that live in positive areas.
    Context(ContextQuery),

    /// Order the points by a payload field.
    OrderBy(OrderByQuery),

    /// Fuse the results of multiple prefetches.
    Fusion(FusionQuery),

    /// Sample points from the collection, non-deterministically.
    Sample(SampleQuery),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NearestQuery {
    pub nearest: VectorInput,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct RecommendQuery {
    pub recommend: RecommendInput,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DiscoverQuery {
    pub discover: DiscoverInput,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ContextQuery {
    pub context: ContextInput,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct OrderByQuery {
    pub order_by: OrderByInterface,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FusionQuery {
    pub fusion: Fusion,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SampleQuery {
    pub sample: Sample,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct Prefetch {
    /// Sub-requests to perform first. If present, the query will be performed on the results of the prefetches.
    #[validate(nested)]
    #[serde(default, with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<Prefetch>")]
    pub prefetch: Option<Vec<Prefetch>>,

    /// Query to perform. If missing without prefetches, returns points ordered by their IDs.
    #[validate(nested)]
    pub query: Option<QueryInterface>,

    /// Define which vector name to use for querying. If missing, the default vector is used.
    pub using: Option<String>,

    /// Filter conditions - return only those points that satisfy the specified conditions.
    #[validate(nested)]
    pub filter: Option<Filter>,

    /// Search params for when there is no prefetch
    #[validate(nested)]
    pub params: Option<SearchParams>,

    /// Return points with scores better than this threshold.
    pub score_threshold: Option<ScoreType>,

    /// Max number of points to return. Default is 10.
    #[validate(range(min = 1))]
    pub limit: Option<usize>,

    /// The location to use for IDs lookup, if not specified - use the current collection and the 'using' vector
    /// Note: the other collection vectors should have the same vector size as the 'using' vector in the current collection
    #[serde(default)]
    pub lookup_from: Option<LookupLocation>,
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
    pub positive: Option<Vec<VectorInput>>,

    /// Try to avoid vectors like the vector from these points
    pub negative: Option<Vec<VectorInput>>,

    /// How to use the provided vectors to find the results
    pub strategy: Option<RecommendStrategy>,
}

impl RecommendInput {
    pub fn iter(&self) -> impl Iterator<Item = &VectorInput> {
        self.positive
            .iter()
            .flatten()
            .chain(self.negative.iter().flatten())
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct DiscoverInput {
    /// Use this as the primary search objective
    #[validate(nested)]
    pub target: VectorInput,

    /// Search space will be constrained by these pairs of vectors
    #[validate(nested)]
    #[serde(with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<ContextPair>")]
    pub context: Option<Vec<ContextPair>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ContextInput(
    /// Search space will be constrained by these pairs of vectors
    #[serde(with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<ContextPair>")]
    pub Option<Vec<ContextPair>>,
);

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct ContextPair {
    /// A positive vector
    #[validate(nested)]
    pub positive: VectorInput,

    /// Repel from this vector
    #[validate(nested)]
    pub negative: VectorInput,
}

impl ContextPair {
    pub fn iter(&self) -> impl Iterator<Item = &VectorInput> {
        std::iter::once(&self.positive).chain(std::iter::once(&self.negative))
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Sample {
    Random,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WithLookup {
    /// Name of the collection to use for points lookup
    #[serde(rename = "collection")]
    pub collection_name: String,

    /// Options for specifying which payload to include (or not)
    #[serde(default = "default_with_payload")]
    pub with_payload: Option<WithPayloadInterface>,

    /// Options for specifying which vectors to include (or not)
    #[serde(alias = "with_vector")]
    #[serde(default)]
    pub with_vectors: Option<WithVector>,
}

#[allow(clippy::unnecessary_wraps)] // Used as serde default
const fn default_with_payload() -> Option<WithPayloadInterface> {
    Some(WithPayloadInterface::Bool(true))
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum WithLookupInterface {
    Collection(String),
    WithLookup(WithLookup),
}

/// Defines a location to use for looking up the vector.
/// Specifies collection and vector field name.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct LookupLocation {
    /// Name of the collection used for lookup
    pub collection: String,
    /// Optional name of the vector field within the collection.
    /// If not provided, the default vector field will be used.
    #[serde(default)]
    pub vector: Option<String>,

    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Validate, Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct BaseGroupRequest {
    /// Payload field to group by, must be a string or number field.
    /// If the field contains more than 1 value, all values will be used for grouping.
    /// One point can be in multiple groups.
    #[schemars(length(min = 1))]
    pub group_by: JsonPath,

    /// Maximum amount of points to return per group
    #[validate(range(min = 1))]
    pub group_size: u32,

    /// Maximum amount of groups to return
    #[validate(range(min = 1))]
    pub limit: u32,

    /// Look for points in another collection using the group ids
    pub with_lookup: Option<WithLookupInterface>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct SearchGroupsRequestInternal {
    /// Look for vectors closest to this
    #[validate(nested)]
    pub vector: NamedVectorStruct,

    /// Look only for points which satisfies this conditions
    #[validate(nested)]
    pub filter: Option<Filter>,

    /// Additional search params
    #[validate(nested)]
    pub params: Option<SearchParams>,

    /// Select which payload to return with the response. Default is false.
    pub with_payload: Option<WithPayloadInterface>,

    /// Options for specifying which vectors to include into response. Default is false.
    #[serde(default, alias = "with_vectors")]
    pub with_vector: Option<WithVector>,

    /// Define a minimal score threshold for the result.
    /// If defined, less similar results will not be returned.
    /// Score of the returned result might be higher or smaller than the threshold depending on the
    /// Distance function used. E.g. for cosine similarity only higher scores will be returned.
    pub score_threshold: Option<ScoreType>,

    #[serde(flatten)]
    #[validate(nested)]
    pub group_request: BaseGroupRequest,
}

/// Search request.
/// Holds all conditions and parameters for the search of most similar points by vector similarity
/// given the filtering restrictions.
#[derive(Deserialize, Serialize, JsonSchema, Validate, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct SearchRequestInternal {
    /// Look for vectors closest to this
    #[validate(nested)]
    pub vector: NamedVectorStruct,
    /// Look only for points which satisfies this conditions
    #[validate(nested)]
    pub filter: Option<Filter>,
    /// Additional search params
    #[validate(nested)]
    pub params: Option<SearchParams>,
    /// Max number of result to return
    #[serde(alias = "top")]
    #[validate(range(min = 1))]
    pub limit: usize,
    /// Offset of the first result to return.
    /// May be used to paginate results.
    /// Note: large offset values may cause performance issues.
    pub offset: Option<usize>,
    /// Select which payload to return with the response. Default is false.
    pub with_payload: Option<WithPayloadInterface>,
    /// Options for specifying which vectors to include into response. Default is false.
    #[serde(default, alias = "with_vectors")]
    pub with_vector: Option<WithVector>,
    /// Define a minimal score threshold for the result.
    /// If defined, less similar results will not be returned.
    /// Score of the returned result might be higher or smaller than the threshold depending on the
    /// Distance function used. E.g. for cosine similarity only higher scores will be returned.
    pub score_threshold: Option<ScoreType>,
}

#[derive(Validate, Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct QueryBaseGroupRequest {
    /// Payload field to group by, must be a string or number field.
    /// If the field contains more than 1 value, all values will be used for grouping.
    /// One point can be in multiple groups.
    #[schemars(length(min = 1))]
    pub group_by: JsonPath,

    /// Maximum amount of points to return per group. Default is 3.
    #[validate(range(min = 1))]
    pub group_size: Option<usize>,

    /// Maximum amount of groups to return. Default is 10.
    #[validate(range(min = 1))]
    pub limit: Option<usize>,

    /// Look for points in another collection using the group ids
    pub with_lookup: Option<WithLookupInterface>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct QueryGroupsRequestInternal {
    /// Sub-requests to perform first. If present, the query will be performed on the results of the prefetch(es).
    #[validate(nested)]
    #[serde(default, with = "MaybeOneOrMany")]
    #[schemars(with = "MaybeOneOrMany<Prefetch>")]
    pub prefetch: Option<Vec<Prefetch>>,

    /// Query to perform. If missing without prefetches, returns points ordered by their IDs.
    #[validate(nested)]
    pub query: Option<QueryInterface>,

    /// Define which vector name to use for querying. If missing, the default vector is used.
    pub using: Option<String>,

    /// Filter conditions - return only those points that satisfy the specified conditions.
    #[validate(nested)]
    pub filter: Option<Filter>,

    /// Search params for when there is no prefetch
    #[validate(nested)]
    pub params: Option<SearchParams>,

    /// Return points with scores better than this threshold.
    pub score_threshold: Option<ScoreType>,

    /// Options for specifying which vectors to include into the response. Default is false.
    pub with_vector: Option<WithVector>,

    /// Options for specifying which payload to include or not. Default is false.
    pub with_payload: Option<WithPayloadInterface>,

    /// The location to use for IDs lookup, if not specified - use the current collection and the 'using' vector
    /// Note: the other collection vectors should have the same vector size as the 'using' vector in the current collection
    #[serde(default)]
    pub lookup_from: Option<LookupLocation>,

    #[serde(flatten)]
    #[validate(nested)]
    pub group_request: QueryBaseGroupRequest,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct QueryGroupsRequest {
    #[validate(nested)]
    #[serde(flatten)]
    pub search_group_request: QueryGroupsRequestInternal,

    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Serialize, Deserialize, JsonSchema, Validate, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct SearchMatrixRequestInternal {
    /// Look only for points which satisfies this conditions
    #[validate(nested)]
    pub filter: Option<Filter>,
    /// How many points to select and search within. Default is 10.
    #[validate(range(min = 2))]
    pub sample: Option<usize>,
    /// How many neighbours per sample to find. Default is 3.
    #[validate(range(min = 1))]
    pub limit: Option<usize>,
    /// Define which vector name to use for querying. If missing, the default vector is used.
    pub using: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct SearchMatrixRequest {
    #[serde(flatten)]
    #[validate(nested)]
    pub search_request: SearchMatrixRequestInternal,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Debug, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct SearchMatrixOffsetsResponse {
    /// Row indices of the matrix
    pub offsets_row: Vec<u64>,
    /// Column indices of the matrix
    pub offsets_col: Vec<u64>,
    /// Scores associated with matrix coordinates
    pub scores: Vec<ScoreType>,
    /// Ids of the points in order
    pub ids: Vec<PointIdType>,
}

#[derive(Debug, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
/// Pair of points (a, b) with score
pub struct SearchMatrixPair {
    pub a: PointIdType,
    pub b: PointIdType,
    pub score: ScoreType,
}

impl SearchMatrixPair {
    pub fn new(a: impl Into<PointIdType>, b: impl Into<PointIdType>, score: ScoreType) -> Self {
        Self {
            a: a.into(),
            b: b.into(),
            score,
        }
    }
}

#[derive(Debug, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct SearchMatrixPairsResponse {
    /// List of pairs of points with scores
    pub pairs: Vec<SearchMatrixPair>,
}

#[derive(Debug, JsonSchema, Serialize, Deserialize, Validate)]
pub struct FacetRequestInternal {
    /// Payload key to use for faceting.
    pub key: JsonPath,

    /// Max number of hits to return. Default is 10.
    #[validate(range(min = 1))]
    pub limit: Option<usize>,

    /// Filter conditions - only consider points that satisfy these conditions.
    pub filter: Option<Filter>,

    /// Whether to do a more expensive exact count for each of the values in the facet. Default is false.
    pub exact: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct FacetRequest {
    #[validate(nested)]
    #[serde(flatten)]
    pub facet_request: FacetRequestInternal,

    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum FacetValue {
    String(String),
    Integer(IntPayloadType),
    Bool(bool),
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FacetValueHit {
    pub value: FacetValue,
    pub count: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FacetResponse {
    pub hits: Vec<FacetValueHit>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct PointStruct {
    /// Point id
    pub id: PointIdType,
    /// Vectors
    #[serde(alias = "vectors")]
    #[validate(nested)]
    pub vector: VectorStruct,
    /// Payload values (optional)
    pub payload: Option<Payload>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Validate, JsonSchema)]
pub struct PointsBatch {
    #[validate(nested)]
    pub batch: Batch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct PointVectors {
    /// Point id
    pub id: PointIdType,
    /// Vectors
    #[serde(alias = "vectors")]
    pub vector: VectorStruct,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct UpdateVectors {
    /// Points with named vectors
    #[validate(nested)]
    #[validate(length(min = 1, message = "must specify points to update"))]
    pub points: Vec<PointVectors>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema, Validate)]
pub struct PointsList {
    #[validate(nested)]
    pub points: Vec<PointStruct>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

impl<'de> serde::Deserialize<'de> for PointInsertOperations {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Object(map) => {
                if map.contains_key("batch") {
                    PointsBatch::deserialize(serde_json::Value::Object(map))
                        .map(PointInsertOperations::PointsBatch)
                        .map_err(serde::de::Error::custom)
                } else if map.contains_key("points") {
                    PointsList::deserialize(serde_json::Value::Object(map))
                        .map(PointInsertOperations::PointsList)
                        .map_err(serde::de::Error::custom)
                } else {
                    Err(serde::de::Error::custom(
                        "Invalid PointInsertOperations format",
                    ))
                }
            }
            _ => Err(serde::de::Error::custom(
                "Invalid PointInsertOperations format",
            )),
        }
    }
}

#[derive(Debug, Serialize, Clone, JsonSchema)]
#[serde(untagged)]
pub enum PointInsertOperations {
    /// Inset points from a batch.
    PointsBatch(PointsBatch),
    /// Insert points from a list
    PointsList(PointsList),
}

impl Validate for PointInsertOperations {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            PointInsertOperations::PointsBatch(batch) => batch.validate(),
            PointInsertOperations::PointsList(list) => list.validate(),
        }
    }
}

impl PointInsertOperations {
    /// Amonut of vectors in the operation request.
    pub fn len(&self) -> usize {
        match self {
            PointInsertOperations::PointsBatch(batch) => batch.batch.ids.len(),
            PointInsertOperations::PointsList(list) => list.points.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
