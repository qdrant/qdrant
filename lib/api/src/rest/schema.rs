use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use common::types::ScoreType;
use common::validation::validate_multi_vector;
use ordered_float::NotNan;
use schemars::JsonSchema;
use segment::common::utils::MaybeOneOrMany;
use segment::data_types::index::{StemmingAlgorithm, StopwordsInterface, TokenizerType};
use segment::data_types::order_by::OrderBy;
use segment::json_path::JsonPath;
use segment::types::{
    Condition, Filter, GeoPoint, IntPayloadType, Payload, PointIdType, SearchParams, ShardKey,
    VectorNameBuf, WithPayloadInterface, WithVector,
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
    Sparse(SparseVector),
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
    Sparse(SparseVector),
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

fn named_vector_example() -> HashMap<VectorNameBuf, Vector> {
    let mut map = HashMap::new();
    map.insert(
        "image-embeddings".into(),
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
    Named(HashMap<VectorNameBuf, Vector>),
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
    Named(HashMap<VectorNameBuf, VectorOutput>),
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

/// Configuration of the local bm25 models.
#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema, PartialEq, Eq)]
pub struct Bm25Config {
    /// Controls term frequency saturation. Higher values mean term frequency has more impact.
    /// Default is 1.2
    #[serde(default = "default_k")]
    pub k: NotNan<f64>,
    #[serde(default = "default_b")]
    /// Controls document length normalization. Ranges from 0 (no normalization) to 1 (full normalization).
    /// Higher values mean longer documents have less impact.
    /// Default is 0.75.
    pub b: NotNan<f64>,
    #[serde(default = "default_avg_len")]
    /// Expected average document length in the collection. Default is 256.
    pub avg_len: NotNan<f64>,
    /// Tokenizer type to use for text preprocessing.
    #[serde(default)]
    pub tokenizer: TokenizerType,
    #[serde(default, flatten)]
    pub text_preprocessing_config: TextPreprocessingConfig,
}

impl Default for Bm25Config {
    fn default() -> Self {
        Self {
            k: default_k(),
            b: default_b(),
            avg_len: default_avg_len(),
            tokenizer: TokenizerType::default(),
            text_preprocessing_config: TextPreprocessingConfig::default(),
        }
    }
}

const fn default_k() -> NotNan<f64> {
    unsafe { NotNan::new_unchecked(1.2) }
}

const fn default_b() -> NotNan<f64> {
    unsafe { NotNan::new_unchecked(0.75) }
}

const fn default_avg_len() -> NotNan<f64> {
    unsafe { NotNan::new_unchecked(256.0) }
}

/// Bm25 tokenizer configurations.
#[derive(Debug, Deserialize, Serialize, Clone, Default, JsonSchema, PartialEq, Eq)]
pub struct TextPreprocessingConfig {
    /// Defines which language to use for text preprocessing.
    /// This parameter is used to construct default stopwords filter and stemmer.
    /// To disable language-specific processing, set this to `"language": "none"`.
    /// If not specified, English is assumed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Lowercase the text before tokenization.
    /// Default is `true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lowercase: Option<bool>,
    /// If true, normalize tokens by folding accented characters to ASCII (e.g., "ação" -> "acao").
    /// Default is `false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ascii_folding: Option<bool>,
    /// Configuration of the stopwords filter. Supports list of pre-defined languages and custom stopwords.
    /// Default: initialized for specified `language` or English if not specified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopwords: Option<StopwordsInterface>,
    /// Configuration of the stemmer. Processes tokens to their root form.
    /// Default: initialized Snowball stemmer for specified `language` or English if not specified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stemmer: Option<StemmingAlgorithm>,
    /// Minimum token length to keep. If token is shorter than this, it will be discarded.
    /// Default is `None`, which means no minimum length.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_token_len: Option<usize>,
    /// Maximum token length to keep. If token is longer than this, it will be discarded.
    /// Default is `None`, which means no maximum length.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_token_len: Option<usize>,
}

impl Bm25Config {
    pub fn to_options(&self) -> HashMap<String, Value> {
        debug_assert!(
            false,
            "this code should never be called, it is only for schema generation",
        );

        let value = serde_json::to_value(self)
            .expect("conversion of internal structure to JSON should never fail");

        match value {
            Value::Null
            | Value::Bool(_)
            | Value::Number(_)
            | Value::String(_)
            | Value::Array(_) => HashMap::default(), // not expected
            Value::Object(map) => map.into_iter().collect(),
        }
    }
}

/// Option variants for text documents.
/// Ether general-purpose options or BM25-specific options.
/// BM25-specific will only take effect if the `qdrant/bm25` is specified as a model.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum DocumentOptions {
    // This option should go first
    Common(HashMap<String, Value>),
    // This should never be deserialized into, but we keep it for schema generation
    Bm25(Bm25Config),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_options_should_deserialize_into_common() {
        let json = Bm25Config {
            tokenizer: TokenizerType::Word,
            ..Default::default()
        };
        let valid_bm25_config = serde_json::to_string(&json).unwrap();
        let options: DocumentOptions = serde_json::from_str(&valid_bm25_config).unwrap();
        // Bm25 option is used only for schema, actual deserialization will happen in specialized code
        assert!(matches!(options, DocumentOptions::Common(_)));
    }
}

impl DocumentOptions {
    pub fn into_options(self) -> HashMap<String, Value> {
        match self {
            DocumentOptions::Common(options) => options,
            DocumentOptions::Bm25(bm25) => bm25.to_options(),
        }
    }
}

impl Hash for DocumentOptions {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let options = match self {
            DocumentOptions::Common(options) => Cow::Borrowed(options),
            DocumentOptions::Bm25(bm25) => Cow::Owned(bm25.to_options()),
        };

        // Order of keys in the map should not affect the hash
        let mut keys: Vec<_> = options.keys().collect();
        keys.sort();
        for key in keys {
            key.hash(state);
            options.get(key).unwrap().hash(state);
        }
    }
}

/// WARN: Work-in-progress, unimplemented
///
/// Text document for embedding. Requires inference infrastructure, unimplemented.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema, Hash, Validate)]
pub struct Document {
    /// Text of the document.
    /// This field will be used as input for the embedding model.
    #[schemars(example = "document_text_example")]
    pub text: String,
    /// Name of the model used to generate the vector.
    /// List of available models depends on a provider.
    #[validate(length(min = 1))]
    #[schemars(length(min = 1), example = "model_example")]
    pub model: String,
    /// Additional options for the model, will be passed to the inference service as-is.
    /// See model cards for available options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<DocumentOptions>,
}

/// WARN: Work-in-progress, unimplemented
///
/// Image object for embedding. Requires inference infrastructure, unimplemented.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema, Validate, Hash)]
pub struct Image {
    /// Image data: base64 encoded image or an URL
    #[schemars(example = "image_value_example")]
    pub image: Value,
    /// Name of the model used to generate the vector.
    /// List of available models depends on a provider.
    #[validate(length(min = 1))]
    #[schemars(length(min = 1), example = "image_model_example")]
    pub model: String,
    /// Parameters for the model.
    /// Values of the parameters are model-specific.
    #[serde(flatten)]
    pub options: Options,
}

/// WARN: Work-in-progress, unimplemented
///
/// Custom object for embedding. Requires inference infrastructure, unimplemented.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema, Hash, Validate)]
pub struct InferenceObject {
    /// Arbitrary data, used as input for the embedding model.
    /// Used if the model requires more than one input or a custom input.
    pub object: Value,
    /// Name of the model used to generate the vector.
    /// List of available models depends on a provider.
    #[validate(length(min = 1))]
    #[schemars(length(min = 1), example = "model_example")]
    pub model: String,
    /// Parameters for the model.
    /// Values of the parameters are model-specific.
    #[serde(flatten)]
    pub options: Options,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum BatchVectorStruct {
    Single(Vec<DenseVector>),
    MultiDense(Vec<MultiDenseVector>),
    Named(HashMap<VectorNameBuf, Vec<Vector>>),
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

#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema, PartialEq, Hash)]
#[serde(untagged)]
#[serde(expecting = "Expected a string or an integer")]
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

#[derive(Deserialize, Serialize, JsonSchema, Clone, Debug, PartialEq, Hash)]
#[serde(untagged)]
#[serde(expecting = "Expected a string, or an object with a key, direction and/or start_from")]
pub enum OrderByInterface {
    Key(JsonPath),
    Struct(OrderBy),
}

/// Fusion algorithm allows to combine results of multiple prefetches.
///
/// Available fusion algorithms:
///
/// * `rrf` - Reciprocal Rank Fusion (with default parameters)
/// * `dbsf` - Distribution-Based Score Fusion
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Fusion {
    Rrf,
    Dbsf,
}

/// Parameters for Reciprocal Rank Fusion
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct Rrf {
    /// K parameter for reciprocal rank fusion
    #[validate(range(min = 1))]
    #[serde(default)]
    pub k: Option<usize>,
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
    pub using: Option<VectorNameBuf>,

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
    #[serde(alias = "with_vectors")]
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
#[serde(expecting = "Expected some form of vector, id, or a type of query")]
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

    /// Apply reciprocal rank fusion to multiple prefetches
    Rrf(RrfQuery),

    /// Score boosting via an arbitrary formula
    Formula(FormulaQuery),

    /// Sample points from the collection, non-deterministically.
    Sample(SampleQuery),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct NearestQuery {
    /// The vector to search for nearest neighbors.
    #[validate(nested)]
    pub nearest: VectorInput,

    /// Perform MMR (Maximal Marginal Relevance) reranking after search,
    /// using the same vector in this query to calculate relevance.
    #[validate(nested)]
    pub mmr: Option<Mmr>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct RecommendQuery {
    #[validate(nested)]
    pub recommend: RecommendInput,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct DiscoverQuery {
    #[validate(nested)]
    pub discover: DiscoverInput,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct ContextQuery {
    #[validate(nested)]
    pub context: ContextInput,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct OrderByQuery {
    #[validate(nested)]
    pub order_by: OrderByInterface,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct FusionQuery {
    #[validate(nested)]
    pub fusion: Fusion,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct RrfQuery {
    #[validate(nested)]
    pub rrf: Rrf,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct FormulaQuery {
    pub formula: Expression,

    #[serde(default)]
    pub defaults: HashMap<String, Value>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct SampleQuery {
    #[validate(nested)]
    pub sample: Sample,
}

/// Maximal Marginal Relevance (MMR) algorithm for re-ranking the points.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct Mmr {
    /// Tunable parameter for the MMR algorithm.
    /// Determines the balance between diversity and relevance.
    ///
    /// A higher value favors diversity (dissimilarity to selected results),
    /// while a lower value favors relevance (similarity to the query vector).
    ///
    /// Must be in the range [0, 1].
    /// Default value is 0.5.
    #[validate(range(min = 0.0, max = 1.0))]
    pub diversity: Option<f32>,

    /// The maximum number of candidates to consider for re-ranking.
    ///
    /// If not specified, the `limit` value is used.
    #[validate(range(max = 16_384))] // artificial maximum, to avoid too expensive query.
    pub candidates_limit: Option<usize>,
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
    pub using: Option<VectorNameBuf>,

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
///
/// * `sum_scores` - Uses custom search objective. Compares against all inputs, sums all the scores.
///   Scores against positive vectors are added, against negatives are subtracted.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum RecommendStrategy {
    #[default]
    AverageVector,
    BestScore,
    SumScores,
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
#[serde(untagged)]
pub enum Expression {
    Constant(f32),
    Variable(String),
    Condition(Box<Condition>),
    GeoDistance(GeoDistance),
    Datetime(DatetimeExpression),
    DatetimeKey(DatetimeKeyExpression),
    Mult(MultExpression),
    Sum(SumExpression),
    Neg(NegExpression),
    Abs(AbsExpression),
    Div(DivExpression),
    Sqrt(SqrtExpression),
    Pow(PowExpression),
    Exp(ExpExpression),
    Log10(Log10Expression),
    Ln(LnExpression),
    LinDecay(LinDecayExpression),
    ExpDecay(ExpDecayExpression),
    GaussDecay(GaussDecayExpression),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GeoDistance {
    pub geo_distance: GeoDistanceParams,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GeoDistanceParams {
    /// The origin geo point to measure from
    pub origin: GeoPoint,
    /// Payload field with the destination geo point
    pub to: JsonPath,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DatetimeExpression {
    pub datetime: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DatetimeKeyExpression {
    pub datetime_key: JsonPath,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct MultExpression {
    #[validate(nested)]
    pub mult: Vec<Expression>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct SumExpression {
    #[validate(nested)]
    pub sum: Vec<Expression>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct NegExpression {
    #[validate(nested)]
    pub neg: Box<Expression>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct AbsExpression {
    #[validate(nested)]
    pub abs: Box<Expression>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct DivExpression {
    #[validate(nested)]
    pub div: DivParams,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct DivParams {
    #[validate(nested)]
    pub left: Box<Expression>,
    #[validate(nested)]
    pub right: Box<Expression>,
    pub by_zero_default: Option<ScoreType>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct SqrtExpression {
    #[validate(nested)]
    pub sqrt: Box<Expression>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct PowExpression {
    pub pow: PowParams,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct PowParams {
    #[validate(nested)]
    pub base: Box<Expression>,
    #[validate(nested)]
    pub exponent: Box<Expression>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct ExpExpression {
    #[validate(nested)]
    pub exp: Box<Expression>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct Log10Expression {
    #[validate(nested)]
    pub log10: Box<Expression>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct LnExpression {
    #[validate(nested)]
    pub ln: Box<Expression>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct LinDecayExpression {
    #[validate(nested)]
    pub lin_decay: DecayParamsExpression,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct ExpDecayExpression {
    #[validate(nested)]
    pub exp_decay: DecayParamsExpression,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct GaussDecayExpression {
    #[validate(nested)]
    pub gauss_decay: DecayParamsExpression,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Validate)]
pub struct DecayParamsExpression {
    /// The variable to decay.
    #[validate(nested)]
    pub x: Box<Expression>,
    /// The target value to start decaying from. Defaults to 0.
    #[validate(nested)]
    pub target: Option<Box<Expression>>,
    /// The scale factor of the decay, in terms of `x`. Defaults to 1.0. Must be a non-zero positive number.
    pub scale: Option<f32>,
    /// The midpoint of the decay. Should be between 0 and 1.Defaults to 0.5. Output will be this value when `|x - target| == scale`.
    pub midpoint: Option<f32>,
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
    pub vector: Option<VectorNameBuf>,

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
    pub using: Option<VectorNameBuf>,

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
    pub using: Option<VectorNameBuf>,
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
    #[validate(nested)]
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

    /// If specified, only points that match this filter will be updated, others will be inserted
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(nested)]
    pub update_filter: Option<Filter>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(nested)]
    pub update_filter: Option<Filter>,
}

#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema, Validate)]
pub struct PointsList {
    #[validate(nested)]
    pub points: Vec<PointStruct>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
    /// If specified, only points that match this filter will be updated, others will be inserted
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(nested)]
    pub update_filter: Option<Filter>,
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
    /// Insert points from a batch.
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
    /// Amount of vectors in the operation request.
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

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Hash, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MaxOptimizationThreadsSetting {
    #[default]
    Auto,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Hash, JsonSchema)]
#[serde(untagged)]
pub enum MaxOptimizationThreads {
    Setting(MaxOptimizationThreadsSetting),
    Threads(usize),
}

impl Default for MaxOptimizationThreads {
    fn default() -> Self {
        MaxOptimizationThreads::Setting(MaxOptimizationThreadsSetting::Auto)
    }
}

impl From<MaxOptimizationThreads> for Option<usize> {
    fn from(value: MaxOptimizationThreads) -> Self {
        match value {
            MaxOptimizationThreads::Setting(MaxOptimizationThreadsSetting::Auto) => None,
            MaxOptimizationThreads::Threads(threads) => Some(threads),
        }
    }
}
