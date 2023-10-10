use std::backtrace::Backtrace;
use std::collections::{BTreeMap, HashMap};
use std::error::Error as _;
use std::fmt::Write as _;
use std::iter;
use std::num::NonZeroU64;
use std::time::SystemTimeError;

use api::grpc::transport_channel_pool::RequestError;
use common::types::ScoreType;
use common::validation::validate_range_generic;
use io::file_operations::FileStorageError;
use itertools::Itertools;
use merge::Merge;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::operation_error::OperationError;
use segment::data_types::groups::GroupId;
use segment::data_types::vectors::{
    Named, NamedRecoQuery, NamedVectorStruct, QueryVector, VectorElementType, VectorStruct,
    VectorType, DEFAULT_VECTOR_NAME,
};
use segment::types::{
    Distance, Filter, Payload, PayloadIndexInfo, PayloadKeyType, PointIdType, QuantizationConfig,
    ScoredPoint, SearchParams, SeqNumberType, WithPayloadInterface, WithVector,
};
use serde;
use serde::{Deserialize, Serialize};
use serde_json::Error as JsonError;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError as OneshotRecvError;
use tokio::task::JoinError;
use tonic::codegen::http::uri::InvalidUri;
use validator::{Validate, ValidationError, ValidationErrors};

use super::config_diff;
use crate::config::{CollectionConfig, CollectionParams};
use crate::lookup::types::WithLookupInterface;
use crate::operations::config_diff::{HnswConfigDiff, QuantizationConfigDiff};
use crate::save_on_disk;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId, ShardKey};
use crate::shards::transfer::shard_transfer::ShardTransferMethod;
use crate::wal::WalError;

/// Current state of the collection.
/// `Green` - all good. `Yellow` - optimization is running, `Red` - some operations failed and was not recovered
#[derive(
    Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Copy, Clone,
)]
#[serde(rename_all = "snake_case")]
pub enum CollectionStatus {
    // Collection if completely ready for requests
    Green,
    // Collection is available, but some segments might be under optimization
    Yellow,
    // Something is not OK:
    // - some operations failed and was not recovered
    Red,
}

/// Current state of the collection
#[derive(
    Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Clone,
)]
#[serde(rename_all = "snake_case")]
pub enum OptimizersStatus {
    /// Optimizers are reporting as expected
    #[default]
    Ok,
    /// Something wrong happened with optimizers
    Error(String),
}

/// Point data
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct Record {
    /// Id of the point
    pub id: PointIdType,
    /// Payload - values assigned to the point
    pub payload: Option<Payload>,
    /// Vector of the point
    pub vector: Option<VectorStruct>,
}

/// Current statistics and configuration of the collection
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct CollectionInfo {
    /// Status of the collection
    pub status: CollectionStatus,
    /// Status of optimizers
    pub optimizer_status: OptimizersStatus,
    /// Number of vectors in collection
    /// All vectors in collection are available for querying
    /// Calculated as `points_count x vectors_per_point`
    /// Where `vectors_per_point` is a number of named vectors in schema
    pub vectors_count: usize,
    /// Number of indexed vectors in the collection.
    /// Indexed vectors in large segments are faster to query,
    /// as it is stored in vector index (HNSW)
    pub indexed_vectors_count: usize,
    /// Number of points (vectors + payloads) in collection
    /// Each point could be accessed by unique id
    pub points_count: usize,
    /// Number of segments in collection.
    /// Each segment has independent vector as payload indexes
    pub segments_count: usize,
    /// Collection settings
    #[validate]
    pub config: CollectionConfig,
    /// Types of stored payload
    pub payload_schema: HashMap<PayloadKeyType, PayloadIndexInfo>,
}

impl CollectionInfo {
    pub fn empty(collection_config: CollectionConfig) -> Self {
        Self {
            status: CollectionStatus::Green,
            optimizer_status: OptimizersStatus::Ok,
            vectors_count: 0,
            indexed_vectors_count: 0,
            points_count: 0,
            segments_count: 0,
            config: collection_config,
            payload_schema: HashMap::new(),
        }
    }
}

/// Current clustering distribution for the collection
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CollectionClusterInfo {
    /// ID of this peer
    pub peer_id: PeerId,
    /// Total number of shards
    pub shard_count: usize,
    /// Local shards
    pub local_shards: Vec<LocalShardInfo>,
    /// Remote shards
    pub remote_shards: Vec<RemoteShardInfo>,
    /// Shard transfers
    pub shard_transfers: Vec<ShardTransferInfo>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct ShardTransferInfo {
    pub shard_id: ShardId,
    pub from: PeerId,
    pub to: PeerId,
    /// If `true` transfer is a synchronization of a replicas
    /// If `false` transfer is a moving of a shard from one peer to another
    pub sync: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<ShardTransferMethod>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct LocalShardInfo {
    /// Local shard id
    pub shard_id: ShardId,
    /// User-defined sharding key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKey>,
    /// Number of points in the shard
    pub points_count: usize,
    /// Is replica active
    pub state: ReplicaState,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct RemoteShardInfo {
    /// Remote shard id
    pub shard_id: ShardId,
    /// User-defined sharding key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKey>,
    /// Remote peer id
    pub peer_id: PeerId,
    /// Is replica active
    pub state: ReplicaState,
}

/// `Acknowledged` - Request is saved to WAL and will be process in a queue.
/// `Completed` - Request is completed, changes are actual.
#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStatus {
    Acknowledged,
    Completed,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct UpdateResult {
    /// Sequential number of the operation
    pub operation_id: SeqNumberType,
    /// Update status
    pub status: UpdateStatus,
}

/// Scroll request - paginate over all points which matches given condition
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ScrollRequest {
    /// Start ID to read points from.
    pub offset: Option<PointIdType>,
    /// Page size. Default: 10
    #[validate(range(min = 1))]
    pub limit: Option<usize>,
    /// Look only for points which satisfies this conditions. If not provided - all points.
    #[validate]
    pub filter: Option<Filter>,
    /// Select which payload to return with the response. Default: All
    pub with_payload: Option<WithPayloadInterface>,
    /// Whether to return the point vector with the result?
    #[serde(default, alias = "with_vectors")]
    pub with_vector: WithVector,
}

impl Default for ScrollRequest {
    fn default() -> Self {
        ScrollRequest {
            offset: None,
            limit: Some(10),
            filter: None,
            with_payload: Some(WithPayloadInterface::Bool(true)),
            with_vector: WithVector::Bool(false),
        }
    }
}

/// Result of the points read request
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ScrollResult {
    /// List of retrieved points
    pub points: Vec<Record>,
    /// Offset which should be used to retrieve a next page result
    pub next_page_offset: Option<PointIdType>,
}

/// Search request.
/// Holds all conditions and parameters for the search of most similar points by vector similarity
/// given the filtering restrictions.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SearchRequest {
    /// Look for vectors closest to this
    pub vector: NamedVectorStruct,
    /// Look only for points which satisfies this conditions
    #[validate]
    pub filter: Option<Filter>,
    /// Additional search params
    #[validate]
    pub params: Option<SearchParams>,
    /// Max number of result to return
    #[serde(alias = "top")]
    #[validate(range(min = 1))]
    pub limit: usize,
    /// Offset of the first result to return.
    /// May be used to paginate results.
    /// Note: large offset values may cause performance issues.
    #[serde(default)]
    pub offset: usize,
    /// Select which payload to return with the response. Default: None
    pub with_payload: Option<WithPayloadInterface>,
    /// Whether to return the point vector with the result?
    #[serde(default, alias = "with_vectors")]
    pub with_vector: Option<WithVector>,
    /// Define a minimal score threshold for the result.
    /// If defined, less similar results will not be returned.
    /// Score of the returned result might be higher or smaller than the threshold depending on the
    /// Distance function used. E.g. for cosine similarity only higher scores will be returned.
    pub score_threshold: Option<ScoreType>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SearchRequestBatch {
    #[validate]
    pub searches: Vec<SearchRequest>,
}

#[derive(Debug, Clone)]
pub enum QueryEnum {
    Nearest(NamedVectorStruct),
    RecommendBestScore(NamedRecoQuery),
}

impl QueryEnum {
    pub fn get_vector_name(&self) -> &str {
        match self {
            QueryEnum::Nearest(vector) => vector.get_name(),
            QueryEnum::RecommendBestScore(reco_query) => reco_query.get_name(),
        }
    }
}

impl From<Vec<VectorElementType>> for QueryEnum {
    fn from(vector: Vec<VectorElementType>) -> Self {
        QueryEnum::Nearest(NamedVectorStruct::Default(vector))
    }
}

impl AsRef<QueryEnum> for QueryEnum {
    fn as_ref(&self) -> &QueryEnum {
        self
    }
}

#[derive(Debug, Clone)]
pub struct CoreSearchRequest {
    /// Every kind of query that can be performed on segment level
    pub query: QueryEnum,
    /// Look only for points which satisfies this conditions
    pub filter: Option<Filter>,
    /// Additional search params
    pub params: Option<SearchParams>,
    /// Max number of result to return
    pub limit: usize,
    /// Offset of the first result to return.
    /// May be used to paginate results.
    /// Note: large offset values may cause performance issues.
    pub offset: usize,
    /// Select which payload to return with the response. Default: None
    pub with_payload: Option<WithPayloadInterface>,
    /// Whether to return the point vector with the result?
    pub with_vector: Option<WithVector>,
    pub score_threshold: Option<ScoreType>,
}

#[derive(Debug, Clone)]
pub struct CoreSearchRequestBatch {
    pub searches: Vec<CoreSearchRequest>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct SearchGroupsRequest {
    /// Look for vectors closest to this
    pub vector: NamedVectorStruct,

    /// Look only for points which satisfies this conditions
    #[validate]
    pub filter: Option<Filter>,

    /// Additional search params
    #[validate]
    pub params: Option<SearchParams>,

    /// Select which payload to return with the response. Default: None
    pub with_payload: Option<WithPayloadInterface>,

    /// Whether to return the point vector with the result?
    #[serde(default, alias = "with_vectors")]
    pub with_vector: Option<WithVector>,

    /// Define a minimal score threshold for the result.
    /// If defined, less similar results will not be returned.
    /// Score of the returned result might be higher or smaller than the threshold depending on the
    /// Distance function used. E.g. for cosine similarity only higher scores will be returned.
    pub score_threshold: Option<ScoreType>,

    #[serde(flatten)]
    #[validate]
    pub group_request: BaseGroupRequest,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct PointRequest {
    /// Look for points with ids
    pub ids: Vec<PointIdType>,
    /// Select which payload to return with the response. Default: All
    pub with_payload: Option<WithPayloadInterface>,
    /// Whether to return the point vector with the result?
    #[serde(default, alias = "with_vectors")]
    pub with_vector: WithVector,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(untagged)]
pub enum RecommendExample {
    PointId(PointIdType),
    Vector(VectorType),
}

impl RecommendExample {
    pub fn as_point_id(&self) -> Option<PointIdType> {
        match self {
            RecommendExample::PointId(id) => Some(*id),
            _ => None,
        }
    }
}

impl From<u64> for RecommendExample {
    fn from(id: u64) -> Self {
        RecommendExample::PointId(id.into())
    }
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case", untagged)]
pub enum UsingVector {
    Name(String),
}

impl From<String> for UsingVector {
    fn from(name: String) -> Self {
        UsingVector::Name(name)
    }
}

/// Defines a location to use for looking up the vector.
/// Specifies collection and vector field name.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct LookupLocation {
    /// Name of the collection used for lookup
    pub collection: String,
    /// Optional name of the vector field within the collection.
    /// If not provided, the default vector field will be used.
    #[serde(default)]
    pub vector: Option<String>,
}

/// Recommendation request.
/// Provides positive and negative examples of the vectors, which can be ids of points that
/// are already stored in the collection, raw vectors, or even ids and vectors combined.
///
/// Service should look for the points which are closer to positive examples and at the same time
/// further to negative examples. The concrete way of how to compare negative and positive distances
/// is up to the `strategy` chosen.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub struct RecommendRequest {
    /// Look for vectors closest to those
    #[serde(default)]
    pub positive: Vec<RecommendExample>,

    /// Try to avoid vectors like this
    #[serde(default)]
    pub negative: Vec<RecommendExample>,

    /// How to use positive and negative examples to find the results
    pub strategy: Option<RecommendStrategy>,

    /// Look only for points which satisfies this conditions
    #[validate]
    pub filter: Option<Filter>,

    /// Additional search params
    #[validate]
    pub params: Option<SearchParams>,

    /// Max number of result to return
    #[serde(alias = "top")]
    #[validate(range(min = 1))]
    pub limit: usize,

    /// Offset of the first result to return.
    /// May be used to paginate results.
    /// Note: large offset values may cause performance issues.
    #[serde(default)]
    pub offset: usize,

    /// Select which payload to return with the response. Default: None
    pub with_payload: Option<WithPayloadInterface>,

    /// Whether to return the point vector with the result?
    #[serde(default, alias = "with_vectors")]
    pub with_vector: Option<WithVector>,

    /// Define a minimal score threshold for the result.
    /// If defined, less similar results will not be returned.
    /// Score of the returned result might be higher or smaller than the threshold depending on the
    /// Distance function used. E.g. for cosine similarity only higher scores will be returned.
    pub score_threshold: Option<ScoreType>,

    /// Define which vector to use for recommendation, if not specified - try to use default vector
    #[serde(default)]
    pub using: Option<UsingVector>,

    /// The location used to lookup vectors. If not specified - use current collection.
    /// Note: the other collection should have the same vector size as the current collection
    #[serde(default)]
    pub lookup_from: Option<LookupLocation>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct RecommendRequestBatch {
    #[validate]
    pub searches: Vec<RecommendRequest>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct RecommendGroupsRequest {
    /// Look for vectors closest to those
    #[serde(default)]
    pub positive: Vec<RecommendExample>,

    /// Try to avoid vectors like this
    #[serde(default)]
    pub negative: Vec<RecommendExample>,

    /// How to use positive and negative examples to find the results
    #[serde(default)]
    pub strategy: Option<RecommendStrategy>,

    /// Look only for points which satisfies this conditions
    #[validate]
    pub filter: Option<Filter>,

    /// Additional search params
    #[validate]
    pub params: Option<SearchParams>,

    /// Select which payload to return with the response. Default: None
    pub with_payload: Option<WithPayloadInterface>,

    /// Whether to return the point vector with the result?
    #[serde(default, alias = "with_vectors")]
    pub with_vector: Option<WithVector>,

    /// Define a minimal score threshold for the result.
    /// If defined, less similar results will not be returned.
    /// Score of the returned result might be higher or smaller than the threshold depending on the
    /// Distance function used. E.g. for cosine similarity only higher scores will be returned.
    pub score_threshold: Option<ScoreType>,

    /// Define which vector to use for recommendation, if not specified - try to use default vector
    #[serde(default)]
    pub using: Option<UsingVector>,

    /// The location used to lookup vectors. If not specified - use current collection.
    /// Note: the other collection should have the same vector size as the current collection
    #[serde(default)]
    pub lookup_from: Option<LookupLocation>,

    #[serde(flatten)]
    pub group_request: BaseGroupRequest,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct PointGroup {
    /// Scored points that have the same value of the group_by key
    pub hits: Vec<ScoredPoint>,
    /// Value of the group_by key, shared across all the hits in the group
    pub id: GroupId,
    /// Record that has been looked up using the group id
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lookup: Option<Record>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GroupsResult {
    pub groups: Vec<PointGroup>,
}

/// Count Request
/// Counts the number of points which satisfy the given filter.
/// If filter is not provided, the count of all points in the collection will be returned.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct CountRequest {
    /// Look only for points which satisfies this conditions
    #[validate]
    pub filter: Option<Filter>,
    /// If true, count exact number of points. If false, count approximate number of points faster.
    /// Approximate count might be unreliable during the indexing process. Default: true
    #[serde(default = "default_exact_count")]
    pub exact: bool,
}

pub const fn default_exact_count() -> bool {
    true
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CountResult {
    /// Number of points which satisfy the conditions
    pub count: usize,
}

#[derive(Error, Debug, Clone)]
#[error("{0}")]
pub enum CollectionError {
    #[error("Wrong input: {description}")]
    BadInput { description: String },
    #[error("{what} not found")]
    NotFound { what: String },
    #[error("No point with id {missed_point_id} found")]
    PointNotFound { missed_point_id: PointIdType },
    #[error("Service internal error: {error}")]
    ServiceError {
        error: String,
        backtrace: Option<String>,
    },
    #[error("Bad request: {description}")]
    BadRequest { description: String },
    #[error("Operation Cancelled: {description}")]
    Cancelled { description: String },
    #[error("Bad shard selection: {description}")]
    BadShardSelection { description: String },
    #[error(
    "{shards_failed} out of {shards_total} shards failed to apply operation. First error captured: {first_err}"
    )]
    InconsistentShardFailure {
        shards_total: u32,
        shards_failed: u32,
        first_err: Box<CollectionError>,
    },
    #[error("Remote shard on {peer_id} failed during forward proxy operation: {error}")]
    ForwardProxyError { peer_id: PeerId, error: Box<Self> },
    #[error("Out of memory, free: {free}, {description}")]
    OutOfMemory { description: String, free: u64 },
    #[error("Timeout error: {description}")]
    Timeout { description: String },
}

impl CollectionError {
    pub fn timeout(timeout_sec: usize, operation: impl Into<String>) -> CollectionError {
        CollectionError::Timeout {
            description: format!(
                "Operation '{}' timed out after {timeout_sec} seconds",
                operation.into()
            ),
        }
    }

    pub fn service_error(error: impl Into<String>) -> CollectionError {
        CollectionError::ServiceError {
            error: error.into(),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }

    pub fn bad_input(description: String) -> CollectionError {
        CollectionError::BadInput { description }
    }

    pub fn bad_request(description: String) -> CollectionError {
        CollectionError::BadRequest { description }
    }

    pub fn bad_shard_selection(description: String) -> CollectionError {
        CollectionError::BadShardSelection { description }
    }

    pub fn forward_proxy_error(peer_id: PeerId, error: impl Into<Self>) -> Self {
        Self::ForwardProxyError {
            peer_id,
            error: Box::new(error.into()),
        }
    }

    pub fn remote_peer_id(&self) -> Option<PeerId> {
        match self {
            Self::ForwardProxyError { peer_id, .. } => Some(*peer_id),
            _ => None,
        }
    }

    /// Returns true if the error is transient and the operation can be retried.
    /// Returns false if the error is not transient and the operation should fail on all replicas.
    pub fn is_transient(&self) -> bool {
        match self {
            // Transient
            Self::ServiceError { .. } => true,
            Self::Timeout { .. } => true,
            Self::Cancelled { .. } => true,
            Self::OutOfMemory { .. } => true,
            // Not transient
            Self::BadInput { .. } => false,
            Self::NotFound { .. } => false,
            Self::PointNotFound { .. } => false,
            Self::BadRequest { .. } => false,
            Self::BadShardSelection { .. } => false,
            Self::InconsistentShardFailure { .. } => false,
            Self::ForwardProxyError { .. } => false,
        }
    }
}

impl From<SystemTimeError> for CollectionError {
    fn from(error: SystemTimeError) -> CollectionError {
        CollectionError::ServiceError {
            error: format!("System time error: {error}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<String> for CollectionError {
    fn from(error: String) -> CollectionError {
        CollectionError::ServiceError {
            error,
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<OperationError> for CollectionError {
    fn from(err: OperationError) -> Self {
        match err {
            OperationError::WrongVector { .. } => Self::BadInput {
                description: format!("{err}"),
            },
            OperationError::VectorNameNotExists { .. } => Self::BadInput {
                description: format!("{err}"),
            },
            OperationError::MissedVectorName { .. } => Self::BadInput {
                description: format!("{err}"),
            },
            OperationError::PointIdError { missed_point_id } => {
                Self::PointNotFound { missed_point_id }
            }
            OperationError::ServiceError {
                description,
                backtrace,
            } => Self::ServiceError {
                error: description,
                backtrace,
            },
            OperationError::TypeError { .. } => Self::BadInput {
                description: format!("{err}"),
            },
            OperationError::Cancelled { description } => Self::Cancelled { description },
            OperationError::TypeInferenceError { .. } => Self::BadInput {
                description: format!("{err}"),
            },
            OperationError::OutOfMemory { description, free } => {
                Self::OutOfMemory { description, free }
            }
            OperationError::InconsistentStorage { .. } => Self::ServiceError {
                error: format!("{err}"),
                backtrace: None,
            },
            OperationError::ValidationError { .. } => Self::BadInput {
                description: format!("{err}"),
            },
            OperationError::WrongSparse => Self::BadInput {
                description: "Conversion between sparse and regular vectors failed".to_string(),
            },
        }
    }
}

impl From<OneshotRecvError> for CollectionError {
    fn from(err: OneshotRecvError) -> Self {
        Self::ServiceError {
            error: format!("{err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<JoinError> for CollectionError {
    fn from(err: JoinError) -> Self {
        Self::ServiceError {
            error: format!("{err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<WalError> for CollectionError {
    fn from(err: WalError) -> Self {
        Self::ServiceError {
            error: format!("{err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl<T> From<SendError<T>> for CollectionError {
    fn from(err: SendError<T>) -> Self {
        Self::ServiceError {
            error: format!("Can't reach one of the workers: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<JsonError> for CollectionError {
    fn from(err: JsonError) -> Self {
        CollectionError::ServiceError {
            error: format!("Json error: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<futures::io::Error> for CollectionError {
    fn from(err: futures::io::Error) -> Self {
        CollectionError::ServiceError {
            error: format!("File IO error: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<tonic::transport::Error> for CollectionError {
    fn from(err: tonic::transport::Error) -> Self {
        CollectionError::ServiceError {
            error: format!("Tonic transport error: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<InvalidUri> for CollectionError {
    fn from(err: InvalidUri) -> Self {
        CollectionError::ServiceError {
            error: format!("Invalid URI error: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<tonic::Status> for CollectionError {
    fn from(err: tonic::Status) -> Self {
        match err.code() {
            tonic::Code::InvalidArgument => CollectionError::BadInput {
                description: format!("InvalidArgument: {err}"),
            },
            tonic::Code::AlreadyExists => CollectionError::BadInput {
                description: format!("AlreadyExists: {err}"),
            },
            tonic::Code::NotFound => CollectionError::NotFound {
                what: format!("{err}"),
            },
            tonic::Code::Internal => CollectionError::ServiceError {
                error: format!("Internal error: {err}"),
                backtrace: Some(Backtrace::force_capture().to_string()),
            },
            tonic::Code::DeadlineExceeded => CollectionError::Timeout {
                description: format!("Deadline Exceeded: {err}"),
            },
            tonic::Code::Cancelled => CollectionError::Cancelled {
                description: format!("{err}"),
            },
            _other => CollectionError::ServiceError {
                error: format!("Tonic status error: {err}"),
                backtrace: Some(Backtrace::force_capture().to_string()),
            },
        }
    }
}

impl<Guard> From<std::sync::PoisonError<Guard>> for CollectionError {
    fn from(err: std::sync::PoisonError<Guard>) -> Self {
        CollectionError::ServiceError {
            error: format!("Mutex lock poisoned: {err}"),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<FileStorageError> for CollectionError {
    fn from(err: FileStorageError) -> Self {
        Self::service_error(err.to_string())
    }
}

impl From<RequestError<tonic::Status>> for CollectionError {
    fn from(err: RequestError<tonic::Status>) -> Self {
        match err {
            RequestError::FromClosure(status) => status.into(),
            RequestError::Tonic(err) => {
                let mut msg = err.to_string();
                for src in iter::successors(err.source(), |&src| src.source()) {
                    write!(&mut msg, ": {src}").unwrap();
                }
                CollectionError::service_error(msg)
            }
        }
    }
}

impl From<save_on_disk::Error> for CollectionError {
    fn from(err: save_on_disk::Error) -> Self {
        CollectionError::ServiceError {
            error: err.to_string(),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

impl From<validator::ValidationErrors> for CollectionError {
    fn from(err: validator::ValidationErrors) -> Self {
        CollectionError::BadInput {
            description: format!("{err}"),
        }
    }
}

pub type CollectionResult<T> = Result<T, CollectionError>;

impl Record {
    pub fn vector_names(&self) -> Vec<&str> {
        match &self.vector {
            None => vec![],
            Some(vectors) => match vectors {
                VectorStruct::Single(_) => vec![DEFAULT_VECTOR_NAME],
                VectorStruct::Multi(vectors) => vectors.keys().map(|x| x.as_str()).collect(),
            },
        }
    }

    pub fn get_vector_by_name(&self, name: &str) -> Option<&VectorType> {
        match &self.vector {
            Some(VectorStruct::Single(vector)) => (name == DEFAULT_VECTOR_NAME).then_some(vector),
            Some(VectorStruct::Multi(vectors)) => vectors.get(name),
            None => None,
        }
    }
}

/// Params of single vector data storage
#[derive(Debug, Hash, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct VectorParams {
    /// Size of a vectors used
    #[validate(custom = "validate_nonzerou64_range_min_1_max_65536")]
    pub size: NonZeroU64,
    /// Type of distance function used for measuring distance between vectors
    pub distance: Distance,
    /// Custom params for HNSW index. If none - values from collection configuration are used.
    #[serde(default, skip_serializing_if = "is_hnsw_diff_empty")]
    #[validate]
    pub hnsw_config: Option<HnswConfigDiff>,
    /// Custom params for quantization. If none - values from collection configuration are used.
    #[serde(
        default,
        alias = "quantization",
        skip_serializing_if = "Option::is_none"
    )]
    #[validate]
    pub quantization_config: Option<QuantizationConfig>,
    /// If true, vectors are served from disk, improving RAM usage at the cost of latency
    /// Default: false
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

/// Validate the value is in `[1, 65536]` or `None`.
pub fn validate_nonzerou64_range_min_1_max_65536(
    value: &NonZeroU64,
) -> Result<(), ValidationError> {
    validate_range_generic(value.get(), Some(1), Some(65536))
}

/// Is considered empty if `None` or if diff has no field specified
fn is_hnsw_diff_empty(hnsw_config: &Option<HnswConfigDiff>) -> bool {
    hnsw_config
        .as_ref()
        .and_then(|config| config_diff::is_empty(config).ok())
        .unwrap_or(true)
}

impl Anonymize for VectorParams {
    fn anonymize(&self) -> Self {
        self.clone()
    }
}

/// Vector params separator for single and multiple vector modes
/// Single mode:
///
/// { "size": 128, "distance": "Cosine" }
///
/// or multiple mode:
///
/// {
///      "default": {
///          "size": 128,
///          "distance": "Cosine"
///      }
/// }
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case", untagged)]
pub enum VectorsConfig {
    Single(VectorParams),
    Multi(BTreeMap<String, VectorParams>),
}

impl VectorsConfig {
    pub fn empty() -> Self {
        VectorsConfig::Multi(BTreeMap::new())
    }

    pub fn vectors_num(&self) -> usize {
        match self {
            Self::Single(_) => 1,
            Self::Multi(vectors) => vectors.len(),
        }
    }

    pub fn get_params(&self, name: &str) -> Option<&VectorParams> {
        match self {
            VectorsConfig::Single(params) => (name == DEFAULT_VECTOR_NAME).then_some(params),
            VectorsConfig::Multi(params) => params.get(name),
        }
    }

    pub fn get_params_mut(&mut self, name: &str) -> Option<&mut VectorParams> {
        match self {
            VectorsConfig::Single(params) => (name == DEFAULT_VECTOR_NAME).then_some(params),
            VectorsConfig::Multi(params) => params.get_mut(name),
        }
    }

    /// Iterate over the named vector parameters.
    ///
    /// If this is `Single` it iterates over a single parameter named [`DEFAULT_VECTOR_NAME`].
    pub fn params_iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&str, &VectorParams)> + 'a> {
        match self {
            VectorsConfig::Single(p) => Box::new(std::iter::once((DEFAULT_VECTOR_NAME, p))),
            VectorsConfig::Multi(p) => Box::new(p.iter().map(|(n, p)| (n.as_str(), p))),
        }
    }

    // TODO: Further unify `check_compatible` and `check_compatible_with_segment_config`?
    pub fn check_compatible(&self, other: &Self) -> CollectionResult<()> {
        match (self, other) {
            (Self::Single(_), Self::Single(_)) | (Self::Multi(_), Self::Multi(_)) => (),
            _ => {
                return Err(incompatible_vectors_error(
                    self.params_iter().map(|(name, _)| name),
                    other.params_iter().map(|(name, _)| name),
                ));
            }
        };

        for (vector_name, this) in self.params_iter() {
            let Some(other) = other.get_params(vector_name) else {
                return Err(missing_vector_error(vector_name));
            };

            VectorParamsBase::from(this).check_compatibility(&other.into(), vector_name)?;
        }

        Ok(())
    }

    // TODO: Further unify `check_compatible` and `check_compatible_with_segment_config`?
    pub fn check_compatible_with_segment_config(
        &self,
        other: &HashMap<String, segment::types::VectorDataConfig>,
        exact: bool,
    ) -> CollectionResult<()> {
        if exact && self.vectors_num() != other.len() {
            return Err(incompatible_vectors_error(
                self.params_iter().map(|(name, _)| name),
                other.keys().map(String::as_str),
            ));
        }

        for (vector_name, this) in self.params_iter() {
            let Some(other) = other.get(vector_name) else {
                return Err(missing_vector_error(vector_name));
            };

            VectorParamsBase::from(this).check_compatibility(&other.into(), vector_name)?;
        }

        Ok(())
    }
}

fn incompatible_vectors_error<'a, 'b>(
    this: impl Iterator<Item = &'a str>,
    other: impl Iterator<Item = &'b str>,
) -> CollectionError {
    let this_vectors = this.collect::<Vec<_>>().join(", ");
    let other_vectors = other.collect::<Vec<_>>().join(", ");

    CollectionError::BadInput {
        description: format!(
            "Vectors configuration is not compatible: \
             origin collection have vectors [{}], \
             while other vectors [{}]",
            this_vectors, other_vectors
        ),
    }
}

fn missing_vector_error(vector_name: &str) -> CollectionError {
    CollectionError::BadInput {
        description: format!(
            "Vectors configuration is not compatible: \
             origin collection have vector {}, while other collection does not",
            vector_name
        ),
    }
}

impl Anonymize for VectorsConfig {
    fn anonymize(&self) -> Self {
        match self {
            VectorsConfig::Single(params) => VectorsConfig::Single(params.clone()),
            VectorsConfig::Multi(params) => VectorsConfig::Multi(params.anonymize()),
        }
    }
}

impl Validate for VectorsConfig {
    #[allow(clippy::manual_try_fold)] // `try_fold` can't be used because it shortcuts on Err
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            VectorsConfig::Single(single) => single.validate(),
            VectorsConfig::Multi(multi) => {
                let errors = multi
                    .values()
                    .filter_map(|v| v.validate().err())
                    .fold(Err(ValidationErrors::new()), |bag, err| {
                        ValidationErrors::merge(bag, "?", Err(err))
                    })
                    .unwrap_err();
                errors.errors().is_empty().then_some(()).ok_or(errors)
            }
        }
    }
}

impl From<VectorParams> for VectorsConfig {
    fn from(params: VectorParams) -> Self {
        VectorsConfig::Single(params)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct VectorParamsBase {
    /// Size of a vectors used
    size: usize,
    /// Type of distance function used for measuring distance between vectors
    distance: Distance,
}

impl VectorParamsBase {
    fn check_compatibility(&self, other: &Self, vector_name: &str) -> CollectionResult<()> {
        if self.size != other.size {
            return Err(CollectionError::BadInput {
                description: format!(
                    "Vectors configuration is not compatible: \
                     origin vector {} size: {}, while other vector size: {}",
                    vector_name, self.size, other.size
                ),
            });
        }

        if self.distance != other.distance {
            return Err(CollectionError::BadInput {
                description: format!(
                    "Vectors configuration is not compatible: \
                     origin vector {} distance: {:?}, while other vector distance: {:?}",
                    vector_name, self.distance, other.distance
                ),
            });
        }

        Ok(())
    }
}

impl From<&VectorParams> for VectorParamsBase {
    fn from(params: &VectorParams) -> Self {
        Self {
            size: params.size.get() as _, // TODO!?
            distance: params.distance,
        }
    }
}

impl From<&segment::types::VectorDataConfig> for VectorParamsBase {
    fn from(config: &segment::types::VectorDataConfig) -> Self {
        Self {
            size: config.size,
            distance: config.distance,
        }
    }
}

#[derive(
    Debug, Hash, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq, Merge,
)]
#[serde(rename_all = "snake_case")]
pub struct VectorParamsDiff {
    /// Update params for HNSW index. If empty object - it will be unset.
    #[serde(default, skip_serializing_if = "is_hnsw_diff_empty")]
    #[validate]
    pub hnsw_config: Option<HnswConfigDiff>,
    /// Update params for quantization. If none - it is left unchanged.
    #[serde(
        default,
        alias = "quantization",
        skip_serializing_if = "Option::is_none"
    )]
    #[validate]
    pub quantization_config: Option<QuantizationConfigDiff>,
    /// If true, vectors are served from disk, improving RAM usage at the cost of latency
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

/// Vector update params for multiple vectors
///
/// {
///     "vector_name": {
///         "hnsw_config": { "m": 8 }
///     }
/// }
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
pub struct VectorsConfigDiff(pub BTreeMap<String, VectorParamsDiff>);

impl VectorsConfigDiff {
    /// Check that the vector names in this config are part of the given collection.
    ///
    /// Returns an error if incompatible.
    pub fn check_vector_names(&self, collection: &CollectionParams) -> CollectionResult<()> {
        for vector_name in self.0.keys() {
            collection
                .vectors
                .get_params(vector_name)
                .map(|_| ())
                .ok_or_else(|| OperationError::VectorNameNotExists {
                    received_name: vector_name.into(),
                })?;
        }
        Ok(())
    }
}

impl Validate for VectorsConfigDiff {
    #[allow(clippy::manual_try_fold)] // `try_fold` can't be used because it shortcuts on Err
    fn validate(&self) -> Result<(), ValidationErrors> {
        let errors = self
            .0
            .values()
            .filter_map(|v| v.validate().err())
            .fold(Err(ValidationErrors::new()), |bag, err| {
                ValidationErrors::merge(bag, "?", Err(err))
            })
            .unwrap_err();
        errors.errors().is_empty().then_some(()).ok_or(errors)
    }
}

impl From<VectorParamsDiff> for VectorsConfigDiff {
    fn from(params: VectorParamsDiff) -> Self {
        VectorsConfigDiff(BTreeMap::from([("".into(), params)]))
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct AliasDescription {
    pub alias_name: String,
    pub collection_name: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CollectionsAliasesResponse {
    pub aliases: Vec<AliasDescription>,
}

#[derive(Clone, Debug, Deserialize, Default, Copy, PartialEq)]
pub enum NodeType {
    /// Regular node, participates in the cluster
    #[default]
    Normal,
    /// Node that does only receive data, but is not used for search/read operations
    /// This is useful for nodes that are only used for writing data
    /// and backup purposes
    Listener,
}

#[derive(Validate, Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct BaseGroupRequest {
    /// Payload field to group by, must be a string or number field.
    /// If the field contains more than 1 value, all values will be used for grouping.
    /// One point can be in multiple groups.
    #[validate(length(min = 1))]
    pub group_by: String,

    /// Maximum amount of points to return per group
    #[validate(range(min = 1))]
    pub group_size: u32,

    /// Maximum amount of groups to return
    #[validate(range(min = 1))]
    pub limit: u32,

    /// Look for points in another collection using the group ids
    pub with_lookup: Option<WithLookupInterface>,
}

impl From<SearchRequestBatch> for CoreSearchRequestBatch {
    fn from(batch: SearchRequestBatch) -> Self {
        CoreSearchRequestBatch {
            searches: batch.searches.into_iter().map_into().collect(),
        }
    }
}

impl From<SearchRequest> for CoreSearchRequest {
    fn from(request: SearchRequest) -> Self {
        Self {
            query: QueryEnum::Nearest(request.vector),
            filter: request.filter,
            params: request.params,
            limit: request.limit,
            offset: request.offset,
            with_payload: request.with_payload,
            with_vector: request.with_vector,
            score_threshold: request.score_threshold,
        }
    }
}

impl From<QueryEnum> for QueryVector {
    fn from(query: QueryEnum) -> Self {
        match query {
            QueryEnum::Nearest(named) => QueryVector::Nearest(named.into()),
            QueryEnum::RecommendBestScore(named) => QueryVector::Recommend(named.query.into()),
        }
    }
}
