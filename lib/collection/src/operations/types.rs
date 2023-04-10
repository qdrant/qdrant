use std::backtrace::Backtrace;
use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::time::SystemTimeError;

use api::grpc::transport_channel_pool::RequestError;
use futures::io;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::file_operations::FileStorageError;
use segment::data_types::vectors::{
    NamedVectorStruct, VectorStruct, VectorType, DEFAULT_VECTOR_NAME,
};
use segment::entry::entry_point::OperationError;
use segment::types::{
    Distance, Filter, Payload, PayloadIndexInfo, PayloadKeyType, PointIdType, QuantizationConfig,
    ScoreType, SearchParams, SeqNumberType, WithPayloadInterface, WithVector,
};
use serde;
use serde::{Deserialize, Serialize};
use serde_json::Error as JsonError;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError as OneshotRecvError;
use tokio::task::JoinError;
use tonic::codegen::http::uri::InvalidUri;
use validator::{Validate, ValidationErrors};

use crate::config::CollectionConfig;
use crate::operations::config_diff::HnswConfigDiff;
use crate::save_on_disk;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
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
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct LocalShardInfo {
    /// Local shard id
    pub shard_id: ShardId,
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
    pub filter: Option<Filter>,
    /// Select which payload to return with the response. Default: All
    pub with_payload: Option<WithPayloadInterface>,
    /// Whether to return the point vector with the result?
    #[serde(default)]
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
    pub filter: Option<Filter>,
    /// Additional search params
    pub params: Option<SearchParams>,
    /// Max number of result to return
    #[serde(alias = "top")]
    pub limit: usize,
    /// Offset of the first result to return.
    /// May be used to paginate results.
    /// Note: large offset values may cause performance issues.
    #[serde(default)]
    pub offset: usize,
    /// Select which payload to return with the response. Default: None
    pub with_payload: Option<WithPayloadInterface>,
    /// Whether to return the point vector with the result?
    #[serde(default)]
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
    pub searches: Vec<SearchRequest>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct PointRequest {
    /// Look for points with ids
    pub ids: Vec<PointIdType>,
    /// Select which payload to return with the response. Default: All
    pub with_payload: Option<WithPayloadInterface>,
    /// Whether to return the point vector with the result?
    #[serde(default)]
    pub with_vector: WithVector,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
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
/// Provides positive and negative examples of the vectors, which
/// are already stored in the collection.
///
/// Service should look for the points which are closer to positive examples and at the same time
/// further to negative examples. The concrete way of how to compare negative and positive distances
/// is up to implementation in `segment` crate.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Default)]
#[serde(rename_all = "snake_case")]
pub struct RecommendRequest {
    /// Look for vectors closest to those
    pub positive: Vec<PointIdType>,
    /// Try to avoid vectors like this
    #[serde(default)]
    pub negative: Vec<PointIdType>,
    /// Look only for points which satisfies this conditions
    pub filter: Option<Filter>,
    /// Additional search params
    pub params: Option<SearchParams>,
    /// Max number of result to return
    #[serde(alias = "top")]
    pub limit: usize,
    /// Offset of the first result to return.
    /// May be used to paginate results.
    /// Note: large offset values may cause performance issues.
    #[serde(default)]
    pub offset: usize,
    /// Select which payload to return with the response. Default: None
    pub with_payload: Option<WithPayloadInterface>,
    /// Whether to return the point vector with the result?
    #[serde(default)]
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
    pub searches: Vec<RecommendRequest>,
}

/// Count Request
/// Counts the number of points which satisfy the given filter.
/// If filter is not provided, the count of all points in the collection will be returned.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct CountRequest {
    /// Look only for points which satisfies this conditions
    pub filter: Option<Filter>,
    /// If true, count exact number of points. If false, count approximate number of points faster.
    /// Approximate count might be unreliable during the indexing process. Default: true
    #[serde(default = "default_exact_count")]
    pub exact: bool,
}

pub fn default_exact_count() -> bool {
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
}

impl CollectionError {
    pub fn service_error(error: String) -> CollectionError {
        CollectionError::ServiceError {
            error,
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

impl From<io::Error> for CollectionError {
    fn from(err: io::Error) -> Self {
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
            other => CollectionError::ServiceError {
                error: format!("Tonic status error: {other}"),
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
        match err {
            FileStorageError::IoError { description } => {
                CollectionError::service_error(description)
            }
            FileStorageError::UserAtomicIoError => {
                CollectionError::service_error("Unknown atomic write error".to_string())
            }
            FileStorageError::GenericError { description } => {
                CollectionError::service_error(description)
            }
        }
    }
}

impl From<RequestError<tonic::Status>> for CollectionError {
    fn from(err: RequestError<tonic::Status>) -> Self {
        match err {
            RequestError::FromClosure(status) => status.into(),
            RequestError::Tonic(err) => CollectionError::service_error(format!("{err}")),
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

pub fn is_service_error<T>(err: &CollectionResult<T>) -> bool {
    match err {
        Ok(_) => false,
        Err(error) => matches!(error, CollectionError::ServiceError { .. }),
    }
}

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
            Some(VectorStruct::Single(vector)) => {
                if name == DEFAULT_VECTOR_NAME {
                    Some(vector)
                } else {
                    None
                }
            }
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
    pub size: NonZeroU64,
    /// Type of distance function used for measuring distance between vectors
    pub distance: Distance,
    /// Custom params for HNSW index. If none - values from collection configuration are used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum VectorsConfig {
    Single(VectorParams),
    Multi(BTreeMap<String, VectorParams>),
}

impl VectorsConfig {
    pub fn get_params(&self, name: &str) -> Option<&VectorParams> {
        match self {
            VectorsConfig::Single(params) => {
                if name == DEFAULT_VECTOR_NAME {
                    Some(params)
                } else {
                    None
                }
            }
            VectorsConfig::Multi(params) => params.get(name),
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
