use std::backtrace::Backtrace;
use std::collections::{BTreeMap, HashMap};
use std::error::Error as _;
use std::fmt::{Debug, Write as _};
use std::iter;
use std::num::NonZeroU64;
use std::time::SystemTimeError;

use api::grpc::transport_channel_pool::RequestError;
use api::rest::{
    BaseGroupRequest, LookupLocation, OrderByInterface, RecommendStrategy, Record,
    SearchGroupsRequestInternal, SearchRequestInternal, ShardKeySelector, VectorStructOutput,
};
use common::defaults;
use common::ext::OptionExt;
use common::types::ScoreType;
use common::validation::validate_range_generic;
use io::file_operations::FileStorageError;
use issues::IssueRecord;
use merge::Merge;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::operation_error::OperationError;
use segment::data_types::groups::GroupId;
use segment::data_types::order_by::{OrderBy, OrderValue};
use segment::data_types::vectors::{
    DenseVector, QueryVector, VectorRef, VectorStructInternal, DEFAULT_VECTOR_NAME,
};
use segment::types::{
    Distance, Filter, HnswConfig, MultiVectorConfig, Payload, PayloadIndexInfo, PayloadKeyType,
    PointIdType, QuantizationConfig, SearchParams, SeqNumberType, ShardKey,
    SparseVectorStorageType, StrictModeConfig, VectorStorageDatatype, WithPayloadInterface,
    WithVector,
};
use semver::Version;
use serde;
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, Map, Value};
use sparse::common::sparse_vector::SparseVector;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError as OneshotRecvError;
use tokio::task::JoinError;
use tonic::codegen::http::uri::InvalidUri;
use validator::{Validate, ValidationError, ValidationErrors};

use super::config_diff::{self};
use super::ClockTag;
use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::config_diff::{HnswConfigDiff, QuantizationConfigDiff};
use crate::operations::point_ops::{PointStructPersisted, VectorStructPersisted};
use crate::operations::query_enum::QueryEnum;
use crate::operations::universal_query::shard_query::{ScoringQuery, ShardQueryRequest};
use crate::optimizers_builder::OptimizersConfig;
use crate::save_on_disk;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::transfer::ShardTransferMethod;
use crate::wal::WalError;

/// Current state of the collection.
/// `Green` - all good. `Yellow` - optimization is running, 'Grey' - optimizations are possible but not triggered, `Red` - some operations failed and was not recovered
#[derive(Debug, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum CollectionStatus {
    // Collection is completely ready for requests
    Green,
    // Collection is available, but some segments are under optimization
    Yellow,
    // Collection is available, but some segments are pending optimization
    Grey,
    // Something is not OK:
    // - some operations failed and was not recovered
    Red,
}

/// Current state of the shard (supports same states as the collection)
///
/// `Green` - all good. `Yellow` - optimization is running, 'Grey' - optimizations are possible but not triggered, `Red` - some operations failed and was not recovered
#[derive(Debug, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ShardStatus {
    // Shard is completely ready for requests
    Green,
    // Shard is available, but some segments are under optimization
    Yellow,
    // Shard is available, but some segments are pending optimization
    Grey,
    // Something is not OK:
    // - some operations failed and was not recovered
    Red,
}

impl From<ShardStatus> for CollectionStatus {
    fn from(info: ShardStatus) -> Self {
        match info {
            ShardStatus::Green => Self::Green,
            ShardStatus::Yellow => Self::Yellow,
            ShardStatus::Grey => Self::Grey,
            ShardStatus::Red => Self::Red,
        }
    }
}

/// State of existence of a collection,
/// true = exists, false = does not exist
#[derive(Debug, Serialize, JsonSchema, Clone)]
pub struct CollectionExistence {
    pub exists: bool,
}

/// Current state of the collection
#[derive(Debug, Default, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[serde(rename_all = "snake_case")]
pub enum OptimizersStatus {
    /// Optimizers are reporting as expected
    #[default]
    Ok,
    /// Something wrong happened with optimizers
    Error(String),
}

/// Point data
#[derive(Clone, Debug, PartialEq)]
pub struct RecordInternal {
    /// Id of the point
    pub id: PointIdType,
    /// Payload - values assigned to the point
    pub payload: Option<Payload>,
    /// Vector of the point
    pub vector: Option<VectorStructInternal>,
    /// Shard Key
    pub shard_key: Option<ShardKey>,
    /// Order value, if used for order_by
    pub order_value: Option<OrderValue>,
}

/// Warn: panics if the vector is empty
impl TryFrom<RecordInternal> for PointStructPersisted {
    type Error = String;

    fn try_from(record: RecordInternal) -> Result<Self, Self::Error> {
        let RecordInternal {
            id,
            payload,
            vector,
            shard_key: _,
            order_value: _,
        } = record;

        if vector.is_none() {
            return Err("Vector is empty".to_string());
        }

        Ok(Self {
            id,
            payload,
            vector: VectorStructPersisted::from(vector.unwrap()),
        })
    }
}

impl TryFrom<Record> for PointStructPersisted {
    type Error = String;

    fn try_from(record: Record) -> Result<Self, Self::Error> {
        let Record {
            id,
            payload,
            vector,
            shard_key: _,
            order_value: _,
        } = record;

        if vector.is_none() {
            return Err("Vector is empty".to_string());
        }

        Ok(Self {
            id,
            payload,
            vector: VectorStructPersisted::from(vector.unwrap()),
        })
    }
}

// Version of the collection config we can present to the user
/// Information about the collection configuration
#[derive(Debug, Serialize, JsonSchema)]
pub struct CollectionConfig {
    #[validate(nested)]
    pub params: CollectionParams,
    #[validate(nested)]
    pub hnsw_config: HnswConfig,
    #[validate(nested)]
    pub optimizer_config: OptimizersConfig,
    #[validate(nested)]
    pub wal_config: Option<WalConfig>,
    #[serde(default)]
    pub quantization_config: Option<QuantizationConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strict_mode_config: Option<StrictModeConfig>,
}

impl From<CollectionConfigInternal> for CollectionConfig {
    fn from(config: CollectionConfigInternal) -> Self {
        let CollectionConfigInternal {
            params,
            hnsw_config,
            optimizer_config,
            wal_config,
            quantization_config,
            strict_mode_config,
            // Internal UUID to identify unique collections in consensus snapshots
            uuid: _,
        } = config;

        CollectionConfig {
            params,
            hnsw_config,
            optimizer_config,
            wal_config: Some(wal_config),
            quantization_config,
            strict_mode_config,
        }
    }
}

/// Current statistics and configuration of the collection
#[derive(Debug, Serialize, JsonSchema)]
pub struct CollectionInfo {
    /// Status of the collection
    pub status: CollectionStatus,
    /// Status of optimizers
    pub optimizer_status: OptimizersStatus,
    /// DEPRECATED:
    /// Approximate number of vectors in collection.
    /// All vectors in collection are available for querying.
    /// Calculated as `points_count x vectors_per_point`.
    /// Where `vectors_per_point` is a number of named vectors in schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vectors_count: Option<usize>,
    /// Approximate number of indexed vectors in the collection.
    /// Indexed vectors in large segments are faster to query,
    /// as it is stored in a specialized vector index.
    pub indexed_vectors_count: Option<usize>,
    /// Approximate number of points (vectors + payloads) in collection.
    /// Each point could be accessed by unique id.
    pub points_count: Option<usize>,
    /// Number of segments in collection.
    /// Each segment has independent vector as payload indexes
    pub segments_count: usize,
    /// Collection settings
    pub config: CollectionConfig,
    /// Types of stored payload
    pub payload_schema: HashMap<PayloadKeyType, PayloadIndexInfo>,
}

impl CollectionInfo {
    pub fn empty(collection_config: CollectionConfigInternal) -> Self {
        Self {
            status: CollectionStatus::Green,
            optimizer_status: OptimizersStatus::Ok,
            vectors_count: Some(0),
            indexed_vectors_count: Some(0),
            points_count: Some(0),
            segments_count: 0,
            config: CollectionConfig::from(collection_config),
            payload_schema: HashMap::new(),
        }
    }
}

impl From<ShardInfoInternal> for CollectionInfo {
    fn from(info: ShardInfoInternal) -> Self {
        Self {
            status: info.status.into(),
            optimizer_status: info.optimizer_status,
            vectors_count: Some(info.vectors_count),
            indexed_vectors_count: Some(info.indexed_vectors_count),
            points_count: Some(info.points_count),
            segments_count: info.segments_count,
            config: CollectionConfig::from(info.config),
            payload_schema: info.payload_schema,
        }
    }
}

/// Internal statistics and configuration of the collection.
#[derive(Debug)]
pub struct ShardInfoInternal {
    /// Status of the shard
    pub status: ShardStatus,
    /// Status of optimizers
    pub optimizer_status: OptimizersStatus,
    /// Approximate number of vectors in shard.
    /// All vectors in shard are available for querying.
    /// Calculated as `points_count x vectors_per_point`.
    /// Where `vectors_per_point` is a number of named vectors in schema.
    pub vectors_count: usize,
    /// Approximate number of indexed vectors in the shard.
    /// Indexed vectors in large segments are faster to query,
    /// as it is stored in vector index (HNSW).
    pub indexed_vectors_count: usize,
    /// Approximate number of points (vectors + payloads) in shard.
    /// Each point could be accessed by unique id.
    pub points_count: usize,
    /// Number of segments in shard.
    /// Each segment has independent vector as payload indexes
    pub segments_count: usize,
    /// Collection settings
    pub config: CollectionConfigInternal,
    /// Types of stored payload
    pub payload_schema: HashMap<PayloadKeyType, PayloadIndexInfo>,
}

/// Current clustering distribution for the collection
#[derive(Debug, Serialize, JsonSchema)]
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
    /// Resharding operations
    // TODO(resharding): remove this skip when releasing resharding
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resharding_operations: Option<Vec<ReshardingInfo>>,
}

#[derive(Debug, Serialize, JsonSchema, Clone)]
pub struct ShardTransferInfo {
    pub shard_id: ShardId,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)] // TODO(resharding): expose once we release resharding
    pub to_shard_id: Option<ShardId>,

    /// Source peer id
    pub from: PeerId,

    /// Destination peer id
    pub to: PeerId,

    /// If `true` transfer is a synchronization of a replicas
    /// If `false` transfer is a moving of a shard from one peer to another
    pub sync: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<ShardTransferMethod>,

    /// A human-readable report of the transfer progress. Available only on the source peer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema, Clone)]
pub struct ReshardingInfo {
    pub direction: ReshardingDirection,

    pub shard_id: ShardId,

    pub peer_id: PeerId,

    pub shard_key: Option<ShardKey>,

    /// A human-readable report of the operation progress. Available only on the source peer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
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

#[derive(Debug, Serialize, JsonSchema)]
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
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStatus {
    Acknowledged,
    Completed,
    /// Internal: update is rejected due to an outdated clock
    #[schemars(skip)]
    ClockRejected,
}

#[derive(Copy, Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct UpdateResult {
    /// Sequential number of the operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<SeqNumberType>,

    /// Update status
    pub status: UpdateStatus,

    /// Updated value for the external clock tick
    /// Provided if incoming update request also specify clock tick
    #[serde(skip)]
    pub clock_tag: Option<ClockTag>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ScrollRequest {
    #[serde(flatten)]
    #[validate(nested)]
    pub scroll_request: ScrollRequestInternal,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

/// Scroll request - paginate over all points which matches given condition
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ScrollRequestInternal {
    /// Start ID to read points from.
    pub offset: Option<PointIdType>,

    /// Page size. Default: 10
    #[validate(range(min = 1))]
    pub limit: Option<usize>,

    /// Look only for points which satisfies this conditions. If not provided - all points.
    #[validate(nested)]
    pub filter: Option<Filter>,

    /// Select which payload to return with the response. Default is true.
    pub with_payload: Option<WithPayloadInterface>,

    /// Options for specifying which vectors to include into response. Default is false.
    #[serde(default, alias = "with_vectors")]
    pub with_vector: WithVector,

    /// Order the records by a payload field.
    pub order_by: Option<OrderByInterface>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum ScrollOrder {
    #[default]
    ById,
    ByField(OrderBy),
    Random,
}

/// Scroll request, used as a part of query request
#[derive(Debug, Clone, PartialEq)]
pub struct QueryScrollRequestInternal {
    /// Page size. Default: 10
    pub limit: usize,

    /// Look only for points which satisfies this conditions. If not provided - all points.
    pub filter: Option<Filter>,

    /// Select which payload to return with the response. Default is true.
    pub with_payload: WithPayloadInterface,

    /// Options for specifying which vectors to include into response. Default is false.
    pub with_vector: WithVector,

    /// Order the records by a payload field.
    pub scroll_order: ScrollOrder,
}

impl ScrollRequestInternal {
    pub(crate) fn default_limit() -> usize {
        10
    }

    pub(crate) fn default_with_payload() -> WithPayloadInterface {
        WithPayloadInterface::Bool(true)
    }

    pub(crate) fn default_with_vector() -> WithVector {
        WithVector::Bool(false)
    }
}

impl Default for ScrollRequestInternal {
    fn default() -> Self {
        ScrollRequestInternal {
            offset: None,
            limit: Some(Self::default_limit()),
            filter: None,
            with_payload: Some(Self::default_with_payload()),
            with_vector: Self::default_with_vector(),
            order_by: None,
        }
    }
}

fn points_example() -> Vec<api::rest::Record> {
    let mut payload_map_1 = Map::new();
    payload_map_1.insert("city".to_string(), Value::String("London".to_string()));
    payload_map_1.insert("color".to_string(), Value::String("green".to_string()));

    let mut payload_map_2 = Map::new();
    payload_map_2.insert("city".to_string(), Value::String("Paris".to_string()));
    payload_map_2.insert("color".to_string(), Value::String("red".to_string()));

    vec![
        api::rest::Record {
            id: PointIdType::NumId(40),
            payload: Some(Payload(payload_map_1)),
            vector: Some(VectorStructOutput::Single(vec![0.875, 0.140625, 0.897_6])),
            shard_key: Some("region_1".into()),
            order_value: None,
        },
        api::rest::Record {
            id: PointIdType::NumId(41),
            payload: Some(Payload(payload_map_2)),
            vector: Some(VectorStructOutput::Single(vec![0.75, 0.640625, 0.8945])),
            shard_key: Some("region_1".into()),
            order_value: None,
        },
    ]
}

/// Result of the points read request
#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ScrollResult {
    /// List of retrieved points
    #[schemars(example = "points_example")]
    pub points: Vec<api::rest::Record>,
    /// Offset which should be used to retrieve a next page result
    pub next_page_offset: Option<PointIdType>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SearchRequest {
    #[serde(flatten)]
    #[validate(nested)]
    pub search_request: SearchRequestInternal,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SearchRequestBatch {
    #[validate(nested)]
    pub searches: Vec<SearchRequest>,
}

#[derive(Debug, Clone, PartialEq)]
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
    /// Select which payload to return with the response. Default is false.
    pub with_payload: Option<WithPayloadInterface>,
    /// Options for specifying which vectors to include into response. Default is false.
    pub with_vector: Option<WithVector>,
    pub score_threshold: Option<ScoreType>,
}

#[derive(Debug, Clone)]
pub struct CoreSearchRequestBatch {
    pub searches: Vec<CoreSearchRequest>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct SearchGroupsRequest {
    #[serde(flatten)]
    #[validate(nested)]
    pub search_group_request: SearchGroupsRequestInternal,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct PointRequest {
    #[serde(flatten)]
    #[validate(nested)]
    pub point_request: PointRequestInternal,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct PointRequestInternal {
    /// Look for points with ids
    pub ids: Vec<PointIdType>,
    /// Select which payload to return with the response. Default is true.
    pub with_payload: Option<WithPayloadInterface>,
    /// Options for specifying which vectors to include into response. Default is false.
    #[serde(default, alias = "with_vectors")]
    pub with_vector: WithVector,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(untagged)]
pub enum RecommendExample {
    PointId(PointIdType),
    Dense(DenseVector),
    Sparse(SparseVector),
}

impl RecommendExample {
    pub fn as_point_id(&self) -> Option<PointIdType> {
        match self {
            RecommendExample::PointId(id) => Some(*id),
            _ => None,
        }
    }
}

impl Validate for RecommendExample {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            RecommendExample::PointId(_) => Ok(()),
            RecommendExample::Dense(_) => Ok(()),
            RecommendExample::Sparse(sparse) => sparse.validate(),
        }
    }
}

impl From<u64> for RecommendExample {
    fn from(id: u64) -> Self {
        RecommendExample::PointId(id.into())
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case", untagged)]
pub enum UsingVector {
    Name(String),
}

impl UsingVector {
    pub fn as_string(&self) -> String {
        match self {
            UsingVector::Name(name) => name.to_string(),
        }
    }
}

impl From<String> for UsingVector {
    fn from(name: String) -> Self {
        UsingVector::Name(name)
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub struct RecommendRequest {
    #[serde(flatten)]
    #[validate(nested)]
    pub recommend_request: RecommendRequestInternal,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

/// Recommendation request.
/// Provides positive and negative examples of the vectors, which can be ids of points that
/// are already stored in the collection, raw vectors, or even ids and vectors combined.
///
/// Service should look for the points which are closer to positive examples and at the same time
/// further to negative examples. The concrete way of how to compare negative and positive distances
/// is up to the `strategy` chosen.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Default, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct RecommendRequestInternal {
    /// Look for vectors closest to those
    #[serde(default)]
    #[validate(nested)]
    pub positive: Vec<RecommendExample>,

    /// Try to avoid vectors like this
    #[serde(default)]
    #[validate(nested)]
    pub negative: Vec<RecommendExample>,

    /// How to use positive and negative examples to find the results
    pub strategy: Option<api::rest::RecommendStrategy>,

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
    #[validate(nested)]
    pub searches: Vec<RecommendRequest>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct RecommendGroupsRequest {
    #[serde(flatten)]
    #[validate(nested)]
    pub recommend_group_request: RecommendGroupsRequestInternal,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq)]
pub struct RecommendGroupsRequestInternal {
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq)]
pub struct ContextExamplePair {
    #[validate(nested)]
    pub positive: RecommendExample,
    #[validate(nested)]
    pub negative: RecommendExample,
}

impl ContextExamplePair {
    pub fn iter(&self) -> impl Iterator<Item = &RecommendExample> {
        iter::once(&self.positive).chain(iter::once(&self.negative))
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct DiscoverRequest {
    #[serde(flatten)]
    #[validate(nested)]
    pub discover_request: DiscoverRequestInternal,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

/// Use context and a target to find the most similar points, constrained by the context.
#[derive(Deserialize, Serialize, JsonSchema, Validate, Clone, Debug, PartialEq)]
pub struct DiscoverRequestInternal {
    /// Look for vectors closest to this.
    ///
    /// When using the target (with or without context), the integer part of the score represents
    /// the rank with respect to the context, while the decimal part of the score relates to the
    /// distance to the target.
    #[validate(nested)]
    pub target: Option<RecommendExample>,

    /// Pairs of { positive, negative } examples to constrain the search.
    ///
    /// When using only the context (without a target), a special search - called context search - is
    /// performed where pairs of points are used to generate a loss that guides the search towards the
    /// zone where most positive examples overlap. This means that the score minimizes the scenario of
    /// finding a point closer to a negative than to a positive part of a pair.
    ///
    /// Since the score of a context relates to loss, the maximum score a point can get is 0.0,
    /// and it becomes normal that many points can have a score of 0.0.
    ///
    /// For discovery search (when including a target), the context part of the score for each pair
    /// is calculated +1 if the point is closer to a positive than to a negative part of a pair,
    /// and -1 otherwise.
    #[validate(nested)]
    pub context: Option<Vec<ContextExamplePair>>,

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
    pub with_vector: Option<WithVector>,

    /// Define which vector to use for recommendation, if not specified - try to use default vector
    #[serde(default)]
    pub using: Option<UsingVector>,

    /// The location used to lookup vectors. If not specified - use current collection.
    /// Note: the other collection should have the same vector size as the current collection
    #[serde(default)]
    pub lookup_from: Option<LookupLocation>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct DiscoverRequestBatch {
    #[validate(nested)]
    pub searches: Vec<DiscoverRequest>,
}

#[derive(Debug, Serialize, JsonSchema, Clone)]
pub struct PointGroup {
    /// Scored points that have the same value of the group_by key
    pub hits: Vec<api::rest::ScoredPoint>,
    /// Value of the group_by key, shared across all the hits in the group
    pub id: GroupId,
    /// Record that has been looked up using the group id
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lookup: Option<api::rest::Record>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct GroupsResult {
    pub groups: Vec<PointGroup>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct CountRequest {
    #[serde(flatten)]
    #[validate(nested)]
    pub count_request: CountRequestInternal,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

/// Count Request
/// Counts the number of points which satisfy the given filter.
/// If filter is not provided, the count of all points in the collection will be returned.
#[derive(Deserialize, Serialize, JsonSchema, Validate, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct CountRequestInternal {
    /// Look only for points which satisfies this conditions
    #[validate(nested)]
    pub filter: Option<Filter>,
    /// If true, count exact number of points. If false, count approximate number of points faster.
    /// Approximate count might be unreliable during the indexing process. Default: true
    #[serde(default = "default_exact_count")]
    pub exact: bool,
}

pub const fn default_exact_count() -> bool {
    true
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CountResult {
    /// Number of points which satisfy the conditions
    pub count: usize,
}

#[derive(Error, Debug, Clone, PartialEq)]
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
    #[error("Precondition failed: {description}")]
    PreConditionFailed { description: String },
    #[error("Object Store error: {what}")]
    ObjectStoreError { what: String },
    #[error("Strict mode error: {description}")]
    StrictMode { description: String },
    #[error("{description}")]
    InferenceError { description: String },
    #[error("Rate limiting exceeded: {description}")]
    RateLimitExceeded { description: String },
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

    pub fn bad_input(description: impl Into<String>) -> CollectionError {
        CollectionError::BadInput {
            description: description.into(),
        }
    }

    pub fn not_found(what: impl Into<String>) -> CollectionError {
        CollectionError::NotFound { what: what.into() }
    }

    pub fn bad_request(description: impl Into<String>) -> CollectionError {
        CollectionError::BadRequest {
            description: description.into(),
        }
    }

    pub fn bad_shard_selection(description: String) -> CollectionError {
        CollectionError::BadShardSelection { description }
    }

    pub fn object_storage_error(what: impl Into<String>) -> CollectionError {
        CollectionError::ObjectStoreError { what: what.into() }
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

    pub fn shard_key_not_found(shard_key: &Option<ShardKey>) -> CollectionError {
        match shard_key {
            Some(shard_key) => CollectionError::NotFound {
                what: format!("Shard key {shard_key} not found"),
            },
            None => CollectionError::NotFound {
                what: "Shard expected, but not provided".to_string(),
            },
        }
    }

    pub fn pre_condition_failed(description: impl Into<String>) -> CollectionError {
        CollectionError::PreConditionFailed {
            description: description.into(),
        }
    }

    pub fn strict_mode(error: impl Into<String>, solution: impl Into<String>) -> Self {
        let description = format!("{}. Help: {}", error.into(), solution.into());
        Self::StrictMode { description }
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
            Self::PreConditionFailed { .. } => true,
            // Not transient
            Self::BadInput { .. } => false,
            Self::NotFound { .. } => false,
            Self::PointNotFound { .. } => false,
            Self::BadRequest { .. } => false,
            Self::BadShardSelection { .. } => false,
            Self::InconsistentShardFailure { .. } => false,
            Self::ForwardProxyError { .. } => false,
            Self::ObjectStoreError { .. } => false,
            Self::StrictMode { .. } => false,
            Self::InferenceError { .. } => false,
            Self::RateLimitExceeded { .. } => false,
        }
    }

    pub fn is_pre_condition_failed(&self) -> bool {
        matches!(self, Self::PreConditionFailed { .. })
    }

    pub fn is_missing_point(&self) -> bool {
        match self {
            CollectionError::NotFound { what } => what.contains("No point with id"),
            CollectionError::PointNotFound { .. } => true,
            _ => false,
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
            OperationError::WrongVectorDimension { .. } => Self::BadInput {
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
            OperationError::WrongMulti => Self::BadInput {
                description: "Conversion between multi and regular vectors failed".to_string(),
            },
            OperationError::MissingRangeIndexForOrderBy { .. } => Self::bad_input(format!("{err}")),
            OperationError::MissingMapIndexForFacet { .. } => Self::bad_input(format!("{err}")),
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

impl From<std::io::Error> for CollectionError {
    fn from(err: std::io::Error) -> Self {
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
            tonic::Code::FailedPrecondition => CollectionError::PreConditionFailed {
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

impl From<cancel::Error> for CollectionError {
    fn from(err: cancel::Error) -> Self {
        match err {
            cancel::Error::Join(err) => err.into(),
            cancel::Error::Cancelled => Self::Cancelled {
                description: err.to_string(),
            },
        }
    }
}

impl From<tempfile::PathPersistError> for CollectionError {
    fn from(err: tempfile::PathPersistError) -> Self {
        Self::service_error(format!(
            "failed to persist temporary file path {}: {}",
            err.path.display(),
            err.error,
        ))
    }
}

pub type CollectionResult<T> = Result<T, CollectionError>;

impl RecordInternal {
    pub fn get_vector_by_name(&self, name: &str) -> Option<VectorRef> {
        match &self.vector {
            Some(VectorStructInternal::Single(vector)) => {
                (name == DEFAULT_VECTOR_NAME).then_some(VectorRef::from(vector))
            }
            Some(VectorStructInternal::MultiDense(vectors)) => {
                (name == DEFAULT_VECTOR_NAME).then_some(VectorRef::from(vectors))
            }
            Some(VectorStructInternal::Named(vectors)) => vectors.get(name).map(VectorRef::from),
            None => None,
        }
    }
}

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Eq, PartialEq, Copy, Clone, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Datatype {
    #[default]
    Float32,
    Uint8,
    Float16,
}

impl From<Datatype> for VectorStorageDatatype {
    fn from(value: Datatype) -> Self {
        match value {
            Datatype::Float32 => VectorStorageDatatype::Float32,
            Datatype::Uint8 => VectorStorageDatatype::Uint8,
            Datatype::Float16 => VectorStorageDatatype::Float16,
        }
    }
}

/// Params of single vector data storage
#[derive(Debug, Hash, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct VectorParams {
    /// Size of a vectors used
    #[validate(custom(function = "validate_nonzerou64_range_min_1_max_65536"))]
    pub size: NonZeroU64,
    /// Type of distance function used for measuring distance between vectors
    pub distance: Distance,
    /// Custom params for HNSW index. If none - values from collection configuration are used.
    #[serde(default, skip_serializing_if = "is_hnsw_diff_empty")]
    #[validate(nested)]
    pub hnsw_config: Option<HnswConfigDiff>,
    /// Custom params for quantization. If none - values from collection configuration are used.
    #[serde(
        default,
        alias = "quantization",
        skip_serializing_if = "Option::is_none"
    )]
    #[validate(nested)]
    pub quantization_config: Option<QuantizationConfig>,
    /// If true, vectors are served from disk, improving RAM usage at the cost of latency
    /// Default: false
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Defines which datatype should be used to represent vectors in the storage.
    /// Choosing different datatypes allows to optimize memory usage and performance vs accuracy.
    ///
    /// - For `float32` datatype - vectors are stored as single-precision floating point numbers,
    ///   4 bytes.
    /// - For `float16` datatype - vectors are stored as half-precision floating point numbers,
    ///   2 bytes.
    /// - For `uint8` datatype - vectors are stored as unsigned 8-bit integers, 1 byte.
    ///   It expects vector elements to be in range `[0, 255]`.
    pub datatype: Option<Datatype>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multivector_config: Option<MultiVectorConfig>,
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

/// If used, include weight modification, which will be applied to sparse vectors at query time:
/// None - no modification (default)
/// Idf - inverse document frequency, based on statistics of the collection
#[derive(Debug, Hash, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Modifier {
    #[default]
    None,
    Idf,
}

/// Params of single sparse vector data storage
#[derive(Debug, Hash, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SparseVectorParams {
    /// Custom params for index. If none - values from collection configuration are used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<SparseIndexParams>,

    /// Configures addition value modifications for sparse vectors.
    /// Default: none
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modifier: Option<Modifier>,
}

impl SparseVectorParams {
    pub fn storage_type(&self, use_new_storage: bool) -> SparseVectorStorageType {
        if use_new_storage {
            SparseVectorStorageType::Mmap
        } else {
            SparseVectorStorageType::default()
        }
    }
}

impl Anonymize for SparseVectorParams {
    fn anonymize(&self) -> Self {
        Self {
            index: self.index.anonymize(),
            modifier: self.modifier.clone(),
        }
    }
}

/// Configuration for sparse inverted index.
#[derive(Debug, Hash, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct SparseIndexParams {
    /// We prefer a full scan search upto (excluding) this number of vectors.
    ///
    /// Note: this is number of vectors, not KiloBytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_scan_threshold: Option<usize>,
    /// Store index on disk. If set to false, the index will be stored in RAM. Default: false
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
    /// Defines which datatype should be used for the index.
    /// Choosing different datatypes allows to optimize memory usage and performance vs accuracy.
    ///
    /// - For `float32` datatype - vectors are stored as single-precision floating point numbers,
    ///   4 bytes.
    /// - For `float16` datatype - vectors are stored as half-precision floating point numbers,
    ///   2 bytes.
    /// - For `uint8` datatype - vectors are quantized to unsigned 8-bit integers, 1 byte.
    ///   Quantization to fit byte range `[0, 255]` happens during indexing automatically, so the
    ///   actual vector data does not need to conform to this range.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatype: Option<Datatype>,
}

impl Anonymize for SparseIndexParams {
    fn anonymize(&self) -> Self {
        SparseIndexParams {
            full_scan_threshold: self.full_scan_threshold,
            on_disk: self.on_disk,
            datatype: self.datatype,
        }
    }
}

impl SparseIndexParams {
    pub fn update_from_other(&mut self, other: SparseIndexParams) {
        let SparseIndexParams {
            full_scan_threshold,
            on_disk,
            datatype,
        } = other;

        self.full_scan_threshold
            .replace_if_some(full_scan_threshold);
        self.on_disk.replace_if_some(on_disk);
        self.datatype.replace_if_some(datatype);
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

impl Default for VectorsConfig {
    fn default() -> Self {
        VectorsConfig::Multi(Default::default())
    }
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
    pub fn params_iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&'a str, &'a VectorParams)> + 'a> {
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

// TODO(sparse): Further unify `check_compatible` and `check_compatible_with_segment_config`?
pub fn check_sparse_compatible(
    self_config: &BTreeMap<String, SparseVectorParams>,
    other_config: &BTreeMap<String, SparseVectorParams>,
) -> CollectionResult<()> {
    for (vector_name, _this) in self_config.iter() {
        let Some(_other) = other_config.get(vector_name) else {
            return Err(missing_vector_error(vector_name));
        };
    }

    Ok(())
}

pub fn check_sparse_compatible_with_segment_config(
    self_config: &BTreeMap<String, SparseVectorParams>,
    other: &HashMap<String, segment::types::SparseVectorDataConfig>,
    exact: bool,
) -> CollectionResult<()> {
    if exact && self_config.len() != other.len() {
        return Err(incompatible_vectors_error(
            self_config.keys().map(String::as_str),
            other.keys().map(String::as_str),
        ));
    }

    for (vector_name, _) in self_config.iter() {
        if other.get(vector_name).is_none() {
            return Err(missing_vector_error(vector_name));
        };
    }

    Ok(())
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
             origin collection have vectors [{this_vectors}], \
             while other vectors [{other_vectors}]"
        ),
    }
}

fn missing_vector_error(vector_name: &str) -> CollectionError {
    CollectionError::BadInput {
        description: format!(
            "Vectors configuration is not compatible: \
             origin collection have vector {vector_name}, while other collection does not"
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
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            VectorsConfig::Single(single) => single.validate(),
            VectorsConfig::Multi(multi) => common::validation::validate_iter(multi.values()),
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
    #[validate(nested)]
    pub hnsw_config: Option<HnswConfigDiff>,
    /// Update params for quantization. If none - it is left unchanged.
    #[serde(
        default,
        alias = "quantization",
        skip_serializing_if = "Option::is_none"
    )]
    #[validate(nested)]
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
    fn validate(&self) -> Result<(), ValidationErrors> {
        common::validation::validate_iter(self.0.values())
    }
}

impl From<VectorParamsDiff> for VectorsConfigDiff {
    fn from(params: VectorParamsDiff) -> Self {
        VectorsConfigDiff(BTreeMap::from([(DEFAULT_VECTOR_NAME.into(), params)]))
    }
}

#[derive(Debug, Hash, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
pub struct SparseVectorsConfig(pub BTreeMap<String, SparseVectorParams>);

impl SparseVectorsConfig {
    /// Check that the vector names in this config are part of the given collection.
    ///
    /// Returns an error if incompatible.
    pub fn check_vector_names(&self, collection: &CollectionParams) -> CollectionResult<()> {
        for vector_name in self.0.keys() {
            collection
                .sparse_vectors
                .as_ref()
                .and_then(|v| v.get(vector_name).map(|_| ()))
                .ok_or_else(|| OperationError::VectorNameNotExists {
                    received_name: vector_name.into(),
                })?;
        }
        Ok(())
    }
}

impl Validate for SparseVectorsConfig {
    fn validate(&self) -> Result<(), ValidationErrors> {
        common::validation::validate_iter(self.0.values())
    }
}

fn alias_description_example() -> AliasDescription {
    AliasDescription {
        alias_name: "blogs-title".to_string(),
        collection_name: "arivx-title".to_string(),
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(example = "alias_description_example")]
pub struct AliasDescription {
    pub alias_name: String,
    pub collection_name: String,
}

#[derive(Debug, Serialize, JsonSchema)]
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

impl From<SearchRequestInternal> for CoreSearchRequest {
    fn from(request: SearchRequestInternal) -> Self {
        Self {
            query: QueryEnum::Nearest(request.vector.into()),
            filter: request.filter,
            params: request.params,
            limit: request.limit,
            offset: request.offset.unwrap_or_default(),
            with_payload: request.with_payload,
            with_vector: request.with_vector,
            score_threshold: request.score_threshold,
        }
    }
}

impl From<SearchRequestInternal> for ShardQueryRequest {
    fn from(value: SearchRequestInternal) -> Self {
        let SearchRequestInternal {
            vector,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        } = value;

        Self {
            prefetches: vec![],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(vector.into()))),
            filter,
            score_threshold,
            limit,
            offset: offset.unwrap_or_default(),
            params,
            with_vector: with_vector.unwrap_or_default(),
            with_payload: with_payload.unwrap_or_default(),
        }
    }
}

impl From<CoreSearchRequest> for ShardQueryRequest {
    fn from(value: CoreSearchRequest) -> Self {
        let CoreSearchRequest {
            query,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        } = value;

        Self {
            prefetches: vec![],
            query: Some(ScoringQuery::Vector(query)),
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector: with_vector.unwrap_or_default(),
            with_payload: with_payload.unwrap_or_default(),
        }
    }
}

impl From<QueryEnum> for QueryVector {
    fn from(query: QueryEnum) -> Self {
        match query {
            QueryEnum::Nearest(named) => QueryVector::Nearest(named.into()),
            QueryEnum::RecommendBestScore(named) => QueryVector::Recommend(named.query),
            QueryEnum::Discover(named) => QueryVector::Discovery(named.query),
            QueryEnum::Context(named) => QueryVector::Context(named.query),
        }
    }
}

/// All the unresolved issues in a Qdrant instance
#[derive(Serialize, JsonSchema, Debug)]
pub struct IssuesReport {
    pub issues: Vec<IssueRecord>,
}

/// Metadata describing extra properties for each peer
#[derive(Debug, Hash, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct PeerMetadata {
    /// Peer Qdrant version
    pub(crate) version: Version,
}

impl PeerMetadata {
    pub fn current() -> Self {
        Self {
            version: defaults::QDRANT_VERSION.clone(),
        }
    }

    /// Whether this metadata has a different version than our current Qdrant instance.
    pub fn is_different_version(&self) -> bool {
        self.version != *defaults::QDRANT_VERSION
    }
}
