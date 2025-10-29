use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{self, Display, Formatter};
use std::hash::{self, Hash, Hasher};
use std::mem;
use std::ops::Deref;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;

use ahash::AHashSet;
use common::stable_hash::StableHash;
use common::types::ScoreType;
use ecow::EcoString;
use fnv::FnvBuildHasher;
use geo::{Contains, Coord, Distance as GeoDistance, Haversine, LineString, Point, Polygon};
use indexmap::IndexSet;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use strum::{EnumIter, EnumString};
use uuid::Uuid;
use validator::{Validate, ValidationError, ValidationErrors};
use zerocopy::native_endian::U64;

use crate::common::anonymize::Anonymize;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::{self, MaybeOneOrMany, MultiValue};
use crate::data_types::index::{
    BoolIndexParams, DatetimeIndexParams, FloatIndexParams, GeoIndexParams, IntegerIndexParams,
    KeywordIndexParams, TextIndexParams, UuidIndexParams,
};
use crate::data_types::modifier::Modifier;
use crate::data_types::order_by::OrderValue;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::tiny_map::TinyMap;
use crate::data_types::vectors::{DenseVector, VectorStructInternal};
use crate::index::field_index::CardinalityEstimation;
use crate::index::sparse_index::sparse_index_config::SparseIndexConfig;
use crate::json_path::JsonPath;
use crate::spaces::metric::{Metric, MetricPostProcessing};
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::utils::unordered_hash_unique;
use crate::utils::maybe_arc::MaybeArc;

pub type PayloadKeyType = JsonPath;
pub type PayloadKeyTypeRef<'a> = &'a JsonPath;
/// Sequential number of modification, applied to segment
pub type SeqNumberType = u64;
/// Type of float point payload
pub type FloatPayloadType = f64;
/// Type of integer point payload
pub type IntPayloadType = i64;
/// Type of datetime point payload
pub type DateTimePayloadType = DateTimeWrapper;
/// Type of Uuid point payload
pub type UuidPayloadType = Uuid;
/// Type of Uuid point payload key
pub type UuidIntType = u128;
/// Name of a vector
pub type VectorName = str;
/// Name of a vector (owned variant)
pub type VectorNameBuf = String;

/// Wraps `DateTime<Utc>` to allow more flexible deserialization
#[derive(Clone, Copy, Serialize, JsonSchema, Debug, PartialEq, Eq, PartialOrd, Hash)]
#[serde(transparent)]
pub struct DateTimeWrapper(pub chrono::DateTime<chrono::Utc>);

impl DateTimeWrapper {
    /// Qdrant's representation of datetime as timestamp is an i64 of microseconds
    pub fn timestamp(&self) -> i64 {
        self.0.timestamp_micros()
    }

    pub fn from_timestamp(ts: i64) -> Option<Self> {
        Some(Self(chrono::DateTime::from_timestamp_micros(ts)?))
    }
}

impl<'de> Deserialize<'de> for DateTimePayloadType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str_datetime = <&str>::deserialize(deserializer)?;
        let parse_result = DateTimePayloadType::from_str(str_datetime).ok();
        match parse_result {
            Some(datetime) => Ok(datetime),
            None => Err(serde::de::Error::custom(format!(
                "'{str_datetime}' is not in a supported date/time format, please use RFC 3339"
            ))),
        }
    }
}

impl FromStr for DateTimePayloadType {
    type Err = chrono::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Attempt to parse the input string in RFC 3339 format
        if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(s)
            // Attempt to parse default to-string format
            .or_else(|_| chrono::DateTime::from_str(s))
            // Attempt to parse the input string in the specified formats:
            // - YYYY-MM-DD'T'HH:MM:SS-HHMM (timezone without colon)
            // - YYYY-MM-DD HH:MM:SS-HHMM (timezone without colon)
            .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%#z"))
            .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z"))
            .map(|dt| chrono::DateTime::<chrono::Utc>::from(dt).into())
        {
            return Ok(datetime);
        }

        // Attempt to parse the input string in the specified formats:
        // - YYYY-MM-DD'T'HH:MM:SS (without timezone or Z)
        // - YYYY-MM-DD HH:MM:SS (without timezone or Z)
        // - YYYY-MM-DD HH:MM
        // - YYYY-MM-DD
        // See: <https://github.com/qdrant/qdrant/issues/3529>
        let datetime = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M"))
            .or_else(|_| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map(Into::into))?;

        // Convert the parsed NaiveDateTime to a DateTime<Utc>
        let datetime_utc = datetime.and_utc().into();
        Ok(datetime_utc)
    }
}

impl Display for DateTimePayloadType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<chrono::DateTime<chrono::Utc>> for DateTimePayloadType {
    fn from(dt: chrono::DateTime<chrono::Utc>) -> Self {
        DateTimeWrapper(dt)
    }
}

fn id_num_example() -> u64 {
    42
}

fn id_uuid_example() -> String {
    "550e8400-e29b-41d4-a716-446655440000".to_string()
}

/// Type, used for specifying point ID in user interface
#[derive(Debug, Serialize, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, JsonSchema)]
#[serde(untagged)]
pub enum ExtendedPointId {
    #[schemars(example = "id_num_example")]
    NumId(u64),
    #[schemars(example = "id_uuid_example")]
    Uuid(Uuid),
}

impl StableHash for ExtendedPointId {
    fn stable_hash<W: FnMut(&[u8])>(&self, write: &mut W) {
        match self {
            ExtendedPointId::NumId(num) => {
                0u64.stable_hash(write); // discriminant for NumId
                num.stable_hash(write);
            }
            ExtendedPointId::Uuid(uuid) => {
                1u64.stable_hash(write); // discriminant for Uuid

                uuid.as_bytes().len().stable_hash(write); // compatibility with uuid <= v1.16.0
                write(uuid.as_bytes());
            }
        }
    }
}

impl ExtendedPointId {
    pub fn is_num_id(&self) -> bool {
        matches!(self, ExtendedPointId::NumId(..))
    }

    pub fn is_uuid(&self) -> bool {
        matches!(self, ExtendedPointId::Uuid(..))
    }
}

impl std::fmt::Display for ExtendedPointId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtendedPointId::NumId(idx) => write!(f, "{idx}"),
            ExtendedPointId::Uuid(uuid) => write!(f, "{uuid}"),
        }
    }
}

impl From<u64> for ExtendedPointId {
    fn from(idx: u64) -> Self {
        ExtendedPointId::NumId(idx)
    }
}

impl FromStr for ExtendedPointId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let try_num: Result<u64, _> = s.parse();
        if let Ok(num) = try_num {
            return Ok(Self::NumId(num));
        }
        let try_uuid = Uuid::from_str(s);
        if let Ok(uuid) = try_uuid {
            return Ok(Self::Uuid(uuid));
        }
        Err(())
    }
}

impl<'de> serde::Deserialize<'de> for ExtendedPointId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_value::Value::deserialize(deserializer)?;

        if let Ok(num) = value.clone().deserialize_into() {
            return Ok(ExtendedPointId::NumId(num));
        }

        if let Ok(uuid) = value.clone().deserialize_into() {
            return Ok(ExtendedPointId::Uuid(uuid));
        }

        Err(serde::de::Error::custom(format!(
            "value {} is not a valid point ID, \
                 valid values are either an unsigned integer or a UUID",
            crate::utils::fmt::SerdeValue(&value),
        )))
    }
}

/// Type of point index across all segments
pub type PointIdType = ExtendedPointId;

/// Compact representation of [`ExtendedPointId`].
/// Unlike [`ExtendedPointId`], this type is 17 bytes long vs 24 bytes.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum CompactExtendedPointId {
    NumId(U64),
    Uuid(Uuid),
}

impl From<ExtendedPointId> for CompactExtendedPointId {
    fn from(id: ExtendedPointId) -> Self {
        match id {
            ExtendedPointId::NumId(num) => CompactExtendedPointId::NumId(U64::new(num)),
            ExtendedPointId::Uuid(uuid) => CompactExtendedPointId::Uuid(uuid),
        }
    }
}

impl From<CompactExtendedPointId> for ExtendedPointId {
    fn from(id: CompactExtendedPointId) -> Self {
        match id {
            CompactExtendedPointId::NumId(num) => ExtendedPointId::NumId(num.get()),
            CompactExtendedPointId::Uuid(uuid) => ExtendedPointId::Uuid(uuid),
        }
    }
}

/// Type of internal tags, build from payload
#[derive(
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    Anonymize,
    Clone,
    Copy,
    FromPrimitive,
    PartialEq,
    Eq,
    Hash,
    EnumString,
    EnumIter,
)]
/// Distance function types used to compare vectors
pub enum Distance {
    // <https://en.wikipedia.org/wiki/Cosine_similarity>
    Cosine,
    // <https://en.wikipedia.org/wiki/Euclidean_distance>
    Euclid,
    // <https://en.wikipedia.org/wiki/Dot_product>
    Dot,
    // <https://simple.wikipedia.org/wiki/Manhattan_distance>
    Manhattan,
}

impl Distance {
    pub fn postprocess_score(&self, score: ScoreType) -> ScoreType {
        match self {
            Distance::Cosine => CosineMetric::postprocess(score),
            Distance::Euclid => EuclidMetric::postprocess(score),
            Distance::Dot => DotProductMetric::postprocess(score),
            Distance::Manhattan => ManhattanMetric::postprocess(score),
        }
    }

    pub fn preprocess_vector<T: PrimitiveVectorElement>(&self, vector: DenseVector) -> DenseVector
    where
        CosineMetric: Metric<T>,
        EuclidMetric: Metric<T>,
        DotProductMetric: Metric<T>,
        ManhattanMetric: Metric<T>,
    {
        match self {
            Distance::Cosine => CosineMetric::preprocess(vector),
            Distance::Euclid => EuclidMetric::preprocess(vector),
            Distance::Dot => DotProductMetric::preprocess(vector),
            Distance::Manhattan => ManhattanMetric::preprocess(vector),
        }
    }

    pub fn distance_order(&self) -> Order {
        match self {
            Distance::Cosine | Distance::Dot => Order::LargeBetter,
            Distance::Euclid | Distance::Manhattan => Order::SmallBetter,
        }
    }

    pub fn is_ordered(&self, left: ScoreType, right: ScoreType) -> bool {
        match self.distance_order() {
            Order::LargeBetter => left >= right,
            Order::SmallBetter => left <= right,
        }
    }

    /// Checks if score satisfies threshold condition
    pub fn check_threshold(&self, score: ScoreType, threshold: ScoreType) -> bool {
        match self.distance_order() {
            Order::LargeBetter => score > threshold,
            Order::SmallBetter => score < threshold,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Order {
    LargeBetter,
    SmallBetter,
}

/// Search result
#[derive(Clone, Debug)]
pub struct ScoredPoint {
    /// Point id
    pub id: PointIdType,
    /// Point version
    pub version: SeqNumberType,
    /// Points vector distance to the query vector
    pub score: ScoreType,
    /// Payload - values assigned to the point
    pub payload: Option<Payload>,
    /// Vector of the point
    pub vector: Option<VectorStructInternal>,
    /// Shard Key
    pub shard_key: Option<ShardKey>,
    /// Order-by value
    pub order_value: Option<OrderValue>,
}

impl Eq for ScoredPoint {}

impl Ord for ScoredPoint {
    /// Compare two scored points by score, unless they have `order_value`, in that case compare by `order_value`.
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.order_value, &other.order_value) {
            (None, None) => OrderedFloat(self.score).cmp(&OrderedFloat(other.score)),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (Some(self_order), Some(other_order)) => self_order.cmp(other_order),
        }
    }
}

impl PartialOrd for ScoredPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScoredPoint {
    fn eq(&self, other: &Self) -> bool {
        (self.id, &self.score) == (other.id, &other.score)
    }
}

/// Type of segment
#[derive(Debug, Serialize, JsonSchema, Anonymize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SegmentType {
    // There are no index built for the segment, all operations are available
    Plain,
    // Segment with some sort of index built. Optimized for search, appending new points will require reindexing
    Indexed,
    // Some index which you better don't touch
    Special,
}

/// Display payload field type & index information
#[derive(Debug, Serialize, JsonSchema, Anonymize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct PayloadIndexInfo {
    pub data_type: PayloadSchemaType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<PayloadSchemaParams>,
    /// Number of points indexed with this index
    pub points: usize,
}

impl PayloadIndexInfo {
    pub fn new(field_type: PayloadFieldSchema, points_count: usize) -> Self {
        match field_type {
            PayloadFieldSchema::FieldType(data_type) => PayloadIndexInfo {
                data_type,
                params: None,
                points: points_count,
            },
            PayloadFieldSchema::FieldParams(schema_params) => PayloadIndexInfo {
                data_type: schema_params.kind(),
                params: Some(schema_params),
                points: points_count,
            },
        }
    }
}

#[derive(Debug, Serialize, JsonSchema, Anonymize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct VectorDataInfo {
    pub num_vectors: usize,
    pub num_indexed_vectors: usize,
    pub num_deleted_vectors: usize,
}

/// Aggregated information about segment
#[derive(Debug, Serialize, JsonSchema, Anonymize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SegmentInfo {
    pub segment_type: SegmentType,
    pub num_vectors: usize,
    pub num_points: usize,
    pub num_indexed_vectors: usize,
    pub num_deleted_vectors: usize,
    /// An ESTIMATION of effective amount of bytes used for vectors
    /// Do NOT rely on this number unless you know what you are doing
    pub vectors_size_bytes: usize,
    /// An estimation of the effective amount of bytes used for payloads
    pub payloads_size_bytes: usize,
    pub ram_usage_bytes: usize,
    pub disk_usage_bytes: usize,
    pub is_appendable: bool,
    pub index_schema: HashMap<PayloadKeyType, PayloadIndexInfo>,
    pub vector_data: HashMap<String, VectorDataInfo>,
}

#[derive(Debug, Default)]
pub struct SizeStats {
    pub num_vectors: usize,
    pub num_vectors_by_name: TinyMap<VectorNameBuf, usize>,
    pub vectors_size_bytes: usize,
    pub payloads_size_bytes: usize,
    pub num_points: usize,
}

/// Additional parameters of the search
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, Copy, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub struct QuantizationSearchParams {
    /// If true, quantized vectors are ignored. Default is false.
    #[serde(default = "default_quantization_ignore_value")]
    pub ignore: bool,

    /// If true, use original vectors to re-score top-k results.
    /// Might require more time in case if original vectors are stored on disk.
    /// If not set, qdrant decides automatically apply rescoring or not.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rescore: Option<bool>,

    /// Oversampling factor for quantization. Default is 1.0.
    ///
    /// Defines how many extra vectors should be pre-selected using quantized index,
    /// and then re-scored using original vectors.
    ///
    /// For example, if `oversampling` is 2.4 and `limit` is 100, then 240 vectors will be pre-selected using quantized index,
    /// and then top-100 will be returned after re-scoring.
    #[serde(default = "default_quantization_oversampling_value")]
    #[validate(range(min = 1.0))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oversampling: Option<f64>,
}

impl Hash for QuantizationSearchParams {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self {
            ignore,
            rescore,
            oversampling,
        } = self;
        ignore.hash(state);
        rescore.hash(state);
        oversampling.map(OrderedFloat).hash(state);
    }
}

pub const fn default_quantization_ignore_value() -> bool {
    false
}

pub const fn default_quantization_oversampling_value() -> Option<f64> {
    None
}

/// Default value for [`AcornSearchParams::max_selectivity`].
///
/// After change, update docs for GRPC and REST API.
pub const ACORN_MAX_SELECTIVITY_DEFAULT: f64 = 0.4;

/// ACORN-related search parameters
#[derive(
    Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, Copy, PartialEq, Default, Hash,
)]
#[serde(rename_all = "snake_case")]
pub struct AcornSearchParams {
    /// If true, then ACORN may be used for the HNSW search based on filters
    /// selectivity.

    /// Improves search recall for searches with multiple low-selectivity
    /// payload filters, at cost of performance.
    #[serde(default)]
    pub enable: bool,

    /// Maximum selectivity of filters to enable ACORN.
    ///
    /// If estimated filters selectivity is higher than this value,
    /// ACORN will not be used. Selectivity is estimated as:
    /// `estimated number of points satisfying the filters / total number of points`.
    ///   
    /// 0.0 for never, 1.0 for always. Default is 0.4.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 0.0, max = 1.0))]
    pub max_selectivity: Option<OrderedFloat<f64>>,
}

/// Additional parameters of the search
#[derive(
    Debug, Deserialize, Serialize, JsonSchema, Validate, Copy, Clone, PartialEq, Default, Hash,
)]
#[serde(rename_all = "snake_case")]
pub struct SearchParams {
    /// Params relevant to HNSW index
    /// Size of the beam in a beam-search. Larger the value - more accurate the result, more time required for search.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hnsw_ef: Option<usize>,

    /// Search without approximation. If set to true, search may run long but with exact results.
    #[serde(default)]
    pub exact: bool,

    /// Quantization params
    #[serde(default)]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantization: Option<QuantizationSearchParams>,

    /// If enabled, the engine will only perform search among indexed or small segments.
    /// Using this option prevents slow searches in case of delayed index, but does not
    /// guarantee that all uploaded vectors will be included in search results
    #[serde(default)]
    pub indexed_only: bool,

    /// ACORN search params
    #[serde(default)]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acorn: Option<AcornSearchParams>,
}

/// Configuration for vectors.
#[derive(Debug, Deserialize, Validate, Clone, PartialEq, Eq)]
pub struct VectorsConfigDefaults {
    #[serde(default)]
    pub on_disk: Option<bool>,
}

/// Vector index configuration
#[derive(Debug, Deserialize, Serialize, JsonSchema, Anonymize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "options")]
pub enum Indexes {
    /// Do not use any index, scan whole vector collection during search.
    /// Guarantee 100% precision, but may be time consuming on large collections.
    Plain {},
    /// Use filterable HNSW index for approximate search. Is very fast even on a very huge collections,
    /// but require additional space to store index and additional time to build it.
    Hnsw(HnswConfig),
}

impl Indexes {
    pub fn is_indexed(&self) -> bool {
        match self {
            Indexes::Plain {} => false,
            Indexes::Hnsw(_) => true,
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            Indexes::Plain {} => false,
            Indexes::Hnsw(config) => config.on_disk.unwrap_or_default(),
        }
    }
}

/// Config of HNSW index
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Anonymize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[anonymize(false)]
pub struct HnswConfig {
    /// Number of edges per node in the index graph. Larger the value - more accurate the search, more space required.
    pub m: usize,
    /// Number of neighbours to consider during the index building. Larger the value - more accurate the search, more time required to build index.
    #[validate(range(min = 4))]
    pub ef_construct: usize,
    /// Minimal size threshold (in KiloBytes) below which full-scan is preferred over HNSW search.
    /// This measures the total size of vectors being queried against.
    /// When the maximum estimated amount of points that a condition satisfies is smaller than
    /// `full_scan_threshold_kb`, the query planner will use full-scan search instead of HNSW index
    /// traversal for better performance.
    /// Note: 1Kb = 1 vector of size 256
    #[serde(alias = "full_scan_threshold_kb")]
    pub full_scan_threshold: usize,
    /// Number of parallel threads used for background index building.
    /// If 0 - automatically select from 8 to 16.
    /// Best to keep between 8 and 16 to prevent likelihood of slow building or broken/inefficient HNSW graphs.
    /// On small CPUs, less threads are used.
    #[serde(default = "default_max_indexing_threads")]
    pub max_indexing_threads: usize,
    /// Store HNSW index on disk. If set to false, index will be stored in RAM. Default: false
    #[serde(default, skip_serializing_if = "Option::is_none")] // Better backward compatibility
    pub on_disk: Option<bool>,
    /// Custom M param for hnsw graph built for payload index. If not set, default M will be used.
    #[serde(default, skip_serializing_if = "Option::is_none")] // Better backward compatibility
    pub payload_m: Option<usize>,
    /// Store copies of original and quantized vectors within the HNSW index file. Default: false.
    /// Enabling this option will trade the search speed for disk usage by reducing amount of
    /// random seeks during the search.
    /// Requires quantized vectors to be enabled. Multi-vectors are not supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_storage: Option<bool>,
}

impl HnswConfig {
    /// Detect configuration mismatch against `other` that requires rebuilding
    ///
    /// Returns true only if both conditions are met:
    /// - this configuration does not match `other`
    /// - to effectively change the configuration, a HNSW rebuild is required
    ///
    /// For example, a change in `max_indexing_threads` will not require rebuilding because it
    /// doesn't affect the final index, and thus this would return false.
    pub fn mismatch_requires_rebuild(&self, other: &Self) -> bool {
        let HnswConfig {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads: _,
            payload_m,
            on_disk,
            inline_storage,
        } = self.clone();

        m != other.m
            || ef_construct != other.ef_construct
            || full_scan_threshold != other.full_scan_threshold
            || payload_m != other.payload_m
            // Data on disk is the same, we have a unit test for that. We can eventually optimize
            // this to just reload the collection rather than optimizing it again as a whole just
            // to flip this flag
            || on_disk != other.on_disk
            || inline_storage != other.inline_storage
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Anonymize, Clone)]
#[serde(rename_all = "snake_case", default)]
#[anonymize(false)]
pub struct HnswGlobalConfig {
    /// Enable HNSW healing if the ratio of missing points is no more than this value.
    /// To disable healing completely, set this value to `0.0`.
    #[validate(range(min = 0.0, max = 1.0))]
    pub healing_threshold: f64,
}

impl Default for HnswGlobalConfig {
    fn default() -> Self {
        Self {
            healing_threshold: 0.3,
        }
    }
}

const fn default_max_indexing_threads() -> usize {
    0
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum CompressionRatio {
    X4,
    X8,
    X16,
    X32,
    X64,
}

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ScalarType {
    #[default]
    Int8,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ScalarQuantizationConfig {
    /// Type of quantization to use
    /// If `int8` - 8 bit quantization will be used
    pub r#type: ScalarType,
    /// Quantile for quantization. Expected value range in [0.5, 1.0]. If not set - use the whole range of values
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 0.5, max = 1.0))]
    pub quantile: Option<f32>,
    /// If true - quantized vectors always will be stored in RAM, ignoring the config of main storage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub always_ram: Option<bool>,
}

impl ScalarQuantizationConfig {
    /// Detect configuration mismatch against `other` that requires rebuilding
    ///
    /// Returns true only if both conditions are met:
    /// - this configuration does not match `other`
    /// - to effectively change the configuration, a quantization rebuild is required
    pub fn mismatch_requires_rebuild(&self, other: &Self) -> bool {
        self != other
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq, Hash)]
pub struct ScalarQuantization {
    #[validate(nested)]
    pub scalar: ScalarQuantizationConfig,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub struct ProductQuantizationConfig {
    pub compression: CompressionRatio,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub always_ram: Option<bool>,
}

impl ProductQuantizationConfig {
    /// Detect configuration mismatch against `other` that requires rebuilding
    ///
    /// Returns true only if both conditions are met:
    /// - this configuration does not match `other`
    /// - to effectively change the configuration, a quantization rebuild is required
    pub fn mismatch_requires_rebuild(&self, other: &Self) -> bool {
        self != other
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq, Hash)]
pub struct ProductQuantization {
    #[validate(nested)]
    pub product: ProductQuantizationConfig,
}

impl Hash for ScalarQuantizationConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.always_ram.hash(state);
        self.r#type.hash(state);
    }
}

impl Eq for ScalarQuantizationConfig {}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum BinaryQuantizationEncoding {
    #[default]
    OneBit,
    TwoBits,
    OneAndHalfBits,
}

impl BinaryQuantizationEncoding {
    pub fn is_one_bit(&self) -> bool {
        matches!(self, BinaryQuantizationEncoding::OneBit)
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub struct BinaryQuantizationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub always_ram: Option<bool>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<BinaryQuantizationEncoding>,

    /// Asymmetric quantization configuration allows a query to have different quantization than stored vectors.
    /// It can increase the accuracy of search at the cost of performance.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_encoding: Option<BinaryQuantizationQueryEncoding>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq, Hash)]
pub struct BinaryQuantization {
    #[validate(nested)]
    pub binary: BinaryQuantizationConfig,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Anonymize, Clone, PartialEq, Eq, Hash)]
#[serde(untagged, rename_all = "snake_case")]
#[anonymize(false)]
pub enum QuantizationConfig {
    Scalar(ScalarQuantization),
    Product(ProductQuantization),
    Binary(BinaryQuantization),
}

impl QuantizationConfig {
    /// Detect configuration mismatch against `other` that requires rebuilding
    ///
    /// Returns true only if both conditions are met:
    /// - this configuration does not match `other`
    /// - to effectively change the configuration, a quantization rebuild is required
    pub fn mismatch_requires_rebuild(&self, other: &Self) -> bool {
        self != other
    }

    pub fn supports_appendable(&self) -> bool {
        matches!(self, QuantizationConfig::Binary(_))
    }
}

impl Validate for QuantizationConfig {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            QuantizationConfig::Scalar(scalar) => scalar.validate(),
            QuantizationConfig::Product(product) => product.validate(),
            QuantizationConfig::Binary(binary) => binary.validate(),
        }
    }
}

#[derive(
    Default, Debug, Deserialize, Serialize, JsonSchema, Anonymize, Clone, Copy, PartialEq, Eq, Hash,
)]
#[serde(rename_all = "lowercase")]
#[anonymize(false)]
pub enum BinaryQuantizationQueryEncoding {
    #[default]
    Default,
    Binary,
    Scalar4Bits,
    Scalar8Bits,
}

impl From<ScalarQuantizationConfig> for QuantizationConfig {
    fn from(config: ScalarQuantizationConfig) -> Self {
        QuantizationConfig::Scalar(ScalarQuantization { scalar: config })
    }
}

impl From<ProductQuantizationConfig> for QuantizationConfig {
    fn from(config: ProductQuantizationConfig) -> Self {
        QuantizationConfig::Product(ProductQuantization { product: config })
    }
}

impl From<BinaryQuantizationConfig> for QuantizationConfig {
    fn from(config: BinaryQuantizationConfig) -> Self {
        QuantizationConfig::Binary(BinaryQuantization { binary: config })
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Default, Hash)]
pub struct StrictModeSparse {
    /// Max length of sparse vector
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1))]
    pub max_length: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Default, Hash)]
#[schemars(deny_unknown_fields)]
pub struct StrictModeSparseConfig {
    #[validate(nested)]
    #[serde(flatten)]
    pub config: BTreeMap<VectorNameBuf, StrictModeSparse>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Anonymize, Clone, PartialEq, Default)]
#[schemars(deny_unknown_fields)]
pub struct StrictModeSparseConfigOutput {
    #[serde(flatten)]
    pub config: BTreeMap<VectorNameBuf, StrictModeSparseOutput>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Anonymize, Clone, PartialEq, Default)]
pub struct StrictModeSparseOutput {
    /// Max length of sparse vector
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub max_length: Option<usize>,
}

impl From<StrictModeSparseConfig> for StrictModeSparseConfigOutput {
    fn from(config: StrictModeSparseConfig) -> Self {
        let StrictModeSparseConfig { config } = config;
        let mut new_config = StrictModeSparseConfigOutput::default();
        for (key, value) in config {
            new_config
                .config
                .insert(key, StrictModeSparseOutput::from(value));
        }
        new_config
    }
}

impl From<StrictModeSparse> for StrictModeSparseOutput {
    fn from(config: StrictModeSparse) -> Self {
        let StrictModeSparse { max_length } = config;
        StrictModeSparseOutput { max_length }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Default, Hash)]
pub struct StrictModeMultivector {
    /// Max number of vectors in a multivector
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1))]
    pub max_vectors: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Default, Hash)]
#[schemars(deny_unknown_fields)]
pub struct StrictModeMultivectorConfig {
    #[validate(nested)]
    #[serde(flatten)]
    pub config: BTreeMap<VectorNameBuf, StrictModeMultivector>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Anonymize, Clone, PartialEq, Default)]
#[schemars(deny_unknown_fields)]
pub struct StrictModeMultivectorConfigOutput {
    #[serde(flatten)]
    pub config: BTreeMap<VectorNameBuf, StrictModeMultivectorOutput>,
}

impl From<StrictModeMultivectorConfig> for StrictModeMultivectorConfigOutput {
    fn from(config: StrictModeMultivectorConfig) -> Self {
        let StrictModeMultivectorConfig { config } = config;
        let mut new_config = StrictModeMultivectorConfigOutput::default();
        for (key, value) in config {
            new_config
                .config
                .insert(key, StrictModeMultivectorOutput::from(value));
        }
        new_config
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Anonymize, Clone, PartialEq, Default)]
pub struct StrictModeMultivectorOutput {
    /// Max number of vectors in a multivector
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub max_vectors: Option<usize>,
}

impl From<StrictModeMultivector> for StrictModeMultivectorOutput {
    fn from(config: StrictModeMultivector) -> Self {
        let StrictModeMultivector { max_vectors } = config;
        StrictModeMultivectorOutput { max_vectors }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Default)]
pub struct StrictModeConfig {
    // Global
    /// Whether strict mode is enabled for a collection or not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    /// Max allowed `limit` parameter for all APIs that don't have their own max limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1))]
    pub max_query_limit: Option<usize>,

    /// Max allowed `timeout` parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1))]
    pub max_timeout: Option<usize>,

    /// Allow usage of unindexed fields in retrieval based (e.g. search) filters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unindexed_filtering_retrieve: Option<bool>,

    /// Allow usage of unindexed fields in filtered updates (e.g. delete by payload).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unindexed_filtering_update: Option<bool>,

    // Search
    /// Max HNSW ef value allowed in search parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_max_hnsw_ef: Option<usize>,

    /// Whether exact search is allowed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_allow_exact: Option<bool>,

    /// Max oversampling value allowed in search.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_max_oversampling: Option<f64>,

    /// Max batchsize when upserting
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upsert_max_batchsize: Option<usize>,

    /// Max size of a collections vector storage in bytes, ignoring replicas.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_collection_vector_size_bytes: Option<usize>,

    /// Max number of read operations per minute per replica
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1))]
    pub read_rate_limit: Option<usize>,

    /// Max number of write operations per minute per replica
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1))]
    pub write_rate_limit: Option<usize>,

    /// Max size of a collections payload storage in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_collection_payload_size_bytes: Option<usize>,

    /// Max number of points estimated in a collection
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1))]
    pub max_points_count: Option<usize>,

    /// Max conditions a filter can have.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_max_conditions: Option<usize>,

    /// Max size of a condition, eg. items in `MatchAny`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_max_size: Option<usize>,

    /// Multivector strict mode configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(nested)]
    pub multivector_config: Option<StrictModeMultivectorConfig>,

    /// Sparse vector strict mode configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(nested)]
    pub sparse_config: Option<StrictModeSparseConfig>,

    /// Max number of payload indexes in a collection
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 0))]
    pub max_payload_index_count: Option<usize>,
}

impl Eq for StrictModeConfig {}

impl Hash for StrictModeConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let Self {
            enabled,
            max_query_limit,
            max_timeout,
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
            search_max_hnsw_ef,
            search_allow_exact,
            // We skip hashing this field because we cannot reliably hash a float
            search_max_oversampling: _,
            upsert_max_batchsize,
            max_collection_vector_size_bytes,
            read_rate_limit,
            write_rate_limit,
            max_collection_payload_size_bytes,
            max_points_count,
            filter_max_conditions,
            condition_max_size,
            multivector_config,
            sparse_config,
            max_payload_index_count,
        } = self;
        enabled.hash(state);
        max_query_limit.hash(state);
        max_timeout.hash(state);
        unindexed_filtering_retrieve.hash(state);
        unindexed_filtering_update.hash(state);
        search_max_hnsw_ef.hash(state);
        search_allow_exact.hash(state);
        upsert_max_batchsize.hash(state);
        max_collection_vector_size_bytes.hash(state);
        read_rate_limit.hash(state);
        write_rate_limit.hash(state);
        max_collection_payload_size_bytes.hash(state);
        max_points_count.hash(state);
        filter_max_conditions.hash(state);
        condition_max_size.hash(state);
        multivector_config.hash(state);
        sparse_config.hash(state);
        max_payload_index_count.hash(state);
    }
}

// Version of the strict mode config we can present to the user
#[derive(Debug, Deserialize, Serialize, JsonSchema, Anonymize, Clone, PartialEq, Default)]
pub struct StrictModeConfigOutput {
    // Global
    /// Whether strict mode is enabled for a collection or not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    /// Max allowed `limit` parameter for all APIs that don't have their own max limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1))]
    #[anonymize(false)]
    pub max_query_limit: Option<usize>,

    /// Max allowed `timeout` parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1))]
    #[anonymize(false)]
    pub max_timeout: Option<usize>,

    /// Allow usage of unindexed fields in retrieval based (e.g. search) filters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unindexed_filtering_retrieve: Option<bool>,

    /// Allow usage of unindexed fields in filtered updates (e.g. delete by payload).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unindexed_filtering_update: Option<bool>,

    // Search
    /// Max HNSW value allowed in search parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub search_max_hnsw_ef: Option<usize>,

    /// Whether exact search is allowed or not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_allow_exact: Option<bool>,

    /// Max oversampling value allowed in search.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub search_max_oversampling: Option<f64>,

    /// Max batchsize when upserting
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub upsert_max_batchsize: Option<usize>,

    /// Max size of a collections vector storage in bytes, ignoring replicas.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub max_collection_vector_size_bytes: Option<usize>,

    /// Max number of read operations per minute per replica
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub read_rate_limit: Option<usize>,

    /// Max number of write operations per minute per replica
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub write_rate_limit: Option<usize>,

    /// Max size of a collections payload storage in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub max_collection_payload_size_bytes: Option<usize>,

    /// Max number of points estimated in a collection
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub max_points_count: Option<usize>,

    /// Max conditions a filter can have.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub filter_max_conditions: Option<usize>,

    /// Max size of a condition, eg. items in `MatchAny`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub condition_max_size: Option<usize>,

    /// Multivector configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multivector_config: Option<StrictModeMultivectorConfigOutput>,

    /// Sparse vector configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_config: Option<StrictModeSparseConfigOutput>,

    /// Max number of payload indexes in a collection
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 0))]
    pub max_payload_index_count: Option<usize>,
}

impl From<StrictModeConfig> for StrictModeConfigOutput {
    fn from(config: StrictModeConfig) -> Self {
        let StrictModeConfig {
            enabled,
            max_query_limit,
            max_timeout,
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
            search_max_hnsw_ef,
            search_allow_exact,
            search_max_oversampling,
            upsert_max_batchsize,
            max_collection_vector_size_bytes,
            read_rate_limit,
            write_rate_limit,
            max_collection_payload_size_bytes,
            max_points_count,
            filter_max_conditions,
            condition_max_size,
            multivector_config,
            sparse_config,
            max_payload_index_count,
        } = config;

        Self {
            enabled,
            max_query_limit,
            max_timeout,
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
            search_max_hnsw_ef,
            search_allow_exact,
            search_max_oversampling,
            upsert_max_batchsize,
            max_collection_vector_size_bytes,
            read_rate_limit,
            write_rate_limit,
            max_collection_payload_size_bytes,
            max_points_count,
            filter_max_conditions,
            condition_max_size,
            multivector_config: multivector_config.map(StrictModeMultivectorConfigOutput::from),
            sparse_config: sparse_config.map(StrictModeSparseConfigOutput::from),
            max_payload_index_count,
        }
    }
}

pub const DEFAULT_HNSW_EF_CONSTRUCT: usize = 100;

impl Default for HnswConfig {
    fn default() -> Self {
        HnswConfig {
            m: 16,
            ef_construct: DEFAULT_HNSW_EF_CONSTRUCT,
            full_scan_threshold: DEFAULT_FULL_SCAN_THRESHOLD,
            max_indexing_threads: 0,
            on_disk: Some(false),
            payload_m: None,
            inline_storage: None,
        }
    }
}

impl Default for Indexes {
    fn default() -> Self {
        Indexes::Plain {}
    }
}

/// Type of payload storage
#[derive(Anonymize, Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(tag = "type", content = "options", rename_all = "snake_case")]
pub enum PayloadStorageType {
    // Store payload in memory and use persistence storage only if vectors are changed
    #[cfg(feature = "rocksdb")]
    InMemory,
    // Store payload on disk only, read each time it is requested
    #[cfg(feature = "rocksdb")]
    OnDisk,
    // Store payload on disk and in memory, read from memory if possible
    Mmap,
    // Store payload on disk and in memory, populate on load
    InRamMmap,
}

#[allow(clippy::derivable_impls)]
impl Default for PayloadStorageType {
    fn default() -> Self {
        #[cfg(feature = "rocksdb")]
        {
            PayloadStorageType::OnDisk
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            PayloadStorageType::Mmap
        }
    }
}

impl PayloadStorageType {
    pub fn is_on_disk(&self) -> bool {
        match self {
            #[cfg(feature = "rocksdb")]
            PayloadStorageType::InMemory => false,
            #[cfg(feature = "rocksdb")]
            PayloadStorageType::OnDisk => true,
            PayloadStorageType::Mmap => true,
            PayloadStorageType::InRamMmap => false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema, Anonymize)]
#[serde(rename_all = "snake_case")]
pub struct SegmentConfig {
    #[serde(default)]
    pub vector_data: HashMap<VectorNameBuf, VectorDataConfig>,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub sparse_vector_data: HashMap<VectorNameBuf, SparseVectorDataConfig>,
    /// Defines payload storage type
    pub payload_storage_type: PayloadStorageType,
}

impl SegmentConfig {
    /// Helper to get vector specific quantization config.
    ///
    /// This grabs the quantization config for the given vector name if it exists.
    ///
    /// If no quantization is configured, `None` is returned.
    pub fn quantization_config(&self, vector_name: &VectorName) -> Option<&QuantizationConfig> {
        self.vector_data
            .get(vector_name)
            .and_then(|v| v.quantization_config.as_ref())
    }

    /// Check if any vector storages are indexed
    pub fn is_any_vector_indexed(&self) -> bool {
        self.vector_data
            .values()
            .any(|config| config.index.is_indexed())
            || self
                .sparse_vector_data
                .values()
                .any(|config| config.is_indexed())
    }

    /// Check if all vector storages are indexed
    pub fn are_all_vectors_indexed(&self) -> bool {
        self.vector_data
            .values()
            .all(|config| config.index.is_indexed())
            && self
                .sparse_vector_data
                .values()
                .all(|config| config.is_indexed())
    }

    /// Check if any vector storage is on-disk
    pub fn is_any_on_disk(&self) -> bool {
        self.vector_data
            .values()
            .any(|config| config.storage_type.is_on_disk())
            || self
                .sparse_vector_data
                .values()
                .any(|config| config.index.index_type.is_on_disk())
    }

    pub fn is_appendable(&self) -> bool {
        self.vector_data
            .values()
            .map(|vector_config| vector_config.is_appendable())
            .chain(
                self.sparse_vector_data
                    .values()
                    .map(|sparse_vector_config| {
                        sparse_vector_config.index.index_type.is_appendable()
                    }),
            )
            .all(|v| v)
    }

    pub fn is_compatible(&self, other: &Self) -> bool {
        // Vector data have to be compatible between two segments.
        // Sparse vector data can be different, but a placeholder check is implemented to catch
        // and enforce compatibility check for future changes.
        // Payload storage type can be different.

        // Assert segment config fields
        let Self {
            vector_data: _,
            sparse_vector_data: _,
            payload_storage_type: _,
        } = self;

        let is_vector_config_compatible = is_map_compatible(
            &self.vector_data,
            &other.vector_data,
            VectorDataConfig::is_compatible,
        );

        let is_sparse_vector_config_compatible = is_map_compatible(
            &self.sparse_vector_data,
            &other.sparse_vector_data,
            SparseVectorDataConfig::is_compatible,
        );

        is_vector_config_compatible && is_sparse_vector_config_compatible
    }
}

fn is_map_compatible<V, C, F>(this: &HashMap<V, C>, other: &HashMap<V, C>, check: F) -> bool
where
    V: Eq + Hash,
    F: Fn(&C, &C) -> bool,
{
    if this.len() != other.len() {
        return false;
    }

    for (vector_name, config) in this {
        let Some(other_config) = other.get(vector_name) else {
            return false;
        };

        if !check(config, other_config) {
            return false;
        }
    }

    true
}

/// Storage types for vectors
#[derive(Debug, Deserialize, Serialize, JsonSchema, Anonymize, Eq, PartialEq, Copy, Clone)]
pub enum VectorStorageType {
    /// Storage in memory (RAM)
    ///
    /// Will be very fast at the cost of consuming a lot of memory.
    Memory,
    /// Storage in mmap file, not appendable
    ///
    /// Search performance is defined by disk speed and the fraction of vectors that fit in memory.
    Mmap,
    /// Storage in chunked mmap files, appendable
    ///
    /// Search performance is defined by disk speed and the fraction of vectors that fit in memory.
    ChunkedMmap,
    /// Same as `ChunkedMmap`, but vectors are forced to be locked in RAM
    /// In this way we avoid cold requests to disk, but risk to run out of memory
    ///
    /// Designed as a replacement for `Memory`, which doesn't depend on RocksDB
    InRamChunkedMmap,
}

#[cfg(any(test, feature = "testing"))]
#[allow(clippy::derivable_impls)]
impl Default for VectorStorageType {
    fn default() -> Self {
        #[cfg(feature = "rocksdb")]
        {
            VectorStorageType::Memory
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            VectorStorageType::InRamChunkedMmap
        }
    }
}

/// Storage types for vectors
#[derive(
    Default, Debug, Deserialize, Serialize, JsonSchema, Anonymize, Eq, PartialEq, Copy, Clone,
)]
#[serde(rename_all = "snake_case")]
pub enum VectorStorageDatatype {
    // Single-precision floating point
    #[default]
    Float32,
    // Half-precision floating point
    Float16,
    // Unsigned 8-bit integer
    Uint8,
}

#[derive(
    Debug, Default, Deserialize, Serialize, JsonSchema, Anonymize, Eq, PartialEq, Copy, Clone, Hash,
)]
#[serde(rename_all = "snake_case")]
pub struct MultiVectorConfig {
    /// How to compare multivector points
    pub comparator: MultiVectorComparator,
}

impl MultiVectorConfig {
    fn is_compatible(&self, other: &Self) -> bool {
        // TODO: Does comparator have to be same for two segments to be compatible? 🤔

        // Assert multi-vector config fields
        let Self { comparator: _ } = self;

        self.comparator == other.comparator // TODO: 🤔
    }
}

#[derive(
    Debug, Default, Deserialize, Serialize, JsonSchema, Anonymize, Eq, PartialEq, Copy, Clone, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum MultiVectorComparator {
    #[default]
    MaxSim,
}

impl VectorStorageType {
    /// Whether this storage type is a mmap on disk
    pub fn is_on_disk(&self) -> bool {
        match self {
            Self::Memory | Self::InRamChunkedMmap => false,
            Self::Mmap | Self::ChunkedMmap => true,
        }
    }
}

/// Config of single vector data storage
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema, Anonymize)]
#[serde(rename_all = "snake_case")]
pub struct VectorDataConfig {
    /// Size/dimensionality of the vectors used
    pub size: usize,
    /// Type of distance function used for measuring distance between vectors
    pub distance: Distance,
    /// Type of storage this vector uses
    pub storage_type: VectorStorageType,
    /// Type of index used for search
    pub index: Indexes,
    /// Vector specific quantization config that overrides collection config
    pub quantization_config: Option<QuantizationConfig>,
    /// Vector specific configuration to enable multiple vectors per point
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multivector_config: Option<MultiVectorConfig>,
    /// Vector specific configuration to set specific storage element type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatype: Option<VectorStorageDatatype>,
}

impl VectorDataConfig {
    /// Whether this vector data can be appended to
    ///
    /// This requires an index and storage type that both support appending.
    pub fn is_appendable(&self) -> bool {
        let is_index_appendable = match self.index {
            Indexes::Plain {} => true,
            Indexes::Hnsw(_) => false,
        };
        let is_storage_appendable = match self.storage_type {
            VectorStorageType::Memory => true,
            VectorStorageType::Mmap => false,
            VectorStorageType::ChunkedMmap => true,
            VectorStorageType::InRamChunkedMmap => true,
        };
        is_index_appendable && is_storage_appendable
    }

    pub fn is_compatible(&self, other: &Self) -> bool {
        // Size and distance have to be the same for both segments.
        // Storage type, index and quantization config can be different.
        //
        // TODO: Can multivector config and datatype be different?

        // Assert vector data config fields
        let Self {
            size: _,
            distance: _,
            storage_type: _,
            index: _,
            quantization_config: _,
            multivector_config: _,
            datatype: _,
        } = self;

        self.size == other.size
            && self.distance == other.distance
            && self.datatype == other.datatype // TODO: 🤔
            && is_opt_compatible(
                self.multivector_config.as_ref(),
                other.multivector_config.as_ref(),
                MultiVectorConfig::is_compatible,
            )
    }
}

fn is_opt_compatible<T, F: Fn(T, T) -> bool>(this: Option<T>, other: Option<T>, check: F) -> bool {
    match (this, other) {
        (Some(this), Some(other)) => check(this, other),
        (None, None) => true,
        _ => false,
    }
}

#[derive(
    Copy, Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize, JsonSchema, Anonymize,
)]
#[serde(rename_all = "snake_case")]
pub enum SparseVectorStorageType {
    /// Storage on disk (rocksdb storage)
    #[cfg(feature = "rocksdb")]
    OnDisk,
    /// Storage in memory maps (gridstore storage)
    #[default]
    Mmap,
}

impl SparseVectorStorageType {
    /// Whether this storage type is a mmap on disk
    pub fn is_on_disk(&self) -> bool {
        match self {
            // Both options are on disk, but we keep it explicit for the case if someone adds a new
            // storage type in the future
            #[cfg(feature = "rocksdb")]
            Self::OnDisk => true,
            Self::Mmap => true,
        }
    }
}

/// Config of single sparse vector data storage
#[derive(
    Copy, Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema, Validate, Anonymize,
)]
#[serde(rename_all = "snake_case")]
pub struct SparseVectorDataConfig {
    /// Sparse inverted index config
    pub index: SparseIndexConfig,

    /// Type of storage this sparse vector uses
    #[serde(default = "default_sparse_vector_storage_type_when_not_in_config")]
    pub storage_type: SparseVectorStorageType,

    /// Configures addition value modifications for sparse vectors.
    /// Default: none
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modifier: Option<Modifier>,
}

/// If the storage type is not in config, it means it is the OnDisk variant
fn default_sparse_vector_storage_type_when_not_in_config() -> SparseVectorStorageType {
    #[cfg(feature = "rocksdb")]
    {
        SparseVectorStorageType::OnDisk
    }
    #[cfg(not(feature = "rocksdb"))]
    {
        SparseVectorStorageType::default()
    }
}

impl SparseVectorDataConfig {
    pub fn is_indexed(&self) -> bool {
        true
    }

    pub fn is_compatible(&self, other: &Self) -> bool {
        // Both index and storage type can be different for two segments to be compatible

        // Assert sparse vector config fields
        let Self {
            index: _,
            storage_type: _,
            modifier,
        } = self;

        modifier == &other.modifier
    }
}

/// Default value based on experiments and observations
pub const DEFAULT_FULL_SCAN_THRESHOLD: usize = 10_000;

pub const DEFAULT_SPARSE_FULL_SCAN_THRESHOLD: usize = 5_000;

/// Persistable state of segment configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SegmentState {
    #[serde(default)]
    pub initial_version: Option<SeqNumberType>,
    pub version: Option<SeqNumberType>,
    pub config: SegmentConfig,
}

pub type RawGeoPoint = (f64, f64);

/// Geo point payload schema
#[derive(
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Default,
    PartialOrd,
    Ord,
)]
#[serde(try_from = "GeoPointShadow")]
pub struct GeoPoint {
    pub lon: OrderedFloat<f64>,
    pub lat: OrderedFloat<f64>,
}

/// Ordered sequence of GeoPoints representing the line
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
pub struct GeoLineString {
    pub points: Vec<GeoPoint>,
}

#[derive(Deserialize)]
struct GeoPointShadow {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Debug)]
pub struct GeoPointValidationError {
    pub lon: f64,
    pub lat: f64,
}

// The error type has to implement Display
impl std::fmt::Display for GeoPointValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Wrong format of GeoPoint payload: expected `lat` = {} within [-90;90] and `lon` = {} within [-180;180]",
            self.lat, self.lon,
        )
    }
}

impl GeoPoint {
    pub fn validate(lon: f64, lat: f64) -> Result<(), GeoPointValidationError> {
        let max_lon = 180f64;
        let min_lon = -180f64;
        let max_lat = 90f64;
        let min_lat = -90f64;

        if !(min_lon..=max_lon).contains(&lon) || !(min_lat..=max_lat).contains(&lat) {
            return Err(GeoPointValidationError { lon, lat });
        }
        Ok(())
    }

    pub fn new(lon: f64, lat: f64) -> Result<Self, GeoPointValidationError> {
        Self::validate(lon, lat)?;
        Ok(Self::new_unchecked(lon, lat))
    }

    pub const fn new_unchecked(lon: f64, lat: f64) -> Self {
        GeoPoint {
            lon: OrderedFloat(lon),
            lat: OrderedFloat(lat),
        }
    }
}

impl TryFrom<GeoPointShadow> for GeoPoint {
    type Error = GeoPointValidationError;

    fn try_from(value: GeoPointShadow) -> Result<Self, Self::Error> {
        let GeoPointShadow { lon, lat } = value;
        GeoPoint::validate(lon, lat)?;

        Ok(Self::new_unchecked(lon, lat))
    }
}

impl From<GeoPoint> for geo::Point {
    fn from(
        GeoPoint {
            lon: OrderedFloat(lon),
            lat: OrderedFloat(lat),
        }: GeoPoint,
    ) -> Self {
        Self::new(lon, lat)
    }
}

impl From<RawGeoPoint> for GeoPoint {
    fn from((lon, lat): RawGeoPoint) -> Self {
        GeoPoint::new(lon, lat).expect("invalid GeoPoint coordinates")
    }
}

impl From<GeoPoint> for RawGeoPoint {
    fn from(geo_point: GeoPoint) -> Self {
        (geo_point.lon.0, geo_point.lat.0)
    }
}

pub trait PayloadContainer {
    /// Return value from payload by path.
    /// If value is not present in the payload, returns empty vector.
    fn get_value(&self, path: &JsonPath) -> MultiValue<&Value>;

    fn get_value_cloned(&self, path: &JsonPath) -> MultiValue<Value> {
        self.get_value(path).into_iter().cloned().collect()
    }
}

/// Construct a [`Payload`] value from a JSON literal.
///
/// Similar to [`serde_json::json!`] but only allows objects (aka maps).
#[macro_export]
macro_rules! payload_json {
    ($($tt:tt)*) => {
        match ::serde_json::json!( { $($tt)* } ) {
            ::serde_json::Value::Object(map) => $crate::types::Payload(map),
            _ => unreachable!(),
        }
    };
}

#[allow(clippy::unnecessary_wraps)] // Used as schemars example
fn payload_example() -> Option<Payload> {
    Some(payload_json! {
        "city": "London",
        "color": "green",
    })
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema, Hash)]
#[schemars(example = "payload_example")]
pub struct Payload(pub Map<String, Value>);

impl Payload {
    pub fn merge(&mut self, value: &Payload) {
        utils::merge_map(&mut self.0, &value.0)
    }

    pub fn merge_by_key(&mut self, value: &Payload, key: &JsonPath) {
        JsonPath::value_set(Some(key), &mut self.0, &value.0);
    }

    pub fn remove(&mut self, path: &JsonPath) -> Vec<Value> {
        path.value_remove(&mut self.0).to_vec()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.0.keys()
    }
}

impl PayloadContainer for Map<String, Value> {
    fn get_value(&self, path: &JsonPath) -> MultiValue<&Value> {
        path.value_get(self)
    }
}

impl PayloadContainer for Payload {
    fn get_value(&self, path: &JsonPath) -> MultiValue<&Value> {
        path.value_get(&self.0)
    }
}

impl PayloadContainer for OwnedPayloadRef<'_> {
    fn get_value(&self, path: &JsonPath) -> MultiValue<&Value> {
        path.value_get(self.as_ref())
    }
}

impl Default for Payload {
    fn default() -> Self {
        Payload(Map::new())
    }
}

impl IntoIterator for Payload {
    type Item = (String, Value);
    type IntoIter = serde_json::map::IntoIter;

    fn into_iter(self) -> serde_json::map::IntoIter {
        self.0.into_iter()
    }
}

impl From<Map<String, Value>> for Payload {
    fn from(value: serde_json::Map<String, Value>) -> Self {
        Payload(value)
    }
}

#[derive(Clone, Debug)]
pub enum OwnedPayloadRef<'a> {
    Ref(&'a Map<String, Value>),
    Owned(Rc<Map<String, Value>>),
}

impl Deref for OwnedPayloadRef<'_> {
    type Target = Map<String, Value>;

    fn deref(&self) -> &Self::Target {
        match self {
            OwnedPayloadRef::Ref(reference) => reference,
            OwnedPayloadRef::Owned(owned) => owned.deref(),
        }
    }
}

impl AsRef<Map<String, Value>> for OwnedPayloadRef<'_> {
    fn as_ref(&self) -> &Map<String, Value> {
        match self {
            OwnedPayloadRef::Ref(reference) => reference,
            OwnedPayloadRef::Owned(owned) => owned.deref(),
        }
    }
}

impl From<Payload> for OwnedPayloadRef<'_> {
    fn from(payload: Payload) -> Self {
        OwnedPayloadRef::Owned(Rc::new(payload.0))
    }
}

impl From<Map<String, Value>> for OwnedPayloadRef<'_> {
    fn from(payload: Map<String, Value>) -> Self {
        OwnedPayloadRef::Owned(Rc::new(payload))
    }
}

impl<'a> From<&'a Payload> for OwnedPayloadRef<'a> {
    fn from(payload: &'a Payload) -> Self {
        OwnedPayloadRef::Ref(&payload.0)
    }
}

impl<'a> From<&'a Map<String, Value>> for OwnedPayloadRef<'a> {
    fn from(payload: &'a Map<String, Value>) -> Self {
        OwnedPayloadRef::Ref(payload)
    }
}

/// Payload interface structure which ensures that user is allowed to pass payload in
/// both - array and single element forms.
///
/// Example:
///
/// Both versions should work:
/// ```json
/// {..., "payload": {"city": {"type": "keyword", "value": ["Berlin", "London"] }}},
/// {..., "payload": {"city": {"type": "keyword", "value": "Moscow" }}},
/// ```
#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Clone)]
#[serde(untagged, rename_all = "snake_case")]
pub enum PayloadVariant<T> {
    List(Vec<T>),
    Value(T),
}

/// All possible names of payload types
#[derive(
    Debug, Deserialize, Serialize, JsonSchema, Anonymize, Clone, Copy, PartialEq, Hash, Eq, EnumIter,
)]
#[serde(rename_all = "snake_case")]
pub enum PayloadSchemaType {
    Keyword,
    Integer,
    Float,
    Geo,
    Text,
    Bool,
    Datetime,
    Uuid,
}

impl PayloadSchemaType {
    /// Human-readable type name
    pub fn name(&self) -> &'static str {
        serde_variant::to_variant_name(&self).unwrap_or("unknown")
    }

    pub fn expand(&self) -> PayloadSchemaParams {
        match self {
            Self::Keyword => PayloadSchemaParams::Keyword(KeywordIndexParams::default()),
            Self::Integer => PayloadSchemaParams::Integer(IntegerIndexParams::default()),
            Self::Float => PayloadSchemaParams::Float(FloatIndexParams::default()),
            Self::Geo => PayloadSchemaParams::Geo(GeoIndexParams::default()),
            Self::Text => PayloadSchemaParams::Text(TextIndexParams::default()),
            Self::Bool => PayloadSchemaParams::Bool(BoolIndexParams::default()),
            Self::Datetime => PayloadSchemaParams::Datetime(DatetimeIndexParams::default()),
            Self::Uuid => PayloadSchemaParams::Uuid(UuidIndexParams::default()),
        }
    }
}

/// Payload type with parameters
#[derive(Debug, Deserialize, Serialize, JsonSchema, Anonymize, Clone, PartialEq, Hash, Eq)]
#[serde(untagged, rename_all = "snake_case")]
#[anonymize(false)]
pub enum PayloadSchemaParams {
    Keyword(KeywordIndexParams),
    Integer(IntegerIndexParams),
    Float(FloatIndexParams),
    Geo(GeoIndexParams),
    Text(TextIndexParams),
    Bool(BoolIndexParams),
    Datetime(DatetimeIndexParams),
    Uuid(UuidIndexParams),
}

impl PayloadSchemaParams {
    /// Human-readable type name
    pub fn name(&self) -> &'static str {
        self.kind().name()
    }

    pub fn kind(&self) -> PayloadSchemaType {
        match self {
            PayloadSchemaParams::Keyword(_) => PayloadSchemaType::Keyword,
            PayloadSchemaParams::Integer(_) => PayloadSchemaType::Integer,
            PayloadSchemaParams::Float(_) => PayloadSchemaType::Float,
            PayloadSchemaParams::Geo(_) => PayloadSchemaType::Geo,
            PayloadSchemaParams::Text(_) => PayloadSchemaType::Text,
            PayloadSchemaParams::Bool(_) => PayloadSchemaType::Bool,
            PayloadSchemaParams::Datetime(_) => PayloadSchemaType::Datetime,
            PayloadSchemaParams::Uuid(_) => PayloadSchemaType::Uuid,
        }
    }

    pub fn tenant_optimization(&self) -> bool {
        match self {
            PayloadSchemaParams::Keyword(keyword) => keyword.is_tenant.unwrap_or_default(),
            PayloadSchemaParams::Integer(integer) => integer.is_principal.unwrap_or_default(),
            PayloadSchemaParams::Float(float) => float.is_principal.unwrap_or_default(),
            PayloadSchemaParams::Datetime(datetime) => datetime.is_principal.unwrap_or_default(),
            PayloadSchemaParams::Uuid(uuid) => uuid.is_tenant.unwrap_or_default(),
            PayloadSchemaParams::Geo(_)
            | PayloadSchemaParams::Text(_)
            | PayloadSchemaParams::Bool(_) => false,
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            PayloadSchemaParams::Keyword(i) => i.on_disk.unwrap_or_default(),
            PayloadSchemaParams::Integer(i) => i.on_disk.unwrap_or_default(),
            PayloadSchemaParams::Float(i) => i.on_disk.unwrap_or_default(),
            PayloadSchemaParams::Datetime(i) => i.on_disk.unwrap_or_default(),
            PayloadSchemaParams::Uuid(i) => i.on_disk.unwrap_or_default(),
            PayloadSchemaParams::Text(i) => i.on_disk.unwrap_or_default(),
            PayloadSchemaParams::Geo(i) => i.on_disk.unwrap_or_default(),
            PayloadSchemaParams::Bool(i) => i.on_disk.unwrap_or_default(),
        }
    }
}

impl Validate for PayloadSchemaParams {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            PayloadSchemaParams::Keyword(_) => Ok(()),
            PayloadSchemaParams::Integer(integer_index_params) => integer_index_params.validate(),
            PayloadSchemaParams::Float(_) => Ok(()),
            PayloadSchemaParams::Geo(_) => Ok(()),
            PayloadSchemaParams::Text(_) => Ok(()),
            PayloadSchemaParams::Bool(_) => Ok(()),
            PayloadSchemaParams::Datetime(_) => Ok(()),
            PayloadSchemaParams::Uuid(_) => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum PayloadFieldSchema {
    FieldType(PayloadSchemaType),
    FieldParams(PayloadSchemaParams),
}

impl PartialEq for PayloadFieldSchema {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::FieldType(this), Self::FieldType(other)) => this == other,
            (Self::FieldParams(this), Self::FieldParams(other)) => this == other,
            (Self::FieldType(this), Self::FieldParams(other)) => &this.expand() == other,
            (Self::FieldParams(this), Self::FieldType(other)) => this == &other.expand(),
        }
    }
}

impl hash::Hash for PayloadFieldSchema {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        match self {
            PayloadFieldSchema::FieldType(default) => default.expand().hash(state),
            PayloadFieldSchema::FieldParams(params) => params.hash(state),
        }
    }
}

impl Validate for PayloadFieldSchema {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            PayloadFieldSchema::FieldType(_) => Ok(()), // nothing to validate
            PayloadFieldSchema::FieldParams(payload_schema_params) => {
                payload_schema_params.validate()
            }
        }
    }
}

impl Display for PayloadFieldSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PayloadFieldSchema::FieldType(t) => write!(f, "{}", t.name()),
            PayloadFieldSchema::FieldParams(params) => match params {
                PayloadSchemaParams::Keyword(_)
                | PayloadSchemaParams::Float(_)
                | PayloadSchemaParams::Geo(_)
                | PayloadSchemaParams::Bool(_)
                | PayloadSchemaParams::Datetime(_)
                | PayloadSchemaParams::Uuid(_) => write!(f, "{}", params.name()),
                PayloadSchemaParams::Integer(integer_params) => {
                    let range = integer_params.range.unwrap_or(true);
                    let lookup = integer_params.lookup.unwrap_or(true);
                    if range && lookup {
                        write!(f, "integer")
                    } else {
                        write!(f, "integer (with range: {range}, lookup: {lookup})")
                    }
                }
                PayloadSchemaParams::Text(text_params) => {
                    if text_params.phrase_matching.unwrap_or_default() {
                        write!(f, "text (with phrase_matching: true)")
                    } else {
                        write!(f, "text")
                    }
                }
            },
        }
    }
}

impl PayloadFieldSchema {
    pub fn expand(&self) -> Cow<'_, PayloadSchemaParams> {
        match self {
            PayloadFieldSchema::FieldType(t) => Cow::Owned(t.expand()),
            PayloadFieldSchema::FieldParams(p) => Cow::Borrowed(p),
        }
    }

    /// Human-readable type name
    pub fn name(&self) -> &'static str {
        match self {
            PayloadFieldSchema::FieldType(field_type) => field_type.name(),
            PayloadFieldSchema::FieldParams(field_params) => field_params.name(),
        }
    }

    pub fn is_tenant(&self) -> bool {
        match self {
            PayloadFieldSchema::FieldType(_) => false,
            PayloadFieldSchema::FieldParams(params) => params.tenant_optimization(),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            PayloadFieldSchema::FieldType(_) => false,
            PayloadFieldSchema::FieldParams(params) => params.is_on_disk(),
        }
    }

    pub fn kind(&self) -> PayloadSchemaType {
        match self {
            PayloadFieldSchema::FieldType(t) => *t,
            PayloadFieldSchema::FieldParams(p) => p.kind(),
        }
    }

    /// Check if this type supports a `match` condition
    pub fn supports_match(&self) -> bool {
        match self {
            PayloadFieldSchema::FieldType(payload_schema_type) => match payload_schema_type {
                PayloadSchemaType::Keyword => true,
                PayloadSchemaType::Integer => true,
                PayloadSchemaType::Uuid => true,
                PayloadSchemaType::Bool => true,
                PayloadSchemaType::Float => false,
                PayloadSchemaType::Geo => false,
                PayloadSchemaType::Text => false,
                PayloadSchemaType::Datetime => false,
            },
            PayloadFieldSchema::FieldParams(payload_schema_params) => match payload_schema_params {
                PayloadSchemaParams::Keyword(_) => true,
                PayloadSchemaParams::Integer(integer_index_params) => {
                    integer_index_params.lookup == Some(true)
                }
                PayloadSchemaParams::Uuid(_) => true,
                PayloadSchemaParams::Bool(_) => true,
                PayloadSchemaParams::Float(_) => false,
                PayloadSchemaParams::Geo(_) => false,
                PayloadSchemaParams::Text(_) => false,
                PayloadSchemaParams::Datetime(_) => false,
            },
        }
    }
}

impl From<PayloadSchemaType> for PayloadFieldSchema {
    fn from(payload_schema_type: PayloadSchemaType) -> Self {
        PayloadFieldSchema::FieldType(payload_schema_type)
    }
}

impl TryFrom<PayloadIndexInfo> for PayloadFieldSchema {
    type Error = String;

    fn try_from(index_info: PayloadIndexInfo) -> Result<Self, Self::Error> {
        let PayloadIndexInfo {
            data_type,
            params,
            points: _,
        } = index_info;

        match params {
            None => Ok(PayloadFieldSchema::FieldType(data_type)),

            Some(params) if data_type == params.kind() => {
                Ok(PayloadFieldSchema::FieldParams(params))
            }

            Some(params) => Err(format!(
                "payload field with type {data_type:?} has parameters of type {:?}",
                params.kind(),
            )),
        }
    }
}

pub fn value_type(value: &Value) -> Option<PayloadSchemaType> {
    match value {
        Value::Null => None,
        Value::Bool(_) => None,
        Value::Number(num) => {
            if num.is_i64() {
                Some(PayloadSchemaType::Integer)
            } else if num.is_f64() {
                Some(PayloadSchemaType::Float)
            } else {
                None
            }
        }
        Value::String(_) => Some(PayloadSchemaType::Keyword),
        Value::Array(_) => None,
        Value::Object(obj) => {
            let lon_op = obj.get("lon").and_then(|x| x.as_f64());
            let lat_op = obj.get("lat").and_then(|x| x.as_f64());

            if let (Some(_), Some(_)) = (lon_op, lat_op) {
                return Some(PayloadSchemaType::Geo);
            }
            None
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum ValueVariants {
    String(String),
    Integer(IntPayloadType),
    Bool(bool),
}

impl ValueVariants {
    pub fn to_value(&self) -> Value {
        match self {
            ValueVariants::String(keyword) => Value::String(keyword.clone()),
            &ValueVariants::Integer(integer) => Value::Number(integer.into()),
            &ValueVariants::Bool(flag) => Value::Bool(flag),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum AnyVariants {
    Strings(IndexSet<String, FnvBuildHasher>),
    Integers(IndexSet<IntPayloadType, FnvBuildHasher>),
}

impl Hash for AnyVariants {
    fn hash<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        match self {
            AnyVariants::Strings(index_set) => {
                for item in index_set.iter() {
                    item.hash(state);
                }
            }
            AnyVariants::Integers(index_set) => {
                for item in index_set.iter() {
                    item.hash(state);
                }
            }
        }
    }
}

impl AnyVariants {
    pub fn len(&self) -> usize {
        match self {
            AnyVariants::Strings(index_set) => index_set.len(),
            AnyVariants::Integers(index_set) => index_set.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            AnyVariants::Strings(index_set) => index_set.is_empty(),
            AnyVariants::Integers(index_set) => index_set.is_empty(),
        }
    }
}

/// Exact match of the given value
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub struct MatchValue {
    pub value: ValueVariants,
}

/// Full-text match of the strings.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub struct MatchText {
    pub text: String,
}

/// Full-text match of at least one token of the string.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub struct MatchTextAny {
    pub text_any: String,
}

impl<S: Into<String>> From<S> for MatchText {
    fn from(text: S) -> Self {
        MatchText { text: text.into() }
    }
}

/// Full-text phrase match of the string.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub struct MatchPhrase {
    pub phrase: String,
}

impl<S: Into<String>> From<S> for MatchPhrase {
    fn from(text: S) -> Self {
        MatchPhrase {
            phrase: text.into(),
        }
    }
}

/// Exact match on any of the given values
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub struct MatchAny {
    pub any: AnyVariants,
}

/// Should have at least one value not matching the any given values
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub struct MatchExcept {
    pub except: AnyVariants,
}

/// Match filter request
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged, rename_all = "snake_case")]
pub enum MatchInterface {
    Value(MatchValue),
    Text(MatchText),
    TextAny(MatchTextAny),
    Phrase(MatchPhrase),
    Any(MatchAny),
    Except(MatchExcept),
}

/// Match filter request
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(untagged, from = "MatchInterface")]
pub enum Match {
    Value(MatchValue),
    Text(MatchText),
    TextAny(MatchTextAny),
    Phrase(MatchPhrase),
    Any(MatchAny),
    Except(MatchExcept),
}

impl Match {
    pub fn new_value(value: ValueVariants) -> Self {
        Self::Value(MatchValue { value })
    }

    pub fn new_text(text: &str) -> Self {
        Self::Text(MatchText { text: text.into() })
    }

    pub fn new_phrase(phrase: &str) -> Self {
        Self::Phrase(MatchPhrase {
            phrase: phrase.into(),
        })
    }

    pub fn new_any(any: AnyVariants) -> Self {
        Self::Any(MatchAny { any })
    }

    pub fn new_except(except: AnyVariants) -> Self {
        Self::Except(MatchExcept { except })
    }
}

impl From<AnyVariants> for Match {
    fn from(any: AnyVariants) -> Self {
        Self::Any(MatchAny { any })
    }
}

impl From<MatchInterface> for Match {
    fn from(value: MatchInterface) -> Self {
        match value {
            MatchInterface::Value(value) => Self::Value(MatchValue { value: value.value }),
            MatchInterface::Text(text) => Self::Text(MatchText { text: text.text }),
            MatchInterface::TextAny(text_any) => Self::TextAny(MatchTextAny {
                text_any: text_any.text_any,
            }),
            MatchInterface::Any(any) => Self::Any(MatchAny { any: any.any }),
            MatchInterface::Except(except) => Self::Except(MatchExcept {
                except: except.except,
            }),
            MatchInterface::Phrase(MatchPhrase { phrase }) => Self::Phrase(MatchPhrase { phrase }),
        }
    }
}

impl From<bool> for Match {
    fn from(flag: bool) -> Self {
        Self::Value(MatchValue {
            value: ValueVariants::Bool(flag),
        })
    }
}

impl From<String> for Match {
    fn from(keyword: String) -> Self {
        Self::Value(MatchValue {
            value: ValueVariants::String(keyword),
        })
    }
}

impl From<EcoString> for Match {
    fn from(keyword: EcoString) -> Self {
        Self::Value(MatchValue {
            value: ValueVariants::String(keyword.into()),
        })
    }
}

impl From<IntPayloadType> for Match {
    fn from(integer: IntPayloadType) -> Self {
        Self::Value(MatchValue {
            value: ValueVariants::Integer(integer),
        })
    }
}

impl From<Vec<String>> for Match {
    fn from(keywords: Vec<String>) -> Self {
        let keywords: IndexSet<String, FnvBuildHasher> = keywords.into_iter().collect();
        Self::Any(MatchAny {
            any: AnyVariants::Strings(keywords),
        })
    }
}

impl From<ValueVariants> for Match {
    fn from(value: ValueVariants) -> Self {
        Self::Value(MatchValue { value })
    }
}

impl From<Vec<String>> for MatchExcept {
    fn from(keywords: Vec<String>) -> Self {
        let keywords: IndexSet<String, FnvBuildHasher> = keywords.into_iter().collect();
        MatchExcept {
            except: AnyVariants::Strings(keywords),
        }
    }
}

impl From<Vec<IntPayloadType>> for Match {
    fn from(integers: Vec<IntPayloadType>) -> Self {
        let integers: IndexSet<_, FnvBuildHasher> = integers.into_iter().collect();
        Self::Any(MatchAny {
            any: AnyVariants::Integers(integers),
        })
    }
}

impl From<Vec<IntPayloadType>> for MatchExcept {
    fn from(integers: Vec<IntPayloadType>) -> Self {
        let integers: IndexSet<_, FnvBuildHasher> = integers.into_iter().collect();
        MatchExcept {
            except: AnyVariants::Integers(integers),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum RangeInterface {
    Float(Range<OrderedFloat<FloatPayloadType>>),
    DateTime(Range<DateTimePayloadType>),
}

impl Hash for RangeInterface {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        match self {
            RangeInterface::Float(range) => {
                let Range { lt, gt, gte, lte } = range;
                lt.hash(state);
                gt.hash(state);
                gte.hash(state);
                lte.hash(state);
            }
            RangeInterface::DateTime(range) => {
                let Range { lt, gt, gte, lte } = range;
                lt.hash(state);
                gt.hash(state);
                gte.hash(state);
                lte.hash(state);
            }
        }
    }
}

type OrderedFloatPayloadType = OrderedFloat<FloatPayloadType>;

/// Range filter request
#[macro_rules_attribute::macro_rules_derive(crate::common::macros::schemars_rename_generics)]
#[derive_args(< OrderedFloatPayloadType > => "Range", < DateTimePayloadType > => "DatetimeRange")]
#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct Range<T> {
    /// point.key < range.lt
    pub lt: Option<T>,
    /// point.key > range.gt
    pub gt: Option<T>,
    /// point.key >= range.gte
    pub gte: Option<T>,
    /// point.key <= range.lte
    pub lte: Option<T>,
}

impl<T: Copy> Range<T> {
    /// Convert range to a range of another type
    pub fn map<U, F: Fn(T) -> U>(&self, f: F) -> Range<U> {
        let Self { lt, gt, gte, lte } = self;
        Range {
            lt: lt.map(&f),
            gt: gt.map(&f),
            gte: gte.map(&f),
            lte: lte.map(&f),
        }
    }
}

impl<T: Copy + PartialOrd> Range<T> {
    pub fn check_range(&self, number: T) -> bool {
        let Self { lt, gt, gte, lte } = self;
        lt.is_none_or(|x| number < x)
            && gt.is_none_or(|x| number > x)
            && lte.is_none_or(|x| number <= x)
            && gte.is_none_or(|x| number >= x)
    }
}

/// Values count filter request
#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub struct ValuesCount {
    /// point.key.length() < values_count.lt
    pub lt: Option<usize>,
    /// point.key.length() > values_count.gt
    pub gt: Option<usize>,
    /// point.key.length() >= values_count.gte
    pub gte: Option<usize>,
    /// point.key.length() <= values_count.lte
    pub lte: Option<usize>,
}

impl ValuesCount {
    pub fn check_count(&self, count: usize) -> bool {
        let Self { lt, gt, gte, lte } = self;
        lt.is_none_or(|x| count < x)
            && gt.is_none_or(|x| count > x)
            && lte.is_none_or(|x| count <= x)
            && gte.is_none_or(|x| count >= x)
    }

    pub fn check_count_from(&self, value: &Value) -> bool {
        let count = match value {
            Value::Null => 0,
            Value::Array(array) => array.len(),
            _ => 1,
        };

        self.check_count(count)
    }
}

#[cfg(test)]
impl From<std::ops::Range<usize>> for ValuesCount {
    fn from(range: std::ops::Range<usize>) -> Self {
        Self {
            gte: Some(range.start),
            lt: Some(range.end),
            gt: None,
            lte: None,
        }
    }
}

/// Geo filter request
///
/// Matches coordinates inside the rectangle, described by coordinates of lop-left and bottom-right edges
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub struct GeoBoundingBox {
    /// Coordinates of the top left point of the area rectangle
    pub top_left: GeoPoint,
    /// Coordinates of the bottom right point of the area rectangle
    pub bottom_right: GeoPoint,
}

impl GeoBoundingBox {
    pub fn check_point(&self, point: &GeoPoint) -> bool {
        let longitude_check = if self.top_left.lon > self.bottom_right.lon {
            // Handle antimeridian crossing
            point.lon > self.top_left.lon || point.lon < self.bottom_right.lon
        } else {
            self.top_left.lon < point.lon && point.lon < self.bottom_right.lon
        };

        let latitude_check = self.bottom_right.lat < point.lat && point.lat < self.top_left.lat;

        longitude_check && latitude_check
    }
}

/// Geo filter request
///
/// Matches coordinates inside the circle of `radius` and center with coordinates `center`
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct GeoRadius {
    /// Coordinates of the top left point of the area rectangle
    pub center: GeoPoint,
    /// Radius of the area in meters
    pub radius: OrderedFloat<f64>,
}

impl Hash for GeoRadius {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        let GeoRadius { center, radius } = self;
        center.hash(state);
        // Hash f64 by converting to bits
        OrderedFloat(*radius).hash(state);
    }
}

impl GeoRadius {
    pub fn check_point(&self, point: &GeoPoint) -> bool {
        let query_center = Point::from(self.center);
        Haversine.distance(query_center, Point::from(*point)) < self.radius.0
    }
}

#[derive(Deserialize)]
pub struct GeoPolygonShadow {
    pub exterior: GeoLineString,
    pub interiors: Option<Vec<GeoLineString>>,
}

pub struct PolygonWrapper {
    pub polygon: Polygon,
}

impl PolygonWrapper {
    pub fn check_point(&self, point: &GeoPoint) -> bool {
        let point_new = Point::new(point.lon.0, point.lat.0);
        self.polygon.contains(&point_new)
    }
}

/// Geo filter request
///
/// Matches coordinates inside the polygon, defined by `exterior` and `interiors`
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(try_from = "GeoPolygonShadow", rename_all = "snake_case")]
pub struct GeoPolygon {
    /// The exterior line bounds the surface
    /// must consist of a minimum of 4 points, and the first and last points
    /// must be the same.
    pub exterior: GeoLineString,
    /// Interior lines (if present) bound holes within the surface
    /// each GeoLineString must consist of a minimum of 4 points, and the first
    /// and last points must be the same.
    pub interiors: Option<Vec<GeoLineString>>,
}

impl GeoPolygon {
    pub fn validate_line_string(line: &GeoLineString) -> OperationResult<()> {
        if line.points.len() <= 3 {
            return Err(OperationError::ValidationError {
                description: format!(
                    "polygon invalid, the size must be at least 4, got {}",
                    line.points.len()
                ),
            });
        }

        if let (Some(first), Some(last)) = (line.points.first(), line.points.last())
            && ((first.lat - last.lat).abs() > f64::EPSILON
                || (first.lon - last.lon).abs() > f64::EPSILON)
        {
            return Err(OperationError::ValidationError {
                description: String::from(
                    "polygon invalid, the first and the last points should be the same to form a closed line",
                ),
            });
        }

        Ok(())
    }

    // convert GeoPolygon to Geo crate Polygon class for checking point intersection
    pub fn convert(&self) -> PolygonWrapper {
        let exterior_line: LineString = LineString(
            self.exterior
                .points
                .iter()
                .map(|p| Coord {
                    x: p.lon.0,
                    y: p.lat.0,
                })
                .collect(),
        );

        // Convert the interior points to coordinates (if any)
        let interior_lines: Vec<LineString> = match &self.interiors {
            None => vec![],
            Some(interiors) => interiors
                .iter()
                .map(|interior_points| {
                    interior_points
                        .points
                        .iter()
                        .map(|p| Coord {
                            x: p.lon.0,
                            y: p.lat.0,
                        })
                        .collect()
                })
                .map(LineString)
                .collect(),
        };
        PolygonWrapper {
            polygon: Polygon::new(exterior_line, interior_lines),
        }
    }
}

impl TryFrom<GeoPolygonShadow> for GeoPolygon {
    type Error = OperationError;

    fn try_from(value: GeoPolygonShadow) -> OperationResult<Self> {
        let GeoPolygonShadow {
            exterior,
            interiors,
        } = value;
        Self::validate_line_string(&exterior)?;

        if let Some(interiors) = &interiors {
            for interior in interiors {
                Self::validate_line_string(interior)?;
            }
        }

        Ok(GeoPolygon {
            exterior,
            interiors,
        })
    }
}

/// All possible payload filtering conditions
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq, Hash)]
#[validate(schema(function = "validate_field_condition"))]
#[serde(rename_all = "snake_case")]
pub struct FieldCondition {
    /// Payload key
    pub key: PayloadKeyType,
    /// Check if point has field with a given value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#match: Option<Match>,
    /// Check if points value lies in a given range
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<RangeInterface>,
    /// Check if points geolocation lies in a given area
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geo_bounding_box: Option<GeoBoundingBox>,
    /// Check if geo point is within a given radius
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geo_radius: Option<GeoRadius>,
    /// Check if geo point is within a given polygon
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geo_polygon: Option<GeoPolygon>,
    /// Check number of values of the field
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values_count: Option<ValuesCount>,
    /// Check that the field is empty, alternative syntax for `is_empty: "field_name"`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_empty: Option<bool>,
    /// Check that the field is null, alternative syntax for `is_null: "field_name"`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_null: Option<bool>,
}

impl FieldCondition {
    pub fn new_match(key: PayloadKeyType, r#match: Match) -> Self {
        Self {
            key,
            r#match: Some(r#match),
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: None,
            is_null: None,
        }
    }

    pub fn new_range(key: PayloadKeyType, range: Range<OrderedFloat<FloatPayloadType>>) -> Self {
        Self {
            key,
            r#match: None,
            range: Some(RangeInterface::Float(range)),
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: None,
            is_null: None,
        }
    }

    pub fn new_datetime_range(
        key: PayloadKeyType,
        datetime_range: Range<DateTimePayloadType>,
    ) -> Self {
        Self {
            key,
            r#match: None,
            range: Some(RangeInterface::DateTime(datetime_range)),
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: None,
            is_null: None,
        }
    }

    pub fn new_geo_bounding_box(key: PayloadKeyType, geo_bounding_box: GeoBoundingBox) -> Self {
        Self {
            key,
            r#match: None,
            range: None,
            geo_bounding_box: Some(geo_bounding_box),
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: None,
            is_null: None,
        }
    }

    pub fn new_geo_radius(key: PayloadKeyType, geo_radius: GeoRadius) -> Self {
        Self {
            key,
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: Some(geo_radius),
            geo_polygon: None,
            values_count: None,
            is_empty: None,
            is_null: None,
        }
    }

    pub fn new_geo_polygon(key: PayloadKeyType, geo_polygon: GeoPolygon) -> Self {
        Self {
            key,
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: Some(geo_polygon),
            values_count: None,
            is_empty: None,
            is_null: None,
        }
    }

    pub fn new_values_count(key: PayloadKeyType, values_count: ValuesCount) -> Self {
        Self {
            key,
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: Some(values_count),
            is_empty: None,
            is_null: None,
        }
    }

    pub fn new_is_empty(key: PayloadKeyType, is_empty: bool) -> Self {
        Self {
            key,
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: Some(is_empty),
            is_null: None,
        }
    }

    pub fn new_is_null(key: PayloadKeyType, is_null: bool) -> Self {
        Self {
            key,
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: None,
            is_null: Some(is_null),
        }
    }

    pub fn all_fields_none(&self) -> bool {
        matches!(
            self,
            FieldCondition {
                r#match: None,
                range: None,
                geo_bounding_box: None,
                geo_radius: None,
                geo_polygon: None,
                values_count: None,
                key: _,
                is_empty: None,
                is_null: None,
            }
        )
    }

    fn input_size(&self) -> usize {
        if self.r#match.is_none() {
            return 0;
        }

        match self.r#match.as_ref().unwrap() {
            Match::Any(match_any) => match_any.any.len(),
            Match::Except(match_except) => match_except.except.len(),
            Match::Value(_) => 0,
            Match::Text(_) => 0,
            Match::Phrase(_) => 0,
            Match::TextAny(_) => 0,
        }
    }
}

pub fn validate_field_condition(field_condition: &FieldCondition) -> Result<(), ValidationError> {
    if field_condition.all_fields_none() {
        Err(ValidationError::new(
            "At least one field condition must be specified",
        ))
    } else {
        Ok(())
    }
}

/// Payload field
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
pub struct PayloadField {
    /// Payload field name
    pub key: PayloadKeyType,
}

/// Select points with empty payload for a specified field
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
pub struct IsEmptyCondition {
    pub is_empty: PayloadField,
}

/// Select points with null payload for a specified field
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
pub struct IsNullCondition {
    pub is_null: PayloadField,
}

impl From<JsonPath> for IsNullCondition {
    fn from(key: PayloadKeyType) -> Self {
        IsNullCondition {
            is_null: PayloadField { key },
        }
    }
}

impl From<JsonPath> for IsEmptyCondition {
    fn from(key: PayloadKeyType) -> Self {
        IsEmptyCondition {
            is_empty: PayloadField { key },
        }
    }
}

/// ID-based filtering condition
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
pub struct HasIdCondition {
    #[schemars(schema_with = "HashSet::<PointIdType>::json_schema")]
    pub has_id: MaybeArc<AHashSet<PointIdType>>,
}

impl Hash for HasIdCondition {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        unordered_hash_unique(state, self.has_id.iter());
    }
}

/// Filter points which have specific vector assigned
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
pub struct HasVectorCondition {
    pub has_vector: VectorNameBuf,
}

impl From<VectorNameBuf> for HasVectorCondition {
    fn from(vector: VectorNameBuf) -> Self {
        HasVectorCondition { has_vector: vector }
    }
}

/// Threshold determining when to use an `Arc` in `HasIdCondition` if the condition includes many points.
/// Since we're cloning filters quite a lot, using an Arc for larger conditions reduces risk of memory leaks
/// and potentially improves performance in some places.
const HAS_ID_CONDITION_ARC_THRESHOLD: usize = 1_000;

impl From<AHashSet<PointIdType>> for HasIdCondition {
    fn from(has_id: AHashSet<PointIdType>) -> Self {
        if has_id.len() > HAS_ID_CONDITION_ARC_THRESHOLD {
            HasIdCondition {
                has_id: MaybeArc::arc(has_id),
            }
        } else {
            HasIdCondition {
                has_id: MaybeArc::no_arc(has_id),
            }
        }
    }
}

impl FromIterator<PointIdType> for HasIdCondition {
    fn from_iter<T: IntoIterator<Item = PointIdType>>(iter: T) -> Self {
        let items: AHashSet<_> = iter.into_iter().collect();
        // Arc-Threshold applies here, since we're reusing the From implementation from AHashSet.
        Self::from(items)
    }
}

/// Select points with payload for a specified nested field
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Validate, Hash)]
pub struct Nested {
    pub key: PayloadKeyType,
    #[validate(nested)]
    pub filter: Filter,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Validate, Hash)]
pub struct NestedCondition {
    #[validate(nested)]
    pub nested: Nested,
}

/// Container to work around the untagged enum limitation for condition
impl NestedCondition {
    pub fn new(nested: Nested) -> Self {
        Self { nested }
    }

    /// Get the raw key without any modifications
    pub fn raw_key(&self) -> &PayloadKeyType {
        &self.nested.key
    }

    /// Nested is made to be used with arrays, so we add `[]` to the key if it is not present for convenience
    pub fn array_key(&self) -> PayloadKeyType {
        self.raw_key().array_key()
    }

    pub fn filter(&self) -> &Filter {
        &self.nested.filter
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(untagged)]
#[serde(
    expecting = "Expected some form of condition, which can be a field condition (like {\"key\": ..., \"match\": ... }), or some other mentioned in the documentation: https://qdrant.tech/documentation/concepts/filtering/#filtering-conditions"
)]
#[allow(clippy::large_enum_variant)]
pub enum Condition {
    /// Check if field satisfies provided condition
    Field(FieldCondition),
    /// Check if payload field is empty: equals to empty array, or does not exists
    IsEmpty(IsEmptyCondition),
    /// Check if payload field equals `NULL`
    IsNull(IsNullCondition),
    /// Check if points id is in a given set
    HasId(HasIdCondition),
    /// Check if point has vector assigned
    HasVector(HasVectorCondition),
    /// Nested filters
    Nested(NestedCondition),
    /// Nested filter
    Filter(Filter),

    #[serde(skip)]
    CustomIdChecker(CustomIdChecker),
}

impl Condition {
    pub fn new_custom(checker: Arc<dyn CustomIdCheckerCondition + Send + Sync + 'static>) -> Self {
        Condition::CustomIdChecker(CustomIdChecker(checker))
    }
}

#[derive(Debug, Clone)]
pub struct CustomIdChecker(pub Arc<dyn CustomIdCheckerCondition + Send + Sync + 'static>);

impl Hash for CustomIdChecker {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        // We cannot hash the inner function
        // This means that two different CustomIdChecker conditions will have the same hash,
        // but that's acceptable since we cannot do better, and only expected to be used
        // for logging and profiling purposes.
        std::ptr::hash(Arc::as_ptr(&self.0), state);
    }
}

impl PartialEq for CustomIdChecker {
    fn eq(&self, other: &Self) -> bool {
        // We cannot compare the inner function
        // This means that two different CustomIdChecker conditions will never be equal,
        // but that's acceptable since we cannot do better, and only expected to be used
        // for logging and profiling purposes.
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for CustomIdChecker {}

impl Condition {
    pub fn new_nested(key: JsonPath, filter: Filter) -> Self {
        Self::Nested(NestedCondition {
            nested: Nested { key, filter },
        })
    }

    pub fn size_estimation(&self) -> usize {
        match self {
            Condition::Field(field_condition) => field_condition.input_size(),
            Condition::HasId(has_id_condition) => has_id_condition.has_id.len(),
            Condition::Filter(filter) => filter.max_condition_input_size(),
            Condition::Nested(nested) => nested.filter().max_condition_input_size(),
            Condition::IsEmpty(_)
            | Condition::IsNull(_)
            | Condition::HasVector(_)
            | Condition::CustomIdChecker(_) => 0,
        }
    }

    pub fn sub_conditions_count(&self) -> usize {
        match self {
            Condition::Nested(nested_condition) => {
                nested_condition.filter().total_conditions_count()
            }
            Condition::Filter(filter) => filter.total_conditions_count(),
            Condition::Field(_)
            | Condition::IsEmpty(_)
            | Condition::IsNull(_)
            | Condition::CustomIdChecker(_)
            | Condition::HasId(_)
            | Condition::HasVector(_) => 1,
        }
    }

    pub fn targeted_key(&self) -> Option<PayloadKeyType> {
        match self {
            Condition::Field(field_condition) => Some(field_condition.key.clone()),
            Condition::IsEmpty(is_empty_condition) => Some(is_empty_condition.is_empty.key.clone()),
            Condition::IsNull(is_null_condition) => Some(is_null_condition.is_null.key.clone()),
            Condition::Nested(nested_condition) => Some(nested_condition.array_key()),
            Condition::Filter(filter) => filter.iter_conditions().find_map(|c| c.targeted_key()),
            Condition::HasId(_) | Condition::HasVector(_) | Condition::CustomIdChecker(_) => None,
        }
    }
}

// The validator crate does not support deriving for enums.
impl Validate for Condition {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            Condition::HasId(_)
            | Condition::IsEmpty(_)
            | Condition::IsNull(_)
            | Condition::HasVector(_) => Ok(()),
            Condition::Field(field_condition) => field_condition.validate(),
            Condition::Nested(nested_condition) => nested_condition.validate(),
            Condition::Filter(filter) => filter.validate(),
            Condition::CustomIdChecker(_) => Ok(()),
        }
    }
}

pub trait CustomIdCheckerCondition: fmt::Debug {
    fn estimate_cardinality(&self, points: usize) -> CardinalityEstimation;
    fn check(&self, point_id: ExtendedPointId) -> bool;
}

/// Options for specifying which payload to include or not
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash)]
#[serde(untagged, rename_all = "snake_case")]
#[serde(
    expecting = "Expected a boolean, an array of strings, or an object with an include/exclude field"
)]
pub enum WithPayloadInterface {
    /// If `true` - return all payload,
    /// If `false` - do not return payload
    Bool(bool),
    /// Specify which fields to return
    Fields(Vec<JsonPath>),
    /// Specify included or excluded fields
    Selector(PayloadSelector),
}

impl From<bool> for WithPayloadInterface {
    fn from(b: bool) -> Self {
        WithPayloadInterface::Bool(b)
    }
}

impl Default for WithPayloadInterface {
    fn default() -> Self {
        WithPayloadInterface::Bool(false)
    }
}

/// Options for specifying which vector to include
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(untagged, rename_all = "snake_case")]
#[serde(expecting = "Expected a boolean, or an array of strings")]
pub enum WithVector {
    /// If `true` - return all vector,
    /// If `false` - do not return vector
    Bool(bool),
    /// Specify which vector to return
    Selector(Vec<VectorNameBuf>),
}

impl WithVector {
    pub fn is_enabled(&self) -> bool {
        match self {
            WithVector::Bool(b) => *b,
            WithVector::Selector(_) => true,
        }
    }

    /// Merges two `WithVector` options, additively.
    pub fn merge(&self, other: &WithVector) -> WithVector {
        match (self, other) {
            // if any is true, then true
            (WithVector::Bool(true), _) => WithVector::Bool(true),
            (_, WithVector::Bool(true)) => WithVector::Bool(true),

            // if both are false, then false
            (WithVector::Bool(false), WithVector::Bool(false)) => WithVector::Bool(false),

            // merge selectors
            (WithVector::Selector(s1), WithVector::Selector(s2)) => {
                WithVector::Selector(s1.iter().chain(s2).unique().cloned().collect())
            }

            // use selector from the other option
            (WithVector::Bool(false), WithVector::Selector(s)) => WithVector::Selector(s.clone()),
            (WithVector::Selector(s), WithVector::Bool(false)) => WithVector::Selector(s.clone()),
        }
    }
}

impl From<bool> for WithVector {
    fn from(b: bool) -> Self {
        WithVector::Bool(b)
    }
}

impl From<VectorNameBuf> for WithVector {
    fn from(name: VectorNameBuf) -> Self {
        WithVector::Selector(vec![name])
    }
}

impl Default for WithVector {
    fn default() -> Self {
        WithVector::Bool(false)
    }
}

impl WithPayloadInterface {
    pub fn is_required(&self) -> bool {
        match self {
            WithPayloadInterface::Bool(b) => *b,
            _ => true,
        }
    }
}

impl From<bool> for WithPayload {
    fn from(x: bool) -> Self {
        WithPayload {
            enable: x,
            payload_selector: None,
        }
    }
}

impl From<WithPayloadInterface> for WithPayload {
    fn from(interface: WithPayloadInterface) -> Self {
        match interface {
            WithPayloadInterface::Bool(enable) => WithPayload {
                enable,
                payload_selector: None,
            },
            WithPayloadInterface::Fields(fields) => WithPayload {
                enable: true,
                payload_selector: Some(PayloadSelector::new_include(fields)),
            },
            WithPayloadInterface::Selector(selector) => WithPayload {
                enable: true,
                payload_selector: Some(selector),
            },
        }
    }
}

impl From<&WithPayloadInterface> for WithPayload {
    fn from(interface: &WithPayloadInterface) -> Self {
        WithPayload::from(interface.clone())
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct PayloadSelectorInclude {
    /// Only include this payload keys
    pub include: Vec<PayloadKeyType>,
}

impl PayloadSelectorInclude {
    pub fn new(include: Vec<PayloadKeyType>) -> Self {
        Self { include }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct PayloadSelectorExclude {
    /// Exclude this fields from returning payload
    pub exclude: Vec<PayloadKeyType>,
}

impl PayloadSelectorExclude {
    pub fn new(exclude: Vec<PayloadKeyType>) -> Self {
        Self { exclude }
    }
}

/// Specifies how to treat payload selector
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(untagged, rename_all = "snake_case")]
pub enum PayloadSelector {
    /// Include only this fields into response payload
    Include(PayloadSelectorInclude),
    /// Exclude this fields from result payload. Keep all other fields.
    Exclude(PayloadSelectorExclude),
}

impl From<PayloadSelectorExclude> for WithPayloadInterface {
    fn from(selector: PayloadSelectorExclude) -> Self {
        WithPayloadInterface::Selector(PayloadSelector::Exclude(selector))
    }
}

impl From<PayloadSelectorInclude> for WithPayloadInterface {
    fn from(selector: PayloadSelectorInclude) -> Self {
        WithPayloadInterface::Selector(PayloadSelector::Include(selector))
    }
}

impl PayloadSelector {
    pub fn new_include(vecs_payload_key_type: Vec<PayloadKeyType>) -> Self {
        PayloadSelector::Include(PayloadSelectorInclude {
            include: vecs_payload_key_type,
        })
    }

    pub fn new_exclude(vecs_payload_key_type: Vec<PayloadKeyType>) -> Self {
        PayloadSelector::Exclude(PayloadSelectorExclude {
            exclude: vecs_payload_key_type,
        })
    }

    /// Process payload selector
    pub fn process(&self, x: Payload) -> Payload {
        match self {
            PayloadSelector::Include(selector) => JsonPath::value_filter(&x.0, |key, _| {
                selector
                    .include
                    .iter()
                    .any(|pattern| pattern.check_include_pattern(key))
            })
            .into(),
            PayloadSelector::Exclude(selector) => JsonPath::value_filter(&x.0, |key, _| {
                selector
                    .exclude
                    .iter()
                    .all(|pattern| !pattern.check_exclude_pattern(key))
            })
            .into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct WithPayload {
    /// Enable return payloads or not
    pub enable: bool,
    /// Filter include and exclude payloads
    pub payload_selector: Option<PayloadSelector>,
}

#[derive(
    Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq, Default, Hash,
)]
#[serde(rename_all = "snake_case")]
pub struct MinShould {
    #[validate(nested)]
    pub conditions: Vec<Condition>,
    pub min_count: usize,
}

#[derive(
    Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq, Eq, Default, Hash,
)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Filter {
    /// At least one of those conditions should match
    #[validate(nested)]
    #[serde(
        default,
        with = "MaybeOneOrMany",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "MaybeOneOrMany<Condition>")]
    pub should: Option<Vec<Condition>>,
    /// At least minimum amount of given conditions should match
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_should: Option<MinShould>,
    /// All conditions must match
    #[validate(nested)]
    #[serde(
        default,
        with = "MaybeOneOrMany",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "MaybeOneOrMany<Condition>")]
    pub must: Option<Vec<Condition>>,
    /// All conditions must NOT match
    #[validate(nested)]
    #[serde(
        default,
        with = "MaybeOneOrMany",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "MaybeOneOrMany<Condition>")]
    pub must_not: Option<Vec<Condition>>,
}

impl Filter {
    pub fn new() -> Self {
        Filter {
            should: None,
            min_should: None,
            must: None,
            must_not: None,
        }
    }

    pub fn new_should(condition: Condition) -> Self {
        Filter {
            should: Some(vec![condition]),
            min_should: None,
            must: None,
            must_not: None,
        }
    }

    pub fn new_min_should(min_should: MinShould) -> Self {
        Filter {
            should: None,
            min_should: Some(min_should),
            must: None,
            must_not: None,
        }
    }

    pub fn new_must(condition: Condition) -> Self {
        Filter {
            should: None,
            min_should: None,
            must: Some(vec![condition]),
            must_not: None,
        }
    }

    pub fn new_must_not(condition: Condition) -> Self {
        Filter {
            should: None,
            min_should: None,
            must: None,
            must_not: Some(vec![condition]),
        }
    }

    /// Create an extended filtering condition, which would also include filter by given list of IDs.
    pub fn with_point_ids(self, ids: impl IntoIterator<Item = PointIdType>) -> Filter {
        let has_id_condition: HasIdCondition = ids.into_iter().collect();

        let Filter {
            should,
            min_should,
            must,
            must_not,
        } = self;

        let new_must = match must {
            Some(mut must) => {
                must.push(Condition::HasId(has_id_condition));
                Some(must)
            }
            None => Some(vec![Condition::HasId(has_id_condition)]),
        };

        Filter {
            should,
            min_should,
            must: new_must,
            must_not,
        }
    }

    pub fn merge(&self, other: &Filter) -> Filter {
        self.clone().merge_owned(other.clone())
    }

    pub fn merge_owned(self, other: Filter) -> Filter {
        let merge_component = |this, other| -> Option<Vec<Condition>> {
            match (this, other) {
                (None, None) => None,
                (Some(this), None) => Some(this),
                (None, Some(other)) => Some(other),
                (Some(mut this), Some(mut other)) => {
                    this.append(&mut other);
                    Some(this)
                }
            }
        };
        Filter {
            should: merge_component(self.should, other.should),
            min_should: {
                match (self.min_should, other.min_should) {
                    (None, None) => None,
                    (Some(this), None) => Some(this),
                    (None, Some(other)) => Some(other),
                    (Some(mut this), Some(mut other)) => {
                        this.conditions.append(&mut other.conditions);

                        // The union of conditions should be able to have at least the bigger of the two min_counts
                        this.min_count = this.min_count.max(other.min_count);

                        Some(this)
                    }
                }
            },
            must: merge_component(self.must, other.must),
            must_not: merge_component(self.must_not, other.must_not),
        }
    }

    pub fn merge_opts(this: Option<Self>, other: Option<Self>) -> Option<Self> {
        match (this, other) {
            (None, None) => None,
            (Some(this), None) => Some(this),
            (None, Some(other)) => Some(other),
            (Some(this), Some(other)) => Some(this.merge_owned(other)),
        }
    }

    pub fn iter_conditions(&self) -> impl Iterator<Item = &Condition> {
        self.must
            .iter()
            .flatten()
            .chain(self.must_not.iter().flatten())
            .chain(self.should.iter().flatten())
            .chain(self.min_should.iter().flat_map(|i| &i.conditions))
    }

    /// Returns the total amount of conditions of the filter, including all nested filter.
    pub fn total_conditions_count(&self) -> usize {
        fn count_all_conditions(field: Option<&Vec<Condition>>) -> usize {
            field
                .map(|i| i.iter().map(|j| j.sub_conditions_count()).sum::<usize>())
                .unwrap_or(0)
        }

        count_all_conditions(self.should.as_ref())
            + count_all_conditions(self.min_should.as_ref().map(|i| &i.conditions))
            + count_all_conditions(self.must.as_ref())
            + count_all_conditions(self.must_not.as_ref())
    }

    /// Returns the size of the largest condition.
    pub fn max_condition_input_size(&self) -> usize {
        self.iter_conditions()
            .map(|i| i.size_estimation())
            .max()
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SnapshotFormat {
    /// Created by Qdrant `<0.11.0`.
    ///
    /// The collection snapshot contains nested tar archives for segments.
    /// Segment tar archives contain a plain copy of the segment directory.
    ///
    /// ```plaintext
    /// ./0/segments/
    /// ├── 0b31e274-dc65-40e4-8493-67ebed4bcf10.tar
    /// │   ├── segment.json
    /// │   ├── CURRENT
    /// │   ├── 000009.sst
    /// │   ├── 000010.sst
    /// │   └── …
    /// ├── 1d6c96ec-7965-491a-9c45-362d55361e9b.tar
    /// └── …
    /// ```
    Ancient,
    /// Qdrant `>=0.11.0` `<=1.13` (and maybe even later).
    ///
    /// The collection snapshot contains nested tar archives for segments.
    /// Distinguished by a single top-level directory `snapshot` in each segment
    /// tar archive. RocksDB data stored as backups and requires unpacking
    /// procedure.
    ///
    /// ```plaintext
    /// ./0/segments/
    /// ├── 0b31e274-dc65-40e4-8493-67ebed4bcf10.tar
    /// │   └── snapshot/                               # single top-level dir
    /// │       ├── db_backup/                          # rockdb backup
    /// │       │   ├── meta/
    /// │       │   ├── private/
    /// │       │   └── shared_checksum/
    /// │       ├── payload_index_db_backup             # rocksdb backup
    /// │       │   ├── meta/
    /// │       │   ├── private/
    /// │       │   └── shared_checksum/
    /// │       └── files/                              # regular files
    /// │           ├── segment.json
    /// │           └── …
    /// ├── 1d6c96ec-7965-491a-9c45-362d55361e9b.tar
    /// └── …
    /// ```
    Regular,
    /// New experimental format.
    ///
    /// ```plaintext
    /// ./0/segments/
    /// ├── 0b31e274-dc65-40e4-8493-67ebed4bcf10/
    /// │   ├── db_backup/                              # rockdb backup
    /// │   │   ├── meta/
    /// │   │   ├── private/
    /// │   │   └── shared_checksum/
    /// │   ├── payload_index_db_backup                 # rocksdb backup
    /// │   │   ├── meta/
    /// │   │   ├── private/
    /// │   │   └── shared_checksum/
    /// │   └── files/                                  # regular files
    /// │       ├── segment.json
    /// │       └── …
    /// ├── 1d6c96ec-7965-491a-9c45-362d55361e9b/
    /// └── …
    /// ```
    Streamable,
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::{GeoLineString, GeoPoint, GeoPolygon};

    pub fn build_polygon(exterior_points: Vec<(f64, f64)>) -> GeoPolygon {
        let exterior_line = GeoLineString {
            points: exterior_points
                .into_iter()
                .map(|(lon, lat)| GeoPoint::new_unchecked(lon, lat))
                .collect(),
        };

        GeoPolygon {
            exterior: exterior_line,
            interiors: None,
        }
    }

    pub fn build_polygon_with_interiors(
        exterior_points: Vec<(f64, f64)>,
        interiors_points: Vec<Vec<(f64, f64)>>,
    ) -> GeoPolygon {
        let exterior_line = GeoLineString {
            points: exterior_points
                .into_iter()
                .map(|(lon, lat)| GeoPoint::new_unchecked(lon, lat))
                .collect(),
        };

        let interior_lines = Some(
            interiors_points
                .into_iter()
                .map(|points| GeoLineString {
                    points: points
                        .into_iter()
                        .map(|(lon, lat)| GeoPoint::new_unchecked(lon, lat))
                        .collect(),
                })
                .collect(),
        );

        GeoPolygon {
            exterior: exterior_line,
            interiors: interior_lines,
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rstest::rstest;
    use serde::de::DeserializeOwned;
    use serde_json;

    use super::test_utils::build_polygon_with_interiors;
    use super::*;

    #[allow(dead_code)]
    fn check_rms_serialization<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(
        record: T,
    ) {
        let binary_entity = rmp_serde::to_vec(&record).expect("serialization ok");
        let de_record: T = rmp_serde::from_slice(&binary_entity).expect("deserialization ok");

        assert_eq!(record, de_record);
    }

    #[test]
    #[ignore]
    fn test_rmp_vs_cbor_deserialize() {
        let payload = payload_json! {"payload_key": "payload_value"};
        let raw = rmp_serde::to_vec(&payload).unwrap();
        let de_record: Payload = serde_cbor::from_slice(&raw).unwrap();
        eprintln!("payload = {payload:#?}");
        eprintln!("de_record = {de_record:#?}");
    }

    #[rstest]
    #[case::rfc_3339("2020-03-01T00:00:00Z")]
    #[case::rfc_3339_custom_tz("2020-03-01T00:00:00-09:00")]
    #[case::rfc_3339_custom_tz_no_colon("2020-03-01 00:00:00-0900")]
    #[case::rfc_3339_custom_tz_no_colon_and_t("2020-03-01T00:00:00-0900")]
    #[case::rfc_3339_custom_tz_no_minutes("2020-03-01 00:00:00-09")]
    #[case::rfc_3339_and_decimals("2020-03-01T00:00:00.123456Z")]
    #[case::without_z("2020-03-01T00:00:00")]
    #[case::without_z_and_decimals("2020-03-01T00:00:00.12")]
    #[case::space_sep_without_z("2020-03-01 00:00:00")]
    #[case::space_sep_without_z_and_decimals("2020-03-01 00:00:00.123456")]
    fn test_datetime_deserialization(#[case] datetime: &str) {
        let datetime = DateTimePayloadType::from_str(datetime).unwrap();
        let serialized = serde_json::to_string(&datetime).unwrap();
        let deserialized: DateTimePayloadType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(datetime, deserialized);
    }

    #[test]
    fn test_datetime_deserialization_equivalency() {
        let datetime_str = "2020-03-01T01:02:03.123456Z";
        let datetime_str_no_z = "2020-03-01T01:02:03.123456";
        let datetime = DateTimePayloadType::from_str(datetime_str).unwrap();
        let datetime_no_z = DateTimePayloadType::from_str(datetime_str_no_z).unwrap();

        // Having or not the Z at the end of the string both mean UTC time
        assert_eq!(datetime.timestamp(), datetime_no_z.timestamp());
    }

    #[test]
    fn test_datetime_wrapper_transcoding() {
        let expected = DateTimeWrapper(chrono::Utc::now());
        let transcoded = DateTimeWrapper::from_str(&expected.to_string()).unwrap();
        assert_eq!(expected, transcoded);
    }

    #[test]
    fn test_timezone_ordering() {
        let datetimes = [
            "2000-06-08 00:18:53+0900",
            "2000-06-07 07:25:34-1100",
            "2000-07-10T00:18:53+0100",
            "2000-07-11 00:25:34-01:00",
            "2000-07-11 00:25:35-01",
        ];

        let sorted_datetimes: Vec<_> = datetimes
            .iter()
            .enumerate()
            .map(|(i, s)| (i, DateTimePayloadType::from_str(s).unwrap()))
            .sorted_by_key(|(_, dt)| dt.timestamp())
            .collect();

        sorted_datetimes.windows(2).for_each(|pair| {
            let (i1, dt1) = pair[0];
            let (i2, dt2) = pair[1];
            assert!(
                i1 < i2,
                "i1: {}, dt1: {}, ts1: {}\ni2: {}, dt2: {}, ts2: {}",
                i1,
                dt1.0,
                dt1.timestamp(),
                i2,
                dt2.0,
                dt2.timestamp()
            );
        });
    }

    #[test]
    fn test_geo_radius_check_point() {
        let radius = GeoRadius {
            center: GeoPoint::new_unchecked(0.0, 0.0),
            radius: OrderedFloat(80000.0),
        };

        let inside_result = radius.check_point(&GeoPoint::new_unchecked(0.5, 0.5));
        assert!(inside_result);

        let outside_result = radius.check_point(&GeoPoint::new_unchecked(1.5, 1.5));
        assert!(!outside_result);
    }

    #[test]
    fn test_geo_boundingbox_check_point() {
        let bounding_box = GeoBoundingBox {
            top_left: GeoPoint::new_unchecked(-1.0, 1.0),
            bottom_right: GeoPoint::new_unchecked(1.0, -1.0),
        };

        // haversine distance between (0, 0) and (0.5, 0.5) is 78626.29627999048
        let inside_result = bounding_box.check_point(&GeoPoint::new_unchecked(-0.5, 0.5));
        assert!(inside_result);

        // haversine distance between (0, 0) and (0.5, 0.5) is 235866.91169814655
        let outside_result = bounding_box.check_point(&GeoPoint::new_unchecked(1.5, 1.5));
        assert!(!outside_result);
    }

    #[test]
    fn test_geo_boundingbox_antimeridian_check_point() {
        // Use the bounding box for USA: (74.071028, 167), (18.7763, -66.885417)
        let bounding_box = GeoBoundingBox {
            top_left: GeoPoint::new_unchecked(167.0, 74.071028),
            bottom_right: GeoPoint::new_unchecked(-66.885417, 18.7763),
        };

        // Test NYC, which is inside the bounding box
        let inside_result =
            bounding_box.check_point(&GeoPoint::new_unchecked(-73.991516, 40.75798));
        assert!(inside_result);

        // Test Berlin, which is outside the bounding box
        let outside_result = bounding_box.check_point(&GeoPoint::new_unchecked(13.41053, 52.52437));
        assert!(!outside_result);
    }

    #[test]
    fn test_geo_polygon_check_point() {
        let test_cases = [
            // Create a GeoPolygon with a square shape
            (
                // Exterior
                vec![
                    (-1.0, -1.0),
                    (1.0, -1.0),
                    (1.0, 1.0),
                    (-1.0, 1.0),
                    (-1.0, -1.0),
                ],
                // Interiors
                vec![vec![]],
                // Expected results
                vec![((0.5, 0.5), true), ((1.5, 1.5), false), ((1.0, 0.0), false)],
            ),
            // Create a GeoPolygon as a `twisted square`
            (
                // Exterior
                vec![
                    (-1.0, -1.0),
                    (1.0, 1.0),
                    (1.0, -1.0),
                    (-1.0, 1.0),
                    (-1.0, -1.0),
                ],
                // Interiors
                vec![vec![]],
                // Expected results
                vec![((0.5, 0.0), true), ((0.0, 0.5), false), ((0.0, 0.0), false)],
            ),
            // Create a GeoPolygon with an interior (a 'hole' inside the polygon)
            (
                // Exterior
                vec![
                    (-1.0, -1.0),
                    (1.5, -1.0),
                    (1.5, 1.5),
                    (-1.0, 1.5),
                    (-1.0, -1.0),
                ],
                // Interiors
                vec![vec![
                    (-0.5, -0.5),
                    (-0.5, 0.5),
                    (0.5, 0.5),
                    (0.5, -0.5),
                    (-0.5, -0.5),
                ]],
                // Expected results
                vec![((0.6, 0.6), true), ((0.0, 0.0), false), ((0.5, 0.5), false)],
            ),
        ];

        for (exterior, interiors, points) in test_cases {
            let polygon = build_polygon_with_interiors(exterior, interiors);

            for ((lon, lat), expected_result) in points {
                let inside_result = polygon
                    .convert()
                    .check_point(&GeoPoint::new_unchecked(lon, lat));
                assert_eq!(inside_result, expected_result);
            }
        }
    }

    #[test]
    fn test_serialize_query() {
        let filter = Filter {
            must: Some(vec![Condition::Field(FieldCondition::new_match(
                JsonPath::new("hello"),
                "world".to_owned().into(),
            ))]),
            must_not: None,
            should: None,
            min_should: None,
        };
        let json = serde_json::to_string_pretty(&filter).unwrap();
        eprintln!("{json}")
    }

    #[test]
    fn test_deny_unknown_fields() {
        let query1 = r#"
         {
            "wrong": "query"
         }
         "#;
        let filter: Result<Filter, _> = serde_json::from_str(query1);

        assert!(filter.is_err())
    }

    #[test]
    fn test_parse_match_query() {
        let query = r#"
        {
            "key": "hello",
            "match": { "value": 42 }
        }
        "#;
        let condition: FieldCondition = serde_json::from_str(query).unwrap();
        assert_eq!(
            condition.r#match.unwrap(),
            Match::Value(MatchValue {
                value: ValueVariants::Integer(42)
            })
        );

        let query = r#"
        {
            "key": "hello",
            "match": { "value": true }
        }
        "#;
        let condition: FieldCondition = serde_json::from_str(query).unwrap();
        assert_eq!(
            condition.r#match.unwrap(),
            Match::Value(MatchValue {
                value: ValueVariants::Bool(true)
            })
        );

        let query = r#"
        {
            "key": "hello",
            "match": { "value": "world" }
        }
        "#;

        let condition: FieldCondition = serde_json::from_str(query).unwrap();
        assert_eq!(
            condition.r#match.unwrap(),
            Match::Value(MatchValue {
                value: ValueVariants::String("world".to_owned())
            })
        );
    }

    #[test]
    fn test_parse_match_any() {
        let query = r#"
        {
            "should": [
                {
                    "key": "Jason",
                    "match": {
                        "any": [
                            "Bourne",
                            "Momoa",
                            "Statham"
                        ]
                    }
                }
            ]
        }
        "#;

        let filter: Filter = serde_json::from_str(query).unwrap();
        let should = filter.should.unwrap();

        assert_eq!(should.len(), 1);
        let Some(Condition::Field(c)) = should.first() else {
            panic!("Condition::Field expected")
        };

        assert_eq!(c.key.to_string(), "Jason");

        let Match::Any(m) = c.r#match.as_ref().unwrap() else {
            panic!("Match::Any expected")
        };
        if let AnyVariants::Strings(kws) = &m.any {
            assert_eq!(kws.len(), 3);
            let expect: IndexSet<_, FnvBuildHasher> = ["Bourne", "Momoa", "Statham"]
                .into_iter()
                .map(|i| i.to_string())
                .collect();
            assert_eq!(kws, &expect);
        } else {
            panic!("AnyVariants::Keywords expected");
        }
    }

    #[test]
    fn test_parse_match_any_mixed_types() {
        let query = r#"
        {
            "should": [
                {
                    "key": "Jason",
                    "match": {
                        "any": [
                            "Bourne",
                            42
                        ]
                    }
                }
            ]
        }
        "#;

        let result: Result<Filter, _> = serde_json::from_str(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_nested_match_query() {
        let query = r#"
        {
            "key": "hello.nested",
            "match": { "value": 42 }
        }
        "#;
        let condition: FieldCondition = serde_json::from_str(query).unwrap();
        assert_eq!(
            condition.r#match.unwrap(),
            Match::Value(MatchValue {
                value: ValueVariants::Integer(42)
            })
        );

        let query = r#"
        {
            "key": "hello.nested",
            "match": { "value": true }
        }
        "#;
        let condition: FieldCondition = serde_json::from_str(query).unwrap();
        assert_eq!(
            condition.r#match.unwrap(),
            Match::Value(MatchValue {
                value: ValueVariants::Bool(true)
            })
        );

        let query = r#"
        {
            "key": "hello.nested",
            "match": { "value": "world" }
        }
        "#;

        let condition: FieldCondition = serde_json::from_str(query).unwrap();
        assert_eq!(
            condition.r#match.unwrap(),
            Match::Value(MatchValue {
                value: ValueVariants::String("world".to_owned())
            })
        );
    }

    #[test]
    fn test_parse_empty_query() {
        let query = r#"
        {
            "should": [
                {
                    "is_empty" : {
                        "key" : "Jason"
                    }
                }
            ]
        }
        "#;

        let filter: Filter = serde_json::from_str(query).unwrap();
        let should = filter.should.unwrap();

        assert_eq!(should.len(), 1);
        let Some(Condition::IsEmpty(c)) = should.first() else {
            panic!("Condition::IsEmpty expected")
        };

        assert_eq!(c.is_empty.key.to_string(), "Jason");
    }

    #[test]
    fn test_parse_null_query() {
        let query = r#"
        {
            "should": [
                {
                    "is_null" : {
                        "key" : "Jason"
                    }
                }
            ]
        }
        "#;

        let filter: Filter = serde_json::from_str(query).unwrap();
        let should = filter.should.unwrap();

        assert_eq!(should.len(), 1);
        let Some(Condition::IsNull(c)) = should.first() else {
            panic!("Condition::IsNull expected")
        };

        assert_eq!(c.is_null.key.to_string(), "Jason");
    }

    #[test]
    fn test_parse_nested_filter_query() {
        let query = r#"
        {
          "must": [
            {
              "nested": {
                "key": "country.cities",
                "filter": {
                  "must": [
                    {
                      "key": "population",
                      "range": {
                        "gte": 8
                      }
                    },
                    {
                      "key": "sightseeing",
                      "values_count": {
                        "lt": 3
                      }
                    }
                  ]
                }
              }
            }
          ]
        }
        "#;
        let filter: Filter = serde_json::from_str(query).unwrap();
        let musts = filter.must.unwrap();
        assert_eq!(musts.len(), 1);
        match musts.first() {
            Some(Condition::Nested(nested_condition)) => {
                assert_eq!(nested_condition.raw_key().to_string(), "country.cities");
                assert_eq!(nested_condition.array_key().to_string(), "country.cities[]");
                let nested_musts = nested_condition.filter().must.as_ref().unwrap();
                assert_eq!(nested_musts.len(), 2);
                let first_must = nested_musts.first().unwrap();
                match first_must {
                    Condition::Field(c) => {
                        assert_eq!(c.key.to_string(), "population");
                        assert!(c.range.is_some());
                    }
                    _ => panic!("Condition::Field expected"),
                }

                let second_must = nested_musts.get(1).unwrap();
                match second_must {
                    Condition::Field(c) => {
                        assert_eq!(c.key.to_string(), "sightseeing");
                        assert!(c.values_count.is_some());
                    }
                    _ => panic!("Condition::Field expected"),
                }
            }
            o => panic!("Condition::Nested expected but got {o:?}"),
        };
    }

    #[test]
    fn test_parse_single_nested_filter_query() {
        let query = r#"
        {
          "must": {
              "nested": {
                "key": "country.cities",
                "filter": {
                  "must": {
                      "key": "population",
                      "range": {
                        "gte": 8
                      }
                    }
                }
              }
            }
        }
        "#;
        let filter: Filter = serde_json::from_str(query).unwrap();
        let musts = filter.must.unwrap();
        assert_eq!(musts.len(), 1);

        let first_must = musts.first().unwrap();
        let Condition::Nested(nested_condition) = first_must else {
            panic!("Condition::Nested expected but got {first_must:?}")
        };

        assert_eq!(nested_condition.raw_key().to_string(), "country.cities");
        assert_eq!(nested_condition.array_key().to_string(), "country.cities[]");

        let nested_must = nested_condition.filter().must.as_ref().unwrap();
        assert_eq!(nested_must.len(), 1);

        let must = nested_must.first().unwrap();
        let Condition::Field(c) = must else {
            panic!("Condition::Field expected, got {must:?}")
        };

        assert_eq!(c.key.to_string(), "population");
        assert!(c.range.is_some());
    }

    #[test]
    fn test_payload_query_parse() {
        let query1 = r#"
        {
            "must": [
                {
                    "key": "hello",
                    "match": {
                        "value": 42
                    }
                },
                {
                    "must_not": [
                        {
                            "has_id": [1, 2, 3, 4]
                        },
                        {
                            "key": "geo_field",
                            "geo_bounding_box": {
                                "top_left": {
                                    "lon": 13.410146,
                                    "lat": 52.519289
                                },
                                "bottom_right": {
                                    "lon": 13.432683,
                                    "lat": 52.505582
                                }
                            }
                        }
                    ]
                }
            ]
        }
        "#;

        let filter: Filter = serde_json::from_str(query1).unwrap();
        eprintln!("{filter:?}");
        let must = filter.must.unwrap();
        let _must_not = filter.must_not;
        assert_eq!(must.len(), 2);
        match must.get(1) {
            Some(Condition::Filter(f)) => {
                let must_not = &f.must_not;
                match must_not {
                    Some(v) => assert_eq!(v.len(), 2),
                    None => panic!("Filter expected"),
                }
            }
            _ => panic!("Condition expected"),
        }
    }

    #[test]
    fn test_nested_payload_query_parse() {
        let query1 = r#"
        {
            "must": [
                {
                    "key": "hello.nested.world",
                    "match": {
                        "value": 42
                    }
                },
                {
                    "key": "foo.nested.bar",
                    "match": {
                        "value": 1
                    }
                }
            ]
        }
        "#;

        let filter: Filter = serde_json::from_str(query1).unwrap();
        let must = filter.must.unwrap();
        assert_eq!(must.len(), 2);
    }

    #[test]
    fn test_min_should_query_parse() {
        let query1 = r#"
        {
            "min_should": {
                "conditions": [
                    {
                        "key": "hello.nested.world",
                        "match": {
                            "value": 42
                        }
                    },
                    {
                        "key": "foo.nested.bar",
                        "match": {
                            "value": 1
                        }
                    }
                ],
                "min_count": 2
            }
        }
        "#;

        let filter: Filter = serde_json::from_str(query1).unwrap();
        let min_should = filter.min_should.unwrap();
        assert_eq!(min_should.conditions.len(), 2);
    }

    #[test]
    fn test_min_should_nested_parse() {
        let query1 = r#"
        {
            "must": [
                {
                    "min_should": {
                        "conditions": [
                            {
                                "key": "hello.nested.world",
                                "match": {
                                    "value": 42
                                }
                            },
                            {
                                "key": "foo.nested.bar",
                                "match": {
                                    "value": 1
                                }
                            }
                        ],
                        "min_count": 2
                    }
                }
            ]
        }
        "#;

        let filter: Filter = serde_json::from_str(query1).unwrap();
        let must = filter.must.unwrap();
        assert_eq!(must.len(), 1);

        match must.first() {
            Some(Condition::Filter(f)) => {
                let min_should = &f.min_should;
                match min_should {
                    Some(v) => assert_eq!(v.conditions.len(), 2),
                    None => panic!("Filter expected"),
                }
            }
            _ => panic!("Condition expected"),
        }
    }

    #[test]
    fn test_geo_validation() {
        let query1 = r#"
        {
            "must": [
                {
                    "key": "geo_field",
                    "geo_bounding_box": {
                        "top_left": {
                            "lon": 1113.410146,
                            "lat": 52.519289
                        },
                        "bottom_right": {
                            "lon": 13.432683,
                            "lat": 52.505582
                        }
                    }
                }
            ]
        }
        "#;
        let filter: Result<Filter, _> = serde_json::from_str(query1);
        assert!(filter.is_err());

        let query2 = r#"
        {
            "must": [
                {
                    "key": "geo_field",
                    "geo_polygon": {
                        "exterior": {},
                        "interiors": []
                    }
                }
            ]
        }
        "#;
        let filter: Result<Filter, _> = serde_json::from_str(query2);
        assert!(filter.is_err());

        let query3 = r#"
        {
            "must": [
                {
                    "key": "geo_field",
                    "geo_polygon": {
                        "exterior":{
                            "points": [
                                {"lon": -12.0, "lat": -34.0},
                                {"lon": 11.0, "lat": -22.0},
                                {"lon": -32.0, "lat": -14.0}
                            ]
                        },
                        "interiors": []
                    }
                }
            ]
        }
        "#;
        let filter: Result<Filter, _> = serde_json::from_str(query3);
        assert!(filter.is_err());

        let query4 = r#"
        {
            "must": [
                {
                    "key": "geo_field",
                    "geo_polygon": {
                        "exterior": {
                            "points": [
                                {"lon": -12.0, "lat": -34.0},
                                {"lon": 11.0, "lat": -22.0},
                                {"lon": -32.0, "lat": -14.0},
                                {"lon": -12.0, "lat": -34.0}
                            ]
                        },
                        "interiors": []
                    }
                }
            ]
        }
        "#;
        let filter: Result<Filter, _> = serde_json::from_str(query4);
        assert!(filter.is_ok());

        let query5 = r#"
            {
                "must": [
                    {
                        "key": "geo_field",
                        "geo_polygon": {
                            "exterior": {
                                    "points": [
                                        {"lon": -12.0, "lat": -34.0},
                                        {"lon": 11.0, "lat": -22.0},
                                        {"lon": -32.0, "lat": -14.0},
                                        {"lon": -12.0, "lat": -34.0}
                                    ]
                                },
                            "interiors": [
                                {
                                    "points": [
                                        {"lon": -12.0, "lat": -34.0},
                                        {"lon": 11.0, "lat": -22.0},
                                        {"lon": -32.0, "lat": -14.0}
                                    ]
                                }
                            ]
                        }
                    }
                ]
            }
            "#;
        let filter: Result<Filter, _> = serde_json::from_str(query5);
        assert!(filter.is_err());

        let query6 = r#"
            {
                "must": [
                    {
                        "key": "geo_field",
                        "geo_polygon": {
                            "exterior": {
                                    "points": [
                                        {"lon": -12.0, "lat": -34.0},
                                        {"lon": 11.0, "lat": -22.0},
                                        {"lon": -32.0, "lat": -14.0},
                                        {"lon": -12.0, "lat": -34.0}
                                    ]
                                },
                            "interiors": [
                                {
                                    "points": [
                                        {"lon": -12.0, "lat": -34.0},
                                        {"lon": 11.0, "lat": -22.0},
                                        {"lon": -32.0, "lat": -14.0},
                                        {"lon": -12.0, "lat": -34.0}
                                    ]
                                }
                            ]
                        }
                    }
                ]
            }
            "#;
        let filter: Result<Filter, _> = serde_json::from_str(query6);
        assert!(filter.is_ok());
    }

    #[test]
    fn test_payload_parsing() {
        let ft = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
        let ft_json = serde_json::to_string(&ft).unwrap();
        eprintln!("ft_json = {ft_json:?}");

        let ft = PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(Default::default()));
        let ft_json = serde_json::to_string(&ft).unwrap();
        eprintln!("ft_json = {ft_json:?}");

        let query = r#""keyword""#;
        let field_type: PayloadSchemaType = serde_json::from_str(query).unwrap();
        eprintln!("field_type = {field_type:?}");
    }

    #[test]
    fn merge_filters() {
        let condition1 = Condition::Field(FieldCondition::new_match(
            JsonPath::new("summary"),
            Match::new_text("Berlin"),
        ));
        let mut this = Filter::new_must(condition1.clone());
        this.should = Some(vec![condition1.clone()]);

        let condition2 = Condition::Field(FieldCondition::new_match(
            JsonPath::new("city"),
            Match::new_value(ValueVariants::String("Osaka".into())),
        ));
        let other = Filter::new_must(condition2.clone());

        let merged = this.merge(&other);

        assert!(merged.must.is_some());
        assert_eq!(merged.must.as_ref().unwrap().len(), 2);
        assert!(merged.must_not.is_none());
        assert!(merged.should.is_some());
        assert_eq!(merged.should.as_ref().unwrap().len(), 1);

        assert!(merged.must.as_ref().unwrap().contains(&condition1));
        assert!(merged.must.as_ref().unwrap().contains(&condition2));
        assert!(merged.should.as_ref().unwrap().contains(&condition1));
    }

    #[test]
    fn test_payload_selector_include() {
        let payload = payload_json! {
            "a": 1,
            "b": {
                "c": 123,
                "e": {
                    "f": [1,2,3],
                    "g": 7,
                    "h": "text",
                    "i": [
                        {
                            "j": 1,
                            "k": 2

                        },
                        {
                            "j": 3,
                            "k": 4
                        }
                    ]
                }
            }
        };

        // include root & nested
        let selector =
            PayloadSelector::new_include(vec![JsonPath::new("a"), JsonPath::new("b.e.f")]);
        let payload = selector.process(payload);

        let expected = payload_json! {
            "a": 1,
            "b": {
                "e": {
                    "f": [1,2,3],
                }
            }
        };
        assert_eq!(payload, expected);
    }

    #[test]
    fn test_payload_selector_array_include() {
        let payload = payload_json! {
            "a": 1,
            "b": {
                "c": 123,
                "f": [1,2,3,4,5],
            }
        };

        // handles duplicates
        let selector = PayloadSelector::new_include(vec![JsonPath::new("a"), JsonPath::new("a")]);
        let payload = selector.process(payload);

        let expected = payload_json! {
            "a": 1
        };
        assert_eq!(payload, expected);

        // ignore path that points to array
        let selector = PayloadSelector::new_include(vec![JsonPath::new("b.f[0]")]);
        let payload = selector.process(payload);

        // nothing included
        let expected = payload_json! {};
        assert_eq!(payload, expected);
    }

    #[test]
    fn test_payload_selector_no_implicit_array_include() {
        let payload = payload_json! {
            "a": 1,
            "b": {
                "c": [
                    {
                        "d": 1,
                        "e": 2
                    },
                    {
                        "d": 3,
                        "e": 4
                    }
                ],
            }
        };

        let selector = PayloadSelector::new_include(vec![JsonPath::new("b.c")]);
        let selected_payload = selector.process(payload.clone());

        let expected = payload_json! {
            "b": {
                "c": [
                    {
                        "d": 1,
                        "e": 2
                    },
                    {
                        "d": 3,
                        "e": 4
                    }
                ]
            }
        };
        assert_eq!(selected_payload, expected);

        // with explicit array traversal ([] notation)
        let selector = PayloadSelector::new_include(vec![JsonPath::new("b.c[].d")]);
        let selected_payload = selector.process(payload.clone());

        let expected = payload_json! {
            "b": {
                "c": [
                    {"d": 1},
                    {"d": 3}
                ]
            }
        };
        assert_eq!(selected_payload, expected);

        // shortcuts implicit array traversal
        let selector = PayloadSelector::new_include(vec![JsonPath::new("b.c.d")]);
        let selected_payload = selector.process(payload);

        let expected = payload_json! {
            "b": {
                "c": []
            }
        };
        assert_eq!(selected_payload, expected);
    }

    #[test]
    fn test_payload_selector_exclude() {
        let payload = payload_json! {
            "a": 1,
            "b": {
                "c": 123,
                "e": {
                    "f": [1,2,3],
                    "g": 7,
                    "h": "text",
                    "i": [
                        {
                            "j": 1,
                            "k": 2

                        },
                        {
                            "j": 3,
                            "k": 4
                        }
                    ]
                }
            }
        };

        // exclude
        let selector =
            PayloadSelector::new_exclude(vec![JsonPath::new("a"), JsonPath::new("b.e.f")]);
        let payload = selector.process(payload);

        // root removal & nested removal
        let expected = payload_json! {
            "b": {
                "c": 123,
                "e": {
                    "g": 7,
                    "h": "text",
                    "i": [
                        {
                            "j": 1,
                            "k": 2

                        },
                        {
                            "j": 3,
                            "k": 4
                        }
                    ]
                }
            }
        };
        assert_eq!(payload, expected);
    }

    #[test]
    fn test_payload_selector_array_exclude() {
        let payload = payload_json! {
            "a": 1,
            "b": {
                "c": 123,
                "f": [1,2,3,4,5],
            }
        };

        // handles duplicates
        let selector = PayloadSelector::new_exclude(vec![JsonPath::new("a"), JsonPath::new("a")]);
        let payload = selector.process(payload);

        // single removal
        let expected = payload_json! {
            "b": {
                "c": 123,
                "f": [1,2,3,4,5],
            }
        };
        assert_eq!(payload, expected);

        // ignore path that points to array
        let selector = PayloadSelector::new_exclude(vec![JsonPath::new("b.f[0]")]);

        let payload = selector.process(payload);

        // no removal
        let expected = payload_json! {
            "b": {
                "c": 123,
                "f": [1,2,3,4,5],
            }
        };
        assert_eq!(payload, expected);
    }
}

fn shard_key_string_example() -> String {
    "region_1".to_string()
}

fn shard_key_number_example() -> u64 {
    12
}

#[derive(Deserialize, Serialize, JsonSchema, Anonymize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum ShardKey {
    #[schemars(
        schema_with = "String::json_schema",
        example = "shard_key_string_example"
    )]
    Keyword(EcoString),
    #[schemars(example = "shard_key_number_example")]
    #[anonymize(false)]
    Number(u64),
}

impl From<String> for ShardKey {
    fn from(s: String) -> Self {
        ShardKey::Keyword(EcoString::from(s))
    }
}

impl From<EcoString> for ShardKey {
    fn from(s: EcoString) -> Self {
        ShardKey::Keyword(s)
    }
}

impl From<&str> for ShardKey {
    fn from(s: &str) -> Self {
        ShardKey::Keyword(EcoString::from(s))
    }
}

impl From<u64> for ShardKey {
    fn from(n: u64) -> Self {
        ShardKey::Number(n)
    }
}

impl Display for ShardKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardKey::Keyword(keyword) => write!(f, "\"{keyword}\""),
            ShardKey::Number(number) => write!(f, "{number}"),
        }
    }
}
