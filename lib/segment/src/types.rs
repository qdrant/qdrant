use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Formatter;
use std::mem::size_of;
use std::ops::Deref;
use std::rc::Rc;
use std::str::FromStr;

use geo::prelude::HaversineDistance;
use geo::Point;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::common::utils;
use crate::common::utils::MultiValue;
use crate::data_types::text_index::TextIndexParams;
use crate::data_types::vectors::{VectorElementType, VectorStruct};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};

/// Type of point index inside a segment
pub type PointOffsetType = u32;
pub type PayloadKeyType = String;
pub type PayloadKeyTypeRef<'a> = &'a str;
pub type SeqNumberType = u64;
/// Sequential number of modification, applied to segment
pub type ScoreType = f32;
/// Type of vector matching score
pub type TagType = u64;
/// Type of float point payload
pub type FloatPayloadType = f64;
/// Type of integer point payload
pub type IntPayloadType = i64;

pub const VECTOR_ELEMENT_SIZE: usize = size_of::<VectorElementType>();

/// Type, used for specifying point ID in user interface
#[derive(Debug, Serialize, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, JsonSchema)]
#[serde(untagged)]
pub enum ExtendedPointId {
    NumId(u64),
    Uuid(Uuid),
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
        let value = match serde_value::Value::deserialize(deserializer) {
            Ok(val) => val,
            Err(err) => return Err(err),
        };

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

/// Type of internal tags, build from payload
#[derive(
    Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, FromPrimitive, PartialEq, Eq, Hash,
)]
/// Distance function types used to compare vectors
pub enum Distance {
    // <https://en.wikipedia.org/wiki/Cosine_similarity>
    Cosine,
    // <https://en.wikipedia.org/wiki/Euclidean_distance>
    Euclid,
    // <https://en.wikipedia.org/wiki/Dot_product>
    Dot,
}

impl Distance {
    pub fn preprocess_vector(
        &self,
        vector: &[VectorElementType],
    ) -> Option<Vec<VectorElementType>> {
        match self {
            Distance::Cosine => CosineMetric::preprocess(vector),
            Distance::Euclid => EuclidMetric::preprocess(vector),
            Distance::Dot => DotProductMetric::preprocess(vector),
        }
    }

    pub fn postprocess_score(&self, score: ScoreType) -> ScoreType {
        match self {
            Distance::Cosine => CosineMetric::postprocess(score),
            Distance::Euclid => EuclidMetric::postprocess(score),
            Distance::Dot => DotProductMetric::postprocess(score),
        }
    }

    pub fn distance_order(&self) -> Order {
        match self {
            Distance::Cosine | Distance::Dot => Order::LargeBetter,
            Distance::Euclid => Order::SmallBetter,
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

pub enum Order {
    LargeBetter,
    SmallBetter,
}

/// Search result
#[derive(Deserialize, Serialize, JsonSchema, Clone, Debug)]
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
    pub vector: Option<VectorStruct>,
}

impl Eq for ScoredPoint {}

impl Ord for ScoredPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.score).cmp(&OrderedFloat(other.score))
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
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct PayloadIndexInfo {
    pub data_type: PayloadSchemaType,
    #[serde(default)]
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
            PayloadFieldSchema::FieldParams(schema_params) => match schema_params {
                PayloadSchemaParams::Text(_) => PayloadIndexInfo {
                    data_type: PayloadSchemaType::Text,
                    params: Some(schema_params),
                    points: points_count,
                },
            },
        }
    }
}

/// Aggregated information about segment
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SegmentInfo {
    pub segment_type: SegmentType,
    pub num_vectors: usize,
    pub num_points: usize,
    pub num_deleted_vectors: usize,
    pub ram_usage_bytes: usize,
    pub disk_usage_bytes: usize,
    pub is_appendable: bool,
    pub index_schema: HashMap<PayloadKeyType, PayloadIndexInfo>,
}

/// Additional parameters of the search
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct QuantizationSearchParams {
    /// If true, quantized vectors are ignored. Default is false.
    #[serde(default = "default_quantization_ignore_value")]
    pub ignore: bool,

    /// If true, use original vectors to re-score top-k results.
    /// Might require more time in case if original vectors are stored on disk.
    /// Default is false.
    #[serde(default = "default_quantization_rescore_value")]
    pub rescore: bool,
}

pub fn default_quantization_ignore_value() -> bool {
    false
}

pub fn default_quantization_rescore_value() -> bool {
    false
}

/// Additional parameters of the search
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct SearchParams {
    /// Params relevant to HNSW index
    /// /// Size of the beam in a beam-search. Larger the value - more accurate the result, more time required for search.
    pub hnsw_ef: Option<usize>,

    /// Search without approximation. If set to true, search may run long but with exact results.
    #[serde(default)]
    pub exact: bool,

    /// Quantization params
    #[serde(default)]
    pub quantization: Option<QuantizationSearchParams>,
}

/// Vector index configuration of the segment
#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
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

/// Config of HNSW index
#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct HnswConfig {
    /// Number of edges per node in the index graph. Larger the value - more accurate the search, more space required.
    pub m: usize,
    /// Number of neighbours to consider during the index building. Larger the value - more accurate the search, more time required to build index.
    pub ef_construct: usize,
    /// Minimal size (in KiloBytes) of vectors for additional payload-based indexing.
    /// If payload chunk is smaller than `full_scan_threshold_kb` additional indexing won't be used -
    /// in this case full-scan search should be preferred by query planner and additional indexing is not required.
    /// Note: 1Kb = 1 vector of size 256
    #[serde(alias = "full_scan_threshold_kb")]
    pub full_scan_threshold: usize,
    /// Number of parallel threads used for background index building. If 0 - auto selection.
    #[serde(default = "default_max_indexing_threads")]
    pub max_indexing_threads: usize,
    /// Store HNSW index on disk. If set to false, index will be stored in RAM. Default: false
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")] // Better backward compatibility
    pub on_disk: Option<bool>,
    /// Custom M param for hnsw graph built for payload index. If not set, default M will be used.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")] // Better backward compatibility
    pub payload_m: Option<usize>,
}

fn default_max_indexing_threads() -> usize {
    0
}

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ScalarType {
    #[default]
    Int8,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ScalarQuantizationConfig {
    /// Type of quantization to use
    /// If `int8` - 8 bit quantization will be used
    pub r#type: ScalarType,
    /// Quantile for quantization. Expected value range in (0, 1.0]. If not set - use the whole range of values
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantile: Option<f32>,
    /// If true - quantized vectors always will be stored in RAM, ignoring the config of main storage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub always_ram: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
pub struct ScalarQuantization {
    pub scalar: ScalarQuantizationConfig,
}

impl PartialEq for ScalarQuantizationConfig {
    fn eq(&self, other: &Self) -> bool {
        self.quantile == other.quantile
            && self.always_ram == other.always_ram
            && self.r#type == other.r#type
    }
}

impl std::hash::Hash for ScalarQuantizationConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.always_ram.hash(state);
        self.r#type.hash(state);
    }
}

impl Eq for ScalarQuantizationConfig {}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum QuantizationConfig {
    Scalar(ScalarQuantization),
}

impl From<ScalarQuantizationConfig> for QuantizationConfig {
    fn from(config: ScalarQuantizationConfig) -> Self {
        QuantizationConfig::Scalar(ScalarQuantization { scalar: config })
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
        }
    }
}

impl Indexes {
    pub fn default_hnsw() -> Self {
        Indexes::Hnsw(Default::default())
    }
}

impl Default for Indexes {
    fn default() -> Self {
        Indexes::Plain {}
    }
}

/// Type of payload index
#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "options")]
pub enum PayloadIndexType {
    // Do not index anything, just keep of what should be indexed later
    #[default]
    Plain,
    // Build payload index. Index is saved on disc, but index itself is in RAM
    Struct,
}

/// Type of vector storage
#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "options")]
pub enum StorageType {
    // Store vectors in memory and use persistence storage only if vectors are changed
    #[default]
    InMemory,
    // Use memmap to store vectors, a little slower than `InMemory`, but requires little RAM
    Mmap,
}

/// Type of payload storage
#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "options")]
pub enum PayloadStorageType {
    // Store payload in memory and use persistence storage only if vectors are changed
    #[default]
    InMemory,
    // Store payload on disk only, read each time it is requested
    OnDisk,
}

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SegmentConfig {
    pub vector_data: HashMap<String, VectorDataConfig>,
    /// Type of index used for search
    pub index: Indexes,
    /// Type of vector storage
    pub storage_type: StorageType,
    /// Defines payload storage type
    #[serde(default)]
    pub payload_storage_type: PayloadStorageType,
    /// Quantization parameters. If none - quantization is disabled.
    #[serde(default)]
    pub quantization_config: Option<QuantizationConfig>,
}

/// Config of single vector data storage
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct VectorDataConfig {
    /// Size of a vectors used
    pub size: usize,
    /// Type of distance function used for measuring distance between vectors
    pub distance: Distance,
}

/// Default value based on <https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md>
pub const DEFAULT_FULL_SCAN_THRESHOLD: usize = 20_000;

/// Persistable state of segment configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SegmentState {
    pub version: SeqNumberType,
    pub config: SegmentConfig,
}

/// Geo point payload schema
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(try_from = "GeoPointShadow")]
pub struct GeoPoint {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Deserialize)]
struct GeoPointShadow {
    pub lon: f64,
    pub lat: f64,
}

pub struct GeoPointValidationError {
    pub lon: f64,
    pub lat: f64,
}

// The error type has to implement Display
impl std::fmt::Display for GeoPointValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Wrong format of GeoPoint payload: expected `lat` = {} within [-90;90] and `lon` = {} within [-180;180]", self.lat, self.lon)
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
        Ok(GeoPoint { lon, lat })
    }
}

impl TryFrom<GeoPointShadow> for GeoPoint {
    type Error = GeoPointValidationError;

    fn try_from(value: GeoPointShadow) -> Result<Self, Self::Error> {
        GeoPoint::validate(value.lon, value.lat)?;

        Ok(Self {
            lon: value.lon,
            lat: value.lat,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Payload(pub Map<String, Value>);

impl Payload {
    pub fn merge(&mut self, value: &Payload) {
        for (key, value) in &value.0 {
            match value {
                Value::Null => self.0.remove(key),
                _ => self.0.insert(key.to_owned(), value.to_owned()),
            };
        }
    }

    pub fn get_value(&self, path: &str) -> MultiValue<&Value> {
        utils::get_value_from_json_map(path, &self.0)
    }

    pub fn remove(&mut self, path: &str) -> Vec<Value> {
        utils::remove_value_from_json_map(path, &mut self.0).values()
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

    pub fn iter(&self) -> serde_json::map::Iter {
        self.0.iter()
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

impl From<Value> for Payload {
    fn from(value: Value) -> Self {
        match value {
            Value::Object(map) => Payload(map),
            _ => panic!("cannot convert from {value:?}"),
        }
    }
}

impl From<Map<String, Value>> for Payload {
    fn from(value: serde_json::Map<String, Value>) -> Self {
        Payload(value)
    }
}

#[derive(Clone)]
pub enum OwnedPayloadRef<'a> {
    Ref(&'a Payload),
    Owned(Rc<Payload>),
}

impl<'a> Deref for OwnedPayloadRef<'a> {
    type Target = Payload;

    fn deref(&self) -> &Self::Target {
        match self {
            OwnedPayloadRef::Ref(reference) => reference,
            OwnedPayloadRef::Owned(owned) => owned.deref(),
        }
    }
}

impl<'a> AsRef<Payload> for OwnedPayloadRef<'a> {
    fn as_ref(&self) -> &Payload {
        match self {
            OwnedPayloadRef::Ref(reference) => reference,
            OwnedPayloadRef::Owned(owned) => owned.deref(),
        }
    }
}

impl<'a> From<Payload> for OwnedPayloadRef<'a> {
    fn from(payload: Payload) -> Self {
        OwnedPayloadRef::Owned(Rc::new(payload))
    }
}

impl<'a> From<&'a Payload> for OwnedPayloadRef<'a> {
    fn from(payload: &'a Payload) -> Self {
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
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum PayloadVariant<T> {
    List(Vec<T>),
    Value(T),
}

impl<T: Clone> PayloadVariant<T> {
    pub fn to_list(&self) -> Vec<T> {
        match self {
            PayloadVariant::Value(x) => vec![x.clone()],
            PayloadVariant::List(vec) => vec.clone(),
        }
    }
}

/// Json representation of a payload
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum JsonPayload {
    Keyword(PayloadVariant<String>),
    Integer(PayloadVariant<IntPayloadType>),
    Float(PayloadVariant<FloatPayloadType>),
    Geo(PayloadVariant<GeoPoint>),
}

/// All possible names of payload types
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PayloadSchemaType {
    Keyword,
    Integer,
    Float,
    Geo,
    Text,
}

/// Payload type with parameters
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum PayloadSchemaParams {
    Text(TextIndexParams),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum PayloadFieldSchema {
    FieldType(PayloadSchemaType),
    FieldParams(PayloadSchemaParams),
}

impl From<PayloadSchemaType> for PayloadFieldSchema {
    fn from(payload_schema_type: PayloadSchemaType) -> Self {
        PayloadFieldSchema::FieldType(payload_schema_type)
    }
}

impl TryFrom<PayloadIndexInfo> for PayloadFieldSchema {
    type Error = String;

    fn try_from(index_info: PayloadIndexInfo) -> Result<Self, Self::Error> {
        match (index_info.data_type, index_info.params) {
            (PayloadSchemaType::Text, Some(PayloadSchemaParams::Text(params))) => Ok(
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(params)),
            ),
            (data_type, Some(_)) => Err(format!(
                "Payload field with type {data_type:?} has unexpected params"
            )),
            (data_type, None) => Ok(PayloadFieldSchema::FieldType(data_type)),
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

pub fn infer_value_type(value: &Value) -> Option<PayloadSchemaType> {
    match value {
        Value::Array(array) => infer_collection_value_type(array),
        _ => value_type(value),
    }
}

pub fn infer_collection_value_type<'a, I>(values: I) -> Option<PayloadSchemaType>
where
    I: IntoIterator<Item = &'a Value>,
{
    let possible_types = values.into_iter().map(value_type).unique().collect_vec();
    if possible_types.len() != 1 {
        None // There is an ambiguity or empty array
    } else {
        possible_types.into_iter().next().unwrap()
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum ValueVariants {
    Keyword(String),
    Integer(IntPayloadType),
    Bool(bool),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum AnyVariants {
    Keywords(Vec<String>),
    Integers(Vec<IntPayloadType>),
}

/// Exact match of the given value
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MatchValue {
    pub value: ValueVariants,
}

/// Full-text match of the strings.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MatchText {
    pub text: String,
}

impl From<String> for MatchText {
    fn from(text: String) -> Self {
        MatchText { text }
    }
}

/// Exact match on any of the given values
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MatchAny {
    pub any: AnyVariants,
}

/// Match filter request
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum MatchInterface {
    Value(MatchValue),
    Text(MatchText),
    Any(MatchAny),
}

/// Match filter request
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(from = "MatchInterface")]
#[serde(untagged)]
pub enum Match {
    Value(MatchValue),
    Text(MatchText),
    Any(MatchAny),
}

impl From<MatchInterface> for Match {
    fn from(value: MatchInterface) -> Self {
        match value {
            MatchInterface::Value(value) => Self::Value(MatchValue { value: value.value }),
            MatchInterface::Text(text) => Self::Text(MatchText { text: text.text }),
            MatchInterface::Any(any) => Self::Any(MatchAny { any: any.any }),
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
            value: ValueVariants::Keyword(keyword),
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
        Self::Any(MatchAny {
            any: AnyVariants::Keywords(keywords),
        })
    }
}

impl From<Vec<IntPayloadType>> for Match {
    fn from(integers: Vec<IntPayloadType>) -> Self {
        Self::Any(MatchAny {
            any: AnyVariants::Integers(integers),
        })
    }
}

/// Range filter request
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct Range {
    /// point.key < range.lt
    pub lt: Option<FloatPayloadType>,
    /// point.key > range.gt
    pub gt: Option<FloatPayloadType>,
    /// point.key >= range.gte
    pub gte: Option<FloatPayloadType>,
    /// point.key <= range.lte
    pub lte: Option<FloatPayloadType>,
}

impl Range {
    pub fn check_range(&self, number: FloatPayloadType) -> bool {
        self.lt.map_or(true, |x| number < x)
            && self.gt.map_or(true, |x| number > x)
            && self.lte.map_or(true, |x| number <= x)
            && self.gte.map_or(true, |x| number >= x)
    }
}

/// Values count filter request
#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
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
    pub fn check_count(&self, value: &Value) -> bool {
        let count = match value {
            Value::Null => 0,
            Value::Array(array) => array.len(),
            _ => 1,
        };

        self.lt.map_or(true, |x| count < x)
            && self.gt.map_or(true, |x| count > x)
            && self.lte.map_or(true, |x| count <= x)
            && self.gte.map_or(true, |x| count >= x)
    }
}

/// Geo filter request
///
/// Matches coordinates inside the rectangle, described by coordinates of lop-left and bottom-right edges
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct GeoBoundingBox {
    /// Coordinates of the top left point of the area rectangle
    pub top_left: GeoPoint,
    /// Coordinates of the bottom right point of the area rectangle
    pub bottom_right: GeoPoint,
}

impl GeoBoundingBox {
    pub fn check_point(&self, lon: f64, lat: f64) -> bool {
        (self.top_left.lon < lon)
            && (lon < self.bottom_right.lon)
            && (self.bottom_right.lat < lat)
            && (lat < self.top_left.lat)
    }
}

/// Geo filter request
///
/// Matches coordinates inside the circle of `radius` and center with coordinates `center`
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct GeoRadius {
    /// Coordinates of the top left point of the area rectangle
    pub center: GeoPoint,
    /// Radius of the area in meters
    pub radius: f64,
}

impl GeoRadius {
    pub fn check_point(&self, lon: f64, lat: f64) -> bool {
        let query_center = Point::new(self.center.lon, self.center.lat);
        query_center.haversine_distance(&Point::new(lon, lat)) < self.radius
    }
}

/// All possible payload filtering conditions
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct FieldCondition {
    /// Payload key
    pub key: PayloadKeyType,
    /// Check if point has field with a given value
    pub r#match: Option<Match>,
    /// Check if points value lies in a given range
    pub range: Option<Range>,
    /// Check if points geo location lies in a given area
    pub geo_bounding_box: Option<GeoBoundingBox>,
    /// Check if geo point is within a given radius
    pub geo_radius: Option<GeoRadius>,
    /// Check number of values of the field
    pub values_count: Option<ValuesCount>,
}

impl FieldCondition {
    pub fn new_match(key: PayloadKeyType, r#match: Match) -> Self {
        Self {
            key,
            r#match: Some(r#match),
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            values_count: None,
        }
    }

    pub fn new_range(key: PayloadKeyType, range: Range) -> Self {
        Self {
            key,
            r#match: None,
            range: Some(range),
            geo_bounding_box: None,
            geo_radius: None,
            values_count: None,
        }
    }

    pub fn new_geo_bounding_box(key: PayloadKeyType, geo_bounding_box: GeoBoundingBox) -> Self {
        Self {
            key,
            r#match: None,
            range: None,
            geo_bounding_box: Some(geo_bounding_box),
            geo_radius: None,
            values_count: None,
        }
    }

    pub fn new_geo_radius(key: PayloadKeyType, geo_radius: GeoRadius) -> Self {
        Self {
            key,
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: Some(geo_radius),
            values_count: None,
        }
    }

    pub fn new_values_count(key: PayloadKeyType, values_count: ValuesCount) -> Self {
        Self {
            key,
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            values_count: Some(values_count),
        }
    }
}

/// Payload field
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
pub struct PayloadField {
    /// Payload field name
    pub key: PayloadKeyType,
}

/// Select points with empty payload for a specified field
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
pub struct IsEmptyCondition {
    pub is_empty: PayloadField,
}

/// ID-based filtering condition
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
pub struct HasIdCondition {
    pub has_id: HashSet<PointIdType>,
}

impl From<HashSet<PointIdType>> for HasIdCondition {
    fn from(set: HashSet<PointIdType>) -> Self {
        HasIdCondition { has_id: set }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum Condition {
    /// Check if field satisfies provided condition
    Field(FieldCondition),
    /// Check if payload field is empty: equals to `NULL`, empty array, or does not exists
    IsEmpty(IsEmptyCondition),
    /// Check if points id is in a given set
    HasId(HasIdCondition),
    /// Nested filter
    Filter(Filter),
}

/// Options for specifying which payload to include or not
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum WithPayloadInterface {
    /// If `true` - return all payload,
    /// If `false` - do not return payload
    Bool(bool),
    /// Specify which fields to return
    Fields(Vec<String>),
    /// Specify included or excluded fields
    Selector(PayloadSelector),
}

/// Options for specifying which vector to include
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum WithVector {
    /// If `true` - return all vector,
    /// If `false` - do not return vector
    Bool(bool),
    /// Specify which vector to return
    Selector(Vec<String>),
}

impl WithVector {
    pub fn is_some(&self) -> bool {
        match self {
            WithVector::Bool(b) => *b,
            WithVector::Selector(_) => true,
        }
    }
}

impl From<bool> for WithVector {
    fn from(b: bool) -> Self {
        WithVector::Bool(b)
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

impl From<&WithPayloadInterface> for WithPayload {
    fn from(interface: &WithPayloadInterface) -> Self {
        match interface {
            WithPayloadInterface::Bool(x) => WithPayload {
                enable: *x,
                payload_selector: None,
            },
            WithPayloadInterface::Fields(x) => WithPayload {
                enable: true,
                payload_selector: Some(PayloadSelector::new_include(x.clone())),
            },
            WithPayloadInterface::Selector(x) => WithPayload {
                enable: true,
                payload_selector: Some(x.clone()),
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct PayloadSelectorInclude {
    /// Only include this payload keys
    pub include: Vec<PayloadKeyType>,
}

impl PayloadSelectorInclude {
    pub fn new(include: Vec<PayloadKeyType>) -> Self {
        Self { include }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged)]
#[serde(rename_all = "snake_case")]
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

    #[allow(clippy::ptr_arg)]
    pub fn check(&self, key: &PayloadKeyType) -> bool {
        match self {
            PayloadSelector::Include(selector) => selector.include.contains(key),
            PayloadSelector::Exclude(selector) => !selector.exclude.contains(key),
        }
    }

    pub fn process(&self, x: Payload) -> Payload {
        let map: serde_json::Map<String, Value> =
            x.into_iter().filter(|(key, _)| self.check(key)).collect();
        map.into()
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct WithPayload {
    /// Enable return payloads or not
    pub enable: bool,
    /// Filter include and exclude payloads
    pub payload_selector: Option<PayloadSelector>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub struct Filter {
    /// At least one of those conditions should match
    pub should: Option<Vec<Condition>>,
    /// All conditions must match
    pub must: Option<Vec<Condition>>,
    /// All conditions must NOT match
    pub must_not: Option<Vec<Condition>>,
}

impl Filter {
    pub fn new_should(condition: Condition) -> Self {
        Filter {
            should: Some(vec![condition]),
            must: None,
            must_not: None,
        }
    }

    pub fn new_must(condition: Condition) -> Self {
        Filter {
            should: None,
            must: Some(vec![condition]),
            must_not: None,
        }
    }

    pub fn new_must_not(condition: Condition) -> Self {
        Filter {
            should: None,
            must: None,
            must_not: Some(vec![condition]),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::de::DeserializeOwned;
    use serde_json;
    use serde_json::json;

    use super::*;
    use crate::common::utils::remove_value_from_json_map;

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
        let payload: Payload = json!({"payload_key":"payload_value"}).into();
        let raw = rmp_serde::to_vec(&payload).unwrap();
        let de_record: Payload = serde_cbor::from_slice(&raw).unwrap();
        eprintln!("payload = {payload:#?}");
        eprintln!("de_record = {de_record:#?}");
    }

    #[test]
    fn test_serialize_query() {
        let filter = Filter {
            must: Some(vec![Condition::Field(FieldCondition::new_match(
                "hello".to_owned(),
                "world".to_owned().into(),
            ))]),
            must_not: None,
            should: None,
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
                value: ValueVariants::Keyword("world".to_owned())
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
        println!("{filter:?}");
        let should = filter.should.unwrap();

        assert_eq!(should.len(), 1);
        let c = match should.get(0) {
            Some(Condition::Field(c)) => c,
            _ => panic!("Condition::Field expected"),
        };

        assert_eq!(c.key.as_str(), "Jason");

        let m = match c.r#match.as_ref().unwrap() {
            Match::Any(m) => m,
            _ => panic!("Match::Any expected"),
        };
        if let AnyVariants::Keywords(kws) = &m.any {
            assert_eq!(kws.len(), 3);
            assert_eq!(kws.to_owned(), vec!["Bourne", "Momoa", "Statham"]);
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
                value: ValueVariants::Keyword("world".to_owned())
            })
        );
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
    }

    #[test]
    fn test_remove_key() {
        let mut payload: Payload = serde_json::from_str(
            r#"
        {
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
        }
        "#,
        )
        .unwrap();
        let removed = remove_value_from_json_map("b.c", &mut payload.0).values();
        assert_eq!(removed, vec![Value::Number(123.into())]);
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("b.e.f[1]", &mut payload.0).values();
        assert_eq!(removed, vec![Value::Number(2.into())]);
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("b.e.i[0].j", &mut payload.0).values();
        assert_eq!(removed, vec![Value::Number(1.into())]);
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("b.e.i[].k", &mut payload.0).values();
        assert_eq!(
            removed,
            vec![Value::Number(2.into()), Value::Number(4.into())]
        );
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("b.e.i[]", &mut payload.0).values();
        assert_eq!(
            removed,
            vec![Value::Array(vec![
                Value::Object(serde_json::Map::from_iter(vec![])),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "j".to_string(),
                    Value::Number(3.into())
                ),])),
            ])]
        );
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("b.e.i", &mut payload.0).values();
        assert_eq!(removed, vec![Value::Array(vec![])]);
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("b.e.f", &mut payload.0).values();
        assert_eq!(removed, vec![Value::Array(vec![1.into(), 3.into()])]);
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("k", &mut payload.0);
        assert!(removed.is_empty());
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("", &mut payload.0);
        assert!(removed.is_empty());
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("b.e.l", &mut payload.0);
        assert!(removed.is_empty());
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("a", &mut payload.0).values();
        assert_eq!(removed, vec![Value::Number(1.into())]);
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("b.e", &mut payload.0).values();
        assert_eq!(
            removed,
            vec![Value::Object(serde_json::Map::from_iter(vec![
                // ("f".to_string(), Value::Array(vec![1.into(), 2.into(), 3.into()])), has been removed
                ("g".to_string(), Value::Number(7.into())),
                ("h".to_string(), Value::String("text".to_owned())),
            ]))]
        );
        assert_ne!(payload, Default::default());

        let removed = remove_value_from_json_map("b", &mut payload.0).values();
        assert_eq!(
            removed,
            vec![Value::Object(serde_json::Map::from_iter(vec![]))]
        ); // empty object left
        assert_eq!(payload, Default::default());
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
}

pub type TheMap<K, V> = BTreeMap<K, V>;
