use ordered_float::OrderedFloat;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Formatter;
use std::str::FromStr;
use uuid::Uuid;

/// Type of point index inside a segment
pub type PointOffsetType = u32;
pub type PayloadKeyType = String;
pub type PayloadKeyTypeRef<'a> = &'a str;
pub type SeqNumberType = u64;
/// Sequential number of modification, applied to segment
pub type ScoreType = f32;
/// Type of vector matching score
pub type TagType = u64;
/// Type of vector element.
pub type VectorElementType = f32;
/// Type of float point payload
pub type FloatPayloadType = f64;
/// Type of integer point payload
pub type IntPayloadType = i64;

/// Type, used for specifying point ID in user interface
#[derive(
    Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, JsonSchema,
)]
#[serde(untagged)]
pub enum ExtendedPointId {
    NumId(u64),
    Uuid(Uuid),
}

impl std::fmt::Display for ExtendedPointId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtendedPointId::NumId(idx) => write!(f, "{}", idx),
            ExtendedPointId::Uuid(uuid) => write!(f, "{}", uuid),
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

/// Type of point index across all segments
pub type PointIdType = ExtendedPointId;

/// Type of internal tags, build from payload
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, FromPrimitive)]
/// Distance function types used to compare vectors
pub enum Distance {
    /// https://en.wikipedia.org/wiki/Cosine_similarity
    Cosine,
    /// https://en.wikipedia.org/wiki/Euclidean_distance
    Euclid,
    /// https://en.wikipedia.org/wiki/Dot_product
    Dot,
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
    pub vector: Option<Vec<VectorElementType>>,
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
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SegmentType {
    /// There are no index built for the segment, all operations are available
    Plain,
    /// Segment with some sort of index built. Optimized for search, appending new points will require reindexing
    Indexed,
    /// Some index which you better don't touch
    Special,
}

/// Payload field type & index information
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct PayloadSchemaInfo {
    pub data_type: PayloadSchemaType,
    pub indexed: bool,
}

/// Aggregated information about segment
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct SegmentInfo {
    pub segment_type: SegmentType,
    pub num_vectors: usize,
    pub num_deleted_vectors: usize,
    pub ram_usage_bytes: usize,
    pub disk_usage_bytes: usize,
    pub is_appendable: bool,
    pub schema: HashMap<PayloadKeyType, PayloadSchemaInfo>,
}

/// Additional parameters of the search
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct SearchParams {
    /// Params relevant to HNSW index
    /// /// Size of the beam in a beam-search. Larger the value - more accurate the result, more time required for search.
    pub hnsw_ef: Option<usize>,
}

/// This function only stores mapping between distance and preferred result order
pub fn distance_order(distance: &Distance) -> Order {
    match distance {
        Distance::Cosine | Distance::Dot => Order::LargeBetter,
        Distance::Euclid => Order::SmallBetter,
    }
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
    /// Minimal amount of points for additional payload-based indexing.
    /// If payload chunk is smaller than `full_scan_threshold` additional indexing won't be used -
    /// in this case full-scan search should be preferred by query planner and additional indexing is not required.
    pub full_scan_threshold: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        HnswConfig {
            m: 16,
            ef_construct: 100,
            full_scan_threshold: DEFAULT_FULL_SCAN_THRESHOLD,
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
#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "options")]
pub enum PayloadIndexType {
    /// Do not index anything, just keep of what should be indexed later
    Plain,
    /// Build payload index. Index is saved on disc, but index itself is in RAM
    Struct,
}

impl Default for PayloadIndexType {
    fn default() -> Self {
        PayloadIndexType::Plain
    }
}

/// Type of vector storage
#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "options")]
pub enum StorageType {
    /// Store vectors in memory and use persistence storage only if vectors are changed
    InMemory,
    /// Use memmap to store vectors, a little slower than `InMemory`, but requires little RAM
    Mmap,
}

impl Default for StorageType {
    fn default() -> Self {
        StorageType::InMemory
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SegmentConfig {
    /// Size of a vectors used
    pub vector_size: usize,
    /// Type of distance function used for measuring distance between vectors
    pub distance: Distance,
    /// Type of index used for search
    pub index: Indexes,
    /// Payload Indexes
    pub payload_index: Option<PayloadIndexType>,
    /// Type of vector storage
    pub storage_type: StorageType,
}

/// Default value based on <https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md>
pub const DEFAULT_FULL_SCAN_THRESHOLD: usize = 20_000;

/// Persistable state of segment configuration
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
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

pub struct GeoPointValidationError;

// The error type has to implement Display
impl std::fmt::Display for GeoPointValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Wrong format of GeoPoint payload: expected `lat` within [-90;90] and `lon` within [-180;180]")
    }
}

impl TryFrom<GeoPointShadow> for GeoPoint {
    type Error = GeoPointValidationError;

    fn try_from(value: GeoPointShadow) -> Result<Self, Self::Error> {
        let max_lat = 90f64;
        let min_lat = -90f64;
        let max_lon = 180f64;
        let min_lon = -180f64;

        if !(min_lon..=max_lon).contains(&value.lon) || !(min_lat..=max_lat).contains(&value.lat) {
            return Err(GeoPointValidationError);
        }

        Ok(Self {
            lon: value.lon,
            lat: value.lat,
        })
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
pub struct Payload(Value);

impl Payload {
    pub fn merge(&mut self, value: &Payload) {
        json_patch::merge(&mut self.0, &value.0)
    }

    pub fn get(&self, path: &str) -> Option<PayloadType> {
        let path_elements: Vec<&str> = path.split('.').collect();
        let mut payload_type: Option<PayloadType> = None;
        get_payload_type(&path_elements, 0, &self.0, &mut payload_type);
        payload_type
    }

    pub fn get_schema_type(&self, path: &str) -> PayloadSchemaType {
        match self.get(path) {
            Some(payload) => match payload {
                PayloadType::Keyword(_) => PayloadSchemaType::Keyword,
                PayloadType::Integer(_) => PayloadSchemaType::Integer,
                PayloadType::Float(_) => PayloadSchemaType::Float,
                PayloadType::Geo(_) => PayloadSchemaType::Geo,
                PayloadType::Unknown => PayloadSchemaType::Unknown,
            },
            None => PayloadSchemaType::Unknown,
        }
    }

    pub fn remove(&mut self, path: &str) -> Option<Value> {
        self.0.as_object_mut().unwrap().remove(path)
    }

    pub fn len(&self) -> usize {
        self.0.as_object().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.as_object().unwrap().is_empty()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.as_object().unwrap().contains_key(key)
    }
}

impl Default for Payload {
    fn default() -> Self {
        Payload(Value::Object(Map::new()))
    }
}

impl IntoIterator for Payload {
    type Item = (String, Value);
    type IntoIter = serde_json::map::IntoIter;

    fn into_iter(self) -> serde_json::map::IntoIter {
        match self.0 {
            Value::Object(map) => map.into_iter(),
            _ => panic!(),
        }
    }
}

impl From<Value> for Payload {
    fn from(value: Value) -> Self {
        match value {
            Value::Object(_) => Payload(value),
            _ => panic!("cannot convert from {:?}", value),
        }
    }
}

impl From<serde_json::Map<String, Value>> for Payload {
    fn from(value: serde_json::Map<String, Value>) -> Self {
        Payload(Value::Object(value))
    }
}

fn get_payload_type(
    path_elements: &[&str],
    pos: usize,
    value: &Value,
    payload_type: &mut Option<PayloadType>,
) {
    if let Some(PayloadType::Unknown) = payload_type {
        return;
    }

    let path_element = path_elements.get(pos);

    match path_element {
        Some(property_name) => match value {
            Value::Object(map) => match map.get(*property_name) {
                Some(value) => {
                    get_payload_type(path_elements, pos + 1, value, payload_type);
                }
                None => *payload_type = None,
            },
            Value::Array(values) => {
                for v in values {
                    get_payload_type(path_elements, pos, v, payload_type)
                }
            }
            _ => *payload_type = Some(PayloadType::Unknown),
        },
        None => match value {
            Value::Number(num) => {
                if let Some(n) = num.as_i64() {
                    match payload_type {
                        Some(PayloadType::Integer(ref mut vec)) => {
                            vec.push(n);
                        }
                        None => {
                            *payload_type = Some(PayloadType::Integer(vec![n]));
                        }
                        _ => *payload_type = Some(PayloadType::Unknown),
                    }
                    return;
                }

                if let Some(n) = num.as_f64() {
                    match payload_type {
                        Some(PayloadType::Float(ref mut vec)) => {
                            vec.push(n);
                        }
                        None => {
                            *payload_type = Some(PayloadType::Float(vec![n]));
                        }
                        _ => *payload_type = Some(PayloadType::Unknown),
                    }
                    return;
                }

                *payload_type = Some(PayloadType::Unknown);
            }

            Value::String(s) => match payload_type {
                Some(PayloadType::Keyword(ref mut vec)) => {
                    vec.push(s.to_owned());
                }
                None => {
                    *payload_type = Some(PayloadType::Keyword(vec![s.to_owned()]));
                }
                _ => *payload_type = Some(PayloadType::Unknown),
            },

            Value::Array(values) => {
                for v in values {
                    get_payload_type(path_elements, pos, v, payload_type)
                }
            }
            Value::Object(map) => match try_build_geo(map) {
                Some(geo_point) => match payload_type {
                    Some(PayloadType::Geo(ref mut vec)) => {
                        vec.push(geo_point);
                    }
                    None => {
                        *payload_type = Some(PayloadType::Geo(vec![geo_point]));
                    }
                    _ => *payload_type = Some(PayloadType::Unknown),
                },
                None => *payload_type = Some(PayloadType::Unknown),
            },
            _ => *payload_type = Some(PayloadType::Unknown),
        },
    };
}

fn try_build_geo(map: &serde_json::Map<String, Value>) -> Option<GeoPoint> {
    if map.len() != 2 {
        return None;
    };

    let lon = get_f64(map, "lon");
    let lat = get_f64(map, "lat");

    match (lon, lat) {
        (Some(lon), Some(lat)) => Some(GeoPoint { lon, lat }),
        _ => None,
    }
}

fn get_f64(map: &serde_json::Map<String, Value>, key: &str) -> Option<f64> {
    match map.get(key) {
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

/// All possible payload types
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
pub enum PayloadType {
    Keyword(Vec<String>),
    Integer(Vec<IntPayloadType>),
    Float(Vec<FloatPayloadType>),
    Geo(Vec<GeoPoint>),
    Unknown,
}

/// All possible names of payload types
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
pub enum PayloadSchemaType {
    Keyword,
    Integer,
    Float,
    Geo,
    Unknown,
}

/// Match by keyword
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct MatchKeyword {
    /// Keyword value to match
    pub keyword: String,
}

/// Match filter request
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct MatchInteger {
    /// Integer value to match
    pub integer: IntPayloadType,
}

/// Match filter request
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum Match {
    Keyword(MatchKeyword),
    Integer(MatchInteger),
}

impl From<String> for Match {
    fn from(keyword: String) -> Self {
        Self::Keyword(MatchKeyword { keyword })
    }
}

impl From<IntPayloadType> for Match {
    fn from(integer: IntPayloadType) -> Self {
        Self::Integer(MatchInteger { integer })
    }
}

/// Range filter request
#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq)]
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

/// All possible payload filtering conditions
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct FieldCondition {
    pub key: PayloadKeyType,
    /// Check if point has field with a given value
    pub r#match: Option<Match>,
    /// Check if points value lies in a given range
    pub range: Option<Range>,
    /// Check if points geo location lies in a given area
    pub geo_bounding_box: Option<GeoBoundingBox>,
    /// Check if geo point is within a given radius
    pub geo_radius: Option<GeoRadius>,
}

/// ID-based filtering condition
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
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
pub enum Condition {
    /// Check if field satisfies provided condition
    Field(FieldCondition),
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
    Bool(bool),
    Fields(Vec<String>),
    Selector(PayloadSelector),
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
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
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum PayloadSelector {
    Include(PayloadSelectorInclude),
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
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
    /// At least one of thous conditions should match
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
    use super::*;

    use serde::de::DeserializeOwned;
    use serde_json;
    use serde_json::json;

    #[test]
    fn test_value_parse() {
        // strict no longer supported.
        //
        // let geo_query_strict =
        //     r#"{ "geo_property" : {"type": "geo", "value": {"lon": 1.0, "lat": 1.0}}}"#;
        // let payload: Payload = serde_json::from_str(geo_query_strict).unwrap();
        // let payload_geo_query = payload.get("geo_property").unwrap();
        // match &payload_geo_query {
        //     PayloadType::Geo(x) => {
        //         assert_eq!(x.len(), 1);
        //         assert_eq!(x[0].lat, 1.0);
        //         assert_eq!(x[0].lon, 1.0);
        //     }
        //     _ => panic!(),
        // }

        let keyword_query_non_strict = r#"{"keyword":["Berlin", "Barcelona", "Moscow"]}"#;
        let payload: Payload = serde_json::from_str(keyword_query_non_strict).unwrap();
        let payload_keyword_query_non_strict = payload.get("keyword").unwrap();
        match &payload_keyword_query_non_strict {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 3);
                assert_eq!(x[0], "Berlin");
                assert_eq!(x[1], "Barcelona");
                assert_eq!(x[2], "Moscow");
            }
            _ => panic!(),
        }

        // strict no longer supported.
        //
        // let keyword_query_strict = r#"{"keyword_strict_prop":"type": "keyword", "value": ["Berlin", "Barcelona", "Moscow"]}}"#;
        // let payload: Payload = serde_json::from_str(keyword_query_strict).unwrap();
        // let payload_keyword_query_strict: PayloadType = payload.get("keyword_strict_prop").unwrap();
        //
        // match &payload_keyword_query_strict {
        //     PayloadType::Keyword(x) => {
        //         assert_eq!(x.len(), 3);
        //         assert_eq!(x[0], "Berlin");
        //         assert_eq!(x[1], "Barcelona");
        //         assert_eq!(x[2], "Moscow");
        //     }
        //     _ => panic!(),
        // }

        let integer_query_non_strict = r#"{"integer_query_non_strict":[1, 2, 3]}"#;
        let payload: Payload = serde_json::from_str(integer_query_non_strict).unwrap();
        let payload_integer_query_non_strict = payload.get("integer_query_non_strict").unwrap();
        match &payload_integer_query_non_strict {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 3);
                assert_eq!(x[0], 1);
                assert_eq!(x[1], 2);
                assert_eq!(x[2], 3);
            }
            _ => panic!(),
        }

        // strict no longer supported.
        //
        // let integer_query_strict =
        //     r#"{"integer_query_strict":{"type": "integer", "value": [1, 2, 3]}}"#;
        // let payload: Payload = serde_json::from_str(integer_query_strict).unwrap();
        // let payload_integer_query_strict = payload.get("integer_query_strict").unwrap();
        // match &payload_integer_query_strict {
        //     PayloadType::Integer(x) => {
        //         assert_eq!(x.len(), 3);
        //         assert_eq!(x[0], 1);
        //         assert_eq!(x[1], 2);
        //         assert_eq!(x[2], 3);
        //     }
        //     _ => panic!(),
        // }

        let float_query_non_strict = r#"{"float_query_non_strict":[1.0, 2.0, 3.0]}"#;
        let payload: Payload = serde_json::from_str(float_query_non_strict).unwrap();
        let payload_float_query_non_strict = payload.get("float_query_non_strict").unwrap();
        match &payload_float_query_non_strict {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 3);
                assert_eq!(x[0], 1.0);
                assert_eq!(x[1], 2.0);
                assert_eq!(x[2], 3.0);
            }
            _ => panic!(),
        }

        // strict no longer supported.
        //
        // let float_query_strict =
        //     r#"{"float_query_strict":{"type": "float", "value": [1.0, 2.0, 3.0]}}"#;
        // let payload: Payload = serde_json::from_str(float_query_strict).unwrap();
        // let payload_float_query_strict = payload.get("float_query_strict").unwrap();
        // match &payload_float_query_strict {
        //     PayloadType::Float(x) => {
        //         assert_eq!(x.len(), 3);
        //         assert_eq!(x[0], 1.0);
        //         assert_eq!(x[1], 2.0);
        //         assert_eq!(x[2], 3.0);
        //     }
        //     _ => panic!(),
        // }
    }

    #[allow(dead_code)]
    fn check_rms_serialization<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(
        record: T,
    ) {
        let binary_entity = rmp_serde::to_vec(&record).expect("serialization ok");
        let de_record: T = rmp_serde::from_slice(&binary_entity).expect("deserialization ok");

        assert_eq!(record, de_record);
    }

    fn check_cbor_serialization<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(
        record: T,
    ) {
        let binary_entity = serde_cbor::to_vec(&record).expect("serialization ok");
        let de_record: T = serde_cbor::from_slice(&binary_entity).expect("deserialization ok");

        assert_eq!(record, de_record);
    }

    fn check_json_serialization<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(
        record: T,
    ) {
        let binary_entity = serde_json::to_vec(&record).expect("serialization ok");
        let de_record: T = serde_json::from_slice(&binary_entity).expect("deserialization ok");

        assert_eq!(record, de_record);
    }

    // strict serde tested on `test_value_parse`
    // #[test]
    // fn test_strict_deserialize() {
    //     let de_record: PayloadInterface =
    //         serde_json::from_str(r#"[1, 2]"#).expect("deserialization ok");
    //     eprintln!("de_record = {:#?}", de_record);
    // }

    #[test]
    #[ignore]
    fn test_rmp_vs_cbor_deserialize() {
        let payload: Payload = json!({"payload_key":"payload_value"}).into();
        let raw = rmp_serde::to_vec(&payload).unwrap();
        let de_record: Payload = serde_cbor::from_slice(&raw).unwrap();
        eprintln!("payload = {:#?}", payload);
        eprintln!("de_record = {:#?}", de_record);
    }

    // ToDo: Check serialization of UUID here later
    // #[test]
    // fn test_long_id_deserialization() {
    //     let query1 = r#"
    //     {
    //         "has_id": [7730993719707444524137094407]
    //     }"#;
    //
    //     let de_record: Condition = serde_json::from_str(query1).expect("deserialization ok");
    //     eprintln!("de_record = {:#?}", de_record);
    //
    //     let query2 = HasIdCondition {
    //         has_id: HashSet::from_iter(vec![7730993719707444524137094407].iter().cloned()),
    //     };
    //
    //     let json = serde_json::to_string(&query2).expect("serialization ok");
    //
    //     eprintln!("json = {:#?}", json);
    // }
    //
    // #[test]
    // fn test_long_ids_serialization() {
    //     let operation = Filter {
    //         should: None,
    //         must: Some(vec![Condition::HasId(HasIdCondition {
    //             has_id: HashSet::from_iter(vec![7730993719707444524137094407].iter().cloned()),
    //         })]),
    //         must_not: None,
    //     };
    //     check_json_serialization(operation.clone());
    //     check_cbor_serialization(operation);
    // }

    // `PayloadInterfaceStrict` not longer used in payload structs
    //
    // #[test]
    // fn test_rms_serialization() {
    //     let payload = PayloadInterface::Payload(PayloadInterfaceStrict::Keyword(
    //         PayloadVariant::Value("val".to_string()),
    //     ));
    //     check_cbor_serialization(payload.clone());
    //     check_json_serialization(payload);
    //
    //     let payload = PayloadVariant::Value("val".to_string());
    //     check_cbor_serialization(payload.clone());
    //     check_json_serialization(payload);
    //
    //     let payload = PayloadVariant::Value(1.22);
    //     check_cbor_serialization(payload.clone());
    //     check_json_serialization(payload);
    //
    //     let payload = PayloadVariant::Value(1.);
    //     check_cbor_serialization(payload.clone());
    //     check_json_serialization(payload);
    //
    //     let payload = PayloadVariant::Value(1);
    //     check_cbor_serialization(payload.clone());
    //     check_json_serialization(payload);
    //
    //     let payload = PayloadVariant::List(vec!["val".to_string(), "val2".to_string()]);
    //     check_cbor_serialization(payload.clone());
    //     check_json_serialization(payload);
    //
    //     let payload =
    //         PayloadInterface::Payload(PayloadInterfaceStrict::Integer(PayloadVariant::List(vec![
    //             1, 2,
    //         ])));
    //     check_cbor_serialization(payload.clone());
    //     check_json_serialization(payload);
    //
    //     let payload = PayloadVariant::List(vec![1, 2]);
    //     check_cbor_serialization(payload.clone());
    //     check_json_serialization(payload);
    //
    //     let payload = PayloadInterface::IntShortcut(PayloadVariant::List(vec![1, 2]));
    //     check_cbor_serialization(payload.clone());
    //     check_json_serialization(payload);
    //
    //     let payload = PayloadInterface::KeywordShortcut(PayloadVariant::Value("val".to_string()));
    //     check_cbor_serialization(payload.clone());
    //     check_json_serialization(payload);
    //
    //     let payload = PayloadInterface::KeywordShortcut(PayloadVariant::List(vec![
    //         "val".to_string(),
    //         "val2".to_string(),
    //     ]));
    //     check_cbor_serialization(payload.clone());
    //     check_json_serialization(payload);
    // }

    #[test]
    fn test_name() {
        let label = PayloadType::Keyword(vec!["Hello".to_owned()]);
        let label_json = serde_json::to_string(&label).unwrap();
        println!("{}", label_json);
    }

    #[test]
    fn test_serialize_query() {
        let filter = Filter {
            must: Some(vec![Condition::Field(FieldCondition {
                key: "hello".to_owned(),
                r#match: Some("world".to_owned().into()),
                range: None,
                geo_bounding_box: None,
                geo_radius: None,
            })]),
            must_not: None,
            should: None,
        };
        let json = serde_json::to_string_pretty(&filter).unwrap();
        println!("{}", json)
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
    fn test_payload_query_parse() {
        let query1 = r#"
        {
            "must": [
                {
                    "key": "hello",
                    "match": {
                        "integer": 42
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
        println!("{:?}", filter);
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
    fn test_payload_extraction() {
        let payload_json = r#"
            {
              "key_int_1": {
                "nested": [
                  {"a": 1},
                  {"a": 2}
                ]
              },
              "key_int_2": [3,4,5],
              "key_int_3": 6,
              "key_float_1": [0.43,1.5,0.38],
              "key_float_2": 0.17,
              "key_keyword1": ["a","b","c"],
              "key_keyword2": [{"k": "k1"},{"k": "k2"}],
              "key_keyword3": "d",
              "key_unknown1": ["a","b",1],
              "key_geo1": {"lat":52.519134783833024,"lon":13.372463822170474},
              "key_geo2": [
                {"lat":52.519134783833024,"lon":13.372463822170474},
                {"lat":52.52966189901996,"lon":13.357482978515312}
              ]
            }
            "#;

        let payload_value: Value = serde_json::from_str(payload_json).unwrap();

        let payload: Payload = payload_value.into();

        assert_eq!(
            payload.get("key_int_1.nested.a").unwrap(),
            PayloadType::Integer(vec![1, 2])
        );
        assert_eq!(
            payload.get("key_int_2").unwrap(),
            PayloadType::Integer(vec![3, 4, 5])
        );
        assert_eq!(
            payload.get("key_int_3").unwrap(),
            PayloadType::Integer(vec![6])
        );
        assert_eq!(
            payload.get("key_float_1").unwrap(),
            PayloadType::Float(vec![0.43, 1.5, 0.38])
        );
        assert_eq!(
            payload.get("key_float_2").unwrap(),
            PayloadType::Float(vec![0.17])
        );
        assert_eq!(
            payload.get("key_keyword1").unwrap(),
            PayloadType::Keyword(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
        assert_eq!(
            payload.get("key_keyword2.k").unwrap(),
            PayloadType::Keyword(vec!["k1".to_string(), "k2".to_string()])
        );
        assert_eq!(payload.get("key_keyword2").unwrap(), PayloadType::Unknown);
        assert_eq!(
            payload.get("key_keyword3").unwrap(),
            PayloadType::Keyword(vec!["d".to_string()])
        );
        assert_eq!(payload.get("key_unknown1").unwrap(), PayloadType::Unknown);
        assert_eq!(
            payload.get("key_geo1").unwrap(),
            PayloadType::Geo(vec![GeoPoint {
                lat: 52.519134783833024,
                lon: 13.372463822170474
            }])
        );
        assert_eq!(
            payload.get("key_geo2").unwrap(),
            PayloadType::Geo(vec![
                GeoPoint {
                    lat: 52.519134783833024,
                    lon: 13.372463822170474
                },
                GeoPoint {
                    lat: 52.52966189901996,
                    lon: 13.357482978515312
                },
            ])
        );
    }
}

pub type TheMap<K, V> = BTreeMap<K, V>;
