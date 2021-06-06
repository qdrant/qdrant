use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};
use std::cmp::{Ordering};
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashSet, HashMap};

pub type PointIdType = u64;
/// Type of point index across all segments
pub type PointOffsetType = u32;
/// Type of point index inside a segment
pub type PayloadKeyType = String;
pub type SeqNumberType = u64;
/// Sequential number of modification, applied to segemnt
pub type ScoreType = f32;
/// Type of vector matching score
pub type TagType = u64;
/// Type of vector element.
pub type VectorElementType = f32;
/// Type of float point payload
pub type FloatPayloadType = f64;
/// Type of integer point payload
pub type IntPayloadType = i64;

/// Type of internal tags, build from payload
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
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

#[derive(Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Debug)]
pub struct ScoredPoint {
    /// Point id
    pub id: PointIdType,
    /// Points vector distance to the query vector
    pub score: ScoreType,
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SegmentType {
    /// There are no index built for the segment
    Plain,
    /// Segment with some sort of index built. Optimized for search, appending new points will require reindexing
    Indexed,
    /// Some index which you better don't touch
    Special,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct PayloadSchemaInfo {
    pub data_type: PayloadSchemaType,
    pub indexed: bool,
}

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


#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
/// Additional parameters of the search
pub struct SearchParams {
    /// Params relevant to HNSW index
    /// /// Size of the beam in a beam-search. Larger the value - more accurate the result, more time required for search.
    pub hnsw_ef: Option<usize>
}

/// This function only stores mapping between distance and preferred result order
pub fn distance_order(distance: &Distance) -> Order {
    match distance {
        Distance::Cosine => Order::LargeBetter,
        Distance::Euclid => Order::SmallBetter,
        Distance::Dot => Order::LargeBetter,
    }
}

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
    pub full_scan_threshold: usize
}

impl Default for HnswConfig {
    fn default() -> Self { HnswConfig { m: 16, ef_construct: 100, full_scan_threshold: DEFAULT_FULL_SCAN_THRESHOLD }}
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "options")]
/// Type of payload index
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "options")]
/// Type of vector storage
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

/// Default value based on https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md
pub const DEFAULT_FULL_SCAN_THRESHOLD: usize = 20_000;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SegmentState {
    pub version: SeqNumberType,
    pub config: SegmentConfig,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct GeoPoint {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
pub enum PayloadType {
    Keyword(Vec<String>),
    Integer(Vec<IntPayloadType>),
    Float(Vec<FloatPayloadType>),
    Geo(Vec<GeoPoint>),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
pub enum PayloadSchemaType {
    Keyword,
    Integer,
    Float,
    Geo,
}

impl From<&PayloadType> for PayloadSchemaType {
    fn from(payload_type: &PayloadType) -> Self {
        match payload_type {
            PayloadType::Keyword(_) => PayloadSchemaType::Keyword,
            PayloadType::Integer(_) => PayloadSchemaType::Integer,
            PayloadType::Float(_) => PayloadSchemaType::Float,
            PayloadType::Geo(_) => PayloadSchemaType::Geo,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum PayloadVariant<T> {
    Value(T),
    List(Vec<T>)
}

impl<T: Clone> PayloadVariant<T> {
    pub fn to_list(&self) -> Vec<T>{
        match self {
            PayloadVariant::Value(x) => vec![x.clone()],
            PayloadVariant::List(vec) => vec.clone(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum PayloadInterface {
    KeywordShortcut(PayloadVariant<String>),
    IntShortcut(PayloadVariant<i64>),
    FloatShortcut(PayloadVariant<f64>),
    Payload(PayloadInterfaceStrict),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type",  content = "value")]
pub enum PayloadInterfaceStrict {
    Keyword(PayloadVariant<String>),
    Integer(PayloadVariant<i64>),
    Float(PayloadVariant<f64>),
    Geo(PayloadVariant<GeoPoint>),
}

// For tests
impl From<PayloadInterfaceStrict> for PayloadInterface {
    fn from(x: PayloadInterfaceStrict) -> Self {
        PayloadInterface::Payload(x)
    }
}

impl From<&PayloadInterfaceStrict> for PayloadType {
    fn from(interface: &PayloadInterfaceStrict) -> Self {
        match interface {
            PayloadInterfaceStrict::Keyword(x) => PayloadType::Keyword(x.to_list()),
            PayloadInterfaceStrict::Integer(x) => PayloadType::Integer(x.to_list()),
            PayloadInterfaceStrict::Float(x) =>  PayloadType::Float(x.to_list()),
            PayloadInterfaceStrict::Geo(x) => PayloadType::Geo(x.to_list()),
        }
    }
}

impl From<&PayloadInterface> for PayloadType {
    fn from(interface: &PayloadInterface) -> Self {
        match interface {
            PayloadInterface::Payload(x) => x.into(),
            PayloadInterface::KeywordShortcut(x) => PayloadType::Keyword(x.to_list()),
            PayloadInterface::FloatShortcut(x) => PayloadType::Float(x.to_list()),
            PayloadInterface::IntShortcut(x) => PayloadType::Integer(x.to_list()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Match {
    /// Keyword value to match
    pub keyword: Option<String>,
    /// Integer value to match
    pub integer: Option<IntPayloadType>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct GeoBoundingBox {
    /// Coordinates of the top left point of the area rectangle
    pub top_left: GeoPoint,
    /// Coordinates of the bottom right point of the area rectangle
    pub bottom_right: GeoPoint,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct GeoRadius {
    /// Coordinates of the top left point of the area rectangle
    pub center: GeoPoint,
    /// Radius of the area in meters
    pub radius: f64,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct HasIdCondition {
    pub has_id: HashSet<PointIdType>
}

impl Into<HasIdCondition> for HashSet<PointIdType> {
    fn into(self) -> HasIdCondition {
        HasIdCondition {
            has_id: self
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub enum Condition {
    /// Check if field satisfies provided condition
    Field(FieldCondition),
    /// Check if points id is in a given set
    HasId(HasIdCondition),
    /// Nested filter
    Filter(Filter),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
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

    use serde_json;

    #[test]
    fn test_value_parse() {

        let geo_query_strict = r#"{"type": "geo", "value": {"lon": 1.0, "lat": 1.0}}"#;
        let val_geo_query_strict: serde_json::value::Value = serde_json::from_str(geo_query_strict).unwrap();
        let payload_interface_geo_query: PayloadInterface = serde_json::from_value(val_geo_query_strict).unwrap();
        let payload_geo_query: PayloadType = (&payload_interface_geo_query).into();
        match &payload_geo_query {
            PayloadType::Geo(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0].lat, 1.0);
                assert_eq!(x[0].lon, 1.0);
            },
            _ => assert!(false)
        }

        let keyword_query_non_strict = r#"["Berlin", "Barcelona", "Moscow"]"#;
        let val_keyword_query_non_strict: serde_json::value::Value = serde_json::from_str(keyword_query_non_strict).unwrap();
        let payload_interface_keyword_query_non_strict: PayloadInterface = serde_json::from_value(val_keyword_query_non_strict).unwrap();
        let payload_keyword_query_non_strict: PayloadType = (&payload_interface_keyword_query_non_strict).into();
        match &payload_keyword_query_non_strict {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 3);
                assert_eq!(x[0], "Berlin");
                assert_eq!(x[1], "Barcelona");
                assert_eq!(x[2], "Moscow");
            },
            _ => assert!(false)
        }

        let keyword_query_strict = r#"{"type": "keyword", "value": ["Berlin", "Barcelona", "Moscow"]}"#;
        let val_keyword_query_strict: serde_json::value::Value = serde_json::from_str(keyword_query_strict).unwrap();
        let payload_interface_keyword_query_strict: PayloadInterface = serde_json::from_value(val_keyword_query_strict).unwrap();
        let payload_keyword_query_strict: PayloadType = (&payload_interface_keyword_query_strict).into();
        match &payload_keyword_query_strict {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 3);
                assert_eq!(x[0], "Berlin");
                assert_eq!(x[1], "Barcelona");
                assert_eq!(x[2], "Moscow");
            },
            _ => assert!(false)
        }

        let integer_query_non_strict = r#"[1, 2, 3]"#;
        let val_integer_query_non_strict: serde_json::value::Value = serde_json::from_str(integer_query_non_strict).unwrap();
        let payload_interface_integer_query_non_strict: PayloadInterface = serde_json::from_value(val_integer_query_non_strict).unwrap();
        let payload_integer_query_non_strict: PayloadType = (&payload_interface_integer_query_non_strict).into();
        match &payload_integer_query_non_strict {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 3);
                assert_eq!(x[0], 1);
                assert_eq!(x[1], 2);
                assert_eq!(x[2], 3);
            },
            _ => assert!(false)
        }

        let integer_query_strict = r#"{"type": "integer", "value": [1, 2, 3]}"#;
        let val_integer_query_strict: serde_json::value::Value = serde_json::from_str(integer_query_strict).unwrap();
        let payload_interface_integer_query_strict: PayloadInterface = serde_json::from_value(val_integer_query_strict).unwrap();
        let payload_integer_query_strict: PayloadType = (&payload_interface_integer_query_strict).into();
        match &payload_integer_query_strict {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 3);
                assert_eq!(x[0], 1);
                assert_eq!(x[1], 2);
                assert_eq!(x[2], 3);
            },
            _ => assert!(false)
        }

        let float_query_non_strict = r#"[1.0, 2.0, 3.0]"#;
        let val_float_query_non_strict: serde_json::value::Value = serde_json::from_str(float_query_non_strict).unwrap();
        let payload_interface_float_query_non_strict: PayloadInterface = serde_json::from_value(val_float_query_non_strict).unwrap();
        let payload_float_query_non_strict: PayloadType = (&payload_interface_float_query_non_strict).into();
        match &payload_float_query_non_strict {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 3);
                assert_eq!(x[0], 1.0);
                assert_eq!(x[1], 2.0);
                assert_eq!(x[2], 3.0);
            },
            _ => assert!(false)
        }

        let float_query_strict = r#"{"type": "float", "value": [1.0, 2.0, 3.0]}"#;
        let val_float_query_strict: serde_json::value::Value = serde_json::from_str(float_query_strict).unwrap();
        let payload_interface_float_query_strict: PayloadInterface = serde_json::from_value(val_float_query_strict).unwrap();
        let payload_float_query_strict: PayloadType = (&payload_interface_float_query_strict).into();
        match &payload_float_query_strict {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 3);
                assert_eq!(x[0], 1.0);
                assert_eq!(x[1], 2.0);
                assert_eq!(x[2], 3.0);
            },
            _ => assert!(false)
        }
    }

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
                r#match: Some(Match {
                    keyword: Some("world".to_owned()),
                    integer: None,
                }),
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
                    None => assert!(false, "Filter expected"),
                }
            }
            _ => assert!(false, "Condition expected"),
        }
    }
}

pub type TheMap<K, V> = BTreeMap<K, V>;

