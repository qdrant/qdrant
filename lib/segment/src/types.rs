use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};
use std::cmp::{Ordering};
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashSet};

pub type PointIdType = u64;
/// Type of point index across all segments
pub type PointOffsetType = usize;
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
pub type FPPayloadType = f64;
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
    /// Segment cheap insert & delete operations
    Plain,
    /// Segment with some sort of index built. Optimized for search, appending new points will require reindexing
    Indexed,
    /// Some index which you better don't touch
    Special,
}


#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct SegmentInfo {
    pub segment_type: SegmentType,
    pub num_vectors: usize,
    pub num_deleted_vectors: usize,
    pub ram_usage_bytes: usize,
    pub disk_usage_bytes: usize,
    pub is_appendable: bool,
}


#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
/// Additional parameters of the search
pub enum SearchParams {
    /// Params relevant to HNSW index
    Hnsw {
        /// Size of the beam in a beam-search. Larger the value - more accurate the result, more time required for search.
        ef: usize
    }
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
    Hnsw {
        /// Number of edges per node in the index graph. Larger the value - more accurate the search, more space required.
        m: usize,
        /// Number of neighbours to consider during the index building. Larger the value - more accurate the search, more time required to build index.
        ef_construct: usize,
    },
}

impl Default for Indexes {
    fn default() -> Self {
        Indexes::Plain {}
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
    /// Type of index used for search
    pub index: Indexes,
    /// Type of distance function used for measuring distance between vectors
    pub distance: Distance,
    /// Type of vector storage
    pub storage_type: StorageType
}

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
    Float(Vec<FPPayloadType>),
    Geo(Vec<GeoPoint>),
}


#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Match {
    /// Name of the field to match with
    pub key: PayloadKeyType,
    /// Keyword value to match
    pub keyword: Option<String>,
    /// Integer value to match
    pub integer: Option<IntPayloadType>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Range {
    /// Name of the field to match with
    pub key: PayloadKeyType,
    /// point.key < range.lt
    pub lt: Option<FPPayloadType>,
    /// point.key > range.gt
    pub gt: Option<FPPayloadType>,
    /// point.key >= range.gte
    pub gte: Option<FPPayloadType>,
    /// point.key <= range.lte
    pub lte: Option<FPPayloadType>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct GeoBoundingBox {
    /// Name of the field to match with
    pub key: PayloadKeyType,
    /// Coordinates of the top left point of the area rectangle
    pub top_left: GeoPoint,
    /// Coordinates of the bottom right point of the area rectangle
    pub bottom_right: GeoPoint,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct GeoRadius {
    /// Name of the field to match with
    pub key: PayloadKeyType,
    /// Coordinates of the top left point of the area rectangle
    pub center: GeoPoint,
    /// Radius of the area in meters
    pub radius: f64,
}


#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Condition {
    /// Nested filter
    Filter(Filter),
    /// Check if point has field with a given value
    Match(Match),
    /// Check if points value lies in a given range
    Range(Range),
    /// Check if points geo location lies in a given area
    GeoBoundingBox(GeoBoundingBox),
    /// Check if geo point is within a given radius
    GeoRadius(GeoRadius),
    /// Check if points id is in a given set
    HasId(HashSet<PointIdType>),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
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
    fn test_name() {
        let label = PayloadType::Keyword(vec!["Hello".to_owned()]);
        let label_json = serde_json::to_string(&label).unwrap();
        println!("{}", label_json);
    }

    #[test]
    fn test_serialize_query() {
        let filter = Filter {
            must: Some(vec![Condition::Match(Match {
                key: "hello".to_owned(),
                keyword: Some("world".to_owned()),
                integer: None,
            })]),
            must_not: None,
            should: None,
        };
        let json = serde_json::to_string_pretty(&filter).unwrap();
        println!("{}", json)
    }

    #[test]
    fn test_payload_query_parse() {
        let query1 = r#"
        {
            "must":[
               {
                  "match":{
                     "key":"hello",
                     "integer":42
                  }
               },
               {
                   "filter": {
                       "must_not": [
                           {
                                "has_id": [1, 2, 3, 4, 5, 6]
                           },
                           {
                                "geo_bounding_box": {
                                    "key": "geo_field",
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

