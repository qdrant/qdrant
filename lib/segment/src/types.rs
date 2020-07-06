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
/// Type of vector element. Note: used in interface only, storage vector type is NOT specified here
pub type VectorElementType = f64;

/// Type of internal tags, build from payload

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum Distance {
    Cosine,
    Euclid,
    Dot
}


#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct GeoPoint {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type",  content = "value")]
pub enum PayloadType {
    Keyword(Vec<String>),
    Integer(Vec<i64>),
    Float(Vec<f64>),
    Geo(Vec<GeoPoint>),
}


#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Match {
    pub key: PayloadKeyType,
    pub keyword: Option<String>,
    pub integer: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Range {
    pub key: PayloadKeyType,
    pub lt: Option<f64>,
    pub gt: Option<f64>,
    pub gte: Option<f64>,
    pub lte: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct GeoBoundingBox {
    pub key: PayloadKeyType,
    pub top_left: GeoPoint,
    pub bottom_right: GeoPoint,
}


#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Condition {
    Filter(Filter),
    Match(Match),
    Range(Range),
    GeoBoundingBox(GeoBoundingBox),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Filter {
    pub should: Option<Vec<Condition>>,
    pub must: Option<Vec<Condition>>,
    pub must_not: Option<Vec<Condition>>,
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
            should: None
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
                    Some(v) => assert_eq!(v.len(), 1),
                    None => assert!(false, "Filter expected"),
                }
            }
            _ => assert!(false, "Condition expected"),
        }
    }
}
