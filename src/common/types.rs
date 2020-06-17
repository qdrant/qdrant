pub type PointIdType = u64;
pub type PayloadKeyType = String;
pub type VectorType = Vec<f64>;
pub type SeqNumberType = u64; 

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GeoPoint {
    lon: f64,
    lat: f64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadType {
    Keyword(String),
    Integer(i64),
    Float(f64),
    Geo(GeoPoint),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Condition {
    Filter(Filter),
    Match {
        key: PayloadKeyType,
        keyword: Option<String>,
        integer: Option<i64>,
    },
    Range {
        key: PayloadKeyType,
        lt: Option<f64>,
        gt: Option<f64>,
        gte: Option<f64>,
        lte: Option<f64>,
    },
    GeoBoundingBox {
        key: PayloadKeyType,
        top_left: GeoPoint,
        bottom_right: GeoPoint,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Filter {
    must: Option<Vec<Condition>>,
    must_not: Option<Vec<Condition>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json;

    #[test]
    fn test_name() {
        let label = PayloadType::Keyword("Hello".to_owned());
        let label_json = serde_json::to_string(&label).unwrap();
        println!("{}", label_json);
    }

    #[test]
    fn test_serialize_query() {
        let filter = Filter {
            must: Some(vec![Condition::Match {
                key: "hello".to_owned(),
                keyword: Some("world".to_owned()),
                integer: None,
            }]),
            must_not: None,
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
        let must_not = filter.must_not;
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
