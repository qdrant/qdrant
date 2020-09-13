

use serde;
use serde::{Deserialize, Serialize};
use segment::types::{PointIdType, PayloadKeyType, PayloadType, GeoPoint};
use std::collections::HashMap;


#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type",  content = "value")]
pub enum PayloadInterface {
    Keyword(PayloadVariant<String>),
    Integer(PayloadVariant<i64>),
    Float(PayloadVariant<f64>),
    Geo(PayloadVariant<GeoPoint>),
}

impl PayloadInterface {
    pub fn to_payload(&self) -> PayloadType {
        match self {
            PayloadInterface::Keyword(x) => PayloadType::Keyword(x.to_list()),
            PayloadInterface::Integer(x) => PayloadType::Integer(x.to_list()),
            PayloadInterface::Float(x) =>  PayloadType::Float(x.to_list()),
            PayloadInterface::Geo(x) => PayloadType::Geo(x.to_list()),
        }
    }
}


/// Define operations description for point payloads manipulation
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadOps {
    /// Overrides
    SetPayload {
        payload: HashMap<PayloadKeyType, PayloadInterface>,
        points: Vec<PointIdType>,
    },
    /// Deletes specified Payload if they are assigned
    DeletePayload {
        keys: Vec<PayloadKeyType>,
        points: Vec<PointIdType>,
    },
    /// Drops all Payload associated with given points.
    ClearPayload {
        points: Vec<PointIdType>,
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization() {
        let query1 = r#"
        {
            "set_payload": {
                "points": [1, 2, 3],
                "payload": {
                    "key1": {"type": "keyword", "value": "hello"},
                    "key2": {"type": "integer", "value": [1,2,3,4]}
                }
            }
        }
        "#;

        let operation: PayloadOps = serde_json::from_str(query1).unwrap();

        match operation {
            PayloadOps::SetPayload {
                payload,
                points: _
            } => {
                assert_eq!(payload.len(), 2);

                assert!(payload.contains_key("key1"));

                let payload_interface = payload.get("key1").expect("No key key1");
                let payload1 = payload_interface.to_payload();

                match payload1 {
                    PayloadType::Keyword(x) => assert_eq!(x, vec!["hello".to_owned()]),
                    _ => assert!(false, "Wrong payload type"),
                }
            },
            _ => assert!(false, "Wrong operation"),
        }
    }
}