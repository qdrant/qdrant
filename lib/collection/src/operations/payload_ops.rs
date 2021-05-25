

use serde;
use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};
use segment::types::{PointIdType, PayloadKeyType, PayloadType, GeoPoint};
use std::collections::HashMap;


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
    Regular(PayloadInterfaceStrict),
    KeywordShortcut(PayloadVariant<String>),
    FloatShortcut(PayloadVariant<f64>),
    IntShortcut(PayloadVariant<i64>),
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
        PayloadInterface::Regular(x)
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
            PayloadInterface::Regular(x) => x.into(),
            PayloadInterface::KeywordShortcut(x) => PayloadType::Keyword(x.to_list()),
            PayloadInterface::FloatShortcut(x) => PayloadType::Float(x.to_list()),
            PayloadInterface::IntShortcut(x) => PayloadType::Integer(x.to_list()),
        }
    }
}


/// Define operations description for point payloads manipulation
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PayloadOps {
    /// Set payload value, overrides if it is already exists
    SetPayload {
        payload: HashMap<PayloadKeyType, PayloadInterface>,
        /// Assigns payload to each point in this list
        points: Vec<PointIdType>,
    },
    /// Deletes specified payload values if they are assigned
    DeletePayload {
        keys: Vec<PayloadKeyType>,
        /// Deletes values from each point in this list
        points: Vec<PointIdType>,
    },
    /// Drops all Payload values associated with given points.
    ClearPayload {
        points: Vec<PointIdType>,
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn test_value_parse() {
        let query = r#"["Berlin", "Barcelona", "Moscow"]"#;

        let val: Value = serde_json::from_str(query).unwrap();

        let payload_interface: PayloadInterface = serde_json::from_value(val).unwrap();

        let payload: PayloadType = (&payload_interface).into();

        eprintln!("payload = {:#?}", payload);
    }

    #[test]
    fn test_serialization() {
        let query1 = r#"
        {
            "set_payload": {
                "points": [1, 2, 3],
                "payload": {
                    "key1": {"type": "keyword", "value": "hello"},
                    "key2": {"type": "integer", "value": [1,2,3,4]},
                    "city": "Berlin",
                    "prices": [2.13, 4.55]
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
                eprintln!("payload = {:#?}", payload);

                assert_eq!(payload.len(), 4);

                assert!(payload.contains_key("key1"));

                let payload_interface = payload.get("key1").expect("No key key1");
                let payload1 = payload_interface.into();

                match payload1 {
                    PayloadType::Keyword(x) => assert_eq!(x, vec!["hello".to_owned()]),
                    _ => assert!(false, "Wrong payload type"),
                }
            },
            _ => assert!(false, "Wrong operation"),
        }
    }
}