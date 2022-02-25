use schemars::JsonSchema;
use segment::types::{Filter, PayloadKeyType, PointIdType};
use serde;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SetPayload {
    pub payload: Map<String, Value>,
    /// Assigns payload to each point in this list
    pub points: Vec<PointIdType>, // ToDo: replace with point selector
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeletePayload {
    pub keys: Vec<PayloadKeyType>,
    /// Deletes values from each point in this list
    pub points: Vec<PointIdType>, // ToDo: replace with point selector
}

/// Define operations description for point payloads manipulation
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PayloadOps {
    /// Set payload value, overrides if it is already exists
    SetPayload(SetPayload),
    /// Deletes specified payload values if they are assigned
    DeletePayload(DeletePayload),
    /// Drops all Payload values associated with given points.
    ClearPayload { points: Vec<PointIdType> },
    /// Clear all Payload values by given filter criteria.
    ClearPayloadByFilter(Filter),
}

#[cfg(test)]
mod tests {
    use super::*;
    use segment::types::{Payload, PayloadType};

    #[test]
    fn test_serialization() {
        let query1 = r#"
        {
            "set_payload": {
                "points": [1, 2, 3],
                "payload": {
                    "key1":  "hello" ,
                    "key2": [1,2,3,4],
                    "key3": {"json": {"key1":"value1"} }
                }
            }
        }
        "#;

        let operation: PayloadOps = serde_json::from_str(query1).unwrap();

        match operation {
            PayloadOps::SetPayload(set_payload) => {
                let payload: Payload = set_payload.payload.into();
                assert_eq!(payload.len(), 3);

                assert!(payload.contains_key("key1"));

                let payload_type = payload.get("key1").expect("No key key1");

                match payload_type {
                    PayloadType::Keyword(x) => assert_eq!(x, ["hello".to_owned()]),
                    _ => panic!("Wrong payload type"),
                }

                let payload_type_json = payload.get("key3");

                assert!(matches!(payload_type_json, None))
            }
            _ => panic!("Wrong operation"),
        }
    }
}
