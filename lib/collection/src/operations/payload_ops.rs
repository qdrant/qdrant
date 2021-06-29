use schemars::JsonSchema;
use segment::types::{PayloadInterface, PayloadKeyType, PointIdType};
use serde;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    ClearPayload { points: Vec<PointIdType> },
}

#[cfg(test)]
mod tests {
    use super::*;
    use segment::types::PayloadType;

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
            PayloadOps::SetPayload { payload, points: _ } => {
                assert_eq!(payload.len(), 2);

                assert!(payload.contains_key("key1"));

                let payload_interface = payload.get("key1").expect("No key key1");
                let payload1 = payload_interface.into();

                match payload1 {
                    PayloadType::Keyword(x) => assert_eq!(x, vec!["hello".to_owned()]),
                    _ => assert!(false, "Wrong payload type"),
                }
            }
            _ => assert!(false, "Wrong operation"),
        }
    }
}
