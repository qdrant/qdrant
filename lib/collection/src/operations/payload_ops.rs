use schemars::JsonSchema;
use segment::types::{Filter, Payload, PayloadKeyType, PointIdType};
use serde;
use serde::{Deserialize, Serialize};

use crate::{hash_ring::HashRing, ShardId};

use super::{split_iter_by_shard, OperationToShard, SplitByShard};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct SetPayload {
    pub payload: Payload,
    /// Assigns payload to each point in this list
    pub points: Vec<PointIdType>, // ToDo: replace with point selector
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct DeletePayload {
    pub keys: Vec<PayloadKeyType>,
    /// Deletes values from each point in this list
    pub points: Vec<PointIdType>, // ToDo: replace with point selector
}

/// Define operations description for point payloads manipulation
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
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

impl SplitByShard for PayloadOps {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self> {
        match self {
            PayloadOps::SetPayload(operation) => {
                operation.split_by_shard(ring).map(PayloadOps::SetPayload)
            }
            PayloadOps::DeletePayload(operation) => operation
                .split_by_shard(ring)
                .map(PayloadOps::DeletePayload),
            PayloadOps::ClearPayload { points } => split_iter_by_shard(points, |id| *id, ring)
                .map(|points| PayloadOps::ClearPayload { points }),
            operation @ PayloadOps::ClearPayloadByFilter(_) => OperationToShard::to_all(operation),
        }
    }
}

impl SplitByShard for DeletePayload {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self> {
        split_iter_by_shard(self.points, |id| *id, ring).map(|points| DeletePayload {
            points,
            keys: self.keys.clone(),
        })
    }
}

impl SplitByShard for SetPayload {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self> {
        split_iter_by_shard(self.points, |id| *id, ring).map(|points| SetPayload {
            points,
            payload: self.payload.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use segment::types::Payload;
    use serde_json::Value;

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
                let payload: Payload = set_payload.payload;
                assert_eq!(payload.len(), 3);

                assert!(payload.contains_key("key1"));

                let payload_type = payload.get_value("key1").expect("No key key1");

                match payload_type {
                    Value::String(x) => assert_eq!(x, "hello"),
                    _ => panic!("Wrong payload type"),
                }

                let payload_type_json = payload.get_value("key3");

                assert!(matches!(payload_type_json, Some(Value::Object(_))))
            }
            _ => panic!("Wrong operation"),
        }
    }
}
