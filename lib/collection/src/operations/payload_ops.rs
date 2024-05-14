use schemars::JsonSchema;
use segment::json_path::JsonPath;
use segment::types::{Filter, Payload, PayloadKeyType, PointIdType};
use serde;
use serde::{Deserialize, Serialize};
use strum::{EnumDiscriminants, EnumIter};
use validator::Validate;

use super::{split_iter_by_shard, OperationToShard, SplitByShard};
use crate::operations::shard_key_selector::ShardKeySelector;
use crate::shards::shard_holder::ShardHashRing;

/// This data structure is used in API interface and applied across multiple shards
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(try_from = "SetPayloadShadow")]
pub struct SetPayload {
    pub payload: Payload,
    /// Assigns payload to each point in this list
    pub points: Option<Vec<PointIdType>>,
    /// Assigns payload to each point that satisfy this filter condition
    pub filter: Option<Filter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
    /// Assigns payload to each point that satisfy this path of property
    pub key: Option<JsonPath>,
}

/// This data structure is used inside shard operations queue
/// and supposed to be written into WAL of individual shard.
///
/// Unlike `SetPayload` it does not contain `shard_key` field
/// as individual shard does not need to know about shard key
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Validate)]
pub struct SetPayloadOp {
    pub payload: Payload,
    /// Assigns payload to each point in this list
    pub points: Option<Vec<PointIdType>>,
    /// Assigns payload to each point that satisfy this filter condition
    pub filter: Option<Filter>,
    /// Payload selector to indicate property of payload, e.g. `a.b.c`
    pub key: Option<JsonPath>,
}

#[derive(Deserialize)]
struct SetPayloadShadow {
    pub payload: Payload,
    pub points: Option<Vec<PointIdType>>,
    pub filter: Option<Filter>,
    pub shard_key: Option<ShardKeySelector>,
    pub key: Option<JsonPath>,
}

pub struct PointsSelectorValidationError;

impl std::fmt::Display for PointsSelectorValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Either list of point ids or filter must be provided"
        )
    }
}

impl TryFrom<SetPayloadShadow> for SetPayload {
    type Error = PointsSelectorValidationError;

    fn try_from(value: SetPayloadShadow) -> Result<Self, Self::Error> {
        if value.points.is_some() || value.filter.is_some() {
            Ok(SetPayload {
                payload: value.payload,
                points: value.points,
                filter: value.filter,
                shard_key: value.shard_key,
                key: value.key,
            })
        } else {
            Err(PointsSelectorValidationError)
        }
    }
}

/// This data structure is used in API interface and applied across multiple shards
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(try_from = "DeletePayloadShadow")]
pub struct DeletePayload {
    /// List of payload keys to remove from payload
    pub keys: Vec<PayloadKeyType>,
    /// Deletes values from each point in this list
    pub points: Option<Vec<PointIdType>>,
    /// Deletes values from points that satisfy this filter condition
    pub filter: Option<Filter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

/// This data structure is used inside shard operations queue
/// and supposed to be written into WAL of individual shard.
///
/// Unlike `DeletePayload` it does not contain `shard_key` field
/// as individual shard does not need to know about shard key
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Validate)]
pub struct DeletePayloadOp {
    /// List of payload keys to remove from payload
    pub keys: Vec<PayloadKeyType>,
    /// Deletes values from each point in this list
    pub points: Option<Vec<PointIdType>>,
    /// Deletes values from points that satisfy this filter condition
    pub filter: Option<Filter>,
}

#[derive(Deserialize)]
struct DeletePayloadShadow {
    pub keys: Vec<PayloadKeyType>,
    pub points: Option<Vec<PointIdType>>,
    pub filter: Option<Filter>,
    pub shard_key: Option<ShardKeySelector>,
}

impl TryFrom<DeletePayloadShadow> for DeletePayload {
    type Error = PointsSelectorValidationError;

    fn try_from(value: DeletePayloadShadow) -> Result<Self, Self::Error> {
        if value.points.is_some() || value.filter.is_some() {
            Ok(DeletePayload {
                keys: value.keys,
                points: value.points,
                filter: value.filter,
                shard_key: value.shard_key,
            })
        } else {
            Err(PointsSelectorValidationError)
        }
    }
}

/// Define operations description for point payloads manipulation
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants)]
#[strum_discriminants(derive(EnumIter))]
#[serde(rename_all = "snake_case")]
pub enum PayloadOps {
    /// Set payload value, overrides if it is already exists
    SetPayload(SetPayloadOp),
    /// Deletes specified payload values if they are assigned
    DeletePayload(DeletePayloadOp),
    /// Drops all Payload values associated with given points.
    ClearPayload { points: Vec<PointIdType> },
    /// Clear all Payload values by given filter criteria.
    ClearPayloadByFilter(Filter),
    /// Overwrite full payload with given keys
    OverwritePayload(SetPayloadOp),
}

impl PayloadOps {
    pub fn is_write_operation(&self) -> bool {
        match self {
            PayloadOps::SetPayload(_) => true,
            PayloadOps::DeletePayload(_) => false,
            PayloadOps::ClearPayload { .. } => false,
            PayloadOps::ClearPayloadByFilter(_) => false,
            PayloadOps::OverwritePayload(_) => true,
        }
    }
}

impl Validate for PayloadOps {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            PayloadOps::SetPayload(operation) => operation.validate(),
            PayloadOps::DeletePayload(operation) => operation.validate(),
            PayloadOps::ClearPayload { .. } => Ok(()),
            PayloadOps::ClearPayloadByFilter(_) => Ok(()),
            PayloadOps::OverwritePayload(operation) => operation.validate(),
        }
    }
}

impl SplitByShard for PayloadOps {
    fn split_by_shard(self, ring: &ShardHashRing) -> OperationToShard<Self> {
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
            PayloadOps::OverwritePayload(operation) => operation
                .split_by_shard(ring)
                .map(PayloadOps::OverwritePayload),
        }
    }
}

impl SplitByShard for DeletePayloadOp {
    fn split_by_shard(self, ring: &ShardHashRing) -> OperationToShard<Self> {
        match (&self.points, &self.filter) {
            (Some(_), _) => {
                split_iter_by_shard(self.points.unwrap(), |id| *id, ring).map(|points| {
                    DeletePayloadOp {
                        points: Some(points),
                        keys: self.keys.clone(),
                        filter: self.filter.clone(),
                    }
                })
            }
            (None, Some(_)) => OperationToShard::to_all(self),
            (None, None) => OperationToShard::to_none(),
        }
    }
}

impl SplitByShard for SetPayloadOp {
    fn split_by_shard(self, ring: &ShardHashRing) -> OperationToShard<Self> {
        match (&self.points, &self.filter) {
            (Some(_), _) => {
                split_iter_by_shard(self.points.unwrap(), |id| *id, ring).map(|points| {
                    SetPayloadOp {
                        points: Some(points),
                        payload: self.payload.clone(),
                        filter: self.filter.clone(),
                        key: self.key.clone(),
                    }
                })
            }
            (None, Some(_)) => OperationToShard::to_all(self),
            (None, None) => OperationToShard::to_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use segment::types::{Payload, PayloadContainer};
    use serde_json::Value;

    use super::*;

    #[derive(Debug, Deserialize, Serialize)]
    pub struct TextSelector {
        pub points: Vec<PointIdType>,
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct TextSelectorOpt {
        pub points: Option<Vec<PointIdType>>,
        pub filter: Option<Filter>,
    }

    #[test]
    fn test_replace_with_opt_in_cbor() {
        let obj1 = TextSelector {
            points: vec![1.into(), 2.into(), 3.into()],
        };
        let raw_cbor = serde_cbor::to_vec(&obj1).unwrap();
        let obj2 = serde_cbor::from_slice::<TextSelectorOpt>(&raw_cbor).unwrap();
        eprintln!("obj2 = {obj2:#?}");
        assert_eq!(obj1.points, obj2.points.unwrap());
    }

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

                let payload_type = payload
                    .get_value(&"key1".parse().unwrap())
                    .into_iter()
                    .next()
                    .cloned()
                    .expect("No key key1");

                match payload_type {
                    Value::String(x) => assert_eq!(x, "hello"),
                    _ => panic!("Wrong payload type"),
                }

                let payload_type_json = payload
                    .get_value(&"key3".parse().unwrap())
                    .into_iter()
                    .next()
                    .cloned();

                assert!(matches!(payload_type_json, Some(Value::Object(_))))
            }
            _ => panic!("Wrong operation"),
        }
    }
}
