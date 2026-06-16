use segment::types::{ExtendedPointId, Payload};
use serde_json::Value as JsonValue;
use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

use super::Vectors;

#[derive(Debug, Clone)]
pub struct PointStruct(pub PointStructPersisted);

impl PointStruct {
    pub fn new(
        id: impl Into<ExtendedPointId>,
        vectors: impl Into<Vectors>,
        payload: JsonValue,
    ) -> Self {
        let payload = match payload {
            JsonValue::Object(map) => Payload(map.into_iter().collect()),
            JsonValue::Null
            | JsonValue::Bool(_)
            | JsonValue::Number(_)
            | JsonValue::String(_)
            | JsonValue::Array(_) => panic!("payload must be a JSON object, got {payload}"),
        };
        Self(PointStructPersisted {
            id: id.into(),
            vector: VectorStructPersisted::from(vectors.into().0),
            payload: Some(payload),
        })
    }
}

impl From<PointStruct> for PointStructPersisted {
    fn from(p: PointStruct) -> Self {
        p.0
    }
}

impl From<PointStructPersisted> for PointStruct {
    fn from(p: PointStructPersisted) -> Self {
        Self(p)
    }
}
