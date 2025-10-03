use api::conversions::json::payload_to_proto;
use api::grpc::conversions::convert_shard_key_to_grpc;
use segment::data_types::order_by::OrderValue;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorRef, VectorStructInternal};
use segment::types::{Payload, PointIdType, ShardKey, VectorName};

use crate::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

/// Point data
#[derive(Clone, Debug, PartialEq)]
pub struct RecordInternal {
    /// Id of the point
    pub id: PointIdType,
    /// Payload - values assigned to the point
    pub payload: Option<Payload>,
    /// Vector of the point
    pub vector: Option<VectorStructInternal>,
    /// Shard Key
    pub shard_key: Option<ShardKey>,
    /// Order value, if used for order_by
    pub order_value: Option<OrderValue>,
}

impl RecordInternal {
    pub fn get_vector_by_name(&self, name: &VectorName) -> Option<VectorRef<'_>> {
        match &self.vector {
            Some(VectorStructInternal::Single(vector)) => {
                (name == DEFAULT_VECTOR_NAME).then_some(VectorRef::from(vector))
            }
            Some(VectorStructInternal::MultiDense(vectors)) => {
                (name == DEFAULT_VECTOR_NAME).then_some(VectorRef::from(vectors))
            }
            Some(VectorStructInternal::Named(vectors)) => vectors.get(name).map(VectorRef::from),
            None => None,
        }
    }
}

/// Warn: panics if the vector is empty
impl TryFrom<RecordInternal> for PointStructPersisted {
    type Error = String;

    fn try_from(record: RecordInternal) -> Result<Self, Self::Error> {
        let RecordInternal {
            id,
            payload,
            vector,
            shard_key: _,
            order_value: _,
        } = record;

        if vector.is_none() {
            return Err("Vector is empty".to_string());
        }

        Ok(Self {
            id,
            payload,
            vector: VectorStructPersisted::from(vector.unwrap()),
        })
    }
}

impl From<RecordInternal> for api::grpc::qdrant::RetrievedPoint {
    fn from(record: RecordInternal) -> Self {
        let RecordInternal {
            id,
            payload,
            vector,
            shard_key,
            order_value,
        } = record;
        Self {
            id: Some(id.into()),
            payload: payload.map(payload_to_proto).unwrap_or_default(),
            vectors: vector.map(api::grpc::qdrant::VectorsOutput::from),
            shard_key: shard_key.map(convert_shard_key_to_grpc),
            order_value: order_value.map(From::from),
        }
    }
}

impl From<RecordInternal> for api::rest::Record {
    fn from(value: RecordInternal) -> Self {
        let RecordInternal {
            id,
            payload,
            vector,
            shard_key,
            order_value,
        } = value;
        Self {
            id,
            payload,
            vector: vector.map(api::rest::VectorStructOutput::from),
            shard_key,
            order_value,
        }
    }
}
