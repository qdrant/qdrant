use super::types::RecordInternal;

impl From<RecordInternal> for api::rest::Record {
    fn from(value: RecordInternal) -> Self {
        Self {
            id: value.id,
            payload: value.payload,
            vector: value.vector.map(api::rest::VectorStructOutput::from),
            shard_key: value.shard_key,
            order_value: value.order_value,
        }
    }
}
