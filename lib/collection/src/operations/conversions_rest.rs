use super::types::RecordInternal;

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
