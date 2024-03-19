use segment::data_types::vectors::VectorStruct;

use super::types::Record;

impl From<Record> for api::rest::Record {
    fn from(value: Record) -> Self {
        Self {
            id: value.id,
            payload: value.payload,
            vector: value.vector.map(api::rest::VectorStruct::from),
            shard_key: value.shard_key,
        }
    }
}

impl From<api::rest::Record> for Record {
    fn from(value: api::rest::Record) -> Self {
        Self {
            id: value.id,
            payload: value.payload,
            vector: value.vector.map(VectorStruct::from),
            shard_key: value.shard_key,
        }
    }
}
