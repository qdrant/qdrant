use segment::data_types::vectors::VectorStruct;

use super::types::{GroupsResult, PointGroup, Record};

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

impl From<GroupsResult> for api::rest::GroupsResult {
    fn from(value: GroupsResult) -> Self {
        Self {
            groups: value
                .groups
                .into_iter()
                .map(api::rest::PointGroup::from)
                .collect(),
        }
    }
}

impl From<PointGroup> for api::rest::PointGroup {
    fn from(value: PointGroup) -> Self {
        Self {
            hits: value
                .hits
                .into_iter()
                .map(api::rest::ScoredPoint::from)
                .collect(),
            id: value.id,
            lookup: value.lookup.map(api::rest::Record::from),
        }
    }
}
