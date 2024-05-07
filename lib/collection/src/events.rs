use segment::json_path::JsonPathV2;
use segment::types::Filter;

use crate::shards::CollectionId;

pub struct CollectionDeletedEvent {
    pub collection_id: CollectionId,
}

pub struct SlowQueryEvent {
    pub collection_id: CollectionId,
    pub filters: Vec<Filter>,
}

pub struct IndexCreatedEvent {
    pub collection_id: CollectionId,
    pub field_name: JsonPathV2,
}
