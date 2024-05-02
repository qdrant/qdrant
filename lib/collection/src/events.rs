use std::collections::HashMap;

use segment::json_path::JsonPathV2;
use segment::types::{Filter, PayloadFieldSchema};

use crate::shards::CollectionId;

pub struct CollectionCreatedEvent;

pub struct CollectionDeletedEvent {
    pub collection_id: CollectionId,
}

pub struct SlowQueryEvent {
    pub collection_id: CollectionId,
    pub filters: Vec<Filter>,
    pub schema: HashMap<JsonPathV2, PayloadFieldSchema>,
}

pub struct IndexCreatedEvent {
    pub collection_id: CollectionId,
    pub field_name: JsonPathV2,
}
