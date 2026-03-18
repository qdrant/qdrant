use std::collections::HashMap;

use segment::json_path::JsonPath;
use segment::types::{Filter, PayloadFieldSchema};

use crate::shards::CollectionId;

pub struct CollectionDeletedEvent {
    pub collection_id: CollectionId,
}

pub struct SlowQueryEvent {
    pub collection_id: CollectionId,
    pub filters: Vec<Filter>,
    pub schema: HashMap<JsonPath, PayloadFieldSchema>,
}

pub struct IndexCreatedEvent {
    pub collection_id: CollectionId,
    pub field_name: JsonPath,
}
