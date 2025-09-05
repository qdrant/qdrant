use std::hash::{DefaultHasher, Hash, Hasher};

use serde_json::{Value, json};
use shard::operations::CollectionUpdateOperations;

use crate::operations::universal_query::shard_query::ShardQueryRequest;

pub trait Loggable {
    fn to_log_value(&self) -> serde_json::Value;

    fn request_name(&self) -> &'static str;

    /// Hash of the query, which is going to be used for approximate deduplication and counting.
    fn request_hash(&self) -> u64;
}

impl Loggable for CollectionUpdateOperations {
    fn to_log_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_default()
    }

    fn request_name(&self) -> &'static str {
        "points-update"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Loggable for Vec<ShardQueryRequest> {
    fn to_log_value(&self) -> Value {
        json!({
            "todo": "implement me",
        })
    }

    fn request_name(&self) -> &'static str {
        "query"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}
