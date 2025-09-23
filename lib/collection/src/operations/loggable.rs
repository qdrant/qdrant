use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use segment::data_types::facets::FacetParams;
use serde_json::Value;
use shard::operations::CollectionUpdateOperations;

use crate::operations::types::{CountRequestInternal, PointRequestInternal, ScrollRequestInternal};
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
        serde_json::to_value(self).unwrap_or_default()
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

impl Loggable for ScrollRequestInternal {
    fn to_log_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_default()
    }

    fn request_name(&self) -> &'static str {
        "scroll"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl<T: Loggable> Loggable for Arc<T> {
    fn to_log_value(&self) -> Value {
        self.as_ref().to_log_value()
    }

    fn request_name(&self) -> &'static str {
        self.as_ref().request_name()
    }

    fn request_hash(&self) -> u64 {
        self.as_ref().request_hash()
    }
}

impl Loggable for FacetParams {
    fn to_log_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_default()
    }

    fn request_name(&self) -> &'static str {
        "facet"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Loggable for CountRequestInternal {
    fn to_log_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_default()
    }

    fn request_name(&self) -> &'static str {
        "count"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Loggable for PointRequestInternal {
    fn to_log_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_default()
    }

    fn request_name(&self) -> &'static str {
        "retrieve"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}
