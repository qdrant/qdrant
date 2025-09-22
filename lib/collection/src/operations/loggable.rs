use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use segment::data_types::facets::FacetParams;
use serde_json::Value;
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

pub struct ScrollRequestLoggable(pub Value);

impl Loggable for ScrollRequestLoggable {
    fn to_log_value(&self) -> Value {
        self.0.clone()
    }

    fn request_name(&self) -> &'static str {
        "scroll"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.0.hash(&mut hasher);
        hasher.finish()
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

impl Loggable for Arc<FacetParams> {
    fn to_log_value(&self) -> Value {
        self.as_ref().to_log_value()
    }

    fn request_name(&self) -> &'static str {
        "facet"
    }

    fn request_hash(&self) -> u64 {
        self.as_ref().request_hash()
    }
}
