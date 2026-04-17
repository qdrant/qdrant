use std::collections::BTreeMap;
use std::sync::Arc;

use collection::collection::Collection;
use collection::shards::CollectionId;

use crate::content_manager::errors::StorageError;

/// `BTreeMap` for deterministic iteration order by `CollectionId` — iteration is externally
/// observable (list API, consensus state apply, snapshot enumeration, telemetry).
pub type Collections = BTreeMap<CollectionId, Arc<Collection>>;

pub trait Checker {
    fn collection_exists(&self, collection_name: &str) -> bool;

    fn validate_collection_not_exists(&self, collection_name: &str) -> Result<(), StorageError> {
        if self.collection_exists(collection_name) {
            return Err(StorageError::already_exists(format!(
                "Collection `{collection_name}` already exists!"
            )));
        }
        Ok(())
    }

    fn validate_collection_exists(&self, collection_name: &str) -> Result<(), StorageError> {
        if !self.collection_exists(collection_name) {
            return Err(StorageError::not_found(format!(
                "Collection `{collection_name}` doesn't exist!"
            )));
        }
        Ok(())
    }
}

impl Checker for Collections {
    fn collection_exists(&self, collection_name: &str) -> bool {
        self.contains_key(collection_name)
    }
}
