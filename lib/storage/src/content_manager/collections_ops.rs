use std::collections::HashMap;

use collection::collection::Collection;
use collection::shards::CollectionId;

use crate::content_manager::errors::StorageError;

pub type Collections = HashMap<CollectionId, Collection>;

pub trait Checker {
    fn collection_exists(&self, collection_name: &str) -> bool;

    async fn validate_collection_not_exists(
        &self,
        collection_name: &str,
    ) -> Result<(), StorageError> {
        if self.collection_exists(collection_name) {
            return Err(StorageError::AlreadyExists {
                description: format!("Collection `{collection_name}` already exists!"),
            });
        }
        Ok(())
    }

    async fn validate_collection_exists(&self, collection_name: &str) -> Result<(), StorageError> {
        if !self.collection_exists(collection_name) {
            return Err(StorageError::NotFound {
                description: format!("Collection `{collection_name}` doesn't exist!"),
            });
        }
        Ok(())
    }
}

impl Checker for Collections {
    fn collection_exists(&self, collection_name: &str) -> bool {
        self.contains_key(collection_name)
    }
}
