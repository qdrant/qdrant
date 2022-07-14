use crate::content_manager::errors::StorageError;
use async_trait::async_trait;
use collection::collection::Collection;
use std::collections::HashMap;

pub type Collections = HashMap<String, Collection>;

#[async_trait]
pub trait Checker {
    fn is_collection_exists(&self, collection_name: &str) -> bool;

    async fn validate_collection_not_exists(
        &self,
        collection_name: &str,
    ) -> Result<(), StorageError> {
        if self.is_collection_exists(collection_name) {
            return Err(StorageError::BadInput {
                description: format!("Collection `{}` already exists!", collection_name),
            });
        }
        Ok(())
    }

    async fn validate_collection_exists(&self, collection_name: &str) -> Result<(), StorageError> {
        if !self.is_collection_exists(collection_name) {
            return Err(StorageError::NotFound {
                description: format!("Collection `{}` doesn't exist!", collection_name),
            });
        }
        Ok(())
    }
}

impl Checker for Collections {
    fn is_collection_exists(&self, collection_name: &str) -> bool {
        self.contains_key(collection_name)
    }
}
