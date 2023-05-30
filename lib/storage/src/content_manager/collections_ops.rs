use std::collections::HashMap;

use async_trait::async_trait;
use collection::collection::Collection;
use collection::shards::CollectionId;

use crate::content_manager::errors::StorageError;

pub type Collections = HashMap<CollectionId, Collection>;

const INVALID_CHARS: [char; 11] = ['<', '>', ':', '"', '/', '\\', '|', '?', '*', '\0', '\u{1F}'];

#[async_trait]
pub trait Checker {
    fn is_collection_exists(&self, collection_name: &str) -> bool;

    fn is_valid_collection_name(&self, collection_name: &str) -> bool {
        !collection_name.contains(|c| INVALID_CHARS.contains(&c))
    }

    async fn validate_collection_not_exists(
        &self,
        collection_name: &str,
    ) -> Result<(), StorageError> {
        if self.is_collection_exists(collection_name) {
            return Err(StorageError::BadInput {
                description: format!("Collection `{collection_name}` already exists!"),
            });
        }
        Ok(())
    }

    async fn validate_collection_name(&self, collection_name: &str) -> Result<(), StorageError> {
        if !self.is_valid_collection_name(collection_name) {
            return Err(StorageError::BadInput {
                description: format!(
                    "Collection name `{}` contains invalid characters like '/' or '\0' !",
                    collection_name
                ),
            });
        }
        Ok(())
    }

    async fn validate_collection_exists(&self, collection_name: &str) -> Result<(), StorageError> {
        if !self.is_collection_exists(collection_name) {
            return Err(StorageError::NotFound {
                description: format!("Collection `{collection_name}` doesn't exist!"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validate_collection_name() {
        let collections = Collections::new();
        let checker = collections; // assuming Collections implements Checker

        // Test a valid collection name
        let valid_collection_name = "valid_name";
        assert!(checker.is_valid_collection_name(valid_collection_name));

        // Test collection name with forward slash
        let invalid_name_slash = "invalid/name";
        assert!(!checker.is_valid_collection_name(invalid_name_slash));

        // Test collection name with null character
        let invalid_name_null = "invalid\0name";
        assert!(!checker.is_valid_collection_name(invalid_name_null));
    }
}
