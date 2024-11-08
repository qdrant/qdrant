use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use collection::shards::CollectionId;
use io::file_operations::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};

use crate::content_manager::errors::StorageError;

pub const ALIAS_MAPPING_CONFIG_FILE: &str = "data.json";

type Alias = String;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
pub struct AliasMapping(HashMap<Alias, CollectionId>);

impl AliasMapping {
    pub fn load(path: &Path) -> Result<Self, StorageError> {
        Ok(read_json(path)?)
    }

    pub fn save(&self, path: &Path) -> Result<(), StorageError> {
        Ok(atomic_save_json(path, self)?)
    }
}

/// Persists mapping between alias and collection name. The data is assumed to be relatively small.
/// - Reads are served from memory.
/// - Writes are durably saved.
#[derive(Debug)]
pub struct AliasPersistence {
    data_path: PathBuf,
    alias_mapping: AliasMapping,
}

impl AliasPersistence {
    pub fn get_config_path(path: &Path) -> PathBuf {
        path.join(ALIAS_MAPPING_CONFIG_FILE)
    }

    fn init_file(dir_path: &Path) -> Result<PathBuf, StorageError> {
        let data_path = Self::get_config_path(dir_path);
        if !data_path.exists() {
            atomic_save_json(&data_path, &AliasMapping::default())?;
        }
        Ok(data_path)
    }

    pub fn open(dir_path: &Path) -> Result<Self, StorageError> {
        if !dir_path.exists() {
            fs::create_dir_all(dir_path)?;
        }
        let data_path = Self::init_file(dir_path)?;
        let alias_mapping = AliasMapping::load(&data_path)?;
        Ok(AliasPersistence {
            data_path,
            alias_mapping,
        })
    }

    pub fn get(&self, alias: &str) -> Option<String> {
        self.alias_mapping.0.get(alias).cloned()
    }

    pub fn insert(&mut self, alias: String, collection_name: String) -> Result<(), StorageError> {
        self.alias_mapping.0.insert(alias, collection_name);
        self.alias_mapping.save(&self.data_path)?;
        Ok(())
    }

    pub fn remove(&mut self, alias: &str) -> Result<Option<String>, StorageError> {
        let output = self.alias_mapping.0.remove(alias);

        if output.is_some() {
            self.alias_mapping.save(&self.data_path)?;
        }

        Ok(output)
    }

    /// Removes all aliases for a given collection.
    pub fn remove_collection(&mut self, collection_name: &str) -> Result<(), StorageError> {
        let prev_len = self.alias_mapping.0.len();

        self.alias_mapping.0.retain(|_, v| v != collection_name);

        if prev_len != self.alias_mapping.0.len() {
            self.alias_mapping.save(&self.data_path)?;
        }

        Ok(())
    }

    pub fn rename_alias(
        &mut self,
        old_alias_name: &str,
        new_alias_name: String,
    ) -> Result<(), StorageError> {
        match self.get(old_alias_name) {
            None => Err(StorageError::NotFound {
                description: format!("Alias {old_alias_name} does not exists!"),
            }),
            Some(collection_name) => {
                self.alias_mapping.0.remove(old_alias_name);
                self.alias_mapping.0.insert(new_alias_name, collection_name);
                // 'remove' & 'insert' saved atomically
                self.alias_mapping.save(&self.data_path)?;
                Ok(())
            }
        }
    }

    pub fn collection_aliases(&self, collection_name: &str) -> Vec<String> {
        let mut result = vec![];
        for (alias, target_collection) in self.alias_mapping.0.iter() {
            if collection_name == target_collection {
                result.push(alias.clone());
            }
        }
        result
    }

    pub fn state(&self) -> &AliasMapping {
        &self.alias_mapping
    }

    pub fn apply_state(&mut self, alias_mapping: AliasMapping) -> Result<(), StorageError> {
        self.alias_mapping = alias_mapping;
        self.alias_mapping.save(&self.data_path)?;
        Ok(())
    }

    pub fn check_alias_exists(&self, alias: &str) -> bool {
        self.alias_mapping.0.contains_key(alias)
    }
}
