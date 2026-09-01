use std::collections::HashMap;
use std::path::{Path, PathBuf};

use collection::shards::CollectionId;
use common::fs::{atomic_save_json, read_json};
use fs_err as fs;
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

    /// Aliases pointing at `collection_name`.
    pub fn collection_aliases<'a>(
        &'a self,
        collection_name: &'a str,
    ) -> impl Iterator<Item = Alias> + 'a {
        self.0
            .iter()
            .filter(move |&(_, target)| target == collection_name)
            .map(|(alias, _)| alias.clone())
    }

    /// Iterate over aliases and collections they point at.
    pub fn iter(&self) -> impl Iterator<Item = (&Alias, &CollectionId)> {
        self.0.iter()
    }

    /// Returns collection `alias` points at, or `None` if it does not exist.
    pub fn get(&self, alias: &str) -> Option<&CollectionId> {
        self.0.get(alias)
    }

    /// Point `alias` at `collection_name`, replacing collection it pointed at before.
    pub fn insert(&mut self, alias: Alias, collection_name: CollectionId) {
        self.0.insert(alias, collection_name);
    }

    /// Drop `alias`, if it exists.
    pub fn remove(&mut self, alias: &str) {
        self.0.remove(alias);
    }

    /// Drop every alias pointing at `collection_name`.
    /// Returns `false` if there were none.
    pub fn remove_collection(&mut self, collection_name: &str) -> bool {
        let len = self.0.len();

        self.0.retain(|_, target| target != collection_name);

        self.0.len() != len
    }

    /// Rename `old_alias` as `new_alias`, keeping collection it points at.
    /// Returns `false` if `old_alias` does not exist.
    pub fn rename(&mut self, old_alias: &str, new_alias: Alias) -> bool {
        let Some(collection_name) = self.0.remove(old_alias) else {
            return false;
        };

        self.0.insert(new_alias, collection_name);

        true
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
        self.alias_mapping.get(alias).cloned()
    }

    pub fn insert(&mut self, alias: String, collection_name: String) -> Result<(), StorageError> {
        self.alias_mapping.insert(alias, collection_name);
        self.alias_mapping.save(&self.data_path)?;
        Ok(())
    }

    /// Removes all aliases for a given collection.
    pub fn remove_collection(&mut self, collection_name: &str) -> Result<(), StorageError> {
        if self.alias_mapping.remove_collection(collection_name) {
            self.alias_mapping.save(&self.data_path)?;
        }

        Ok(())
    }

    pub fn collection_aliases(&self, collection_name: &str) -> Vec<String> {
        self.alias_mapping
            .collection_aliases(collection_name)
            .collect()
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
        self.alias_mapping.get(alias).is_some()
    }
}
