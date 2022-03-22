use crate::content_manager::errors::StorageError;
use segment::common::file_operations::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const ALIAS_MAPPING_CONFIG_FILE: &str = "data.json";

type Alias = String;
type CollectionName = String;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
struct AliasMapping(HashMap<Alias, CollectionName>);

impl AliasMapping {
    pub fn load(path: &Path) -> Result<Self, StorageError> {
        read_json(path).map_err(|err| err.into())
    }

    pub fn save(&self, path: &Path) -> Result<(), StorageError> {
        atomic_save_json(path, self).map_err(|err| err.into())
    }
}

/// Not thread-safe, accesses must be synchronized by an exclusive lock at the call site
/// - Reads are served from memory.
/// - Writes are durably saved.
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
            let mut file = fs::File::create(&data_path)?;
            let empty_json = "{}";
            file.write_all(empty_json.as_bytes())?;
        }
        Ok(data_path)
    }

    pub fn open(dir_path: PathBuf) -> Result<Self, StorageError> {
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)?;
        }
        let data_path = Self::init_file(&dir_path)?;
        let alias_mapping = AliasMapping::load(&data_path)?;
        Ok(AliasPersistence {
            data_path,
            alias_mapping,
        })
    }

    pub fn get(&self, alias: &str) -> Option<String> {
        self.alias_mapping.0.get(alias).cloned()
    }

    pub fn contains_alias(&self, alias: &str) -> bool {
        self.alias_mapping.0.contains_key(alias)
    }

    pub fn insert(&mut self, alias: String, collection_name: String) -> Result<(), StorageError> {
        self.alias_mapping.0.insert(alias, collection_name);
        self.alias_mapping.save(&self.data_path)?;
        Ok(())
    }

    pub fn remove(&mut self, alias: &str) -> Result<Option<String>, StorageError> {
        let res = self.alias_mapping.0.remove(alias);
        self.alias_mapping.save(&self.data_path)?;
        Ok(res)
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
}
