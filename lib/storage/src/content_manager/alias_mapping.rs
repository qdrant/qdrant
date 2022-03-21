use crate::content_manager::errors::StorageError;
use segment::common::file_operations::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const ALIAS_MAPPING_CONFIG_FILE: &str = "data.json";

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
struct AliasMapping(HashMap<String, String>);

impl AliasMapping {
    // TODO error type conversion
    pub fn load(path: &Path) -> Result<Self, StorageError> {
        read_json(path).map_err(|err| {
            println!("failed to read_json {:?}", err);
            StorageError::ServiceError {
                description: "something bad happened".to_string(),
            }
        })
    }

    // TODO error type conversion
    pub fn save(&self, path: &Path) -> Result<(), StorageError> {
        atomic_save_json(path, self).map_err(|err| {
            println!("failed to save_json {:?}", err);
            StorageError::ServiceError {
                description: "something bad happened".to_string(),
            }
        })
    }
}

/// Not thread-safe, accesses must be synchronized by an exclusive lock at the call site
pub struct AliasPersistence {
    data_path: PathBuf,
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
        Ok(AliasPersistence { data_path })
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, StorageError> {
        let alias = AliasMapping::load(&self.data_path)?;
        Ok(alias.0.get(key).cloned())
    }

    pub fn contains_alias(&self, key: &str) -> Result<bool, StorageError> {
        let alias = AliasMapping::load(&self.data_path)?;
        Ok(alias.0.contains_key(key))
    }

    pub fn insert(&self, key: String, value: String) -> Result<(), StorageError> {
        let mut alias = AliasMapping::load(&self.data_path)?;
        println!("inserting {} {}", &key, &value);
        // not checking if it already existed
        alias.0.insert(key, value);
        alias.save(&self.data_path)?;
        Ok(())
    }

    pub fn remove(&self, key: &str) -> Result<Option<String>, StorageError> {
        let mut alias = AliasMapping::load(&self.data_path)?;
        let res = alias.0.remove(key);
        alias.save(&self.data_path)?;
        Ok(res)
    }

    pub fn collection_aliases(&self, collection_name: &str) -> Result<Vec<String>, StorageError> {
        let mut result = vec![];
        let alias = AliasMapping::load(&self.data_path)?;
        for (alias, target_collection) in alias.0.into_iter() {
            if collection_name == target_collection {
                result.push(alias);
            }
        }
        Ok(result)
    }
}
