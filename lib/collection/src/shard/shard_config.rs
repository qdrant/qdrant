use crate::{CollectionError, CollectionResult, PeerId};
use std::path::{Path, PathBuf};

use segment::common::file_operations::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;

pub const SHARD_CONFIG_FILE: &str = "shard_config.json";

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum ShardType {
    Local,
    Remote { peer_id: PeerId },
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ShardConfig {
    pub r#type: ShardType,
}

impl ShardConfig {
    fn get_config_path(shard_path: &Path) -> PathBuf {
        shard_path.join(SHARD_CONFIG_FILE)
    }

    /// Initialize an empty config. file if it does not already exist.
    pub fn init_file(dir_path: &Path) -> Result<(), CollectionError> {
        let file_path = Self::get_config_path(dir_path);
        log::info!("Initialize shard config in {:?}", file_path);
        if !file_path.exists() {
            let mut file = fs::File::create(&file_path)?;
            let empty_json = "{}";
            file.write_all(empty_json.as_bytes())?;
        }
        Ok(())
    }

    pub fn new_remote(peer_id: PeerId) -> Self {
        let r#type = ShardType::Remote { peer_id };
        Self { r#type }
    }

    pub fn new_local() -> Self {
        let r#type = ShardType::Local;
        Self { r#type }
    }

    pub fn load(shard_path: &Path) -> CollectionResult<Self> {
        let config_path = Self::get_config_path(shard_path);
        // shard config was introduced in 0.8.0
        // therefore we need to generate a shard config for existing local shards
        if !config_path.exists() {
            log::info!("Detected missing shard config file in {:?}", shard_path);
            Self::init_file(shard_path)?;
            let shard_config = Self::new_local();
            shard_config.save(shard_path)?;
        }
        Ok(read_json(&config_path)?)
    }

    pub fn save(&self, shard_path: &Path) -> CollectionResult<()> {
        let config_path = Self::get_config_path(shard_path);
        Ok(atomic_save_json(&config_path, self)?)
    }
}
