use std::path::{Path, PathBuf};

use segment::common::file_operations::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};

use crate::operations::types::CollectionResult;
use crate::shard::PeerId;

pub const SHARD_CONFIG_FILE: &str = "shard_config.json";

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq)]
pub enum ShardType {
    Local,
    Remote { peer_id: PeerId },
    Temporary, // same as local, but not ready yet
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ShardConfig {
    pub r#type: ShardType,
}

impl ShardConfig {
    pub fn get_config_path(shard_path: &Path) -> PathBuf {
        shard_path.join(SHARD_CONFIG_FILE)
    }

    pub fn new_remote(peer_id: PeerId) -> Self {
        let r#type = ShardType::Remote { peer_id };
        Self { r#type }
    }

    pub fn new_local() -> Self {
        let r#type = ShardType::Local;
        Self { r#type }
    }

    pub fn new_temp() -> Self {
        let r#type = ShardType::Temporary;
        Self { r#type }
    }

    pub fn load(shard_path: &Path) -> CollectionResult<Self> {
        let config_path = Self::get_config_path(shard_path);
        // shard config was introduced in 0.8.0
        // therefore we need to generate a shard config for existing local shards
        if !config_path.exists() {
            log::info!("Detected missing shard config file in {:?}", shard_path);
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
