use std::path::{Path, PathBuf};

use segment::common::file_operations::{atomic_save_json, read_json};
use serde::{Deserialize, Serialize};

use crate::operations::types::CollectionResult;
use crate::shards::shard::PeerId;

pub const SHARD_CONFIG_FILE: &str = "shard_config.json";

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum ShardType {
    Local,                      // Deprecated
    Remote { peer_id: PeerId }, // Deprecated
    Temporary,                  // Deprecated
    ReplicaSet,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct ShardConfig {
    pub r#type: ShardType,
}

impl ShardConfig {
    pub fn get_config_path(shard_path: &Path) -> PathBuf {
        shard_path.join(SHARD_CONFIG_FILE)
    }

    pub fn new_replica_set() -> Self {
        Self {
            r#type: ShardType::ReplicaSet,
        }
    }

    pub fn load(shard_path: &Path) -> CollectionResult<Option<Self>> {
        let config_path = Self::get_config_path(shard_path);
        if !config_path.exists() {
            log::info!("Detected missing shard config file in {:?}", shard_path);
            return Ok(None);
        }
        Ok(Some(read_json(&config_path)?))
    }

    pub fn save(&self, shard_path: &Path) -> CollectionResult<()> {
        let config_path = Self::get_config_path(shard_path);
        Ok(atomic_save_json(&config_path, self)?)
    }
}
