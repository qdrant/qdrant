use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use common::tar_ext;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use segment::types::ShardKey;
use serde::{Deserialize, Serialize};

use crate::save_on_disk::{Error, SaveOnDisk};
use crate::shards::shard::ShardId;

pub type ShardKeyMapping = HashMap<ShardKey, HashSet<ShardId>>;

/// A `SaveOnDisk`-like structure for the shard key mapping
///
/// Hold the shard key mapping, and persists in a different format to disk.
pub(super) struct SaveOnDiskShardKeyMappingWrapper {
    /// Persist mapping in a robust format that doesn't loose shard key type information
    persisted: SaveOnDisk<ShardKeyMappingWrapper>,
    /// Shard key mapping view in original format
    mapping: RwLock<ShardKeyMapping>,
}

impl SaveOnDiskShardKeyMappingWrapper {
    /// Wraps `SaveOnDisk::load_or_init_default`, using a different persisted type.
    pub fn load_or_init_default(path: impl Into<PathBuf>) -> Result<Self, Error> {
        let persisted: SaveOnDisk<ShardKeyMappingWrapper> = SaveOnDisk::load_or_init_default(path)?;
        let mapping = persisted.read().clone().into_mapping();
        Ok(Self {
            persisted,
            mapping: RwLock::new(mapping),
        })
    }

    /// Wraps `SaveOnDisk::write_optional`, using a different persisted type.
    pub fn write_optional(
        &self,
        f: impl FnOnce(&ShardKeyMapping) -> Option<ShardKeyMapping>,
    ) -> Result<bool, Error> {
        let read_lock = self.mapping.upgradable_read();

        let mut updated_mapping = None;

        let is_changed = self.persisted.write_optional(|mapping| {
            let persisted_mapping = f(&mapping.clone().into_mapping());

            // If persisted mapping will be changed, clone it to update our mapping view
            if let Some(ref mapping) = persisted_mapping {
                updated_mapping.replace(mapping.clone());
            }

            persisted_mapping.map(ShardKeyMappingWrapper::from_mapping)
        })?;

        // Persisted mapping got changed, also update our mapping view
        if let Some(new_mapping) = updated_mapping {
            let mut write_lock = RwLockUpgradableReadGuard::upgrade(read_lock);
            *write_lock = new_mapping;
        }

        Ok(is_changed)
    }

    /// Wraps `SaveOnDisk::save_to_tar`, using a different persisted type.
    pub async fn save_to_tar(
        &self,
        tar: &tar_ext::BuilderExt,
        path: impl AsRef<Path>,
    ) -> Result<(), Error> {
        self.persisted.save_to_tar(tar, path).await
    }
}

impl Deref for SaveOnDiskShardKeyMappingWrapper {
    type Target = RwLock<ShardKeyMapping>;

    fn deref(&self) -> &Self::Target {
        &self.mapping
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum ShardKeyMappingWrapper {
    Old(ShardKeyMapping),
    New(Vec<NewShardKeyMapping>),
}

impl Default for ShardKeyMappingWrapper {
    fn default() -> Self {
        Self::Old(Default::default())
    }
}

impl ShardKeyMappingWrapper {
    fn from_mapping(mapping: ShardKeyMapping) -> Self {
        if mapping.keys().any(|key| matches!(key, ShardKey::Number(_))) {
            Self::New(
                mapping
                    .into_iter()
                    .map(|(key, shard_ids)| NewShardKeyMapping { key, shard_ids })
                    .collect(),
            )
        } else {
            Self::Old(mapping)
        }
    }

    fn into_mapping(self) -> ShardKeyMapping {
        match self {
            ShardKeyMappingWrapper::Old(mapping) => mapping,
            ShardKeyMappingWrapper::New(mappings) => mappings
                .into_iter()
                .map(|mapping| (mapping.key, mapping.shard_ids))
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct NewShardKeyMapping {
    /// Shard key
    ///
    /// The key is persisted as untagged variant. The JSON value gives us enough information
    /// however to distinguish between a shard key number and string.
    key: ShardKey,
    /// Associalted shard IDs.
    shard_ids: HashSet<ShardId>,
}
