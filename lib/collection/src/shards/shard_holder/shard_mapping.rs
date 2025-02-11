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

/// Whether to use the new format to persist shard keys
///
/// The old format fails to persist shard key numbers correctly, converting them into strings on
/// load. While this is false, the new format is only used if any shard key is a number.
// TODO(1.14): set to true, remove other branches in code, and remove this constant
const USE_NEW_SHARD_KEY_MAPPING_FORMAT: bool = false;

/// A `SaveOnDisk`-like structure for the shard key mapping
///
/// Hold the shard key mapping and persists in a different format to disk.
/// See [`ShardKeyMappingWrapper`].
pub(super) struct SaveOnDiskShardKeyMappingWrapper {
    /// Persist mapping in a robust format that doesn't lose shard key type information
    persisted: SaveOnDisk<ShardKeyMappingWrapper>,
    /// Shard key mapping view in original format
    mapping: RwLock<ShardKeyMapping>,
}

impl SaveOnDiskShardKeyMappingWrapper {
    /// Wraps `SaveOnDisk::load_or_init_default`, using a different persisted type.
    pub fn load_or_init_default(path: impl Into<PathBuf>) -> Result<Self, Error> {
        let persisted: SaveOnDisk<ShardKeyMappingWrapper> = SaveOnDisk::load_or_init_default(path)?;
        let mapping = ShardKeyMapping::from(persisted.read().clone());
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
        let mapping_read = self.mapping.upgradable_read();

        let mut updated_mapping = None;

        let is_changed = self.persisted.write_optional(|mapping| {
            let persisted_mapping = f(&ShardKeyMapping::from(mapping.clone()));

            // If persisted mapping will be changed, keep to to update our mapping view
            if let Some(ref mapping) = persisted_mapping {
                updated_mapping.replace(mapping.clone());
            }

            persisted_mapping.map(ShardKeyMappingWrapper::from)
        })?;

        // Persisted mapping got changed, also update our mapping view
        if let Some(new_mapping) = updated_mapping {
            let mut mapping_write = RwLockUpgradableReadGuard::upgrade(mapping_read);
            *mapping_write = new_mapping;
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

/// A wrapper type for persisting the shard key mapping
///
/// This type supports two different persisted formats:
///
/// - The `Old` format is the original format from when shard key mappings were implemented.
/// - The `New` format is a more robust, properly persisting shard key numbers.
///
/// The old format is problematic because it does not persist shard key numbers properly. On load,
/// shard key numbers would be converted into shard key strings breaking shard key mappings.
///
/// This type functions as a compatibility layer between the two different persisted formats.
///
/// Bug: <https://github.com/qdrant/qdrant/pull/5838>
#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum ShardKeyMappingWrapper {
    /// The `Old` format is the original format from when shard key mappings were implemented
    // TODO(1.15): either fully remove support for the old format, or keep it a bit longer
    // TODO(1.15): if removing the old format, change back to regular SaveOnDisk<T> type
    Old(ShardKeyMapping),
    /// The `New` format is a more robust format, properly persisting shard key numbers
    New(Vec<NewShardKeyMapping>),
}

impl Default for ShardKeyMappingWrapper {
    fn default() -> Self {
        if USE_NEW_SHARD_KEY_MAPPING_FORMAT {
            Self::New(Default::default())
        } else {
            Self::Old(Default::default())
        }
    }
}

impl From<ShardKeyMapping> for ShardKeyMappingWrapper {
    fn from(mapping: ShardKeyMapping) -> Self {
        // If any shard key is a number, always prefer new persisted format
        // The old format is broken and fails to deserialize shard key numbers
        let any_number = mapping.keys().any(|key| matches!(key, ShardKey::Number(_)));

        if USE_NEW_SHARD_KEY_MAPPING_FORMAT || any_number {
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
}

impl From<ShardKeyMappingWrapper> for ShardKeyMapping {
    fn from(mapping: ShardKeyMappingWrapper) -> Self {
        match mapping {
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
