use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use common::flags::feature_flags;
use common::tar_ext;
use itertools::Itertools;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use segment::types::ShardKey;
use serde::{Deserialize, Serialize};

use crate::save_on_disk::{Error, SaveOnDisk};
use crate::shards::shard::ShardId;

/// Shard key mapping type
///
/// # Warning
///
/// This type is unstable when serializing! It is recommended to use [`ShardKeyMappingWrapper`]
/// instead, which will fully replace this type in a future version.
pub type ShardKeyMapping = HashMap<ShardKey, HashSet<ShardId>>;

/// A `SaveOnDisk`-like structure for the shard key mapping
///
/// Hold the shard key mapping and persists in a different format to disk.
/// See [`ShardKeyMappingWrapper`].
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
/// Bug: <https://github.com/qdrant/qdrant/pull/6209>
#[derive(Serialize, Deserialize, Clone, Debug, Eq)]
#[serde(untagged)]
pub enum ShardKeyMappingWrapper {
    /// The `Old` format is the original format from when shard key mappings were implemented
    // TODO(1.15): either fully remove support for the old format, or keep it a bit longer
    // TODO(1.15): if removing the old format, change back to regular SaveOnDisk<T> type
    Old(ShardKeyMapping),
    /// The `New` format is a more robust format, properly persisting shard key numbers
    #[allow(private_interfaces)]
    New(Vec<NewShardKeyMapping>),
}

impl ShardKeyMappingWrapper {
    /// Convert shard key mappings into key to shard IDs map
    pub fn to_map(&self) -> ShardKeyMapping {
        ShardKeyMapping::from(self.clone())
    }

    /// Get a mapping of all shards and their key
    pub fn shards(&self) -> HashMap<ShardId, ShardKey> {
        match self {
            ShardKeyMappingWrapper::Old(mapping) => mapping
                .iter()
                .flat_map(|(shard_key, shard_ids)| {
                    shard_ids
                        .iter()
                        .map(|shard_id| (*shard_id, shard_key.clone()))
                })
                .collect(),
            ShardKeyMappingWrapper::New(mappings) => mappings
                .iter()
                .flat_map(|mapping| {
                    mapping
                        .shard_ids
                        .iter()
                        .map(|shard_id| (*shard_id, mapping.key.clone()))
                })
                .collect(),
        }
    }

    /// Get list of shard keys
    pub fn keys(&self) -> Vec<ShardKey> {
        match self {
            ShardKeyMappingWrapper::Old(mapping) => mapping.keys().cloned().collect(),
            ShardKeyMappingWrapper::New(mappings) => {
                // Collect into hash set first to deduplicate
                let keys = mappings
                    .iter()
                    .map(|mapping| &mapping.key)
                    .collect::<HashSet<_>>();
                keys.into_iter().cloned().collect()
            }
        }
    }

    /// Iterate over all shard IDs from the mappings
    pub fn iter_shard_ids<'a>(&'a self) -> Box<dyn Iterator<Item = ShardId> + 'a> {
        match self {
            ShardKeyMappingWrapper::Old(mapping) => Box::new(
                mapping
                    .values()
                    .flat_map(|shard_ids| shard_ids.iter().copied()),
            ),
            ShardKeyMappingWrapper::New(mappings) => Box::new(
                mappings
                    .iter()
                    .flat_map(|mapping| mapping.shard_ids.iter().copied()),
            ),
        }
    }

    /// Return all shard IDs from the mappings
    pub fn shard_ids(&self) -> Vec<ShardId> {
        let ids = self.iter_shard_ids().collect::<Vec<_>>();
        debug_assert!(
            ids.iter().all_unique(),
            "shard mapping contains duplicate shard IDs",
        );
        ids
    }

    /// Get the shard IDs for a specific shard key
    pub fn get(&self, shard_key: &ShardKey) -> Option<HashSet<ShardId>> {
        match self {
            ShardKeyMappingWrapper::Old(mapping) => mapping.get(shard_key).cloned(),
            ShardKeyMappingWrapper::New(mappings) => {
                let shards: HashSet<ShardId> = mappings
                    .iter()
                    .filter(|mapping| &mapping.key == shard_key)
                    .flat_map(|mapping| &mapping.shard_ids)
                    .copied()
                    .collect();
                (!shards.is_empty()).then_some(shards)
            }
        }
    }

    /// Get the shard key for a given shard ID
    ///
    /// `None` is returned if the shard ID has no key, or if the shard ID is unknown
    pub fn get_key(&self, shard_id: ShardId) -> Option<ShardKey> {
        match self {
            ShardKeyMappingWrapper::Old(mapping) => mapping
                .iter()
                .find(|(_, shard_ids)| shard_ids.contains(&shard_id))
                .map(|(key, _)| key.clone()),
            ShardKeyMappingWrapper::New(mappings) => mappings
                .iter()
                .find(|mapping| mapping.shard_ids.contains(&shard_id))
                .map(|mapping| mapping.key.clone()),
        }
    }

    /// Whether this shard key mapping contains a specific key.
    pub fn contains_key(&self, shard_key: &ShardKey) -> bool {
        match self {
            ShardKeyMappingWrapper::Old(mapping) => mapping.contains_key(shard_key),
            ShardKeyMappingWrapper::New(mappings) => {
                mappings.iter().any(|mapping| &mapping.key == shard_key)
            }
        }
    }
}

impl PartialEq for ShardKeyMappingWrapper {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Old mappings we can compare directly
            (ShardKeyMappingWrapper::Old(a), ShardKeyMappingWrapper::Old(b)) => a == b,
            // New mappings and asymmetric mappings must be compared in a stable format
            (a, b) => {
                let a = ShardKeyMapping::from(a.clone());
                let b = ShardKeyMapping::from(b.clone());
                a == b
            }
        }
    }
}

impl Default for ShardKeyMappingWrapper {
    fn default() -> Self {
        let use_new_format = feature_flags().use_new_shard_key_mapping_format;
        if use_new_format {
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

        let use_new_format = feature_flags().use_new_shard_key_mapping_format;
        if use_new_format || any_number {
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct NewShardKeyMapping {
    /// Shard key
    ///
    /// The key is persisted as untagged variant. The JSON value gives us enough information
    /// however to distinguish between a shard key number and string.
    key: ShardKey,
    /// Associalted shard IDs.
    shard_ids: HashSet<ShardId>,
}
