use std::collections::{HashMap, HashSet};
use std::ops;

use ahash::AHashMap;
use itertools::Itertools;
use segment::types::ShardKey;
use serde::{Deserialize, Serialize};

use crate::shards::shard::ShardId;

/// Shard key mapping type
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(from = "SerdeHelper", into = "SerdeHelper")]
pub struct ShardKeyMapping {
    shard_key_to_shard_ids: HashMap<ShardKey, HashSet<ShardId>>,
}

impl ops::Deref for ShardKeyMapping {
    type Target = HashMap<ShardKey, HashSet<ShardId>>;

    fn deref(&self) -> &Self::Target {
        &self.shard_key_to_shard_ids
    }
}

impl ops::DerefMut for ShardKeyMapping {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.shard_key_to_shard_ids
    }
}

impl ShardKeyMapping {
    /// Get an inverse mapping, all shard IDs and their key
    pub fn shard_id_to_shard_key(&self) -> AHashMap<ShardId, ShardKey> {
        self.shard_key_to_shard_ids
            .iter()
            .flat_map(|(shard_key, shard_ids)| {
                shard_ids
                    .iter()
                    .map(|shard_id| (*shard_id, shard_key.clone()))
            })
            .collect()
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

    /// Iterate over all shard IDs from the mappings
    pub fn iter_shard_ids(&self) -> impl Iterator<Item = ShardId> {
        self.shard_key_to_shard_ids
            .values()
            .flat_map(|shard_ids| shard_ids.iter().copied())
    }

    /// Get the shard key for a given shard ID
    ///
    /// `None` is returned if the shard ID has no key, or if the shard ID is unknown
    pub fn shard_key(&self, shard_id: ShardId) -> Option<ShardKey> {
        self.shard_key_to_shard_ids
            .iter()
            .find(|(_, shard_ids)| shard_ids.contains(&shard_id))
            .map(|(key, _)| key.clone())
    }
}

impl From<SerdeHelper> for ShardKeyMapping {
    fn from(helper: SerdeHelper) -> Self {
        Self {
            shard_key_to_shard_ids: helper.0.into_iter().map(KeyIdsPair::into_parts).collect(),
        }
    }
}

/// Helper structure for persisting shard key mapping in safe format
///
/// Bug: <https://github.com/qdrant/qdrant/pull/5838>
#[derive(Deserialize, Serialize)]
struct SerdeHelper(Vec<KeyIdsPair>);

impl From<ShardKeyMapping> for SerdeHelper {
    fn from(mapping: ShardKeyMapping) -> Self {
        let key_ids_pairs = mapping
            .shard_key_to_shard_ids
            .into_iter()
            .map(KeyIdsPair::from)
            .collect();
        Self(key_ids_pairs)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct KeyIdsPair {
    /// Shard key
    ///
    /// The key is persisted as untagged variant. The JSON value gives us enough information
    /// however to distinguish between a shard key number and string.
    key: ShardKey,
    /// Associalted shard IDs.
    shard_ids: HashSet<ShardId>,
}

impl KeyIdsPair {
    fn into_parts(self) -> (ShardKey, HashSet<ShardId>) {
        let Self { key, shard_ids } = self;
        (key, shard_ids)
    }
}

impl From<(ShardKey, HashSet<ShardId>)> for KeyIdsPair {
    fn from((key, shard_ids): (ShardKey, HashSet<ShardId>)) -> Self {
        Self { key, shard_ids }
    }
}
