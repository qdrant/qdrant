use std::collections::{HashMap, HashSet};
use std::ops;

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
    pub fn shard_id_to_shard_key(&self) -> HashMap<ShardId, ShardKey> {
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
        let shard_key_to_shard_ids = match helper {
            SerdeHelper::New(key_ids_pairs) => key_ids_pairs
                .into_iter()
                .map(KeyIdsPair::into_parts)
                .collect(),

            SerdeHelper::Old(key_ids_map) => key_ids_map,
        };

        Self {
            shard_key_to_shard_ids,
        }
    }
}

/// Helper structure for persisting shard key mapping
///
/// The original format of persisting shard key mappings as hash map is broken. It forgets type
/// information for the shard key, which resulted in shard key numbers to be converted into
/// strings.
///
/// Bug: <https://github.com/qdrant/qdrant/pull/5838>
#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum SerdeHelper {
    New(Vec<KeyIdsPair>),
    // TODO(1.15): remove this old format, deployment should exclusively be using new format
    Old(HashMap<ShardKey, HashSet<ShardId>>),
}

impl From<ShardKeyMapping> for SerdeHelper {
    fn from(mapping: ShardKeyMapping) -> Self {
        let key_ids_pairs = mapping
            .shard_key_to_shard_ids
            .into_iter()
            .map(KeyIdsPair::from)
            .collect();
        Self::New(key_ids_pairs)
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
