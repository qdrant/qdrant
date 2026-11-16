pub mod recovery_guard;
mod resharding;
mod routing;
mod shard_management;
mod snapshots;
mod transfers;
pub(crate) use resharding::ReshardingCheck;
pub mod shard_mapping;
pub mod shared_shard_holder;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use ahash::AHashMap;
use common::save_on_disk::SaveOnDisk;
use futures::{StreamExt, stream};
use segment::json_path::JsonPath;
use segment::types::{PayloadFieldSchema, ShardKey};
use shard_mapping::ShardKeyMapping;
use tokio::sync::broadcast;

pub use self::shared_shard_holder::*;
use super::resharding::ReshardState;
use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::common::collection_size_stats::CollectionSizeStats;
use crate::config::ShardingMethod;
use crate::hash_ring::HashRingRouter;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::recovery_guard::ActiveRecoveries;
use crate::shards::transfer::{ShardTransfer, ShardTransferKey};

const SHARD_TRANSFERS_FILE: &str = "shard_transfers";
const RESHARDING_STATE_FILE: &str = "resharding_state.json";
pub const SHARD_KEY_MAPPING_FILE: &str = "shard_key_mapping.json";

pub struct ShardHolder {
    /// `BTreeMap` for deterministic iteration order by `ShardId` — iteration is externally
    /// observable (fan-out, telemetry, consensus state apply).
    ///
    /// Values are `Arc` so consumers can clone a handle, drop the outer
    /// `ShardHolder` lock, and still operate on the shard. This matters
    /// especially for searches: holding the `ShardHolder` read lock across
    /// every `core_search` await would block writers (e.g. shard creation)
    /// for the duration of the search.
    shards: BTreeMap<ShardId, Arc<ShardReplicaSet>>,
    pub(crate) shard_transfers: Arc<SaveOnDisk<HashSet<ShardTransfer>>>,
    pub(crate) shard_transfer_changes: broadcast::Sender<ShardTransferChange>,
    pub(crate) resharding_state: SaveOnDisk<Option<ReshardState>>,
    /// Hash rings per shard key
    ///
    /// In case of auto sharding, this only hash a `None` hash ring. In case of custom sharding,
    /// this only has hash rings for defined shard keys excluding `None`.
    pub(crate) rings: HashMap<Option<ShardKey>, HashRingRouter>,
    key_mapping: SaveOnDisk<ShardKeyMapping>,
    // Duplicates the information from `key_mapping` for faster access, does not use locking
    shard_id_to_key_mapping: AHashMap<ShardId, ShardKey>,
    sharding_method: ShardingMethod,
    /// Active snapshot recoveries on this peer (destination side of transfers).
    /// Tracks progress of downloading, unpacking, and restoring snapshots.
    /// Entries are added and removed via [`ShardRecoveryGuard`].
    active_recoveries: ActiveRecoveries,
}

impl ShardHolder {
    pub async fn trigger_optimizers(&self) {
        for shard in self.shards.values() {
            shard.trigger_optimizers().await;
        }
    }

    pub fn new(collection_path: &Path, sharding_method: ShardingMethod) -> CollectionResult<Self> {
        let shard_transfers = Arc::new(SaveOnDisk::load_or_init_default(
            collection_path.join(SHARD_TRANSFERS_FILE),
        )?);
        let resharding_state: SaveOnDisk<Option<ReshardState>> =
            SaveOnDisk::load_or_init_default(collection_path.join(RESHARDING_STATE_FILE))?;

        let key_mapping: SaveOnDisk<ShardKeyMapping> =
            SaveOnDisk::load_or_init_default(collection_path.join(SHARD_KEY_MAPPING_FILE))?;

        let mut shard_id_to_key_mapping = AHashMap::new();

        for (shard_key, shard_ids) in key_mapping.read().iter() {
            for shard_id in shard_ids {
                shard_id_to_key_mapping.insert(*shard_id, shard_key.clone());
            }
        }

        let rings = match sharding_method {
            ShardingMethod::Auto => HashMap::from([(None, HashRingRouter::single())]),
            ShardingMethod::Custom => HashMap::new(),
        };

        let (shard_transfer_changes, _) = broadcast::channel(64);

        Ok(Self {
            shards: BTreeMap::new(),
            shard_transfers,
            shard_transfer_changes,
            resharding_state,
            rings,
            key_mapping,
            shard_id_to_key_mapping,
            sharding_method,
            active_recoveries: ActiveRecoveries::default(),
        })
    }

    pub async fn stop_gracefully(&mut self) {
        let futures = std::mem::take(&mut self.shards)
            .into_values()
            .map(|shard| async move { shard.stop_gracefully().await });
        futures::future::join_all(futures).await;
    }

    pub async fn save_key_mapping_to_tar(
        &self,
        tar: &common::tar_ext::BuilderExt,
    ) -> CollectionResult<()> {
        self.key_mapping
            .save_to_tar(tar, Path::new(SHARD_KEY_MAPPING_FILE))
            .await?;
        Ok(())
    }

    pub fn get_shard_id_to_key_mapping(&self) -> &AHashMap<ShardId, ShardKey> {
        &self.shard_id_to_key_mapping
    }

    pub fn get_shard_key_to_ids_mapping(&self) -> ShardKeyMapping {
        self.key_mapping.read().clone()
    }

    /// All shard keys in the shard key mapping
    pub fn shard_keys(&self) -> Vec<ShardKey> {
        self.key_mapping.read().iter_shard_keys().cloned().collect()
    }

    pub fn get_sharding_method(&self) -> ShardingMethod {
        self.sharding_method
    }

    pub fn contains_shard(&self, shard_id: ShardId) -> bool {
        self.shards.contains_key(&shard_id)
    }

    pub fn get_shard(&self, shard_id: ShardId) -> Option<&Arc<ShardReplicaSet>> {
        self.shards.get(&shard_id)
    }

    pub fn get_shards(&self) -> impl Iterator<Item = (ShardId, &Arc<ShardReplicaSet>)> {
        self.shards.iter().map(|(id, shard)| (*id, shard))
    }

    pub fn all_shards(&self) -> impl Iterator<Item = &Arc<ShardReplicaSet>> {
        self.shards.values()
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    pub fn assert_shard_exists(&self, shard_id: ShardId) -> CollectionResult<()> {
        match self.get_shard(shard_id) {
            Some(_) => Ok(()),
            None => Err(shard_not_found_error(shard_id)),
        }
    }

    async fn assert_shard_is_local(&self, shard_id: ShardId) -> CollectionResult<()> {
        let is_local_shard = self
            .is_shard_local(shard_id)
            .await
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        if is_local_shard {
            Ok(())
        } else {
            Err(CollectionError::bad_input(format!(
                "Shard {shard_id} is not a local shard"
            )))
        }
    }

    async fn assert_shard_is_local_or_queue_proxy(
        &self,
        shard_id: ShardId,
    ) -> CollectionResult<()> {
        let is_local_shard = self
            .is_shard_local_or_queue_proxy(shard_id)
            .await
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        if is_local_shard {
            Ok(())
        } else {
            Err(CollectionError::bad_input(format!(
                "Shard {shard_id} is not a local or queue proxy shard"
            )))
        }
    }

    /// Returns true if shard is explicitly local, false otherwise.
    pub async fn is_shard_local(&self, shard_id: ShardId) -> Option<bool> {
        match self.get_shard(shard_id) {
            Some(shard) => Some(shard.is_local().await),
            None => None,
        }
    }

    /// Returns true if shard is explicitly local or is queue proxy shard, false otherwise.
    pub async fn is_shard_local_or_queue_proxy(&self, shard_id: ShardId) -> Option<bool> {
        match self.get_shard(shard_id) {
            Some(shard) => Some(shard.is_local().await || shard.is_queue_proxy().await),
            None => None,
        }
    }

    /// Return a list of local shards, present on this peer
    pub async fn get_local_shards(&self) -> Vec<ShardId> {
        let mut res = Vec::with_capacity(1);
        for (shard_id, replica_set) in self.get_shards() {
            if replica_set.has_local_shard().await {
                res.push(shard_id);
            }
        }
        res
    }

    pub async fn update_payload_index_schema(&self) -> CollectionResult<()> {
        let payload_index_schema = self
            .all_shards()
            .next()
            .map(|shard| shard.payload_index_schema());

        let Some(payload_index_schema) = payload_index_schema else {
            // Shard holder contains no shards
            return Ok(());
        };

        let schema = self.common_payload_index_schema().await?;

        payload_index_schema.write(|payload_index_schema| {
            *payload_index_schema = PayloadIndexSchema { schema };
        })?;

        Ok(())
    }

    async fn common_payload_index_schema(
        &self,
    ) -> CollectionResult<HashMap<JsonPath, PayloadFieldSchema>> {
        let mut schema = HashMap::new();

        for (shard_idx, shard) in self.all_shards().enumerate() {
            let shard_schema: HashMap<_, _> = shard
                .info(true)
                .await?
                .payload_schema
                .into_iter()
                .filter_map(|(field, info)| {
                    let schema = match PayloadFieldSchema::try_from(info) {
                        Ok(schema) => schema,
                        Err(err) => {
                            log::warn!(
                                "Failed to extract payload index schema from payload index info: \
                                 {err}"
                            );

                            return None;
                        }
                    };

                    Some((field, schema))
                })
                .collect();

            if shard_idx == 0 {
                schema = shard_schema;
            } else {
                schema.retain(|field, schema| {
                    let Some(shard_schema) = shard_schema.get(field) else {
                        return false;
                    };

                    schema == shard_schema
                });
            }

            if schema.is_empty() {
                break;
            }
        }

        Ok(schema)
    }

    pub async fn remove_shards_at_peer(&self, peer_id: PeerId) -> CollectionResult<()> {
        for (_shard_id, replica_set) in self.get_shards() {
            replica_set.remove_peer(peer_id).await?;
        }
        Ok(())
    }

    /// Estimates the collections size based on local shard data. Returns `None` if no shard for the collection was found locally.
    pub async fn estimate_collection_size_stats(
        &self,
    ) -> CollectionResult<Option<CollectionSizeStats>> {
        if self.is_distributed().await {
            // In distributed, we estimate the whole collection size by using a single local shard and multiply by amount of shards in the collection.
            for shard in self.shards.iter() {
                if let Some(shard_stats) = shard.1.calculate_local_shard_stats().await? {
                    // TODO(resharding) take into account the ongoing resharding and exclude shards that are being filled from multiplication.
                    // Project the single shards size to the full collection.
                    let collection_estimate = shard_stats.multiplied_with(self.shards.len());
                    return Ok(Some(collection_estimate));
                }
            }

            return Ok(None);
        }

        // Local mode: return collection size estimations using all shards.
        let mut stats = CollectionSizeStats::default();
        for shard in self.shards.iter() {
            if let Some(shard_stats) = shard.1.calculate_local_shard_stats().await? {
                stats.accumulate_metrics_from(&shard_stats);
            }
        }

        Ok(Some(stats))
    }

    /// Returns `true` if the collection is distributed across multiple nodes.
    async fn is_distributed(&self) -> bool {
        stream::iter(self.shards.iter())
            .any(|i| async { i.1.has_remote_shard().await })
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShardTransferChange {
    Start(ShardTransfer),
    Finish(ShardTransferKey),
    Abort(ShardTransferKey),
}

pub fn shard_not_found_error(shard_id: ShardId) -> CollectionError {
    CollectionError::not_found(format!("shard {shard_id}"))
}

#[cfg(test)]
mod snapshot_preparation_tests;
