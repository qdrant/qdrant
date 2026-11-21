//! Adding, removing and loading shards, and maintaining shard key mappings and hash rings.

use std::collections::{HashMap, HashSet};
use std::debug_assert_matches;
use std::ops::Deref as _;
use std::path::Path;
use std::sync::Arc;

use ahash::AHashMap;
use common::budget::ResourceBudget;
use common::save_on_disk::SaveOnDisk;
use fs_err::tokio as tokio_fs;
use futures::{StreamExt, stream};
use itertools::Itertools;
use segment::types::ShardKey;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use super::shard_mapping::ShardKeyMapping;
use super::{ShardHolder, shard_removal};
use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::config::{CollectionConfigInternal, ShardingMethod};
use crate::hash_ring::HashRingRouter;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState, ShardReplicaSet};
use crate::shards::resharding::ReshardingStage;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::{CollectionId, check_shard_path, shard_initializing_flag_path};

impl ShardHolder {
    /// Set the shard key mappings
    ///
    /// # Warning
    ///
    /// This does not update the shard key inside replica sets. If the shard key mapping changes
    /// and we have existing replica sets, they must be updated as well to reflect the changed
    /// mappings.
    pub fn set_shard_key_mappings(
        &mut self,
        shard_key_mapping: ShardKeyMapping,
    ) -> CollectionResult<()> {
        let shard_id_to_key_mapping = shard_key_mapping.shard_id_to_shard_key();

        self.key_mapping
            .write_optional(move |_| Some(shard_key_mapping))?;

        self.shard_id_to_key_mapping = shard_id_to_key_mapping;

        Ok(())
    }

    pub async fn drop_and_remove_shard(&mut self, shard_id: ShardId) -> CollectionResult<()> {
        // A process crash after saving the reduced shard count or key mapping can leave
        // shard files behind. Those shards are not loaded after restart, so replay may
        // skip their cleanup.
        if let Some(replica_set) = self.shards.get(&shard_id) {
            shard_removal::remove_shard(replica_set).await?;
            self.shards.remove(&shard_id);
        }
        Ok(())
    }

    pub fn remove_shard_from_key_mapping(
        &mut self,
        shard_id: ShardId,
        shard_key: &ShardKey,
    ) -> CollectionResult<()> {
        // Idempotent: only rewrite the mapping if the shard id is actually
        // present under this key. A replay after a successful removal finds
        // nothing to remove and skips the unnecessary disk write.
        self.key_mapping.write_optional(|key_mapping| {
            let shard_ids = key_mapping.get(shard_key)?;
            if !shard_ids.contains(&shard_id) {
                return None;
            }

            let mut key_mapping = key_mapping.clone();
            key_mapping.get_mut(shard_key).unwrap().remove(&shard_id);
            Some(key_mapping)
        })?;
        self.shard_id_to_key_mapping.remove(&shard_id);

        Ok(())
    }

    /// ## Cancel safety
    ///
    /// This function is **not** cancel safe.
    pub async fn add_shard(
        &mut self,
        shard_id: ShardId,
        shard: ShardReplicaSet,
        shard_key: Option<ShardKey>,
    ) -> CollectionResult<()> {
        self.add_shards(vec![(shard_id, shard)], shard_key).await
    }

    /// Add batch of shards to shard holder and atomically update shard key mapping.
    ///
    /// ## Cancel safety
    ///
    /// This function is **not** cancel safe.
    pub async fn add_shards(
        &mut self,
        shards: Vec<(ShardId, ShardReplicaSet)>,
        shard_key: Option<ShardKey>,
    ) -> CollectionResult<()> {
        if shards.is_empty() {
            return Ok(());
        }

        // Persist mapping first: it is the only fallible step, so a failed write leaves
        // in-memory state untouched.
        if let Some(shard_key) = &shard_key {
            let result = self
                .key_mapping
                .write_optional(|mapping| {
                    let mut mapping = mapping.clone();
                    let shard_ids = mapping.entry(shard_key.clone()).or_default();

                    let mut changed = false;

                    for &(shard_id, _) in &shards {
                        changed |= shard_ids.insert(shard_id);
                    }

                    if changed { Some(mapping) } else { None }
                })
                .map_err(CollectionError::from);

            if let Err(error) = result {
                // Replay rebuilds these directories, so their workers must stop before retry
                for (_, shard) in shards {
                    shard.stop_gracefully().await;
                }

                return Err(error);
            }
        }

        let ring = self
            .rings
            .entry(shard_key.clone())
            .or_insert_with(HashRingRouter::single);

        for &(shard_id, _) in &shards {
            ring.add(shard_id);
        }

        for (shard_id, shard) in shards {
            let evicted = self.shards.insert(shard_id, Arc::new(shard));

            if let Some(evicted) = evicted {
                debug_assert!(false, "Overwriting existing shard id {shard_id}");
                evicted.stop_gracefully().await;
            }

            if let Some(shard_key) = &shard_key {
                self.shard_id_to_key_mapping
                    .insert(shard_id, shard_key.clone());
            }
        }

        Ok(())
    }

    /// Remove the key's routing and persisted mapping, leaving shards for separate cleanup
    pub fn remove_shard_key_mapping(&mut self, shard_key: &ShardKey) -> CollectionResult<()> {
        self.key_mapping.write_optional(|key_mapping| {
            if !key_mapping.contains_key(shard_key) {
                return None;
            }

            let mut key_mapping = key_mapping.clone();
            key_mapping.remove(shard_key);
            Some(key_mapping)
        })?;

        self.rings.remove(&Some(shard_key.clone()));
        self.shard_id_to_key_mapping
            .retain(|_, key| key != shard_key);

        Ok(())
    }

    fn rebuild_rings(&mut self) {
        let mut rings = match self.sharding_method {
            // With auto sharding, we have a single hash ring
            ShardingMethod::Auto => HashMap::from([(None, HashRingRouter::single())]),
            // With custom sharding, we have a hash ring per shard key
            ShardingMethod::Custom => HashMap::new(),
        };

        // Add shards and shard keys
        let ids_to_key = self.get_shard_id_to_key_mapping();
        for shard_id in self.shards.keys() {
            let shard_key = ids_to_key.get(shard_id).cloned();
            debug_assert_matches!(
                (self.sharding_method, &shard_key),
                (ShardingMethod::Auto, None) | (ShardingMethod::Custom, Some(_)),
                "auto sharding cannot have shard key, custom sharding must have shard key ({:?}, {shard_key:?})",
                self.sharding_method,
            );
            rings
                .entry(shard_key)
                .or_insert_with(HashRingRouter::single)
                .add(*shard_id);
        }

        // Restore resharding hash ring if resharding is active and haven't reached
        // `WriteHashRingCommitted` stage yet
        if let Some(state) = self.resharding_state.read().deref() {
            let ring = rings
                .get_mut(&state.shard_key)
                .expect("must have hash ring for current resharding shard key");

            ring.start_resharding(state.shard_id, state.direction);

            if state.stage >= ReshardingStage::WriteHashRingCommitted {
                ring.commit_resharding();
            }
        }

        self.rings = rings;
    }

    pub async fn apply_shards_state(
        &mut self,
        shard_ids: HashSet<ShardId>,
        shard_key_mapping: ShardKeyMapping,
        extra_shards: AHashMap<ShardId, ShardReplicaSet>,
    ) -> CollectionResult<()> {
        for (extra_shard_id, extra_shard) in extra_shards {
            let evicted = self.shards.insert(extra_shard_id, Arc::new(extra_shard));
            if let Some(evicted) = evicted {
                evicted.stop_gracefully().await;
            }
        }

        let all_shard_ids: Vec<ShardId> = self.shards.keys().copied().collect();

        self.set_shard_key_mappings(shard_key_mapping)?;

        for shard_id in all_shard_ids {
            if !shard_ids.contains(&shard_id) {
                self.drop_and_remove_shard(shard_id).await?;
            }
        }

        self.rebuild_rings();

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load_shards(
        &mut self,
        collection_path: &Path,
        collection_id: &CollectionId,
        collection_config: Arc<RwLock<CollectionConfigInternal>>,
        effective_optimizers_config: OptimizersConfig,
        shared_storage_config: Arc<SharedStorageConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        channel_service: ChannelService,
        on_peer_failure: ChangePeerFromState,
        abort_shard_transfer: AbortShardTransfer,
        this_peer_id: PeerId,
        update_runtime: Handle,
        search_runtime: AdaptiveSearchHandle,
        optimizer_resource_budget: ResourceBudget,
    ) {
        let shard_number = collection_config.read().await.params.shard_number.get();

        let (shard_ids_list, shard_id_to_key_mapping) = match self.sharding_method {
            ShardingMethod::Auto => {
                let ids_list = (0..shard_number).collect::<Vec<_>>();
                let shard_id_to_key_mapping = AHashMap::new();
                (ids_list, shard_id_to_key_mapping)
            }
            ShardingMethod::Custom => {
                let shard_id_to_key_mapping = self.get_shard_id_to_key_mapping();
                let ids_list = shard_id_to_key_mapping
                    .keys()
                    .cloned()
                    .sorted()
                    .collect::<Vec<_>>();
                (ids_list, shard_id_to_key_mapping.clone())
            }
        };

        // Concurrently load all shards.
        let shard_id_to_key_mapping_clone = shard_id_to_key_mapping.clone();
        let shard_futures = shard_ids_list.into_iter().map(|shard_id| {
            // Check if shard is fully initialized on disk
            // The initialization flag should be absent for a well-formed replica set
            let initializing_flag = shard_initializing_flag_path(collection_path, shard_id).clone();
            let collection_id = collection_id.clone();
            let collection_config = collection_config.clone();
            let effective_optimizers_config = effective_optimizers_config.clone();
            let shared_storage_config = shared_storage_config.clone();
            let payload_index_schema = payload_index_schema.clone();
            let channel_service = channel_service.clone();
            let on_peer_failure = on_peer_failure.clone();
            let abort_shard_transfer = abort_shard_transfer.clone();
            let update_runtime = update_runtime.clone();
            let search_runtime = search_runtime.clone();
            let shard_key = shard_id_to_key_mapping_clone.get(&shard_id).cloned();
            let optimizer_resource_budget = optimizer_resource_budget.clone();
            async move {
                let shard_key_for_add = shard_key.clone();

                // Check if shard is fully initialized on disk
                let is_dirty_shard = tokio_fs::try_exists(&initializing_flag)
                    .await
                    .unwrap_or(false);

                // Validate that shard exists on disk
                let shard_path = check_shard_path(collection_path, shard_id)
                    .await
                    .expect("Failed to check shard path");

                // Load replica set
                let replica_set = ShardReplicaSet::load(
                    shard_id,
                    shard_key,
                    collection_id,
                    &shard_path,
                    is_dirty_shard,
                    collection_config,
                    effective_optimizers_config,
                    shared_storage_config,
                    payload_index_schema,
                    channel_service,
                    on_peer_failure,
                    abort_shard_transfer,
                    this_peer_id,
                    update_runtime,
                    search_runtime,
                    optimizer_resource_budget,
                )
                .await;

                (replica_set, shard_key_for_add)
            }
        });
        let mut shard_stream = stream::iter(shard_futures).buffer_unordered(
            shared_storage_config
                .load_concurrency_config
                .get_concurrent_shards()
                .get(),
        );
        while let Some((replica_set, shard_key)) = shard_stream.next().await {
            // Change local shards stuck in Initializing state to Active
            let local_peer_id = replica_set.this_peer_id();
            let not_distributed = !shared_storage_config.is_distributed;
            let is_local =
                replica_set.this_peer_id() == local_peer_id && replica_set.is_local().await;
            let is_initializing =
                replica_set.peer_state(local_peer_id) == Some(ReplicaState::Initializing);
            if not_distributed && is_local && is_initializing {
                log::warn!(
                    "Local shard {collection_id}:{} stuck in Initializing state, changing to Active",
                    replica_set.shard_id,
                );
                replica_set
                    .set_replica_state(local_peer_id, ReplicaState::Active)
                    .await
                    .expect("Failed to set local shard state");
            }
            self.add_shard(replica_set.shard_id, replica_set, shard_key)
                .await
                .unwrap();
        }

        // If resharding, rebuild the hash rings because they'll be messed up
        if self.resharding_state.read().is_some() {
            self.rebuild_rings();
        }
    }
}

#[cfg(test)]
mod shard_key_mapping_tests {
    use std::collections::HashSet;

    use fs_err as fs;
    use segment::types::ShardKey;

    use crate::config::ShardingMethod;
    use crate::hash_ring::HashRingRouter;
    use crate::shards::shard_holder::shard_mapping::ShardKeyMapping;
    use crate::shards::shard_holder::{SHARD_KEY_MAPPING_FILE, ShardHolder};

    #[test]
    fn remove_shard_key_mapping_save_failure_retry() {
        let dir = tempfile::tempdir().unwrap();
        let mut holder = ShardHolder::new(dir.path(), ShardingMethod::Custom).unwrap();
        let key = ShardKey::from("tenant");
        let mut mapping = ShardKeyMapping::default();
        mapping.insert(key.clone(), HashSet::from([1, 2]));
        holder.set_shard_key_mappings(mapping.clone()).unwrap();
        holder
            .rings
            .insert(Some(key.clone()), HashRingRouter::single());

        // A directory at the destination makes the atomic rename fail
        let path = dir.path().join(SHARD_KEY_MAPPING_FILE);
        let backup = dir.path().join("mapping-backup");
        fs::rename(&path, &backup).unwrap();
        fs::create_dir(&path).unwrap();

        assert!(holder.remove_shard_key_mapping(&key).is_err());
        assert_eq!(holder.get_shard_key_to_ids_mapping(), mapping);
        assert_eq!(
            holder.get_shard_id_to_key_mapping(),
            &mapping.shard_id_to_shard_key()
        );
        assert!(holder.rings.contains_key(&Some(key.clone())));

        fs::remove_dir(&path).unwrap();
        fs::rename(&backup, &path).unwrap();
        holder.remove_shard_key_mapping(&key).unwrap();

        assert!(holder.get_shard_key_to_ids_mapping().is_empty());
        assert!(holder.get_shard_id_to_key_mapping().is_empty());
        assert!(holder.rings.is_empty());
        let persisted = ShardHolder::new(dir.path(), ShardingMethod::Custom).unwrap();
        assert!(persisted.get_shard_key_to_ids_mapping().is_empty());
    }

    #[test]
    fn remove_shard_key_mapping_empty_and_absent() {
        let dir = tempfile::tempdir().unwrap();
        let mut holder = ShardHolder::new(dir.path(), ShardingMethod::Custom).unwrap();
        let key = ShardKey::from("tenant");
        let mut mapping = ShardKeyMapping::default();
        mapping.insert(key.clone(), HashSet::new());
        holder.set_shard_key_mappings(mapping).unwrap();
        holder
            .rings
            .insert(Some(key.clone()), HashRingRouter::single());

        for _ in 0..2 {
            holder.remove_shard_key_mapping(&key).unwrap();

            assert!(holder.get_shard_key_to_ids_mapping().is_empty());
            assert!(holder.rings.is_empty());
        }
    }
}
