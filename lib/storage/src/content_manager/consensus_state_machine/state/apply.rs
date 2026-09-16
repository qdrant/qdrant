use collection::collection::vector_name_schema;
use collection::collection_state::ShardInfo;

use super::*;

impl ClusterState {
    /// Apply one action. Cannot fail.
    pub fn apply_action(&mut self, action: &Action) {
        match action {
            Action::CreateCollection { collection, state } => {
                self.collections
                    .insert(collection.clone(), (**state).clone());
            }

            // Missing collection is legal here: the applier also deletes the directory
            // a collection that failed to load leaves behind
            Action::DropCollection { collection } => {
                self.collections.remove(collection);
            }

            Action::UpdateCollectionConfig { collection, diff } => {
                let Some(state) = self.collection_mut(collection) else {
                    return;
                };

                // Planning validates using the same function, so it should never fail here
                if let Err(err) = diff.apply(&mut state.config) {
                    debug_assert!(false, "rejected config diff reached the state: {err}");
                    log::error!("Failed to update config of {collection}: {err}");
                }
            }

            Action::AddNamedVector {
                collection,
                vector_name,
                config,
            } => {
                let Some(state) = self.collection_mut(collection) else {
                    return;
                };

                // Planning validates using the same function, so it should never fail here
                let res = vector_name_schema::add_vector_to_config(
                    &mut state.config.params,
                    vector_name,
                    config,
                );

                if let Err(err) = res {
                    debug_assert!(false, "rejected named vector reached the state: {err}");
                    log::error!("Failed to add named vector {vector_name} to {collection}: {err}");
                }
            }

            Action::DropNamedVector {
                collection,
                vector_name,
            } => {
                let Some(state) = self.collection_mut(collection) else {
                    return;
                };

                vector_name_schema::remove_vector_from_config(
                    &mut state.config.params,
                    vector_name,
                );
            }

            Action::SetPayloadIndex {
                collection,
                field_name,
                field_schema,
            } => {
                let Some(state) = self.collection_mut(collection) else {
                    return;
                };

                state
                    .payload_index_schema
                    .schema
                    .insert(field_name.clone(), field_schema.clone());
            }

            Action::DropPayloadIndex {
                collection,
                field_name,
            } => {
                let Some(state) = self.collection_mut(collection) else {
                    return;
                };

                state.payload_index_schema.schema.remove(field_name);
            }

            // Builds a shard directory. Shards join collection state through `RegisterShards`.
            Action::CreateShard { .. } => {}

            Action::RegisterShards {
                collection,
                shard_key,
                shards,
            } => {
                let Some(state) = self.collection_mut(collection) else {
                    return;
                };

                for &(shard_id, ref peers, init_state) in shards {
                    let replicas = peers.iter().map(|&peer_id| (peer_id, init_state)).collect();
                    state.shards.insert(shard_id, ShardInfo { replicas });
                }

                if let Some(shard_key) = shard_key {
                    state
                        .shards_key_mapping
                        .entry(shard_key.clone())
                        .or_default()
                        .extend(shards.iter().map(|&(shard_id, _, _)| shard_id));
                }
            }

            // Stops node-local tasks and does not change consensus state
            Action::InvalidateCleanLocalShards { .. } => {}

            Action::RemoveShardKey {
                collection,
                shard_key,
            } => {
                let Some(state) = self.collection_mut(collection) else {
                    return;
                };

                let Some(shard_ids) = state.shards_key_mapping.remove(shard_key) else {
                    return;
                };

                // The mapping is removed before the shard directories are deleted. If the node
                // crashes between those steps, Qdrant ignores the leftover directories during
                // startup because their shards are no longer in the mapping.
                //
                // Remove the shards from modeled state here to match the state after restart.
                // Replaying the operation then has nothing left to do.
                for shard_id in shard_ids {
                    state.shards.remove(&shard_id);
                }
            }

            Action::DropShard {
                collection,
                shard_id,
            } => {
                let Some(state) = self.collection_mut(collection) else {
                    return;
                };

                state.shards.remove(shard_id);
            }

            Action::UpdateAliases { set, remove } => {
                for alias in remove {
                    self.aliases.remove(alias);
                }

                for (alias, collection) in set {
                    self.aliases.insert(alias.clone(), collection.clone());
                }
            }

            Action::SetPeerMetadata { peer_id, metadata } => {
                self.peer_metadata_by_id.insert(*peer_id, metadata.clone());
            }

            Action::SetClusterMetadataKey { key, value } => {
                if value.is_null() {
                    self.cluster_metadata.remove(key);
                } else {
                    self.cluster_metadata.insert(key.clone(), value.clone());
                }
            }

            Action::SetQuotaConfig { config } => {
                self.quota_config = *config;
            }

            // Sleep on a peer, or fail at random. Neither changes the state.
            #[cfg(feature = "staging")]
            Action::TestSlowDown(_) | Action::TestTransientError(_) => {}
        }
    }

    /// State of the collection an action changes.
    ///
    /// An action that changes a collection is never planned against a state without it,
    /// so debug builds assert.
    fn collection_mut(&mut self, collection: &str) -> Option<&mut collection_state::State> {
        let state = self.collections.get_mut(collection);

        debug_assert!(
            state.is_some(),
            "action targets collection {collection}, which is not in the state",
        );

        state
    }
}
