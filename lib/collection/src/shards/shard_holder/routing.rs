//! Routing of operations and read requests to shards: hash ring splitting, shard selectors
//! and shard key fallback routing.

use std::collections::HashSet;
use std::sync::Arc;

use api::rest::ShardKeyWithFallback;
use segment::types::ShardKey;

use super::{ShardHolder, shard_not_found_error};
use crate::config::ShardingMethod;
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::{OperationToShard, SplitByShard};
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::resharding::ReshardingStage;
use crate::shards::shard::ShardId;

impl ShardHolder {
    pub fn split_by_shard<O: SplitByShard + Clone>(
        &self,
        operation: O,
        shard_keys_selection: &Option<ShardKey>,
    ) -> CollectionResult<Vec<(&Arc<ShardReplicaSet>, O)>> {
        let Some(hashring) = self.rings.get(&shard_keys_selection.clone()) else {
            return if let Some(shard_key) = shard_keys_selection {
                Err(CollectionError::bad_input(format!(
                    "Shard key {shard_key} not found"
                )))
            } else {
                Err(CollectionError::bad_input(
                    "Shard key not specified".to_string(),
                ))
            };
        };

        if hashring.is_empty() {
            return Err(CollectionError::bad_input(
                "No shards found for shard key".to_string(),
            ));
        }

        let operation_to_shard = operation.split_by_shard(hashring);
        let shard_ops: Vec<_> = match operation_to_shard {
            OperationToShard::ByShard(by_shard) => by_shard
                .into_iter()
                .map(|(shard_id, operation)| (self.shards.get(&shard_id).unwrap(), operation))
                .collect(),
            OperationToShard::ToAll(operation) => {
                if let Some(shard_key) = shard_keys_selection {
                    let shard_ids = self
                        .key_mapping
                        .read()
                        .get(shard_key)
                        .cloned()
                        .unwrap_or_default();
                    shard_ids
                        .into_iter()
                        .map(|shard_id| (self.shards.get(&shard_id).unwrap(), operation.clone()))
                        .collect()
                } else {
                    self.all_shards()
                        .map(|shard| (shard, operation.clone()))
                        .collect()
                }
            }
        };
        Ok(shard_ops)
    }

    pub fn get_shard_ids_by_key(&self, shard_key: &ShardKey) -> CollectionResult<HashSet<ShardId>> {
        match self.key_mapping.read().get(shard_key).cloned() {
            None => Err(CollectionError::bad_request(format!(
                "Shard key {shard_key} not found"
            ))),
            Some(ids) => Ok(ids),
        }
    }

    pub fn select_shards<'a>(
        &'a self,
        shard_selector: &'a ShardSelectorInternal,
    ) -> CollectionResult<Vec<(&'a Arc<ShardReplicaSet>, Option<&'a ShardKey>)>> {
        let mut res = Vec::new();

        let filter_resharding = match shard_selector {
            ShardSelectorInternal::Empty => {
                debug_assert!(false, "Do not expect empty shard selector");
                false
            }
            ShardSelectorInternal::All => {
                let is_custom_sharding = match self.sharding_method {
                    ShardingMethod::Auto => false,
                    ShardingMethod::Custom => true,
                };

                for (&shard_id, shard) in self.shards.iter() {
                    // Technically, we could skip inactive shards regardless of sharding method,
                    // as we do not expect that shard id can even become inactive on all replicas.
                    // (if it happens, means there is a bug)
                    // But for earlier detection of such issues, we only do this check for custom sharding,
                    // where this situation is expected for the case of tenant promotion.
                    if is_custom_sharding && !shard.shard_is_active() {
                        continue;
                    }

                    let shard_key = self.shard_id_to_key_mapping.get(&shard_id);
                    res.push((shard, shard_key));
                }

                true
            }
            ShardSelectorInternal::ShardKey(shard_key) => {
                for shard_id in self.get_shard_ids_by_key(shard_key)? {
                    if let Some(replica_set) = self.shards.get(&shard_id) {
                        res.push((replica_set, Some(shard_key)));
                    } else {
                        debug_assert!(false, "Shard id {shard_id} not found")
                    }
                }

                true
            }
            ShardSelectorInternal::ShardKeys(shard_keys) => {
                for shard_key in shard_keys {
                    for shard_id in self.get_shard_ids_by_key(shard_key)? {
                        if let Some(replica_set) = self.shards.get(&shard_id) {
                            res.push((replica_set, Some(shard_key)));
                        } else {
                            debug_assert!(false, "Shard id {shard_id} not found")
                        }
                    }
                }

                true
            }
            ShardSelectorInternal::ShardKeyWithFallback(key) => {
                let (shard_ids_to_query, used_shard_key) =
                    self.route_with_fallback_for_read(key)?;

                log::trace!("Search routing with fallback: {used_shard_key:?}");

                for shard_id in shard_ids_to_query {
                    if let Some(replica_set) = self.shards.get(&shard_id) {
                        res.push((replica_set, Some(used_shard_key)));
                    } else {
                        debug_assert!(false, "Shard id {shard_id} not found")
                    }
                }

                true
            }
            ShardSelectorInternal::ShardId(shard_id) => {
                if let Some(replica_set) = self.shards.get(shard_id) {
                    res.push((replica_set, self.shard_id_to_key_mapping.get(shard_id)));
                } else {
                    return Err(shard_not_found_error(*shard_id));
                }

                // Exempt from resharding filter. This selector is used by internal per-shard
                // operations, such as the resharding driver reading back migrated points from the
                // new shard, which must be able to reach the shard before it becomes visible to
                // user-facing selectors
                false
            }
        };

        // Filter out shards that must not be queried while resharding, regardless of how they
        // were selected above
        if filter_resharding && let Some(state) = self.resharding_state.read().as_ref() {
            res.retain(|(shard, _)| {
                if state.shard_id != shard.shard_id {
                    return true;
                }

                // Ignore a new resharding shard until it completed point migration
                // The shard will be marked as active at the end of the migration stage
                let resharding_migrating_up = state.direction == ReshardingDirection::Up
                    && state.stage < ReshardingStage::ReadHashRingCommitted;

                // Skip shard being removed by resharding down once the write
                // hash ring is committed. The shard is logically gone at this
                // point; querying it on a remote peer that already applied
                // `finish_resharding` would return a "shard not found" error.
                let resharding_removing_down = state.direction == ReshardingDirection::Down
                    && state.stage >= ReshardingStage::WriteHashRingCommitted;

                !resharding_migrating_up && !resharding_removing_down
            });
        }

        Ok(res)
    }

    /// Common routing logic for reads when using ShardKeyWithFallback
    ///
    /// Example routing:
    ///
    /// request: {"target": "key1", "fallback": "default"}
    ///
    /// Situation 1:
    /// /// - key1 -> shard_ids {1, 2} (both active)
    /// Request is routed to shard_ids {1, 2} of target key1
    ///
    /// Situation 2:
    /// /// - key1 -> no shards found
    /// Request is routed to shard_ids of fallback key "default"
    ///
    /// Situation 3:
    /// /// - key1 -> shard_ids {1} and it is in Partial state (no active replicas)
    /// Request is routed to shard_ids of fallback key "default"
    ///
    /// Situation 4:
    /// /// - key1 -> shard_ids {1, 2} (shard 1 active, shard 2 partial)
    /// Request is routed to shard_ids of fallback key "default"
    ///
    ///
    /// If at least one of target shards is Active, use target shard. If not, redirect to fallback shard
    pub fn route_with_fallback_for_read<'a>(
        &self,
        key: &'a ShardKeyWithFallback,
    ) -> CollectionResult<(HashSet<ShardId>, &'a ShardKey)> {
        let mut shard_key_to_ids_mapping = self.get_shard_key_to_ids_mapping();

        let target_shard_ids = shard_key_to_ids_mapping.remove(&key.target);
        let fallback_shard_ids = shard_key_to_ids_mapping.remove(&key.fallback);

        if let Some(target_shard_ids) = target_shard_ids {
            let target_replicas = target_shard_ids
                .iter()
                .filter_map(|shard_id| self.shards.get(shard_id))
                .collect::<Vec<_>>();

            let target_shards_active = target_replicas
                .iter()
                .all(|replica_set| !replica_set.readable_shards().is_empty());

            if !target_replicas.is_empty() && target_shards_active {
                // 1st condition is required to handle empty shard keys (2nd one returns true)
                Ok((target_shard_ids, &key.target))
            } else if let Some(fallback_shard_ids) = fallback_shard_ids {
                Ok((fallback_shard_ids, &key.fallback))
            } else {
                Err(CollectionError::shard_unavailable(format!(
                    "Neither target shard key {} nor fallback shard key {} have active replicas",
                    key.target, key.fallback
                )))
            }
        } else if let Some(fallback_shard_ids) = fallback_shard_ids {
            Ok((fallback_shard_ids, &key.fallback))
        } else {
            Err(CollectionError::not_found(format!(
                "Neither target shard key {} nor fallback shard key {} exist",
                key.target, key.fallback
            )))
        }
    }

    /// Common routing logic for writes when using ShardKeyWithFallback
    ///
    /// Similar to read routing, but in case if target shard exists, but is in Partial state, we still want
    /// to route to both target and fallback shards to ensure data consistency.
    pub fn route_with_fallback_for_write(
        &self,
        key: ShardKeyWithFallback,
    ) -> CollectionResult<Vec<(HashSet<ShardId>, ShardKey)>> {
        let ShardKeyWithFallback { target, fallback } = key;

        let (target_shard_ids, fallback_shard_ids) = {
            let key_mapping = self.key_mapping.read();
            let target_shard_ids = key_mapping.get(&target).cloned();
            // No distinct fallback if it is the same as the target
            let fallback_shard_ids = (fallback != target)
                .then(|| key_mapping.get(&fallback).cloned())
                .flatten();
            (target_shard_ids, fallback_shard_ids)
        };

        if let Some(target_shard_ids) = target_shard_ids {
            let target_replicas = target_shard_ids
                .iter()
                .filter_map(|shard_id| self.shards.get(shard_id))
                .collect::<Vec<_>>();

            // Check that at least one active replica per shard exists
            let target_shards_active = target_replicas
                .iter()
                .all(|replica_set| replica_set.has_active_shard());

            if target_replicas.is_empty() {
                return if let Some(fallback_shard_ids) = fallback_shard_ids {
                    Ok(vec![(fallback_shard_ids, fallback)])
                } else {
                    Err(CollectionError::not_found(format!(
                        "Neither target shard key {target} nor fallback shard key {fallback} exist",
                    )))
                };
            }

            if target_shards_active {
                // 1st condition is required to handle empty shard keys (2nd one returns true)
                Ok(vec![(target_shard_ids, target)])
            } else if let Some(fallback_shard_ids) = fallback_shard_ids {
                // target is not active, but it can be in Partial state, so we need extra check

                // Target:
                // Shard_id 1 -> replicas: A (Partial)
                // Shard_id 2 -> replicas: B (Active)
                // In this case target is still receiving updates. We need to fallback.

                // Target:
                // Shard_id 1 -> replicas: A (ActiveRead)
                // Shard_id 2 -> replicas: B (Active)
                // We need to send to both target and fallback to ensure consistency.

                // Target:
                // Shard_id 1 -> replicas: A (Partial) B (Active)
                // This is not possible, as we checked for active shards above

                // Target:
                // Shard_id 1 -> replicas: A (Partial) B (Dead)
                // This is not possible, as we never deactivate last active replica

                // Target:
                // Shard_id 1 -> replicas: A (ActiveRead) B (Dead)
                // This is not possible, as we never deactivate last active replica

                // Target:
                // Shard_id 1 -> replicas: A (ActiveRead)
                // We need to send to both target and fallback to ensure consistency.

                // Target:
                // Shard_id 1 -> replicas: A (Dead)
                // Can be, if transfer failed. We just fallback.

                let is_all_replicas_in_read_active = target_replicas.iter().any(|replica_set| {
                    replica_set.check_peers_state_all(|state| state == ReplicaState::ActiveRead)
                });
                if is_all_replicas_in_read_active {
                    Ok(vec![
                        (target_shard_ids, target),
                        (fallback_shard_ids, fallback),
                    ])
                } else {
                    Ok(vec![(fallback_shard_ids, fallback)])
                }
            } else {
                Err(CollectionError::shard_unavailable(format!(
                    "Neither target shard key {target} nor fallback shard key {fallback} have active replicas",
                )))
            }
        } else if let Some(fallback_shard_ids) = fallback_shard_ids {
            Ok(vec![(fallback_shard_ids, fallback)])
        } else {
            Err(CollectionError::not_found(format!(
                "Neither target shard key {target} nor fallback shard key {fallback} exist",
            )))
        }
    }
}
