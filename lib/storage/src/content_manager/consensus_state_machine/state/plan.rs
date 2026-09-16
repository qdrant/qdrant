use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use collection::collection::vector_name_schema;
use collection::collection_state::ShardInfo;
use collection::config::ShardingMethod;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::types::PeerMetadata;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::{ReshardKey, ReshardState, ReshardingStage};
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::transfer::ShardTransferKey;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_state_machine::{
    Action, NodeContext, TransferOutcome, apply_collection_config_diffs,
};
use crate::content_manager::toc::apply_alias_actions;

type Actions = Vec<Action>;

#[derive(Clone, Copy, Debug, Default)]
struct AbortReshardingScope {
    skip_replica: Option<(ShardId, PeerId)>,
    skip_transfer: Option<ShardTransferKey>,
}

impl ClusterState {
    /// One action: `Collection::new` saves the config as its last step, and a collection whose
    /// config is missing does not load, so creation is atomic already.
    ///
    /// The config is resolved here, from the operation and node-local defaults, because
    /// `TableOfContent::create_collection` resolves it before it writes anything.
    pub fn plan_create_collection(
        &self,
        context: &NodeContext,
        op: &CreateCollectionOperation,
    ) -> StorageResult<Actions> {
        let collection = &op.collection_name;

        if self.has_collection(collection) {
            return Err(StorageError::already_exists(format!(
                "Collection `{collection}` already exists!"
            )));
        }

        if let Some(max_collections) = context.max_collections
            && self.collections.len() >= max_collections
        {
            return Err(StorageError::bad_request(format!(
                "Can't create collection with name {collection}. \
                 Max collections limit reached: {max_collections}",
            )));
        }

        if self.aliases.get(collection).is_some() {
            return Err(StorageError::bad_input(format!(
                "Can't create collection with name {collection}. \
                 Alias with the same name already exists",
            )));
        }

        let distribution = context.shard_distribution(op);
        let config = context.collection_config(&op.create_collection, distribution.len())?;

        // Every replica of a new collection starts `Initializing`, and the peer that has a local
        // one proposes `SetShardReplicaState` once the shard is built
        let shards = distribution
            .into_iter()
            .map(|(shard_id, peers)| {
                let replicas = peers
                    .into_iter()
                    .map(|peer_id| (peer_id, ReplicaState::Initializing))
                    .collect();

                (shard_id, ShardInfo { replicas })
            })
            .collect();

        let state = collection_state::State {
            config,
            shards,
            resharding: None,
            transfers: Default::default(),
            // Shard keys are set up after the collection is created
            shards_key_mapping: Default::default(),
            payload_index_schema: Default::default(),
        };

        Ok(vec![Action::CreateCollection {
            collection: collection.clone(),
            state: Box::new(state),
        }])
    }

    pub fn plan_delete_collection(&self, op: &DeleteCollectionOperation) -> Actions {
        let DeleteCollectionOperation(collection) = op;

        // Collection name is *not* resolved through aliases, `DeleteCollection` must name existing
        // collection directly
        let remove: BTreeSet<_> = self.aliases.collection_aliases(collection).collect();

        // Remove aliases first, then collection itself.
        // Either order is fine, this one simply follows the order `ToC::delete_collection` uses.
        let mut actions = Actions::new();

        // Collection without aliases does not produce an empty `UpdateAliases` action
        // (similar to `plan_change_aliases`)
        if !remove.is_empty() {
            actions.push(Action::UpdateAliases {
                set: Default::default(),
                remove,
            });
        }

        // Produce `DropCollection` action, even if collection does not exist:
        // it removes leftover aliases and storage directory
        actions.push(Action::DropCollection {
            collection: collection.clone(),
        });

        actions
    }

    pub fn plan_update_collection(&self, op: &UpdateCollectionOperation) -> StorageResult<Actions> {
        // TODO:
        //
        // `shard_replica_changes` is unimplemented, because it depends on
        // `Transfer::Abort`/`Resharding::Abort`.
        //
        // If `shard_replica_changes` is set, `plan_collection_meta` returns `NotCovered`
        // instead of calling `plan_update_collection`.

        let UpdateCollectionOperation {
            collection_name,
            update_collection,
            shard_replica_changes: _,
        } = op;

        let collection = self.resolve_collection(collection_name)?;

        // Validate operation by updating a copy of the config,
        // so that `plan` and `apply_action` are always in sync.
        //
        // `ToC::update_collection` checks the operation with the same call, against a copy of
        // the config it applies nothing to on rejection.
        let mut config = self
            .collection(&collection)
            .expect("collection exists")
            .config
            .clone();

        // One action per diff `apply_collection_config_diffs` applies.
        //
        // Every diff kind is idempotent, except `Metadata` on a collection that has none.
        //
        // The first apply saves the whole payload as-is, `null`s included, because there is
        // nothing to merge it into. A replay then merges the payload into what the first apply
        // saved, and a merge *drops* every key set to `null`.
        //
        // E.g., take `{"a": 1, "b": null}` on a collection without metadata.
        //
        // The first apply saves it whole, leaving `{"a": 1, "b": null}`.
        // A replay merges it into itself, leaving `{"a": 1}`.
        //
        // `replay_may_diverge` in `tests/replay.rs` exempts it.

        let planned = apply_collection_config_diffs(&mut config, update_collection)?
            .into_iter()
            .map(|diff| Action::UpdateCollectionConfig {
                collection: collection.clone(),
                diff: Box::new(diff),
            })
            .collect();

        Ok(planned)
    }

    pub fn plan_create_named_vector(&self, op: &CreateNamedVector) -> StorageResult<Actions> {
        let CreateNamedVector {
            collection_name,
            vector_name,
            config,
        } = op;

        let collection = self.resolve_collection(collection_name)?;

        // Reject vector that already exists with different config.
        //
        // Validate by adding vector to the config, so that `plan` and `apply_action`
        // are always in sync.

        let mut params = self
            .collection(&collection)
            .expect("collection exists")
            .config
            .params
            .clone();

        vector_name_schema::add_vector_to_config(&mut params, vector_name, config)?;

        Ok(vec![Action::AddNamedVector {
            collection,
            vector_name: vector_name.clone(),
            config: Box::new(config.clone()),
        }])
    }

    pub fn plan_delete_named_vector(&self, op: &DeleteNamedVector) -> StorageResult<Actions> {
        let DeleteNamedVector {
            collection_name,
            vector_name,
        } = op;

        let collection = self.resolve_collection(collection_name)?;

        // Deleting vector that does not exist is a no-op, not an error

        Ok(vec![Action::DropNamedVector {
            collection,
            vector_name: vector_name.clone(),
        }])
    }

    pub fn plan_change_aliases(&self, op: &ChangeAliasesOperation) -> StorageResult<Actions> {
        let ChangeAliasesOperation { actions } = op;

        // Validate all `actions` before emitting anything, and emit a single `UpdateAliases`,
        // which the applier writes in one go. So either all `actions` apply, or none of them do.
        //
        // `ToC::update_aliases` runs the same actions against a copy of the mapping it saves
        // once, and owns the function both call.

        let mut aliases = self.aliases.clone();

        apply_alias_actions(&mut aliases, actions, |collection| {
            self.has_collection(collection)
        })?;

        let set: BTreeMap<_, _> = aliases
            .iter()
            .filter(|(alias, collection)| self.aliases.get(alias) != Some(collection))
            .map(|(alias, collection)| (alias.clone(), collection.clone()))
            .collect();

        let remove: BTreeSet<_> = self
            .aliases
            .iter()
            .map(|(alias, _)| alias)
            .filter(|alias| aliases.get(alias).is_none())
            .cloned()
            .collect();

        if set.is_empty() && remove.is_empty() {
            return Ok(Actions::new());
        }

        Ok(vec![Action::UpdateAliases { set, remove }])
    }

    pub fn plan_create_payload_index(&self, op: &CreatePayloadIndex) -> StorageResult<Actions> {
        let CreatePayloadIndex {
            collection_name,
            field_name,
            field_schema,
        } = op;

        let collection = self.resolve_collection(collection_name)?;

        Ok(vec![Action::SetPayloadIndex {
            collection,
            field_name: field_name.clone(),
            field_schema: field_schema.clone(),
        }])
    }

    pub fn plan_drop_payload_index(&self, op: &DropPayloadIndex) -> StorageResult<Actions> {
        let DropPayloadIndex {
            collection_name,
            field_name,
        } = op;

        let collection = self.resolve_collection(collection_name)?;

        Ok(vec![Action::DropPayloadIndex {
            collection,
            field_name: field_name.clone(),
        }])
    }

    pub fn plan_create_shard_key(
        &self,
        context: &NodeContext,
        op: &CreateShardKey,
    ) -> StorageResult<Actions> {
        let CreateShardKey {
            collection_name,
            shard_key,
            placement,
            initial_state,
        } = op;

        let collection = self.resolve_collection(collection_name)?;
        let collection_state = self.collection(&collection).expect("collection exists");

        let sharding_method = collection_state
            .config
            .params
            .sharding_method
            .unwrap_or_default();

        if sharding_method != ShardingMethod::Custom {
            return Err(StorageError::bad_request(format!(
                "Shard Key {shard_key} cannot be created with Auto sharding method"
            )));
        }

        if collection_state.shards_key_mapping.contains_key(shard_key) {
            return Err(StorageError::bad_request(format!(
                "Shard key {shard_key} already exists"
            )));
        }

        // TODO: Check that nested *replica* placement lists are not empty (e.g., `[[], [], []]`)

        if placement.is_empty() {
            return Err(StorageError::bad_request(format!(
                "Shard key {shard_key} placement cannot be empty"
            )));
        }

        let unknown_peers: Vec<_> = placement
            .iter()
            .flatten()
            .filter(|peer_id| !self.peer_address_by_id.contains_key(peer_id))
            .collect();

        if !unknown_peers.is_empty() {
            return Err(StorageError::bad_request(format!(
                "Shard Key {shard_key} placement contains unknown peers: {unknown_peers:?}"
            )));
        }

        let max_id = collection_state
            .shards_key_mapping
            .iter_shard_ids()
            .max()
            .unwrap_or(0);

        let base_id = max_id + 1;

        let init_state = initial_state.unwrap_or_else(|| {
            if context.is_distributed
                && self.all_peers_at_version(&CREATE_CUSTOM_SHARDS_IN_INITIALIZING_STATE)
            {
                ReplicaState::Initializing
            } else {
                ReplicaState::Active
            }
        });

        let shards: Vec<_> = placement
            .iter()
            .enumerate()
            .map(|(idx, replicas)| (base_id + idx as ShardId, replicas.clone(), init_state))
            .collect();

        let mut actions: Actions = shards
            .iter()
            .map(|&(shard_id, ref replicas, init_state)| {
                // inhibit rustfmt
                Action::CreateShard {
                    collection: collection.clone(),
                    shard_id,
                    shard_key: Some(shard_key.clone()),
                    replicas: replicas.clone(),
                    init_state,
                }
            })
            .collect();

        actions.push(Action::RegisterShards {
            collection,
            shard_key: Some(shard_key.clone()),
            shards,
        });

        Ok(actions)
    }

    pub fn plan_drop_shard_key(&self, op: &DropShardKey) -> StorageResult<Actions> {
        let DropShardKey {
            collection_name,
            shard_key,
        } = op;

        let collection = self.resolve_collection(collection_name)?;
        let collection_state = self.collection(&collection).expect("collection exists");

        let sharding_method = collection_state
            .config
            .params
            .sharding_method
            .unwrap_or_default();

        if sharding_method != ShardingMethod::Custom {
            return Err(StorageError::bad_request(format!(
                "shard key {shard_key} cannot be removed with Auto sharding method"
            )));
        }

        let Some(shard_ids) = collection_state.shards_key_mapping.get(shard_key) else {
            return Ok(Actions::new());
        };

        let mut shard_ids: Vec<_> = shard_ids.iter().copied().collect();
        shard_ids.sort_unstable();

        let mut actions = vec![
            Action::InvalidateCleanLocalShards {
                collection: collection.clone(),
                shard_ids: shard_ids.clone(),
            },
            Action::RemoveShardKey {
                collection: collection.clone(),
                shard_key: shard_key.clone(),
            },
        ];

        actions.extend(shard_ids.into_iter().map(|shard_id| Action::DropShard {
            collection: collection.clone(),
            shard_id,
        }));

        Ok(actions)
    }

    pub fn plan_resharding(
        &self,
        context: &NodeContext,
        collection_name: &str,
        op: &ReshardingOperation,
    ) -> StorageResult<Actions> {
        let collection = self.resolve_collection(collection_name)?;

        if !context.is_distributed {
            return Err(StorageError::service_error(
                "Can't handle resharding, this is a single node deployment",
            ));
        }

        match op {
            ReshardingOperation::Start(key) => self.plan_start_resharding(collection, key),

            ReshardingOperation::CommitRead(key) => {
                self.plan_commit_read_resharding(collection, key)
            }

            ReshardingOperation::CommitWrite(key) => {
                self.plan_commit_write_resharding(collection, key)
            }

            ReshardingOperation::Finish(key) => self.plan_finish_resharding(collection, key),

            ReshardingOperation::Abort(key) => self.plan_abort_resharding(
                context,
                collection,
                key,
                false,
                AbortReshardingScope::default(),
            ),
        }
    }

    fn plan_start_resharding(
        &self,
        collection: String,
        key: &ReshardKey,
    ) -> StorageResult<Actions> {
        let state = self.collection(&collection).expect("collection exists");

        let sharding_method = state.config.params.sharding_method.unwrap_or_default();

        match (sharding_method, &key.shard_key) {
            (ShardingMethod::Auto, Some(shard_key)) => {
                return Err(StorageError::bad_request(format!(
                    "cannot specify shard key {shard_key} on collection with auto sharding",
                )));
            }

            (ShardingMethod::Custom, None) => {
                return Err(StorageError::bad_request(
                    "must specify shard key on collection with custom sharding",
                ));
            }

            (ShardingMethod::Auto, None) | (ShardingMethod::Custom, Some(_)) => {}
        }

        if let Some(current) = &state.resharding
            && !current.matches(key)
        {
            return Err(StorageError::bad_request(format!(
                "another resharding is in progress:\n{current:#?}"
            )));
        }

        let is_new_resharding = state.resharding.is_none();

        if is_new_resharding && key.direction == ReshardingDirection::Down {
            let shard_ids = shard_ids_for_key(state, key.shard_key.as_ref());

            if shard_ids.len() <= 1 {
                return Err(StorageError::bad_request(format!(
                    "cannot remove shard {} by resharding down, it is the last shard",
                    key.shard_id,
                )));
            }

            if !state.shards.contains_key(&key.shard_id) {
                return Err(StorageError::bad_request(format!(
                    "shard holder does not contain shard {} replica set",
                    key.shard_id,
                )));
            }
        }

        let mut actions = Actions::new();

        if key.direction == ReshardingDirection::Up && !state.shards.contains_key(&key.shard_id) {
            actions.push(Action::CreateShard {
                collection: collection.clone(),
                shard_id: key.shard_id,
                shard_key: key.shard_key.clone(),
                replicas: vec![key.peer_id],
                init_state: ReplicaState::Resharding,
            });

            actions.push(Action::RegisterShards {
                collection: collection.clone(),
                shard_key: key.shard_key.clone(),
                shards: vec![(key.shard_id, vec![key.peer_id], ReplicaState::Resharding)],
            });
        }

        if is_new_resharding {
            let resharding = ReshardState::new(
                key.uuid,
                key.direction,
                key.peer_id,
                key.shard_id,
                key.shard_key.clone(),
            );

            actions.push(Action::SetReshardingState {
                collection: collection.clone(),
                state: Some(resharding),
            });
        }

        if key.direction == ReshardingDirection::Up && sharding_method == ShardingMethod::Auto {
            let shard_number = key
                .shard_id
                .checked_add(1)
                .and_then(NonZeroU32::new)
                .expect("cannot have more than u32::MAX shards after resharding");

            if state.config.params.shard_number != shard_number {
                actions.push(Action::SetShardNumber {
                    collection,
                    shard_number,
                });
            }
        }

        Ok(actions)
    }

    fn plan_commit_read_resharding(
        &self,
        collection: String,
        key: &ReshardKey,
    ) -> StorageResult<Actions> {
        let state = self.collection(&collection).expect("collection exists");

        let Some(resharding) = &state.resharding else {
            return Ok(Actions::new());
        };

        if resharding.matches(key) && resharding.stage >= ReshardingStage::ReadHashRingCommitted {
            return Ok(Actions::new());
        }

        check_resharding_state(resharding, key, ReshardingStage::MigratingPoints)?;

        let shard_ids = match key.direction {
            ReshardingDirection::Up => shard_ids_for_key(state, key.shard_key.as_ref())
                .into_iter()
                .filter(|&shard_id| shard_id != key.shard_id)
                .collect(),

            ReshardingDirection::Down => vec![key.shard_id],
        };

        Ok(vec![
            Action::SetReshardingStage {
                collection: collection.clone(),
                stage: ReshardingStage::ReadHashRingCommitted,
            },
            Action::InvalidateCleanLocalShards {
                collection,
                shard_ids,
            },
        ])
    }

    fn plan_commit_write_resharding(
        &self,
        collection: String,
        key: &ReshardKey,
    ) -> StorageResult<Actions> {
        let state = self.collection(&collection).expect("collection exists");

        let Some(resharding) = &state.resharding else {
            return Ok(Actions::new());
        };

        if resharding.matches(key) && resharding.stage >= ReshardingStage::WriteHashRingCommitted {
            return Ok(Actions::new());
        }

        check_resharding_state(resharding, key, ReshardingStage::ReadHashRingCommitted)?;

        Ok(vec![Action::SetReshardingStage {
            collection,
            stage: ReshardingStage::WriteHashRingCommitted,
        }])
    }

    fn plan_finish_resharding(
        &self,
        collection: String,
        key: &ReshardKey,
    ) -> StorageResult<Actions> {
        let state = self.collection(&collection).expect("collection exists");

        let Some(resharding) = &state.resharding else {
            return Ok(Actions::new());
        };

        check_resharding_state(resharding, key, ReshardingStage::WriteHashRingCommitted)?;

        let mut actions = Actions::new();

        if key.direction == ReshardingDirection::Down {
            if let Some(shard_key) = &key.shard_key
                && state
                    .shards_key_mapping
                    .get(shard_key)
                    .is_some_and(|shard_ids| shard_ids.contains(&key.shard_id))
            {
                actions.push(Action::RemoveShardFromKeyMapping {
                    collection: collection.clone(),
                    shard_id: key.shard_id,
                    shard_key: shard_key.clone(),
                });
            }

            let is_auto_sharding =
                state.config.params.sharding_method.unwrap_or_default() == ShardingMethod::Auto;

            if is_auto_sharding {
                let shard_number = NonZeroU32::new(key.shard_id)
                    .expect("cannot have zero shards after finishing resharding down");

                if state.config.params.shard_number != shard_number {
                    actions.push(Action::SetShardNumber {
                        collection: collection.clone(),
                        shard_number,
                    });
                }
            }

            if state.shards.contains_key(&key.shard_id) {
                actions.push(Action::DropShard {
                    collection: collection.clone(),
                    shard_id: key.shard_id,
                });
            }
        }

        actions.push(Action::SetReshardingState {
            collection,
            state: None,
        });

        Ok(actions)
    }

    fn plan_abort_resharding(
        &self,
        context: &NodeContext,
        collection: String,
        key: &ReshardKey,
        force: bool,
        scope: AbortReshardingScope,
    ) -> StorageResult<Actions> {
        let state = self.collection(&collection).expect("collection exists");

        if !force {
            let Some(resharding) = &state.resharding else {
                return Ok(Actions::new());
            };

            if !resharding.matches(key) {
                return Ok(Actions::new());
            }

            if resharding.stage >= ReshardingStage::ReadHashRingCommitted {
                return Err(StorageError::bad_request(format!(
                    "can't abort resharding {key}, because read hash ring has been committed \
                     already, resharding must be completed",
                )));
            }
        }

        let shard_ids = match key.direction {
            ReshardingDirection::Up => vec![key.shard_id],
            ReshardingDirection::Down => shard_ids_for_key(state, key.shard_key.as_ref()),
        };

        let mut actions = vec![Action::InvalidateCleanLocalShards {
            collection: collection.clone(),
            shard_ids,
        }];

        if key.direction == ReshardingDirection::Down {
            for (&shard_id, shard) in &state.shards {
                if shard_id == key.shard_id
                    || !shard_belongs_to_key(state, shard_id, key.shard_key.as_ref())
                {
                    continue;
                }

                for (&peer_id, &replica_state) in &shard.replicas {
                    if replica_state.is_resharding()
                        && scope.skip_replica != Some((shard_id, peer_id))
                    {
                        actions.push(Action::SetReplicaState {
                            collection: collection.clone(),
                            shard_id,
                            peer_id,
                            state: ReplicaState::Active,
                        });
                    }
                }
            }

            actions.push(Action::DeleteMigratedPoints {
                collection: collection.clone(),
                key: key.clone(),
            });
        }

        actions.push(Action::RevertHashRing {
            collection: collection.clone(),
            key: key.clone(),
        });

        if key.direction == ReshardingDirection::Up {
            let is_auto_sharding =
                state.config.params.sharding_method.unwrap_or_default() == ShardingMethod::Auto;

            if is_auto_sharding {
                let shard_number = NonZeroU32::new(key.shard_id)
                    .expect("cannot have zero shards after aborting resharding up");

                if state.config.params.shard_number != shard_number {
                    actions.push(Action::SetShardNumber {
                        collection: collection.clone(),
                        shard_number,
                    });
                }
            }

            if state.shards.contains_key(&key.shard_id) {
                actions.push(Action::DropShard {
                    collection: collection.clone(),
                    shard_id: key.shard_id,
                });
            }
        }

        let mut transfers: Vec<_> = state
            .transfers
            .iter()
            .filter(|transfer| {
                transfer.is_related_to_resharding(key)
                    && scope.skip_transfer != Some(transfer.key())
            })
            .collect();

        transfers.sort_by_key(|transfer| {
            let key = transfer.key();
            (key.shard_id, key.to_shard_id, key.from, key.to)
        });

        for transfer in transfers {
            let transfer_key = transfer.key();

            actions.push(Action::StopTransferDriver {
                collection: collection.clone(),
                key: transfer_key,
            });

            if context.peer_id == transfer.from {
                actions.push(Action::RevertProxyShard {
                    collection: collection.clone(),
                    shard_id: transfer.shard_id,
                });
            }

            actions.push(Action::UnregisterTransfer {
                collection: collection.clone(),
                key: transfer_key,
                outcome: TransferOutcome::Abort,
            });
        }

        actions.push(Action::SetReshardingState {
            collection,
            state: None,
        });

        Ok(actions)
    }

    pub fn plan_update_peer_metadata(&self, peer_id: PeerId, metadata: &PeerMetadata) -> Actions {
        // Check if operation is already applied
        if self.peer_metadata_by_id.get(&peer_id) == Some(metadata) {
            return Actions::new();
        }

        vec![Action::SetPeerMetadata {
            peer_id,
            metadata: metadata.clone(),
        }]
    }

    pub fn plan_update_cluster_metadata(&self, key: &str, value: &serde_json::Value) -> Actions {
        let current = self.cluster_metadata.get(key);

        // Check if operation is already applied
        let applied = match value.is_null() {
            true => current.is_none(),
            false => current == Some(value),
        };

        if applied {
            return Actions::new();
        }

        vec![Action::SetClusterMetadataKey {
            key: key.into(),
            value: value.clone(),
        }]
    }

    pub fn plan_set_quota_config(&self, &config: &QuotaConfig) -> Actions {
        // `QuotaManager::set_config` additionally clears exceeded-quota flags,
        // so we always emit the action, even if the config is the same
        vec![Action::SetQuotaConfig { config }]
    }
}

fn check_resharding_state(
    state: &ReshardState,
    key: &ReshardKey,
    expected_stage: ReshardingStage,
) -> StorageResult<()> {
    if !state.matches(key) {
        return Err(StorageError::bad_request(format!(
            "another resharding is in progress:\n{state:#?}"
        )));
    }

    if state.stage != expected_stage {
        return Err(StorageError::bad_request(format!(
            "expected resharding stage {expected_stage:?}, got {:?}",
            state.stage,
        )));
    }

    Ok(())
}

fn shard_ids_for_key(
    state: &collection::collection_state::State,
    shard_key: Option<&segment::types::ShardKey>,
) -> Vec<ShardId> {
    let mut shard_ids: Vec<_> = match shard_key {
        Some(shard_key) => state
            .shards_key_mapping
            .get(shard_key)
            .into_iter()
            .flatten()
            .copied()
            .collect(),
        None => state.shards.keys().copied().collect(),
    };
    shard_ids.sort_unstable();
    shard_ids
}

fn shard_belongs_to_key(
    state: &collection::collection_state::State,
    shard_id: ShardId,
    shard_key: Option<&segment::types::ShardKey>,
) -> bool {
    match shard_key {
        Some(shard_key) => state
            .shards_key_mapping
            .get(shard_key)
            .is_some_and(|shard_ids| shard_ids.contains(&shard_id)),
        None => true,
    }
}
