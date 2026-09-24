use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use collection::collection::vector_name_schema;
use collection::collection_state::ShardInfo;
use collection::config::ShardingMethod;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::config_diff::DiffConfig as _;
use collection::operations::types::PeerMetadata;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::{ReshardKey, ReshardState, ReshardingStage};
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::transfer::{
    ShardTransfer, ShardTransferKey, ShardTransferMethod, ShardTransferRestart,
};
use segment::types::ShardKey;
use semver::Version;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_state_machine::{
    Action, LocalShardInitMode, NodeContext, TransferOutcome, apply_collection_config_diffs,
};
use crate::content_manager::toc::apply_alias_actions;

type Actions = Vec<Action>;

#[derive(Copy, Clone, Debug, Default)]
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
            (ShardingMethod::Auto, None) => (),
            (ShardingMethod::Custom, Some(_)) => (),

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
                && let Some(shard_ids) = state.shards_key_mapping.get(shard_key)
                && shard_ids.contains(&key.shard_id)
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
                // TODO: When starting resharding, validate that auto-sharding scale-down targets
                // the last nonzero shard, so a malformed entry cannot panic here
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
                if !shard_belongs_to_key(state, shard_id, key.shard_key.as_ref()) {
                    continue;
                }

                if shard_id == key.shard_id {
                    continue;
                }

                for (&peer_id, &replica_state) in &shard.replicas {
                    if !replica_state.is_resharding() {
                        continue;
                    }

                    if scope.skip_replica == Some((shard_id, peer_id)) {
                        continue;
                    }

                    actions.push(Action::SetReplicaState {
                        collection: collection.clone(),
                        shard_id,
                        peer_id,
                        state: ReplicaState::Active,
                    });
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
                // TODO: When starting resharding, validate that auto-sharding scale-up targets
                // the next shard ID, so a malformed entry targeting shard 0 cannot panic here
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
                let is_related = transfer.is_related_to_resharding(key);
                let should_skip = scope.skip_transfer == Some(transfer.key());
                is_related && !should_skip
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

    pub fn plan_transfer(
        &self,
        context: &NodeContext,
        collection_name: &str,
        op: &ShardTransferOperations,
    ) -> StorageResult<Actions> {
        // TODO: Validate transfer keys and operations.
        //
        // `StreamRecords` may specify `to_shard_id`, with or without `filter`;
        // `filter` always requires `to_shard_id`.
        // `Snapshot` and `WalDelta` must specify neither `to_shard_id` nor `filter`.
        // `ReshardingStreamRecords` must specify `to_shard_id` and must not specify `filter`.
        //
        // `Restart` can only restart a non-resharding transfer with neither
        // `to_shard_id` nor `filter`.
        //
        // Current validation accepts some invalid combinations and rejects some valid ones.

        let collection = self.resolve_collection(collection_name)?;

        if !context.is_distributed {
            return Err(StorageError::service_error(
                "Can't handle transfer, this is a single node deployment",
            ));
        }

        match op {
            ShardTransferOperations::Start(transfer) => {
                self.plan_start_transfer(context, collection, transfer)
            }

            ShardTransferOperations::Restart(restart) => {
                self.plan_restart_transfer(context, collection, restart)
            }

            ShardTransferOperations::Finish(transfer) => {
                self.plan_finish_transfer(context, collection, transfer)
            }

            &ShardTransferOperations::SnapshotRecovered(key)
            | &ShardTransferOperations::RecoveryToPartial(key) => {
                self.plan_recovery_to_partial(collection, key)
            }

            &ShardTransferOperations::Abort {
                transfer,
                reason: _,
            } => self.plan_abort_transfer(context, collection, transfer),
        }
    }

    fn plan_start_transfer(
        &self,
        context: &NodeContext,
        collection: String,
        transfer: &ShardTransfer,
    ) -> StorageResult<Actions> {
        let state = self.collection(&collection).expect("collection exists");
        validate_transfer(transfer, &self.peer_address_by_id, state)?;

        let method = self.resolve_transfer_method(context, state, transfer)?;

        let mut transfer = transfer.clone();
        transfer.method = Some(method);

        let mut actions = vec![Action::RegisterTransfer {
            collection: collection.clone(),
            transfer: transfer.clone(),
        }];

        let destination_shard = transfer.to_shard_id.unwrap_or(transfer.shard_id);

        if context.peer_id == transfer.to {
            actions.push(Action::InitLocalShard {
                collection: collection.clone(),
                shard_id: destination_shard,
                mode: LocalShardInitMode::EnsureExists,
            });
        }

        let initial_state = initial_replica_state_for_transfer(state, method)?;

        actions.push(Action::SetReplicaState {
            collection: collection.clone(),
            shard_id: destination_shard,
            peer_id: transfer.to,
            state: initial_state,
        });

        if context.peer_id == transfer.from {
            actions.push(Action::SpawnTransferDriver {
                collection,
                transfer,
            });
        }

        Ok(actions)
    }

    fn resolve_transfer_method(
        &self,
        context: &NodeContext,
        state: &collection::collection_state::State,
        transfer: &ShardTransfer,
    ) -> StorageResult<ShardTransferMethod> {
        if let Some(method) = transfer.method {
            return Ok(method);
        }

        if self.all_peers_at_version(&Version::new(1, 18, 0)) {
            return Err(StorageError::service_error(format!(
                "Shard transfer {}:{} -> {} has no method set; the coordinating peer must pick a \
                 transfer method before submitting to consensus",
                transfer.shard_id, transfer.from, transfer.to,
            )));
        }

        let optimizers = match &context.optimizers_overwrite {
            Some(overwrite) => state.config.optimizer_config.update(overwrite),
            None => state.config.optimizer_config.clone(),
        };
        let prevent_unoptimized = optimizers.prevent_unoptimized.unwrap_or(false);

        if prevent_unoptimized {
            Ok(ShardTransferMethod::Snapshot)
        } else {
            let method = context
                .default_shard_transfer_method
                .unwrap_or(ShardTransferMethod::StreamRecords);

            Ok(method)
        }
    }

    fn plan_restart_transfer(
        &self,
        context: &NodeContext,
        collection: String,
        restart: &ShardTransferRestart,
    ) -> StorageResult<Actions> {
        let state = self.collection(&collection).expect("collection exists");

        let key = restart.key();
        let method = restart.method;

        let Some(mut transfer) = transfer_by_key(state, key).cloned() else {
            return Err(missing_transfer(key));
        };

        if transfer.method == Some(method) {
            return Ok(Actions::new());
        }

        let initial_state = initial_replica_state_for_transfer(state, method)?;

        if !state.shards.contains_key(&transfer.shard_id) {
            return Err(StorageError::bad_request(format!(
                "Shard {} doesn't exist",
                transfer.shard_id,
            )));
        }

        // Transfer restart should only be used for *ordinary* transfers.
        // Ordinary transfers must never specify `to_shard_id` and `filter`.

        let is_ordinary_transfer =
            // inhibit rustfmt
            transfer.method != Some(ShardTransferMethod::ReshardingStreamRecords)
            && transfer.to_shard_id.is_none()
            && transfer.filter.is_none();

        if !is_ordinary_transfer {
            log::error!(
                "Unsupported restart of transfer {key:?} (method: {:?}); \
                 only non-resharding transfers without `to_shard_id` or `filter` can be restarted",
                transfer.method,
            );

            debug_assert_ne!(
                transfer.method,
                Some(ShardTransferMethod::ReshardingStreamRecords),
            );

            debug_assert_eq!(transfer.to_shard_id, None);
            debug_assert_eq!(transfer.filter, None);
        }

        if method == ShardTransferMethod::ReshardingStreamRecords {
            log::error!(
                "Unsupported restart method `ReshardingStreamRecords` requested \
                 for transfer {key:?}",
            );

            debug_assert_ne!(method, ShardTransferMethod::ReshardingStreamRecords);
        }

        transfer.method = Some(method);
        transfer.to_shard_id = None;
        transfer.filter = None;

        let mut actions = vec![Action::StopTransferDriver {
            collection: collection.clone(),
            key,
        }];

        if context.peer_id == transfer.from {
            actions.push(Action::RevertProxyShard {
                collection: collection.clone(),
                shard_id: transfer.shard_id,
            });
        }

        if context.peer_id == restart.to {
            actions.push(Action::InitLocalShard {
                collection: collection.clone(),
                shard_id: transfer.shard_id,
                mode: LocalShardInitMode::ResetToEmpty,
            });
        }

        actions.extend([
            Action::SetReplicaState {
                collection: collection.clone(),
                shard_id: transfer.shard_id,
                peer_id: transfer.to,
                state: initial_state,
            },
            Action::SetTransferMethod {
                collection: collection.clone(),
                key,
                method,
            },
        ]);

        if context.peer_id == restart.from {
            actions.push(Action::SpawnTransferDriver {
                collection,
                transfer,
            });
        }

        Ok(actions)
    }

    fn plan_finish_transfer(
        &self,
        context: &NodeContext,
        collection: String,
        transfer: &ShardTransfer,
    ) -> StorageResult<Actions> {
        let state = self.collection(&collection).expect("collection exists");
        let key = transfer.key();
        if transfer_by_key(state, key).is_none() {
            return Err(missing_transfer(key));
        }

        let mut actions = vec![Action::StopTransferDriver {
            collection: collection.clone(),
            key,
        }];
        let is_resharding = transfer.is_resharding();
        let destination_shard = transfer.to_shard_id.unwrap_or(transfer.shard_id);
        let mut destination_active = false;

        if state
            .shards
            .get(&destination_shard)
            .is_some_and(|shard| shard.replicas.contains_key(&transfer.to))
        {
            let replica_state = if is_resharding {
                match state.resharding.as_ref().map(|state| state.direction) {
                    Some(ReshardingDirection::Up) => ReplicaState::Resharding,
                    Some(ReshardingDirection::Down) => ReplicaState::ReshardingScaleDown,
                    None => ReplicaState::Dead,
                }
            } else {
                ReplicaState::Active
            };
            destination_active = replica_state == ReplicaState::Active;
            actions.push(Action::SetReplicaState {
                collection: collection.clone(),
                shard_id: destination_shard,
                peer_id: transfer.to,
                state: replica_state,
            });
        }

        if state.shards.contains_key(&transfer.shard_id) {
            if transfer.sync || is_resharding {
                if context.peer_id == transfer.from {
                    actions.push(Action::UnproxifyShard {
                        collection: collection.clone(),
                        shard_id: transfer.shard_id,
                    });
                }
            } else if destination_active {
                if context.peer_id == transfer.from {
                    actions.push(Action::InvalidateCleanLocalShards {
                        collection: collection.clone(),
                        shard_ids: vec![transfer.shard_id],
                    });
                }

                actions.push(Action::RemoveReplica {
                    collection: collection.clone(),
                    shard_id: transfer.shard_id,
                    peer_id: transfer.from,
                });
            }
        }

        actions.push(Action::UnregisterTransfer {
            collection,
            key,
            outcome: TransferOutcome::Finish,
        });

        Ok(actions)
    }

    fn plan_recovery_to_partial(
        &self,
        collection: String,
        key: ShardTransferKey,
    ) -> StorageResult<Actions> {
        let state = self.collection(&collection).expect("collection exists");

        let Some(transfer) = transfer_by_key(state, key) else {
            return Err(missing_transfer(key));
        };

        // `RecoveryToPartial` should only be used for `Snapshot` and `WalDelta` transfers.
        // These transfer methods must never specify `to_shard_id` or `filter`.

        let is_snapshot_or_wal_delta = matches!(
            transfer.method,
            Some(ShardTransferMethod::Snapshot | ShardTransferMethod::WalDelta),
        );

        let has_optional_fields = transfer.to_shard_id.is_some() || transfer.filter.is_some();

        if !is_snapshot_or_wal_delta || has_optional_fields {
            log::error!(
                "Unsupported `RecoveryToPartial` for transfer {key:?} (method: {:?}); \
                 expected a `Snapshot` or `WalDelta` transfer without `to_shard_id` or `filter`",
                transfer.method,
            );

            debug_assert!(is_snapshot_or_wal_delta);
            debug_assert_eq!(transfer.to_shard_id, None);
            debug_assert_eq!(transfer.filter, None);
        }

        let current = state
            .shards
            .get(&key.shard_id)
            .and_then(|shard| shard.replicas.get(&key.to))
            .copied()
            .ok_or_else(|| {
                StorageError::bad_input(format!(
                    "Replica {} of {collection}:{} does not exist",
                    key.to, key.shard_id,
                ))
            })?;

        let is_partial_snapshot_or_recovery = matches!(
            current,
            ReplicaState::PartialSnapshot | ReplicaState::Recovery
        );

        if !is_partial_snapshot_or_recovery {
            return Err(StorageError::bad_input(format!(
                "Replica {} of {collection}:{} has unexpected {current:?} (expected {:?} or {:?})",
                key.to,
                key.shard_id,
                ReplicaState::PartialSnapshot,
                ReplicaState::Recovery,
            )));
        }

        Ok(vec![Action::SetReplicaState {
            collection,
            shard_id: key.shard_id,
            peer_id: key.to,
            state: ReplicaState::Partial,
        }])
    }

    fn plan_abort_transfer(
        &self,
        context: &NodeContext,
        collection: String,
        key: ShardTransferKey,
    ) -> StorageResult<Actions> {
        let state = self.collection(&collection).expect("collection exists");
        let Some(transfer) = transfer_by_key(state, key).cloned() else {
            return Err(missing_transfer(key));
        };

        let mut actions = Actions::new();
        if transfer.is_resharding()
            && let Some(resharding) = &state.resharding
        {
            actions.extend(self.plan_abort_resharding(
                context,
                collection.clone(),
                &resharding.key(),
                false,
                AbortReshardingScope {
                    skip_transfer: Some(key),
                    ..Default::default()
                },
            )?);
        }

        actions.extend(self.plan_abort_transfer_record(context, collection, &transfer));
        Ok(actions)
    }

    fn plan_abort_transfer_record(
        &self,
        context: &NodeContext,
        collection: String,
        transfer: &ShardTransfer,
    ) -> Actions {
        let state = self.collection(&collection).expect("collection exists");
        let key = transfer.key();
        let destination_shard = key.to_shard_id.unwrap_or(key.shard_id);
        let mut actions = vec![Action::StopTransferDriver {
            collection: collection.clone(),
            key,
        }];

        if state
            .shards
            .get(&destination_shard)
            .is_some_and(|shard| shard.replicas.contains_key(&transfer.to))
        {
            if transfer.is_resharding() {
                // Resharding abort restores replica state as part of its own cascade
            } else if transfer.sync {
                actions.push(Action::SetReplicaState {
                    collection: collection.clone(),
                    shard_id: destination_shard,
                    peer_id: transfer.to,
                    state: ReplicaState::Dead,
                });
            } else {
                actions.extend([
                    Action::InvalidateCleanLocalShards {
                        collection: collection.clone(),
                        shard_ids: vec![destination_shard],
                    },
                    Action::RemoveReplica {
                        collection: collection.clone(),
                        shard_id: destination_shard,
                        peer_id: transfer.to,
                    },
                ]);
            }
        }

        if context.peer_id == transfer.from {
            actions.push(Action::RevertProxyShard {
                collection: collection.clone(),
                shard_id: transfer.shard_id,
            });
        }

        actions.push(Action::UnregisterTransfer {
            collection,
            key,
            outcome: TransferOutcome::Abort,
        });

        actions
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
    shard_key: Option<&ShardKey>,
) -> Vec<ShardId> {
    let mut shard_ids: Vec<_> = match shard_key {
        Some(shard_key) => state
            .shards_key_mapping
            .get(shard_key)
            .into_iter()
            .flatten()
            .copied()
            .collect(),

        None => state
            .shards
            .keys()
            .copied()
            .filter(|&shard_id| shard_belongs_to_key(state, shard_id, None))
            .collect(),
    };

    shard_ids.sort_unstable();
    shard_ids
}

fn shard_belongs_to_key(
    state: &collection::collection_state::State,
    shard_id: ShardId,
    shard_key: Option<&ShardKey>,
) -> bool {
    match shard_key {
        Some(shard_key) => state
            .shards_key_mapping
            .get(shard_key)
            .is_some_and(|shard_ids| shard_ids.contains(&shard_id)),

        None => state
            .shards_key_mapping
            .values()
            .all(|shard_ids| !shard_ids.contains(&shard_id)),
    }
}

fn transfer_by_key(
    state: &collection::collection_state::State,
    key: ShardTransferKey,
) -> Option<&ShardTransfer> {
    state
        .transfers
        .iter()
        .find(|transfer| transfer.key() == key)
}

fn missing_transfer(key: ShardTransferKey) -> StorageError {
    StorageError::bad_request(format!(
        "There is no transfer for shard {} from {} to {}",
        key.shard_id, key.from, key.to,
    ))
}

fn initial_replica_state_for_transfer(
    state: &collection::collection_state::State,
    method: ShardTransferMethod,
) -> StorageResult<ReplicaState> {
    match method {
        ShardTransferMethod::StreamRecords => Ok(ReplicaState::Partial),
        ShardTransferMethod::Snapshot | ShardTransferMethod::WalDelta => Ok(ReplicaState::Recovery),
        ShardTransferMethod::ReshardingStreamRecords => {
            let direction = state
                .resharding
                .as_ref()
                .map(|resharding| resharding.direction)
                .ok_or_else(|| {
                    StorageError::bad_input(
                        "can't start resharding transfer, because resharding is not in progress",
                    )
                })?;

            match direction {
                ReshardingDirection::Up => Ok(ReplicaState::Resharding),
                ReshardingDirection::Down => Ok(ReplicaState::ReshardingScaleDown),
            }
        }
    }
}

fn validate_transfer(
    transfer: &ShardTransfer,
    peer_addresses: &crate::types::PeerAddressById,
    state: &collection::collection_state::State,
) -> StorageResult<()> {
    let Some(source_replicas) = state
        .shards
        .get(&transfer.shard_id)
        .map(|shard| &shard.replicas)
    else {
        return Err(StorageError::bad_request(format!(
            "Shard {} does not exist",
            transfer.shard_id,
        )));
    };

    if !peer_addresses.contains_key(&transfer.from) {
        return Err(StorageError::bad_request(format!(
            "Peer {} does not exist",
            transfer.from,
        )));
    }
    if !peer_addresses.contains_key(&transfer.to) {
        return Err(StorageError::bad_request(format!(
            "Peer {} does not exist",
            transfer.to,
        )));
    }

    if !matches!(
        source_replicas.get(&transfer.from),
        Some(ReplicaState::Active | ReplicaState::ReshardingScaleDown),
    ) {
        return Err(StorageError::bad_request(format!(
            "Shard {} is not active on peer {}",
            transfer.shard_id, transfer.from,
        )));
    }

    let destination_replicas = transfer
        .to_shard_id
        .and_then(|shard_id| state.shards.get(&shard_id))
        .map(|shard| &shard.replicas);

    if transfer_by_key(state, transfer.key()).is_some() {
        let destination_replicas = destination_replicas.unwrap_or(source_replicas);
        if destination_replicas
            .get(&transfer.to)
            .is_some_and(|state| state.is_partial_or_recovery())
        {
            return Err(StorageError::bad_request(format!(
                "Shard {} is already involved in transfer {} -> {}",
                transfer.shard_id, transfer.from, transfer.to,
            )));
        }
    }

    if let Some(existing) = state
        .transfers
        .iter()
        .filter(|existing| {
            existing.key() != transfer.key() && existing.shard_id == transfer.shard_id
        })
        .find(|existing| {
            existing.from == transfer.from
                || existing.to == transfer.from
                || existing.from == transfer.to
                || existing.to == transfer.to
        })
    {
        return Err(StorageError::bad_request(format!(
            "Shard {} is already involved in transfer {} -> {}",
            transfer.shard_id, existing.from, existing.to,
        )));
    }

    if transfer.method == Some(ShardTransferMethod::ReshardingStreamRecords) {
        let Some(destination_replicas) = destination_replicas else {
            return Err(StorageError::bad_request(format!(
                "Destination shard {} does not exist",
                transfer.shard_id,
            )));
        };
        let Some(to_shard_id) = transfer.to_shard_id else {
            return Err(StorageError::bad_request(
                "Target shard is not set for resharding transfer",
            ));
        };
        if transfer.shard_id == to_shard_id {
            return Err(StorageError::bad_request(format!(
                "Source and target shard must be different for resharding transfer, both are \
                 {to_shard_id}",
            )));
        }
        if destination_replicas.get(&transfer.to) == Some(&ReplicaState::Dead) {
            return Err(StorageError::bad_request(format!(
                "Resharding shard transfer can't be started, because destination shard {}/{to_shard_id} is dead",
                transfer.to,
            )));
        }

        let source_key = state
            .shards_key_mapping
            .iter()
            .find(|(_, shard_ids)| shard_ids.contains(&transfer.shard_id))
            .map(|(key, _)| key);

        let target_key = state
            .shards_key_mapping
            .iter()
            .find(|(_, shard_ids)| shard_ids.contains(&to_shard_id))
            .map(|(key, _)| key);

        if source_key != target_key {
            return Err(StorageError::bad_request(format!(
                "Source and target shard must have the same shard key, but they have \
                 {source_key:?} and {target_key:?}",
            )));
        }
    } else if transfer.filter.is_some() {
        let Some(destination_replicas) = destination_replicas else {
            return Err(StorageError::bad_request(format!(
                "Destination shard {} does not exist",
                transfer.shard_id,
            )));
        };
        let Some(to_shard_id) = transfer.to_shard_id else {
            return Err(StorageError::bad_request(
                "Target shard is not set for filtered points transfer",
            ));
        };
        if transfer.shard_id == to_shard_id {
            return Err(StorageError::bad_request(format!(
                "Source and target shard must be different for filtered points transfer, both are \
                 {to_shard_id}",
            )));
        }
        if destination_replicas.get(&transfer.to) == Some(&ReplicaState::Dead) {
            return Err(StorageError::bad_request(format!(
                "Filtered shard transfer can't be started, because destination shard {}/{to_shard_id} is dead",
                transfer.to,
            )));
        }
    } else if let Some(to_shard_id) = transfer.to_shard_id {
        return Err(StorageError::bad_request(format!(
            "Target shard {to_shard_id} can only be set for {:?} or filtered streaming records \
             transfers",
            ShardTransferMethod::ReshardingStreamRecords,
        )));
    }

    Ok(())
}
