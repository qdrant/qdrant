use std::collections::{BTreeMap, BTreeSet};

use collection::collection::vector_name_schema;
use collection::collection_state::ShardInfo;
use collection::operations::types::PeerMetadata;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard::PeerId;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_state_machine::{
    Action, NodeContext, apply_collection_config_diffs,
};
use crate::content_manager::toc::apply_alias_actions;

type Actions = Vec<Action>;

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
