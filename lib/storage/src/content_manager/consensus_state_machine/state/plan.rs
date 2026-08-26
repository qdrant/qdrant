use std::collections::{BTreeMap, BTreeSet};

use collection::collection::vector_name_schema;
use collection::operations::types::PeerMetadata;
use collection::shards::shard::PeerId;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_state_machine::Action;

type Actions = Vec<Action>;

impl ClusterState {
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

        // TODO:
        //
        // This is intentionally different from `TableOfContent::update_aliases`.
        //
        // `ToC::update_aliases` validates and applies `actions` one by one,
        // and `AliasPersistence` writes the mapping on every insert, remove and rename.
        // So if an `AliasOperation` in the middle of the list is rejected, every action
        // before it is applied and persisted, and every action after it never runs.
        //
        // `plan_change_aliases` validates all `actions` first, and emits a single
        // `UpdateAliases` action, which the applier has to write in one go. So either all
        // `actions` apply, or none of them do.
        //
        // E.g., take a list of two actions:
        // the first creates alias `new`, the second renames alias `missing`, which does not exist.
        //
        // `ToC::update_aliases` would create alias `new`, then return an error.
        // `plan_change_aliases` would return an error *before* creating alias `new`.
        //
        // `ToC::update_aliases` has to validate all `actions` up front, and write the mapping
        // once, to match. Rejecting a replay of an applied operation is only safe when nothing
        // of it is left behind.

        let mut aliases = self.aliases.clone();

        for action in actions {
            match action {
                AliasOperations::CreateAlias(action) => {
                    let CreateAlias {
                        collection_name,
                        alias_name,
                    } = &action.create_alias;

                    // `collection_name` must name a collection, not an alias
                    if !self.has_collection(collection_name) {
                        return Err(StorageError::not_found(format!(
                            "Collection `{collection_name}` does not exist"
                        )));
                    }

                    if self.has_collection(alias_name) {
                        return Err(StorageError::already_exists(format!(
                            "Collection `{alias_name}` already exists"
                        )));
                    }

                    aliases.insert(alias_name.clone(), collection_name.clone());
                }

                AliasOperations::DeleteAlias(action) => {
                    let DeleteAlias { alias_name } = &action.delete_alias;

                    // Deleting an alias that does not exist is a no-op, not an error
                    aliases.remove(alias_name);
                }

                AliasOperations::RenameAlias(action) => {
                    let RenameAlias {
                        old_alias_name,
                        new_alias_name,
                    } = &action.rename_alias;

                    if !aliases.rename(old_alias_name, new_alias_name.clone()) {
                        return Err(StorageError::not_found(format!(
                            "Alias {old_alias_name} does not exist"
                        )));
                    }
                }
            }
        }

        // Emit what the actions add up to, so that replay of an applied operation writes the
        // same values, and an operation that changes nothing writes nothing

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

    /// Metadata is an absolute value, so a peer that already has it needs no action.
    /// Nothing to validate: any peer can report any metadata.
    pub fn plan_update_peer_metadata(&self, peer_id: PeerId, metadata: &PeerMetadata) -> Actions {
        if self.peer_metadata_by_id.get(&peer_id) == Some(metadata) {
            return Actions::new();
        }

        vec![Action::SetPeerMetadata {
            peer_id,
            metadata: metadata.clone(),
        }]
    }

    /// Null value removes the key, so the goal state is reached when the key is gone.
    /// Nothing to validate: any key can hold any value.
    pub fn plan_update_cluster_metadata(&self, key: &str, value: &serde_json::Value) -> Actions {
        let current = self.cluster_metadata.get(key);

        let applied = match value.is_null() {
            true => current.is_none(),
            false => current == Some(value),
        };

        if applied {
            return Actions::new();
        }

        vec![Action::SetClusterMetadataKey {
            key: key.to_string(),
            value: value.clone(),
        }]
    }

    /// Emitted even when the config matches: `QuotaManager::set_config` also drops the limit
    /// verdicts it recorded, so an operator raising a limit is served right away
    pub fn plan_set_quota_config(&self, config: &QuotaConfig) -> Actions {
        vec![Action::SetQuotaConfig { config: *config }]
    }
}
