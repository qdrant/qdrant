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

    /// Plan alias actions in order, each one reading what the actions before it did.
    ///
    /// `TableOfContent::update_aliases` validates and saves one action at a time, so an action
    /// rejected in the middle keeps the ones before it. Here the whole operation is validated
    /// first, and a rejected operation changes nothing.
    pub fn plan_change_aliases(&self, op: &ChangeAliasesOperation) -> StorageResult<Actions> {
        let ChangeAliasesOperation { actions } = op;

        let mut aliases = self.aliases.clone();
        let mut planned = Actions::new();

        for action in actions {
            match action {
                AliasOperations::CreateAlias(action) => {
                    let CreateAlias {
                        collection_name,
                        alias_name,
                    } = &action.create_alias;

                    // Collection has to exist under this name: an alias of an alias is rejected
                    if !self.has_collection(collection_name) {
                        return Err(StorageError::not_found(format!(
                            "Collection `{collection_name}` doesn't exist!"
                        )));
                    }

                    if self.has_collection(alias_name) {
                        return Err(StorageError::already_exists(format!(
                            "Collection `{alias_name}` already exists!"
                        )));
                    }

                    aliases.insert(alias_name.clone(), collection_name.clone());

                    planned.push(Action::SetAlias {
                        alias: alias_name.clone(),
                        collection: collection_name.clone(),
                    });
                }

                AliasOperations::DeleteAlias(action) => {
                    let DeleteAlias { alias_name } = &action.delete_alias;

                    // Deleting an alias that does not exist is a no-op, not an error
                    aliases.remove(alias_name);

                    planned.push(Action::DeleteAlias {
                        alias: alias_name.clone(),
                    });
                }

                AliasOperations::RenameAlias(action) => {
                    let RenameAlias {
                        old_alias_name,
                        new_alias_name,
                    } = &action.rename_alias;

                    // Rename reads the alias it removes, so a replay of an operation whose rename
                    // landed is rejected, and the actions after the rename never apply
                    if !aliases.rename(old_alias_name, new_alias_name.clone()) {
                        return Err(StorageError::not_found(format!(
                            "Alias {old_alias_name} does not exists!"
                        )));
                    }

                    planned.push(Action::RenameAlias {
                        old_alias: old_alias_name.clone(),
                        new_alias: new_alias_name.clone(),
                    });
                }
            }
        }

        Ok(planned)
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
