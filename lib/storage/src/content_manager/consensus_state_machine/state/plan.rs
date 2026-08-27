use collection::collection::vector_name_schema;

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
}
