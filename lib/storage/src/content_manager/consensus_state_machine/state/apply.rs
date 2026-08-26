use collection::collection::vector_name_schema;

use super::*;

impl ClusterState {
    /// Apply one action. Cannot fail.
    ///
    /// Action naming a missing collection changes nothing.
    /// Correct operation never emits one, so debug builds assert.
    pub fn apply_action(&mut self, action: &Action) {
        match action {
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
        }
    }

    fn collection_mut(&mut self, collection: &str) -> Option<&mut collection_state::State> {
        let state = self.collections.get_mut(collection);

        debug_assert!(
            state.is_some(),
            "action targets collection {collection}, which is not in the state",
        );

        state
    }
}
