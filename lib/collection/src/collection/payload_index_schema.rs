use std::collections::HashMap;
use std::path::{Path, PathBuf};

use segment::json_path::JsonPath;
use segment::problems::unindexed_field::Extractor;
use segment::types::{Filter, PayloadFieldSchema, PayloadKeyType};
use serde::{Deserialize, Serialize};

use crate::collection::Collection;
use crate::operations::types::{CollectionResult, UpdateResult};
use crate::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use crate::save_on_disk::SaveOnDisk;

pub const PAYLOAD_INDEX_CONFIG_FILE: &str = "payload_index.json";

#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq)]
pub struct PayloadIndexSchema {
    pub schema: HashMap<PayloadKeyType, PayloadFieldSchema>,
}

impl Collection {
    pub(crate) fn payload_index_file(collection_path: &Path) -> PathBuf {
        collection_path.join(PAYLOAD_INDEX_CONFIG_FILE)
    }

    pub(crate) fn load_payload_index_schema(
        collection_path: &Path,
    ) -> CollectionResult<SaveOnDisk<PayloadIndexSchema>> {
        let payload_index_file = Self::payload_index_file(collection_path);
        let schema: SaveOnDisk<PayloadIndexSchema> =
            SaveOnDisk::load_or_init_default(payload_index_file)?;
        Ok(schema)
    }

    pub async fn create_payload_index(
        &self,
        field_name: JsonPath,
        field_schema: PayloadFieldSchema,
    ) -> CollectionResult<Option<UpdateResult>> {
        // This function is called from consensus, so we use `wait = false`, because we can't afford
        // to wait for the result as indexation may take a long time
        self.create_payload_index_with_wait(field_name, field_schema, false)
            .await
    }

    pub async fn create_payload_index_with_wait(
        &self,
        field_name: JsonPath,
        field_schema: PayloadFieldSchema,
        wait: bool,
    ) -> CollectionResult<Option<UpdateResult>> {
        self.payload_index_schema.write(|schema| {
            schema
                .schema
                .insert(field_name.clone(), field_schema.clone());
        })?;

        // This operation might be redundant, if we also create index as a regular collection op,
        // but it looks better in long term to also have it here, so
        // the creation of payload index may be eventually completely converted
        // into the consensus operation
        let create_index_operation = CollectionUpdateOperations::FieldIndexOperation(
            FieldIndexOperations::CreateIndex(CreateIndex {
                field_name,
                field_schema: Some(field_schema),
            }),
        );

        self.update_all_local(create_index_operation, wait).await
    }

    pub async fn drop_payload_index(
        &self,
        field_name: JsonPath,
    ) -> CollectionResult<Option<UpdateResult>> {
        self.payload_index_schema.write(|schema| {
            schema.schema.remove(&field_name);
        })?;

        let delete_index_operation = CollectionUpdateOperations::FieldIndexOperation(
            FieldIndexOperations::DeleteIndex(field_name),
        );

        let result = self.update_all_local(delete_index_operation, false).await?;

        Ok(result)
    }

    /// Returns an arbitrary payload key used by `filter` which can be indexed but currently is not.
    /// If this function returns `None` all indexable keys in `filter` are indexed.
    pub fn filter_without_index(&self, filter: &Filter) -> Option<JsonPath> {
        self.payload_index_schema
            .read()
            .filter_without_index(filter, &self.name())
    }
}

impl PayloadIndexSchema {
    /// Returns an arbitrary payload key used by `filter` which can be indexed but currently is not.
    /// If this function returns `None` all indexable keys in `filter` are indexed.
    pub fn filter_without_index(&self, filter: &Filter, collection_name: &str) -> Option<JsonPath> {
        let extractor = Extractor::new(filter, &self.schema, collection_name.to_string());
        // Get a random unindexed field from the extractor.
        extractor.unindexed_schema.keys().next().cloned()
    }
}
