use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::StructPayloadIndex;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::FieldIndex;
use crate::index::payload_config::PayloadFieldSchemaWithIndexType;
use crate::index::{BuildIndexResult, PayloadIndex, PayloadIndexRead};
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorage;
use crate::types::{
    Payload, PayloadContainer, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef,
};

impl PayloadIndex for StructPayloadIndex {
    fn build_index(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: &PayloadFieldSchema,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BuildIndexResult> {
        if let Some(prev_schema) = self.config().indices.get(field) {
            // the field is already indexed with the same schema
            // no need to rebuild index and to save the config
            return if prev_schema.schema == *payload_schema {
                Ok(BuildIndexResult::AlreadyBuilt)
            } else {
                Ok(BuildIndexResult::IncompatibleSchema)
            };
        }
        let indexes = self.build_field_indexes(field, payload_schema, hw_counter)?;
        Ok(BuildIndexResult::Built(indexes))
    }

    fn apply_index(
        &mut self,
        field: PayloadKeyType,
        payload_schema: PayloadFieldSchema,
        field_index: Vec<FieldIndex>,
    ) -> OperationResult<()> {
        let index_types: Vec<_> = field_index
            .iter()
            .map(|i| i.get_full_index_type())
            .collect();
        self.field_indexes.insert(field.clone(), field_index);

        self.config.indices.insert(
            field,
            PayloadFieldSchemaWithIndexType::new(payload_schema, index_types),
        );

        self.save_config()?;

        Ok(())
    }

    fn set_indexed(
        &mut self,
        field: PayloadKeyTypeRef,
        payload_schema: impl Into<PayloadFieldSchema>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let payload_schema = payload_schema.into();

        self.drop_index_if_incompatible(field, &payload_schema)?;

        let field_index = match self.build_index(field, &payload_schema, hw_counter)? {
            BuildIndexResult::Built(field_index) => field_index,
            BuildIndexResult::AlreadyBuilt => {
                // Index already built, no need to do anything
                return Ok(());
            }
            BuildIndexResult::IncompatibleSchema => {
                // We should have fixed it by now explicitly
                // If it is not fixed, it is a bug
                return Err(OperationError::service_error(format!(
                    "Incompatible schema for field `{field}`. Please drop the index first."
                )));
            }
        };

        self.apply_index(field.to_owned(), payload_schema, field_index)?;

        Ok(())
    }

    fn drop_index(&mut self, field: PayloadKeyTypeRef) -> OperationResult<bool> {
        let removed_config = self.config.indices.remove(field);
        let removed_indexes = self.field_indexes.remove(field);

        let is_removed = removed_config.is_some() || removed_indexes.is_some();

        if let Some(indexes) = removed_indexes {
            for index in indexes {
                index.wipe()?;
            }
        }

        self.save_config()?;

        Ok(is_removed)
    }

    fn drop_index_if_incompatible(
        &mut self,
        field: PayloadKeyTypeRef,
        new_payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<bool> {
        let Some(current_schema) = self.config().indices.get(field) else {
            return Ok(false);
        };

        // the field is already indexed with the same schema
        // no need to rebuild index and to save the config
        if current_schema.schema == *new_payload_schema {
            return Ok(false);
        }

        self.drop_index(field)
    }

    fn overwrite_payload(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.payload
            .borrow_mut()
            .overwrite(point_id, payload, hw_counter)?;

        for (field, field_index) in &mut self.field_indexes {
            let field_value = payload.get_value(field);
            if !field_value.is_empty() {
                for index in field_index {
                    index.add_point(point_id, &field_value, hw_counter)?;
                }
            } else {
                for index in field_index {
                    index.remove_point(point_id)?;
                }
            }
        }
        Ok(())
    }

    fn set_payload(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &Option<JsonPath>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if let Some(key) = key {
            self.payload
                .borrow_mut()
                .set_by_key(point_id, payload, key, hw_counter)?;
        } else {
            self.payload
                .borrow_mut()
                .set(point_id, payload, hw_counter)?;
        };

        let updated_payload = self.get_payload(point_id, hw_counter)?;
        for (field, field_index) in &mut self.field_indexes {
            if !field.is_affected_by_value_set(&payload.0, key.as_ref()) {
                continue;
            }
            let field_value = updated_payload.get_value(field);
            if !field_value.is_empty() {
                for index in field_index {
                    index.add_point(point_id, &field_value, hw_counter)?;
                }
            } else {
                for index in field_index {
                    index.remove_point(point_id)?;
                }
            }
        }
        Ok(())
    }

    fn delete_payload(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>> {
        if let Some(indexes) = self.field_indexes.get_mut(key) {
            for index in indexes {
                index.remove_point(point_id)?;
            }
        }
        self.payload.borrow_mut().delete(point_id, key, hw_counter)
    }

    fn clear_payload(
        &mut self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        self.clear_index_for_point(point_id)?;
        self.payload.borrow_mut().clear(point_id, hw_counter)
    }

    fn flusher(&self) -> Flusher {
        // Most field indices have either 2 or 3 indices (including null), we also have an extra
        // payload storage flusher. Overallocate to save potential reallocations.
        let mut flushers = Vec::with_capacity(self.field_indexes.len() * 3 + 1);

        for field_indexes in self.field_indexes.values() {
            for index in field_indexes {
                flushers.push(index.flusher());
            }
        }
        flushers.push(self.payload.borrow().flusher());

        Box::new(move || {
            for flusher in flushers {
                flusher()?;
            }
            Ok(())
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self
            .field_indexes
            .values()
            .flat_map(|indexes| indexes.iter().flat_map(|index| index.files().into_iter()))
            .collect::<Vec<PathBuf>>();
        files.push(self.config_path());
        files
    }

    fn immutable_files(&self) -> Vec<(PayloadKeyType, PathBuf)> {
        self.field_indexes
            .iter()
            .flat_map(|(key, indexes)| {
                indexes.iter().flat_map(|index| {
                    index
                        .immutable_files()
                        .into_iter()
                        .map(|file| (key.clone(), file))
                })
            })
            .collect()
    }
}
