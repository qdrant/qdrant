use std::path::PathBuf;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::{StorageType, StructPayloadIndex};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::FieldIndex;
use crate::index::field_index::schema_transition::{SchemaTransition, classify};
use crate::index::payload_config::PayloadFieldSchemaWithIndexType;
use crate::index::{BuildIndexResult, PayloadIndex};
use crate::json_path::JsonPath;
use crate::payload_storage::{PayloadStorage, PayloadStorageRead};
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
            let transition = classify(&prev_schema.schema, payload_schema);
            return match transition {
                SchemaTransition::Identical => Ok(BuildIndexResult::AlreadyBuilt),
                // Only `on_disk` differs: reuse the existing files (loaded in
                // the new mode) instead of rebuilding from payload.
                SchemaTransition::OnlyOnDiskFlipped { .. } => {
                    self.reuse_or_build_index(field, payload_schema, hw_counter)
                }
                SchemaTransition::Incompatible => Ok(BuildIndexResult::IncompatibleSchema),
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

        // Persistence of the config is deferred to the flush pipeline: see
        // `mark_config_dirty` for the durability invariant this preserves.
        self.mark_config_dirty();

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

        // Deferred to the flush pipeline (see `mark_config_dirty`). If a crash leaves
        // the durable config still listing this field with its files wiped, the load
        // path rebuilds it from payload and WAL replay re-applies the drop.
        self.mark_config_dirty();

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

        match classify(&current_schema.schema, new_payload_schema) {
            SchemaTransition::Identical => Ok(false),
            // Only `on_disk` flipped on a non-appendable (mmap) segment: keep the
            // existing files and reload the index in the new mode during
            // `build_index` instead of dropping and rebuilding from payload.
            SchemaTransition::OnlyOnDiskFlipped { .. }
                if matches!(self.storage_type, StorageType::NonAppendable) =>
            {
                Ok(false)
            }
            // Appendable Gridstore (its `on_disk` change goes through the normal
            // drop-and-rebuild path) or any incompatible change: drop so
            // `build_index` rebuilds.
            SchemaTransition::OnlyOnDiskFlipped { .. } | SchemaTransition::Incompatible => {
                self.drop_index(field)
            }
        }
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

        // Re-read the payload after the write so field indexes see the merged
        // value. Inlined `get_payload` to avoid going through `with_view` from
        // a `&mut self` write path.
        let updated_payload = self.payload.borrow().get(point_id, hw_counter)?;
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
        let mut field_flushers = Vec::with_capacity(self.field_indexes.len() * 3);

        for field_indexes in self.field_indexes.values() {
            for index in field_indexes {
                field_flushers.push(index.flusher());
            }
        }
        let payload_flusher = self.payload.borrow().flusher();

        // Config persistence rides the flush pipeline, after the index data it
        // describes (see `mark_config_dirty` for the durability invariant). The
        // snapshot is captured together with the field flushers, so what gets
        // written always describes data flushed in this (or an earlier) cycle.
        let config_snapshot = self.config.clone();
        let config_gen = self.config_gen;
        let persisted_config_gen = Arc::clone(&self.persisted_config_gen);
        let config_path = self.config_path();

        Box::new(move || {
            for flusher in field_flushers {
                match flusher() {
                    Ok(()) => {}
                    // Cancelled = the index storage was dropped after flusher capture (e.g. a
                    // concurrent DropIndex). Skip it but keep flushing: aborting would leave the
                    // already-flushed indexes durably ahead of payload storage and point
                    // versions, and WAL replay would re-derive filter-based operations through
                    // that too-new index, silently skipping points (data loss). The drop itself
                    // is a versioned operation that replay re-applies.
                    Err(OperationError::Cancelled { description }) => {
                        log::debug!("Skipping flush of dropped field index storage: {description}");
                    }
                    Err(err) => return Err(err),
                }
            }
            payload_flusher()?;

            {
                let mut persisted_gen = persisted_config_gen.lock();
                // `>` (not `>=`): skips rewrites when nothing changed and stops a
                // stale flusher from overwriting a newer persisted config.
                if config_gen > *persisted_gen {
                    config_snapshot.save(&config_path)?;
                    *persisted_gen = config_gen;
                }
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
