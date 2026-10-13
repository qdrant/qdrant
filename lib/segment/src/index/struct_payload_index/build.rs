use common::types::PointOffsetType;

use super::StructPayloadIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::IdTrackerRead;
use crate::index::BuildIndexResult;
use crate::index::field_index::index_selector::wipe_field_dirs;
use crate::index::field_index::nested_index::nested_elements;
use crate::index::field_index::{FieldIndex, FieldIndexBuilderTrait as _};
use crate::payload_storage::PayloadStorageRead;
use crate::types::{PayloadContainer, PayloadFieldSchema, PayloadKeyTypeRef};

impl StructPayloadIndex {
    pub fn build_field_indexes(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<Vec<FieldIndex>> {
        // A build must start from a clean slate: files can be left behind by a build
        // that crashed before its config entry was written, or by an index that
        // failed to load. Appendable builders open existing storages, so leftovers
        // would leak stale postings into the fresh build.
        wipe_field_dirs(&self.path, field)?;

        // A per-element index is built over the element map of its array, which is
        // shared with the other per-element indexes of that array. A new map is built
        // and flushed here: it is not installed yet, so no flusher of it is captured.
        let nested = match payload_schema.nested() {
            Some(array_key) => {
                let relative_path = field.strip_prefix(array_key).ok_or_else(|| {
                    OperationError::service_error(format!(
                        "field `{field}` is not inside the elements of `{array_key}`",
                    ))
                })?;
                Some((
                    array_key,
                    relative_path,
                    self.element_map_for_build(array_key)?,
                ))
            }
            None => None,
        };
        let element_map = nested.as_ref().map(|(_, _, map)| map.borrow());

        // Note: durability of the state this build reads (id tracker, payload storage)
        // is the caller's responsibility. Component flushers must not be invoked here:
        // flusher executions are serialized end-to-end with their capture by the shard
        // flush pipeline, and an extra flush run between another flusher's capture and
        // execution corrupts storage (Gridstore double-frees superseded blocks). The
        // live update path pins the observed state by flushing the whole segment under
        // that serialization before building (`shard::update::create_field_index`).
        let payload_storage = self.payload.borrow();
        let id_tracker_borrow = self.id_tracker.borrow();
        let selector = self.selector(payload_schema);
        let deleted_elements = element_map
            .as_ref()
            .map(|map| map.deleted_elements(id_tracker_borrow.deleted_point_bitslice()));
        let mut builders = selector.index_builder(
            field,
            payload_schema,
            deleted_elements
                .as_deref()
                .unwrap_or(id_tracker_borrow.deleted_point_bitslice()),
        )?;

        // Special null index complements every index. Seed it with the segment's total
        // point count so `iter_falses()` returns points that are missing from payload
        // storage (e.g. after `clear_payload`), matching the regular "no value" points.
        // Bug: <https://github.com/qdrant/qdrant/issues/8723>
        let total_point_count = self.id_tracker.borrow().total_point_count();
        let mut null_builder = selector.null_builder(field, total_point_count)?;

        for index in builders.iter_mut().chain([&mut null_builder]) {
            index.init()?;
            if !self.journaled {
                index.disable_journal();
            }
        }

        payload_storage.iter(|point_id, point_payload| {
            let field_value = &point_payload.get_value(field);
            null_builder.add_point(point_id, field_value)?;

            if let (Some((array_key, relative_path, _)), Some(element_map)) =
                (&nested, &element_map)
            {
                let elements = nested_elements(point_payload, array_key);
                let element_ids = element_map.point_elements(point_id);
                if element_ids.len() != elements.len() {
                    log::warn!(
                        "Point {point_id} has {} elements in `{array_key}`, but its element \
                         map has {}; indexing the common part",
                        elements.len(),
                        element_ids.len(),
                    );
                }
                for (element_id, element) in element_ids.zip(elements) {
                    let element_value = &relative_path.value_get(element);
                    for builder in builders.iter_mut() {
                        builder.add_point(element_id, element_value)?;
                    }
                }
            } else {
                for builder in builders.iter_mut() {
                    builder.add_point(point_id, field_value)?;
                }
            }
            Ok(true)
        })?;

        let mut indexes = builders
            .into_iter()
            .map(|builder| builder.finalize())
            .collect::<OperationResult<Vec<_>>>()?;

        if let Some((array_key, _, map)) = &nested {
            drop(element_map);
            indexes = Self::wrap_nested(field, array_key, map, indexes)?;
        }

        indexes.push(null_builder.finalize()?);
        Ok(indexes)
    }

    /// Build a field index by reusing its already-persisted files, loaded in
    /// the representation requested by `payload_schema` (honoring a changed
    /// `on_disk` flag), instead of rebuilding it from payload storage. Falls
    /// back to a full rebuild if the files cannot be loaded.
    pub(super) fn reuse_or_build_index(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<BuildIndexResult> {
        // Per-element indexes are opened over their element map, rebuilding is simpler
        if payload_schema.nested().is_some() {
            return Ok(BuildIndexResult::Built(
                self.build_field_indexes(field, payload_schema)?,
            ));
        }

        let loaded = {
            let selector = self.selector(payload_schema);
            let id_tracker = self.id_tracker.borrow();
            let deleted_points = id_tracker.deleted_point_bitslice();
            match selector.new_index(field, payload_schema, false, deleted_points)? {
                Some(mut indexes) => {
                    if let Some(null_index) = selector.new_null_index(
                        field,
                        false,
                        &id_tracker,
                        selector.default_mutability(),
                    )? {
                        indexes.push(null_index);
                    }
                    Some(indexes)
                }
                None => None,
            }
        };

        match loaded {
            Some(indexes) => Ok(BuildIndexResult::Built(indexes)),
            None => Ok(BuildIndexResult::Built(
                self.build_field_indexes(field, payload_schema)?,
            )),
        }
    }

    pub(super) fn clear_index_for_point(
        &mut self,
        point_id: PointOffsetType,
    ) -> OperationResult<()> {
        for field_indexes in self.field_indexes.values_mut() {
            for index in field_indexes {
                index.remove_point(point_id)?;
            }
        }
        Ok(())
    }
}
