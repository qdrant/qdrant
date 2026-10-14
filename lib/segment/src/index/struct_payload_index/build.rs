use common::bitvec::BitVec;
use common::types::PointOffsetType;

use super::StructPayloadIndex;
use super::nested::NestedBuild;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::BuildIndexResult;
use crate::index::field_index::index_selector::wipe_field_dirs;
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

        // Per-element indexes get their element offsets during the pass below
        let mut nested = payload_schema
            .nested()
            .map(|array_key| NestedBuild::new(field, array_key))
            .transpose()?;

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
        // Deleted points get no elements, so no element is deleted
        let no_deleted_elements = BitVec::new();
        let deleted = match nested {
            Some(_) => &no_deleted_elements,
            None => id_tracker_borrow.deleted_point_bitslice(),
        };
        let mut builders = selector.index_builder(field, payload_schema, deleted)?;

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

            if let Some(nested) = &mut nested {
                if !id_tracker_borrow.is_deleted_point(point_id) {
                    nested.add_point(point_id, point_payload, &mut builders)?;
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

        if let Some(nested) = &nested {
            indexes = self.finish_nested_build(field, nested, indexes)?;
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
        // Per-element indexes keep an element map next to the inner index, rebuilding
        // is simpler than reopening both in another mode
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
