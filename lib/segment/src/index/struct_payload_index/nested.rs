//! Opening per-element indexes and their element maps together.

use super::StructPayloadIndex;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead as _};
use crate::index::field_index::FieldIndex;
use crate::index::field_index::index_selector::IndexSelector;
use crate::index::field_index::nested_index::{ElementMap, NestedFieldIndex};
use crate::index::payload_config::FullPayloadIndexType;
use crate::json_path::JsonPath;
use crate::types::{PayloadFieldSchema, PayloadKeyTypeRef};

impl StructPayloadIndex {
    /// Open the per-element index of `field` stored as `index_type`, `None` if its inner
    /// index or its element map is missing.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn open_nested_index(
        &self,
        selector: &IndexSelector,
        field: PayloadKeyTypeRef,
        array_key: &JsonPath,
        payload_schema: &PayloadFieldSchema,
        index_type: &FullPayloadIndexType,
        create_if_missing: bool,
        id_tracker: &IdTrackerEnum,
    ) -> OperationResult<Option<FieldIndex>> {
        let FullPayloadIndexType {
            index_type: inner_type,
            mutability,
            // The map is stored the same way for every storage of the index
            storage_type: _,
        } = index_type;
        let index_dir = inner_type.storage_dir(&self.path, field);
        let Some(element_map) = ElementMap::open(ElementMap::storage_dir(&index_dir), *mutability)?
        else {
            return Ok(None);
        };
        let deleted_points = id_tracker.deleted_point_bitslice();
        let deleted_elements = element_map.deleted_elements(deleted_points);
        let indexed_points = element_map.indexed_points();
        let Some(inner) = selector.new_index_with_type(
            field,
            payload_schema,
            index_type,
            create_if_missing,
            id_tracker,
            &deleted_elements,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(FieldIndex::Nested(Box::new(NestedFieldIndex::new(
            field,
            array_key.clone(),
            element_map,
            inner,
            indexed_points,
        )?))))
    }
}
