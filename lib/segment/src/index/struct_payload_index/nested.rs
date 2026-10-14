//! Opening and building per-element (nested) field indexes, see
//! [`nested_index`](crate::index::field_index::nested_index).
//!
//! Every per-element index keeps its own [`ElementMap`] next to the inner index it wraps,
//! so the two are opened, built and wiped together.

use common::types::PointOffsetType;

use super::StructPayloadIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead as _};
use crate::index::field_index::index_selector::IndexSelector;
use crate::index::field_index::nested_index::{
    ElementMap, ElementMapBuilder, NestedFieldIndex, nested_elements,
};
use crate::index::field_index::{FieldIndex, FieldIndexBuilder, FieldIndexBuilderTrait as _};
use crate::index::payload_config::FullPayloadIndexType;
use crate::json_path::JsonPath;
use crate::types::{PayloadContainer, PayloadFieldSchema, PayloadKeyTypeRef};

/// State of building the per-element indexes of one field.
pub(super) struct NestedBuild<'a> {
    /// The array whose elements are indexed, e.g. `offers[]`
    array_key: &'a JsonPath,
    /// The indexed field inside an element, e.g. `price` for `offers[].price`
    relative_path: JsonPath,
    elements: ElementMapBuilder,
}

impl<'a> NestedBuild<'a> {
    pub(super) fn new(field: PayloadKeyTypeRef, array_key: &'a JsonPath) -> OperationResult<Self> {
        Ok(Self {
            array_key,
            relative_path: relative_path(field, array_key)?,
            elements: ElementMapBuilder::default(),
        })
    }

    /// Give the elements of `point` their offsets and feed them to the `builders`.
    pub(super) fn add_point(
        &mut self,
        point: PointOffsetType,
        payload: &impl PayloadContainer,
        builders: &mut [FieldIndexBuilder],
    ) -> OperationResult<()> {
        let elements = nested_elements(payload, self.array_key);
        let offsets = self.elements.add(point, elements.len())?;
        for (element, object) in offsets.zip(elements) {
            let values = self.relative_path.value_get(object);
            for builder in builders.iter_mut() {
                builder.add_point(element, &values)?;
            }
        }
        Ok(())
    }
}

impl StructPayloadIndex {
    /// Wrap the built inner indexes of `field` into per-element indexes, each with an
    /// element map of its own.
    ///
    /// The map is as mutable as its inner index.
    pub(super) fn finish_nested_build(
        &self,
        field: PayloadKeyTypeRef,
        build: &NestedBuild,
        indexes: Vec<FieldIndex>,
    ) -> OperationResult<Vec<FieldIndex>> {
        indexes
            .into_iter()
            .map(|inner| {
                let FullPayloadIndexType {
                    index_type,
                    mutability,
                    // The map is stored the same way for every storage of the index
                    storage_type: _,
                } = inner.get_full_index_type();
                let index_dir = index_type.storage_dir(&self.path, field);
                let element_map = ElementMap::create(
                    ElementMap::storage_dir(&index_dir),
                    mutability,
                    &build.elements,
                    self.journaled,
                )?;
                Ok(FieldIndex::Nested(Box::new(NestedFieldIndex::new(
                    field,
                    build.array_key.clone(),
                    element_map,
                    inner,
                )?)))
            })
            .collect()
    }

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
        let deleted_elements = element_map.deleted_elements(id_tracker.deleted_point_bitslice());
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
        )?))))
    }
}

fn relative_path(field: PayloadKeyTypeRef, array_key: &JsonPath) -> OperationResult<JsonPath> {
    field.strip_prefix(array_key).ok_or_else(|| {
        OperationError::service_error(format!(
            "field `{field}` is not inside the elements of `{array_key}`",
        ))
    })
}
