use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use serde_json::Value;

use super::{ElementMap, ElementMapBuilder, NestedFieldIndex, nested_elements};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{FieldIndexBuilder, FieldIndexBuilderTrait};
use crate::json_path::JsonPath;

/// Builds an index over array elements, consuming values at its array key.
pub struct NestedFieldIndexBuilder {
    inner: FieldIndexBuilder,
    dir: PathBuf,
    field: JsonPath,
    array_key: JsonPath,
    relative_path: JsonPath,
    elements: ElementMapBuilder,
    /// Upper bound on points with indexed values, persisted by immutable maps.
    indexed_points: usize,
    last_point: Option<PointOffsetType>,
    journaled: bool,
}

impl NestedFieldIndexBuilder {
    pub fn new(
        dir: &Path,
        field: &JsonPath,
        array_key: &JsonPath,
        inner: FieldIndexBuilder,
    ) -> OperationResult<Self> {
        let relative_path = field.strip_prefix(array_key).ok_or_else(|| {
            OperationError::service_error(format!(
                "field `{field}` is not inside the elements of `{array_key}`",
            ))
        })?;
        Ok(Self {
            inner,
            dir: dir.to_owned(),
            field: field.clone(),
            array_key: array_key.clone(),
            relative_path,
            elements: ElementMapBuilder::default(),
            indexed_points: 0,
            last_point: None,
            journaled: true,
        })
    }

    pub fn input_path(&self) -> &JsonPath {
        &self.array_key
    }

    pub fn disable_journal(&mut self) {
        self.journaled = false;
        self.inner.disable_journal();
    }
}

impl FieldIndexBuilderTrait for NestedFieldIndexBuilder {
    type FieldIndexType = NestedFieldIndex;

    fn init(&mut self) -> OperationResult<()> {
        self.inner.init()
    }

    fn add_point(&mut self, point: PointOffsetType, values: &[&Value]) -> OperationResult<bool> {
        // Immutable maps need points in ascending order, as payload storage visits them.
        debug_assert!(
            self.last_point.is_none_or(|last_point| last_point < point),
            "point {point} added after point {:?}",
            self.last_point,
        );
        self.last_point = Some(point);

        let elements = nested_elements(values);
        let offsets = self.elements.add(point, elements.len())?;
        let mut has_values = false;
        for (element, object) in offsets.zip(elements) {
            let values = self.relative_path.value_get(object);
            has_values |= self.inner.add_point(element, &values)?;
        }
        if has_values {
            self.indexed_points += 1;
        }
        Ok(has_values)
    }

    fn finalize(self) -> OperationResult<NestedFieldIndex> {
        let inner = self.inner.finalize()?;
        let index_type = inner.get_full_index_type();
        let index_dir = index_type.index_type.storage_dir(&self.dir, &self.field);
        let element_map = ElementMap::create(
            ElementMap::storage_dir(&index_dir),
            index_type.mutability,
            &self.elements,
            self.indexed_points,
            self.journaled,
        )?;
        let indexed_points = element_map.indexed_points();
        NestedFieldIndex::new(
            &self.field,
            self.array_key,
            element_map,
            inner,
            indexed_points,
        )
    }
}
