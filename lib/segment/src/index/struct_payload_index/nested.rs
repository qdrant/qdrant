//! Element maps of the per-element (nested) field indexes, see
//! [`nested_index`](crate::index::field_index::nested_index).
//!
//! The payload index owns one [`ElementMap`] per nested array that has at least one
//! per-element index. It keeps the map in step with every payload write, then feeds
//! the elements to the per-element indexes of that array.

use std::collections::HashSet;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;

use super::StructPayloadIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::FieldIndex;
use crate::index::field_index::nested_index::{
    ElementMap, NestedFieldIndex, SharedElementMap, nested_elements,
};
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorageRead;
use crate::types::{PayloadContainer, PayloadKeyTypeRef};

impl StructPayloadIndex {
    /// Open the element map of every nested array of the indexed fields, building the
    /// ones that are missing from payload storage.
    ///
    /// Returns the arrays whose map was built: their per-element indexes refer to
    /// element offsets of a lost map and must be rebuilt.
    pub(super) fn load_element_maps(&mut self) -> OperationResult<HashSet<JsonPath>> {
        let array_keys: HashSet<JsonPath> = self
            .config
            .indices
            .values()
            .filter_map(|indexed| indexed.schema.nested().cloned())
            .collect();

        let mut rebuilt = HashSet::new();
        for array_key in array_keys {
            let path = ElementMap::storage_dir(&self.path, &array_key);
            let map = match ElementMap::open(path)? {
                Some(map) => map,
                None => {
                    log::debug!("Rebuilding element map of `{array_key}`...");
                    rebuilt.insert(array_key.clone());
                    self.build_element_map(&array_key)?
                }
            };
            self.element_maps
                .insert(array_key, Arc::new(AtomicRefCell::new(map)));
        }
        Ok(rebuilt)
    }

    /// Build a new element map of `array_key` from payload storage, and persist it.
    ///
    /// Not shared with anything yet, so flushing it here doesn't interfere with the
    /// flush pipeline.
    fn build_element_map(&self, array_key: &JsonPath) -> OperationResult<ElementMap> {
        let mut map = ElementMap::create(ElementMap::storage_dir(&self.path, array_key))?;
        if !self.journaled {
            map.disable_journal();
        }
        self.payload.borrow().iter(|point_id, payload| {
            let count = nested_elements(payload, array_key).len();
            if count > 0 {
                map.assign(point_id, count)?;
            }
            Ok(true)
        })?;
        map.flusher()()?;
        Ok(map)
    }

    /// The element map of `array_key` for building a per-element index: the open one,
    /// or a new one built from payload storage.
    pub(super) fn element_map_for_build(
        &self,
        array_key: &JsonPath,
    ) -> OperationResult<SharedElementMap> {
        match self.element_maps.get(array_key) {
            Some(map) => Ok(map.clone()),
            None => Ok(Arc::new(AtomicRefCell::new(
                self.build_element_map(array_key)?,
            ))),
        }
    }

    /// Wrap the value indexes of `field` built over the elements of `element_map` into
    /// per-element indexes; the null index stays an index of points.
    pub(super) fn wrap_nested(
        field: PayloadKeyTypeRef,
        array_key: &JsonPath,
        element_map: &SharedElementMap,
        indexes: Vec<FieldIndex>,
    ) -> OperationResult<Vec<FieldIndex>> {
        indexes
            .into_iter()
            .map(|index| {
                if let FieldIndex::NullIndex(_) = index {
                    return Ok(index);
                }
                Ok(FieldIndex::Nested(Box::new(NestedFieldIndex::new(
                    field,
                    array_key.clone(),
                    element_map.clone(),
                    index,
                )?)))
            })
            .collect()
    }

    /// Start using the element map of newly applied per-element `indexes`.
    pub(super) fn register_element_map(&mut self, indexes: &[FieldIndex]) {
        for index in indexes {
            if let FieldIndex::Nested(index) = index {
                let map = self
                    .element_maps
                    .entry(index.array_key().clone())
                    .or_insert_with(|| index.element_map().clone());
                debug_assert!(
                    Arc::ptr_eq(map, index.element_map()),
                    "per-element indexes of one array must share its element map",
                );
            }
        }
    }

    /// Drop and wipe the element maps no indexed field uses anymore.
    pub(super) fn drop_unused_element_maps(&mut self) -> OperationResult<()> {
        let used: HashSet<&JsonPath> = self
            .config
            .indices
            .values()
            .filter_map(|indexed| indexed.schema.nested())
            .collect();
        let unused: Vec<JsonPath> = self
            .element_maps
            .keys()
            .filter(|array_key| !used.contains(array_key))
            .cloned()
            .collect();

        for array_key in unused {
            let Some(map) = self.element_maps.remove(&array_key) else {
                continue;
            };
            match Arc::try_unwrap(map) {
                Ok(map) => map.into_inner().wipe()?,
                Err(_) => {
                    // Still referenced by an index that is being built; its storage
                    // directory goes with the next build of the array
                    log::debug!("Element map of `{array_key}` is still in use, not wiped");
                }
            }
        }
        Ok(())
    }

    /// Bring the element maps of the arrays `is_affected` selects in line with the
    /// `payload` of `point_id`, and index its elements.
    pub(super) fn update_nested_indexes(
        &mut self,
        point_id: PointOffsetType,
        payload: &impl PayloadContainer,
        is_affected: impl Fn(&JsonPath) -> bool,
    ) -> OperationResult<()> {
        let Self {
            element_maps,
            field_indexes,
            ..
        } = self;

        for (array_key, map) in element_maps.iter() {
            if !is_affected(array_key) {
                continue;
            }
            let elements = nested_elements(payload, array_key);
            let update = map.borrow_mut().assign(point_id, elements.len())?;

            for indexes in field_indexes.values_mut() {
                for index in indexes {
                    if let FieldIndex::Nested(index) = index
                        && index.array_key() == array_key
                    {
                        index.update_point(&update, &elements)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Remove `point_id` from all element maps. Its per-element indexes must have
    /// been cleared before.
    pub(super) fn clear_nested_elements(&self, point_id: PointOffsetType) -> OperationResult<()> {
        for map in self.element_maps.values() {
            map.borrow_mut().assign(point_id, 0)?;
        }
        Ok(())
    }
}
