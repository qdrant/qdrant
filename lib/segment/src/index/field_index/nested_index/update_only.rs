//! Per-element indexes of update-only segments: the writer of an inner index keyed by
//! element offsets, and of its element map.

use std::path::Path;

use blobstore::BlobstoreReader;
use common::types::PointOffsetType;
use common::universal_io::{OkNotFound, Populate, UniversalAppend, UniversalAppendFs};
use serde_json::Value;

use super::element_map::UpdateOnlyElementMap;
use super::{ElementMap, ElementOffsetType, nested_elements};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::UpdateOnlyFieldIndex;
use crate::index::payload_config::{FullPayloadIndexType, PayloadIndexType};
use crate::json_path::JsonPath;

/// The write half of a per-element index of an update-only segment.
pub struct UpdateOnlyNestedFieldIndex<S: UniversalAppend + 'static> {
    /// The array whose elements are indexed, as its array key, e.g. `offers[]`
    array_key: JsonPath,
    /// The indexed field inside an element, e.g. `price` for `offers[].price`
    relative_path: JsonPath,
    element_map: UpdateOnlyElementMap<S>,
    /// Writer of the index keyed by element offsets
    inner: UpdateOnlyFieldIndex<S>,
}

impl<S: UniversalAppend + 'static> UpdateOnlyNestedFieldIndex<S> {
    /// Open the writers of `field`'s per-element index stored as `index_type` under the
    /// payload index root `dir`. `inner` is the writer of the inner index.
    pub fn open(
        fs: &impl UniversalAppendFs<AppendFile = S>,
        dir: &Path,
        field: &JsonPath,
        array_key: &JsonPath,
        index_type: &FullPayloadIndexType,
        inner: UpdateOnlyFieldIndex<S>,
    ) -> OperationResult<Self> {
        let relative_path = field.strip_prefix(array_key).ok_or_else(|| {
            OperationError::service_error(format!(
                "field `{field}` is not inside the elements of `{array_key}`",
            ))
        })?;
        let index_dir = index_type.index_type.storage_dir(dir, field);
        let min_next_element = inner_next_element(fs, &index_dir, &index_type.index_type)?;
        let element_map =
            UpdateOnlyElementMap::open(fs, &ElementMap::storage_dir(&index_dir), min_next_element)?;
        Ok(Self {
            array_key: array_key.clone(),
            relative_path,
            element_map,
            inner,
        })
    }

    /// The array whose elements are indexed, as its array key.
    pub fn array_key(&self) -> &JsonPath {
        &self.array_key
    }

    /// Index the elements of `values`, the values at the array key, at the slot the ID
    /// tracker claimed for the point. Buffers only, see [`Self::flush`].
    pub fn add_point(
        &mut self,
        fs: &impl UniversalAppendFs<AppendFile = S>,
        slot: PointOffsetType,
        values: &[&Value],
    ) -> OperationResult<()> {
        let elements = nested_elements(values);
        let offsets = self.element_map.add(fs, slot, elements.len())?;
        for (element, object) in offsets.zip(elements) {
            let values = self.relative_path.value_get(object);
            self.inner.add_point(fs, element, &values)?;
        }
        Ok(())
    }

    /// Persist everything buffered since the last flush.
    pub fn flush(&mut self, fs: &impl UniversalAppendFs<AppendFile = S>) -> OperationResult<()> {
        // Map last: an element offset that the index knows but the map doesn't is never
        // resolved, see `ElementMap`
        self.inner.flush(fs)?;
        self.element_map.flush()
    }
}

/// The lowest element offset the inner index stored in `index_dir` accepts values at, if
/// it has one.
///
/// Value indexes only append, at increasing offsets. After a crash between flushing the
/// index and its map, the index holds values at offsets the map never gave out, and refuses
/// values at them again.
///
/// Bool and null indexes take values at any offset: they set flags in a mask they rewrite
/// whole. Offsets with stale flags can be given out again, since the writer sets the flags
/// of every element it adds, and readers never resolve an offset the map doesn't give to a
/// point.
fn inner_next_element(
    fs: &impl UniversalAppendFs,
    index_dir: &Path,
    index_type: &PayloadIndexType,
) -> OperationResult<Option<ElementOffsetType>> {
    match index_type {
        PayloadIndexType::IntIndex
        | PayloadIndexType::DatetimeIndex
        | PayloadIndexType::IntMapIndex
        | PayloadIndexType::KeywordIndex
        | PayloadIndexType::FloatIndex
        | PayloadIndexType::GeoIndex
        | PayloadIndexType::FullTextIndex
        | PayloadIndexType::UuidIndex
        | PayloadIndexType::UuidMapIndex => {
            let reader =
                BlobstoreReader::<Vec<u8>, _>::open(fs, index_dir.to_path_buf(), Populate::No)
                    .ok_not_found()?;
            match reader {
                Some(reader) => Ok(Some(reader.max_point_offset()?)),
                None => Ok(Some(0)),
            }
        }
        PayloadIndexType::BoolIndex | PayloadIndexType::NullIndex => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use common::ambient;
    use common::bitvec::BitVec;
    use common::universal_io::{MmapFile, MmapFs};
    use serde_json::json;

    use super::*;
    use crate::index::field_index::{FieldIndexRead as _, ReadOnlyFieldIndex};
    use crate::index::payload_config::{IndexMutability, StorageType};
    use crate::types::{PayloadFieldSchema, PayloadSchemaType};

    /// After a crash between flushing the index and its map, the index holds values at
    /// offsets the map never gave out. The next writer gives new elements offsets above
    /// them, as the index only appends.
    #[test]
    fn writer_resumes_after_index_flushed_without_map() {
        let dir = tempfile::tempdir().unwrap();
        let _scope = ambient::test_guard();
        let field = JsonPath::new("offers[].price");
        let schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Integer).nested_in("offers");
        let index_type = FullPayloadIndexType {
            index_type: PayloadIndexType::IntIndex,
            mutability: IndexMutability::Mutable,
            storage_type: StorageType::Gridstore,
        };
        let open = || {
            UpdateOnlyFieldIndex::<MmapFile>::open(
                &MmapFs,
                dir.path(),
                &field,
                &schema,
                &index_type,
            )
            .unwrap()
        };
        // The values at the array key `offers[]`: one object per element
        let offers = |prices: &[i64]| -> Vec<serde_json::Value> {
            prices
                .iter()
                .map(|price| json!({ "price": price }))
                .collect()
        };
        let add = |index: &mut UpdateOnlyFieldIndex<MmapFile>, slot, prices: &[i64]| {
            let elements = offers(prices);
            let values: Vec<_> = elements.iter().collect();
            index.add_point(&MmapFs, slot, &values).unwrap();
        };

        let mut index = open();
        add(&mut index, 0, &[1, 2]);
        let UpdateOnlyFieldIndex::Nested(nested) = &mut index else {
            panic!("must be a per-element index");
        };
        nested.inner.flush(&MmapFs).unwrap();
        drop(index);

        let mut index = open();
        add(&mut index, 1, &[3]);
        add(&mut index, 2, &[4, 5]);
        index.flush(&MmapFs).unwrap();
        drop(index);

        let index = ReadOnlyFieldIndex::open(
            &MmapFs,
            dir.path(),
            &field,
            &schema,
            &index_type,
            3,
            &BitVec::repeat(false, 3),
            None,
        )
        .unwrap()
        .unwrap();
        // The lost point has no elements, the later ones have their own
        assert_eq!(index.values_count(0).unwrap(), 0);
        assert_eq!(index.values_count(1).unwrap(), 1);
        assert_eq!(index.values_count(2).unwrap(), 2);
    }
}
