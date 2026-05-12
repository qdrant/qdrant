use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
use fs_err as fs;
use serde_json::Value;

use super::read_ops::{self, NullIndexRead};
use crate::common::Flusher;
use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;
use crate::common::flags::roaring_flags::RoaringFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexRead,
};
use crate::index::payload_config::IndexMutability;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, PayloadKeyType};

pub(super) const HAS_VALUES_DIRNAME: &str = "has_values";
pub(super) const IS_NULL_DIRNAME: &str = "is_null";

/// Mutable variant of null index that uses roaring bitmaps for in-memory operations
/// and buffers updates before persisting them to DynamicMmapFlags.
pub struct MutableNullIndex {
    base_dir: PathBuf,
    storage: Storage<MmapFile>,
    total_point_count: usize,
}

struct Storage<S> {
    /// Points which have at least one value
    has_values_flags: RoaringFlags<S>,
    /// Points which have null values
    is_null_flags: RoaringFlags<S>,
}

impl MutableNullIndex {
    pub fn builder(
        path: &Path,
        total_point_count: usize,
    ) -> OperationResult<MutableNullIndexBuilder> {
        Ok(MutableNullIndexBuilder(
            Self::open(path, total_point_count, true)?.ok_or_else(|| {
                OperationError::service_error(format!(
                    "Failed to create and open mutable null index at path: {}",
                    path.display(),
                ))
            })?,
        ))
    }

    /// Open and load or create a mutable null index at the given path.
    ///
    /// # Arguments
    /// - `path` - The directory where the index files should live, must be exclusive to this index.
    /// - `total_point_count` - Total number of points in the segment.
    /// - `create_if_missing` - If true, creates the index if it doesn't exist.
    pub fn open(
        path: &Path,
        total_point_count: usize,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        let has_values_dir = path.join(HAS_VALUES_DIRNAME);

        // If has values directory doesn't exist, assume the index doesn't exist on disk
        if !has_values_dir.is_dir() && !create_if_missing {
            return Ok(None);
        }

        Ok(Some(Self::open_or_create(path, total_point_count)?))
    }

    pub(super) fn open_immutable(
        path: &Path,
        total_point_count: usize,
        deleted: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let mutable_null_index = Self::open(path, total_point_count, false)?;
        match mutable_null_index {
            Some(mut mutable_null_index) => {
                for pos in deleted.iter_ones() {
                    mutable_null_index.remove_point_immutable(pos as PointOffsetType);
                }
                Ok(Some(mutable_null_index))
            }
            None => Ok(None),
        }
    }

    fn open_or_create(path: &Path, total_point_count: usize) -> OperationResult<Self> {
        fs::create_dir_all(path).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to create mutable-null-index directory: {err}, path: {path:?}"
            ))
        })?;

        let has_values_path = path.join(HAS_VALUES_DIRNAME);
        let has_values_mmap = DynamicStoredFlags::open(&has_values_path, false)?;
        let has_values_flags = RoaringFlags::new(has_values_mmap)?;

        let is_null_path = path.join(IS_NULL_DIRNAME);
        let is_null_mmap = DynamicStoredFlags::open(&is_null_path, false)?;
        let is_null_flags = RoaringFlags::new(is_null_mmap)?;

        let storage = Storage {
            has_values_flags,
            is_null_flags,
        };

        Ok(Self {
            base_dir: path.to_path_buf(),
            storage,
            total_point_count,
        })
    }

    pub fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut is_null = false;
        let mut has_values = false;

        for value in payload {
            match value {
                Value::Null => {
                    is_null = true;
                }
                Value::Bool(_) => {
                    has_values = true;
                }
                Value::Number(_) => {
                    has_values = true;
                }
                Value::String(_) => {
                    has_values = true;
                }
                Value::Array(array) => {
                    if array.iter().any(|v| v.is_null()) {
                        is_null = true;
                    }
                    if !array.is_empty() {
                        has_values = true;
                    }
                }
                Value::Object(_) => {
                    has_values = true;
                }
            }
            if is_null && has_values {
                break;
            }
        }

        self.storage.has_values_flags.set(id, has_values);
        self.storage.is_null_flags.set(id, is_null);

        // Bump total points
        self.total_point_count = std::cmp::max(self.total_point_count, id as usize + 1);

        // Account for I/O cost as if we were writing to disk now
        hw_counter.payload_index_io_write_counter().incr_delta(2);

        Ok(())
    }

    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        // Update bitmaps immediately
        self.storage.has_values_flags.set(id, false);
        self.storage.is_null_flags.set(id, false);

        // Bump total points
        // We MUST bump the total point count when removing a point too
        // On upsert without this respective field, remove point is called rather than add point
        // Bumping the total point count ensures we correctly estimate the number of points
        // Bug: <https://github.com/qdrant/qdrant/pull/6882>
        self.total_point_count = std::cmp::max(self.total_point_count, id as usize + 1);

        // Account for I/O cost as if we were writing to disk now
        let hw_counter = HardwareCounterCell::disposable();
        hw_counter.payload_index_io_write_counter().incr_delta(2);

        Ok(())
    }

    pub(super) fn remove_point_immutable(&mut self, id: PointOffsetType) {
        // Update bitmaps immediately
        self.storage.has_values_flags.set_immutable(id, false);
        self.storage.is_null_flags.set_immutable(id, false);

        // N.B. We do not update total_point_count because it comes from the id tracker and is not not changed
        // in non-appendable segments.

        // N.B. No I/O, do not update hw_counter.
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        IndexMutability::Mutable
    }
}

impl PayloadFieldIndex for MutableNullIndex {
    fn wipe(self) -> OperationResult<()> {
        let base_dir = self.base_dir.clone();
        // drop mmap handles before deleting files
        drop(self);
        if base_dir.is_dir() {
            fs::remove_dir_all(&base_dir)?;
        }
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let flush_has_values = self.storage.has_values_flags.flusher();
        let flush_is_null = self.storage.is_null_flags.flusher();

        Box::new(move || {
            flush_has_values()?;
            flush_is_null()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        NullIndexRead::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new() // everything is mutable
    }
}

impl NullIndexRead for MutableNullIndex {
    type Flags = RoaringFlags<MmapFile>;

    fn has_values_flags(&self) -> &Self::Flags {
        &self.storage.has_values_flags
    }

    fn is_null_flags(&self) -> &Self::Flags {
        &self.storage.is_null_flags
    }

    fn total_point_count(&self) -> usize {
        self.total_point_count
    }

    fn telemetry_index_type(&self) -> &'static str {
        "mutable_null_index"
    }
}

impl PayloadFieldIndexRead for MutableNullIndex {
    fn count_indexed_points(&self) -> usize {
        self.indexed_points_count()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        _hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        Ok(read_ops::filter(self, condition))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(read_ops::estimate_cardinality(self, condition))
    }

    fn for_each_payload_block(
        &self,
        _threshold: usize,
        _key: PayloadKeyType,
        _f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // No payload blocks
        Ok(())
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        read_ops::condition_checker(self, condition, hw_acc)
    }
}

pub struct MutableNullIndexBuilder(MutableNullIndex);

impl FieldIndexBuilderTrait for MutableNullIndexBuilder {
    type FieldIndexType = MutableNullIndex;

    fn init(&mut self) -> OperationResult<()> {
        // After Self is created, it is already initialized
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&serde_json::Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.0.add_point(id, payload, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        // Flush any remaining buffered updates
        self.0.flusher()()?;
        Ok(self.0)
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use tempfile::TempDir;

    use super::*;
    use crate::json_path::JsonPath;

    #[test]
    fn test_build_and_use_mutable_null_index() {
        let dir = TempDir::with_prefix("test_mutable_null_index").unwrap();

        let null_value = Value::Null;
        let null_value_in_array =
            Value::Array(vec![Value::String("test".to_string()), Value::Null]);

        let n: PointOffsetType = 100;

        let mut builder = MutableNullIndex::builder(dir.path(), n as usize).unwrap();

        let hw_counter = HardwareCounterCell::new();

        for i in 0..n {
            match i % 4 {
                0 => builder.add_point(i, &[&null_value], &hw_counter).unwrap(),
                1 => builder
                    .add_point(i, &[&null_value_in_array], &hw_counter)
                    .unwrap(),
                2 => builder.add_point(i, &[], &hw_counter).unwrap(),
                3 => builder
                    .add_point(i, &[&Value::Bool(true)], &hw_counter)
                    .unwrap(),
                _ => unreachable!(),
            }
        }

        let null_index = builder.finalize().unwrap();
        let key = JsonPath::new("test");

        let filter_is_null = FieldCondition::new_is_null(key.clone(), true);

        let filter_is_not_empty = FieldCondition {
            key: key.clone(),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: Some(false),
            is_null: None,
        };

        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();

        let is_null_values: Vec<_> = null_index
            .filter(&filter_is_null, &hw_counter)
            .unwrap()
            .unwrap()
            .collect();
        let not_empty_values: Vec<_> = null_index
            .filter(&filter_is_not_empty, &hw_counter)
            .unwrap()
            .unwrap()
            .collect();

        let is_empty_values: Vec<_> = (0..n)
            .filter(|&id| null_index.values_is_empty(id))
            .collect();
        let not_null_values: Vec<_> = (0..n)
            .filter(|&id| !null_index.values_is_null(id))
            .collect();

        for i in 0..n {
            match i % 4 {
                0 => {
                    // &[&null_value]
                    assert!(is_null_values.contains(&i));
                    assert!(!not_empty_values.contains(&i));

                    assert!(!not_null_values.contains(&i));
                    assert!(is_empty_values.contains(&i));
                }
                1 => {
                    // &[&null_value_in_array]
                    assert!(is_null_values.contains(&i));
                    assert!(not_empty_values.contains(&i));

                    assert!(!not_null_values.contains(&i));
                    assert!(!is_empty_values.contains(&i));
                }
                2 => {
                    // &[]
                    assert!(!is_null_values.contains(&i));
                    assert!(!not_empty_values.contains(&i));

                    assert!(not_null_values.contains(&i));
                    assert!(is_empty_values.contains(&i));
                }
                3 => {
                    // &[&Value::Bool(true)]
                    assert!(!is_null_values.contains(&i));
                    assert!(not_empty_values.contains(&i));

                    assert!(not_null_values.contains(&i));
                    assert!(!is_empty_values.contains(&i));
                }
                _ => unreachable!(),
            }
        }

        let hw_cell = HardwareCounterCell::new();
        let is_null_cardinality = null_index
            .estimate_cardinality(&filter_is_null, &hw_cell)
            .unwrap()
            .unwrap();
        let non_empty_cardinality = null_index
            .estimate_cardinality(&filter_is_not_empty, &hw_cell)
            .unwrap()
            .unwrap();

        assert_eq!(is_null_cardinality.exp, 50);
        assert_eq!(non_empty_cardinality.exp, 50);
    }

    #[test]
    fn test_manual_buffer_flushing() {
        let dir = TempDir::with_prefix("test_manual_buffer_flushing").unwrap();
        let mut index = MutableNullIndex::builder(dir.path(), 10).unwrap().0;

        let hw_counter = HardwareCounterCell::new();

        // Add points without automatic flushing
        for i in 0..10 {
            index
                .add_point(i as PointOffsetType, &[&Value::Bool(true)], &hw_counter)
                .unwrap();
        }

        // Manually flush via flusher
        index.flusher()().unwrap();

        // Verify data is in bitmaps
        for i in 0..10 {
            assert!(!index.values_is_empty(i as PointOffsetType));
            assert!(!index.values_is_null(i as PointOffsetType));
        }
    }
}
