use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use roaring::RoaringBitmap;
use serde_json::Value;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    PrimaryCondition,
};
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, PayloadKeyType};
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;

const HAS_VALUES_DIRNAME: &str = "has_values";
const IS_NULL_DIRNAME: &str = "is_null";

/// Mutable variant of null index that uses roaring bitmaps for in-memory operations
/// and buffers updates before persisting them to DynamicMmapFlags.
pub struct MutableNullIndex {
    base_dir: PathBuf,
    /// Persistent storage (DynamicMmapFlags only)
    storage: Option<Arc<Mutex<Storage>>>,
    /// In-memory roaring bitmap tracking which points have values
    has_values_bitmap: RoaringBitmap,
    /// In-memory roaring bitmap tracking which points have null values
    is_null_bitmap: RoaringBitmap,
    total_point_count: usize,
    /// Buffer for pending updates to avoid frequent disk writes
    update_buffer: Arc<RwLock<AHashMap<PointOffsetType, Update>>>,
}

struct Storage {
    /// Persistent storage for has_values
    has_values_mmap: DynamicMmapFlags,
    /// Persistent storage for is_null
    is_null_mmap: DynamicMmapFlags,
}

#[derive(Clone, Copy)]
struct Update {
    has_values: bool,
    is_null: bool,
}

impl MutableNullIndex {
    pub fn builder(path: &Path) -> OperationResult<MutableNullIndexBuilder> {
        Ok(MutableNullIndexBuilder(Self::open(path, 0, true)?))
    }

    /// Open or create a mutable null index at the given path.
    ///
    /// # Arguments
    /// - `path` - The directory where the index files should live, must be exclusive to this index.
    /// - `total_point_count` - Total number of points in the segment.
    /// - `create_if_missing` - If true, creates the index if it doesn't exist.
    pub fn open(
        path: &Path,
        total_point_count: usize,
        create_if_missing: bool,
    ) -> OperationResult<Self> {
        let has_values_dir = path.join(HAS_VALUES_DIRNAME);

        // If has values directory doesn't exist, assume the index doesn't exist on disk
        if !has_values_dir.is_dir() && !create_if_missing {
            return Ok(Self {
                base_dir: path.to_path_buf(),
                storage: None,
                has_values_bitmap: RoaringBitmap::new(),
                is_null_bitmap: RoaringBitmap::new(),
                total_point_count,
                update_buffer: Arc::new(RwLock::new(AHashMap::default())),
            });
        }

        Self::open_or_create(path, total_point_count)
    }

    fn open_or_create(path: &Path, total_point_count: usize) -> OperationResult<Self> {
        std::fs::create_dir_all(path).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to create mutable-null-index directory: {err}, path: {path:?}"
            ))
        })?;

        let has_values_path = path.join(HAS_VALUES_DIRNAME);
        let has_values_mmap = DynamicMmapFlags::open(&has_values_path, false)?;

        let is_null_path = path.join(IS_NULL_DIRNAME);
        let is_null_mmap = DynamicMmapFlags::open(&is_null_path, false)?;

        // Initialize roaring bitmaps from mmap data
        let mut has_values_bitmap = RoaringBitmap::new();
        let mut is_null_bitmap = RoaringBitmap::new();

        // Load existing data from mmap into bitmaps
        for id in has_values_mmap.iter_trues() {
            has_values_bitmap.insert(id);
        }
        for id in is_null_mmap.iter_trues() {
            is_null_bitmap.insert(id);
        }

        Ok(Self {
            base_dir: path.to_path_buf(),
            storage: Some(Arc::new(Mutex::new(Storage {
                has_values_mmap,
                is_null_mmap,
            }))),
            has_values_bitmap,
            is_null_bitmap,
            total_point_count,
            update_buffer: Arc::new(RwLock::new(AHashMap::default())),
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

        // Update bitmaps immediately for consistent reads
        if has_values {
            self.has_values_bitmap.insert(id);
        } else {
            self.has_values_bitmap.remove(id);
        }

        if is_null {
            self.is_null_bitmap.insert(id);
        } else {
            self.is_null_bitmap.remove(id);
        }

        // Account for I/O cost as if we were writing to disk now
        // Each update touches 2 bits (has_values and is_null), but we count as 1 byte minimum
        hw_counter.payload_index_io_write_counter().incr_delta(1);

        // Also buffer the updates for persistence
        {
            let mut buffer = self.update_buffer.write().map_err(|e| {
                OperationError::service_error(format!(
                    "Failed to acquire write lock on update buffer: {e}"
                ))
            })?;
            buffer.insert(
                id,
                Update {
                    has_values,
                    is_null,
                },
            );
        }

        // Bump total points
        self.total_point_count = std::cmp::max(self.total_point_count, id as usize + 1);

        Ok(())
    }

    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        // Update bitmaps immediately
        self.has_values_bitmap.remove(id);
        self.is_null_bitmap.remove(id);

        // Account for I/O cost as if we were writing to disk now
        let hw_counter = HardwareCounterCell::disposable();
        hw_counter.payload_index_io_write_counter().incr_delta(1);

        // Also buffer the removal for persistence
        {
            let mut buffer = self.update_buffer.write().map_err(|e| {
                OperationError::service_error(format!(
                    "Failed to acquire write lock on update buffer: {e}"
                ))
            })?;
            buffer.insert(
                id,
                Update {
                    has_values: false,
                    is_null: false,
                },
            );
        }

        // Bump total points (same as in mmap implementation)
        self.total_point_count = std::cmp::max(self.total_point_count, id as usize + 1);

        Ok(())
    }

    pub fn values_count(&self, id: PointOffsetType) -> usize {
        usize::from(self.has_values_bitmap.contains(id))
    }

    pub fn values_is_empty(&self, id: PointOffsetType) -> bool {
        !self.has_values_bitmap.contains(id)
    }

    pub fn values_is_null(&self, id: PointOffsetType) -> bool {
        self.is_null_bitmap.contains(id)
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        let points_count = self.has_values_bitmap.len() as usize;

        PayloadIndexTelemetry {
            field_name: None,
            points_count,
            points_values_count: points_count,
            histogram_bucket_size: None,
            index_type: "mutable_null_index",
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        Ok(())
    }
    pub fn is_on_disk(&self) -> bool {
        false
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        if let Some(storage) = &self.storage {
            let guard = storage.lock().map_err(|e| {
                OperationError::service_error(format!("Failed to acquire mutex: {e}"))
            })?;
            guard.is_null_mmap.clear_cache()?;
            guard.has_values_mmap.clear_cache()?;
        }

        Ok(())
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        IndexMutability::Mutable
    }

    pub fn get_storage_type(&self) -> StorageType {
        StorageType::Mmap {
            is_on_disk: self.is_on_disk(),
        }
    }
}

impl PayloadFieldIndex for MutableNullIndex {
    fn count_indexed_points(&self) -> usize {
        self.has_values_bitmap.len() as usize
    }

    fn load(&mut self) -> OperationResult<bool> {
        let is_loaded = self.storage.is_some();
        Ok(is_loaded)
    }

    fn cleanup(self) -> OperationResult<()> {
        std::fs::remove_dir_all(&self.base_dir)?;
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let Some(storage) = &self.storage else {
            return Box::new(|| Ok(()));
        };

        // Clone Arc references for the closure
        let storage = Arc::clone(storage);
        let update_buffer = Arc::clone(&self.update_buffer);

        Box::new(move || {
            // Flush buffer to storage
            let updates = {
                let mut buffer = update_buffer.write().map_err(|e| {
                    OperationError::service_error(format!("Failed to acquire write lock: {e}"))
                })?;

                std::mem::take(&mut *buffer)
            };

            let Some(max_id) = updates.iter().map(|(&id, _)| id).max() else {
                // No max id means there are no updates
                debug_assert!(updates.is_empty());
                return Ok(());
            };

            let mut storage_guard = storage.lock().map_err(|e| {
                OperationError::service_error(format!("Failed to acquire mutex: {e}"))
            })?;

            // Resize once if needed
            if storage_guard.has_values_mmap.len() <= max_id as usize {
                storage_guard
                    .has_values_mmap
                    .set_len((max_id + 1) as usize)?;
            }
            if storage_guard.is_null_mmap.len() <= max_id as usize {
                storage_guard.is_null_mmap.set_len((max_id + 1) as usize)?;
            }

            // Persist buffered updates to mmap
            for (&id, &update) in &updates {
                storage_guard.has_values_mmap.set(id, update.has_values);
                storage_guard.is_null_mmap.set(id, update.is_null);
            }

            // Then flush mmap data
            storage_guard.has_values_mmap.flusher()()?;
            storage_guard.is_null_mmap.flusher()()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let Some(storage) = &self.storage else {
            return vec![];
        };

        let storage = storage.lock().expect("Should acquire mutex"); // todo: propagate error

        let mut files = storage.has_values_mmap.files();
        files.extend(storage.is_null_mmap.files());
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new() // everything is mutable
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        _hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let FieldCondition {
            key: _,
            r#match: _,
            range: _,
            geo_bounding_box: _,
            geo_radius: _,
            geo_polygon: _,
            values_count: _,
            is_empty,
            is_null,
        } = condition;

        if let Some(is_empty) = is_empty {
            if *is_empty {
                // Return points that don't have values
                let iter = (0..self.total_point_count as u32)
                    .filter(|&id| !self.has_values_bitmap.contains(id));

                Some(Box::new(iter))
            } else {
                // Return points that have values
                let iter = self.has_values_bitmap.iter();
                Some(Box::new(iter))
            }
        } else if let Some(is_null) = is_null {
            if *is_null {
                // Return points that have null values
                let iter = self.is_null_bitmap.iter();
                Some(Box::new(iter))
            } else {
                // Return points that don't have null values
                let iter = (0..self.total_point_count as u32)
                    .filter(|&id| !self.is_null_bitmap.contains(id));
                Some(Box::new(iter))
            }
        } else {
            None
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        _hw_counter: &HardwareCounterCell,
    ) -> Option<CardinalityEstimation> {
        let FieldCondition {
            key,
            r#match: _,
            range: _,
            geo_bounding_box: _,
            geo_radius: _,
            geo_polygon: _,
            values_count: _,
            is_empty,
            is_null,
        } = condition;

        if let Some(is_empty) = is_empty {
            if *is_empty {
                let has_values_count = self.has_values_bitmap.len() as usize;
                let estimated = self.total_point_count.saturating_sub(has_values_count);

                Some(CardinalityEstimation {
                    min: 0,
                    exp: 2 * estimated / 3, // assuming 1/3 of the points are deleted
                    max: estimated,
                    primary_clauses: vec![PrimaryCondition::from(FieldCondition::new_is_empty(
                        key.clone(),
                        true,
                    ))],
                })
            } else {
                let count = self.has_values_bitmap.len() as usize;
                Some(CardinalityEstimation::exact(count).with_primary_clause(
                    PrimaryCondition::from(FieldCondition::new_is_empty(key.clone(), false)),
                ))
            }
        } else if let Some(is_null) = is_null {
            if *is_null {
                let count = self.is_null_bitmap.len() as usize;
                Some(CardinalityEstimation::exact(count).with_primary_clause(
                    PrimaryCondition::from(FieldCondition::new_is_null(key.clone(), true)),
                ))
            } else {
                let is_null_count = self.is_null_bitmap.len() as usize;
                let estimated = self.total_point_count.saturating_sub(is_null_count);

                Some(CardinalityEstimation {
                    min: 0,
                    exp: 2 * estimated / 3, // assuming 1/3 of the points are deleted
                    max: estimated,
                    primary_clauses: vec![PrimaryCondition::from(FieldCondition::new_is_null(
                        key.clone(),
                        false,
                    ))],
                })
            }
        } else {
            None
        }
    }

    fn payload_blocks(
        &self,
        _threshold: usize,
        _key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        // No payload blocks
        Box::new(std::iter::empty())
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

        let mut builder = MutableNullIndex::builder(dir.path()).unwrap();

        let n = 100;

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
            .collect();
        let not_empty_values: Vec<_> = null_index
            .filter(&filter_is_not_empty, &hw_counter)
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
            .unwrap();
        let non_empty_cardinality = null_index
            .estimate_cardinality(&filter_is_not_empty, &hw_cell)
            .unwrap();

        assert_eq!(is_null_cardinality.exp, 50);
        assert_eq!(non_empty_cardinality.exp, 50);
    }

    #[test]
    fn test_manual_buffer_flushing() {
        let dir = TempDir::with_prefix("test_manual_buffer_flushing").unwrap();
        let mut index = MutableNullIndex::builder(dir.path()).unwrap().0;

        let hw_counter = HardwareCounterCell::new();

        // Add points without automatic flushing
        for i in 0..10 {
            index
                .add_point(i as PointOffsetType, &[&Value::Bool(true)], &hw_counter)
                .unwrap();
        }

        // Buffer should still contain all updates
        assert_eq!(index.update_buffer.read().unwrap().len(), 10);

        // Manually flush via flusher
        index.flusher()().unwrap();

        // Buffer should be empty now
        assert_eq!(index.update_buffer.read().unwrap().len(), 0);

        // Verify data is in bitmaps
        for i in 0..10 {
            assert!(!index.values_is_empty(i as PointOffsetType));
            assert!(!index.values_is_null(i as PointOffsetType));
        }
    }
}
