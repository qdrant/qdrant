use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs};
use fs_err as fs;

use super::read_ops::{self, BoolIndexRead};
use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;
use crate::common::flags::roaring_flags::{RoaringFlags, RoaringFlagsRead};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexRead, ValueIndexer,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, PayloadKeyType};

pub(super) const TRUES_DIRNAME: &str = "trues";
pub(super) const FALSES_DIRNAME: &str = "falses";

/// Payload index for boolean values, in-memory via roaring bitmaps, stored in memory-mapped bitslices.
pub struct MutableBoolIndex {
    base_dir: PathBuf,
    indexed_count: usize,
    trues_count: usize,
    falses_count: usize,
    storage: Storage<MmapFile, MmapFs>,
}

struct Storage<S, Fs> {
    trues_flags: RoaringFlags<S, Fs>,
    falses_flags: RoaringFlags<S, Fs>,
}

impl MutableBoolIndex {
    pub fn builder(path: &Path) -> OperationResult<MutableBoolIndexBuilder> {
        Ok(MutableBoolIndexBuilder(
            Self::open(path, true)?.ok_or_else(|| {
                OperationError::service_error("Failed to create and open MutableBoolIndex")
            })?,
        ))
    }

    /// Open and load or create a boolean index at the given path.
    ///
    /// # Arguments
    /// - `path` - The directory where the index files should live, must be exclusive to this index.
    /// - `is_on_disk` - If the index should be kept on disk. Memory will be populated if false.
    /// - `create_if_missing` - If true, creates the index if it doesn't exist.
    pub fn open(path: &Path, create_if_missing: bool) -> OperationResult<Option<Self>> {
        let falses_dir = path.join(FALSES_DIRNAME);

        // If falses directory doesn't exist, assume the index doesn't exist on disk
        if !falses_dir.is_dir() && !create_if_missing {
            return Ok(None);
        }

        Ok(Some(Self::open_or_create(path)?))
    }

    fn open_or_create(path: &Path) -> OperationResult<Self> {
        fs::create_dir_all(path).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to create mmap bool index directory: {err}"
            ))
        })?;

        // Trues bitslice
        let trues_path = path.join(TRUES_DIRNAME);
        let trues_slice = DynamicStoredFlags::open(&MmapFs, &trues_path, false)?;
        let trues_flags = RoaringFlags::new(MmapFs, trues_slice)?;

        // Falses bitslice
        let falses_path = path.join(FALSES_DIRNAME);
        let falses_slice = DynamicStoredFlags::open(&MmapFs, &falses_path, false)?;
        let falses_flags = RoaringFlags::new(MmapFs, falses_slice)?;

        let trues_count = trues_flags.count_trues();
        let falses_count = falses_flags.count_trues();
        let indexed_count = {
            let trues = trues_flags.get_bitmap();
            let falses = falses_flags.get_bitmap();
            trues.union_len(falses) as usize
        };

        Ok(Self {
            base_dir: path.to_path_buf(),
            storage: Storage {
                trues_flags,
                falses_flags,
            },
            trues_count,
            falses_count,
            indexed_count,
        })
    }

    /// Open for an immutable index.
    pub(crate) fn open_immutable(path: &Path, deleted: &BitSlice) -> OperationResult<Option<Self>> {
        let index = Self::open(path, false)?.map(|mut idx| {
            // Mark deleted points as not indexed
            for id in deleted.iter_ones() {
                idx.set_or_insert_immutable(id as u32, false, false);
            }
            idx
        });

        Ok(index)
    }

    fn set_or_insert(&mut self, id: u32, has_true: bool, has_false: bool) {
        // Set or insert the flags
        let prev_true = self.storage.trues_flags.set(id, has_true);
        let prev_false = self.storage.falses_flags.set(id, has_false);

        let was_indexed = prev_true || prev_false;
        let is_indexed = has_true || has_false;

        // update indexed_count
        match (was_indexed, is_indexed) {
            (false, true) => {
                self.indexed_count += 1;
            }
            (true, false) => {
                self.indexed_count = self.indexed_count.saturating_sub(1);
            }
            _ => {}
        }

        // update trues_count
        match (prev_true, has_true) {
            (false, true) => {
                self.trues_count += 1;
            }
            (true, false) => {
                self.trues_count = self.trues_count.saturating_sub(1);
            }
            _ => {}
        }

        // update falses_count
        match (prev_false, has_false) {
            (false, true) => {
                self.falses_count += 1;
            }
            (true, false) => {
                self.falses_count = self.falses_count.saturating_sub(1);
            }
            _ => {}
        }
    }

    /// Set or insert for an immutable index, without modifying the underlying storage.
    pub(crate) fn set_or_insert_immutable(&mut self, id: u32, has_true: bool, has_false: bool) {
        // Set or insert the flags
        let prev_true = self.storage.trues_flags.set_immutable(id, has_true);
        let prev_false = self.storage.falses_flags.set_immutable(id, has_false);

        let was_indexed = prev_true || prev_false;
        let is_indexed = has_true || has_false;

        // update indexed_count
        match (was_indexed, is_indexed) {
            (false, true) => {
                self.indexed_count += 1;
            }
            (true, false) => {
                self.indexed_count = self.indexed_count.saturating_sub(1);
            }
            _ => {}
        }

        // update trues_count
        match (prev_true, has_true) {
            (false, true) => {
                self.trues_count += 1;
            }
            (true, false) => {
                self.trues_count = self.trues_count.saturating_sub(1);
            }
            _ => {}
        }

        // update falses_count
        match (prev_false, has_false) {
            (false, true) => {
                self.falses_count += 1;
            }
            (true, false) => {
                self.falses_count = self.falses_count.saturating_sub(1);
            }
            _ => {}
        }
    }
}

impl BoolIndexRead for MutableBoolIndex {
    type Flags = RoaringFlags<MmapFile, MmapFs>;

    fn trues_flags(&self) -> &Self::Flags {
        &self.storage.trues_flags
    }

    fn falses_flags(&self) -> &Self::Flags {
        &self.storage.falses_flags
    }

    fn indexed_count(&self) -> usize {
        self.indexed_count
    }

    fn telemetry_index_type(&self) -> &'static str {
        "mmap_bool"
    }

    // Override the default impls to use precomputed counts maintained by the
    // write path, avoiding a bitmap scan on every read.
    fn trues_count(&self) -> usize {
        self.trues_count
    }

    fn falses_count(&self) -> usize {
        self.falses_count
    }
}

pub struct MutableBoolIndexBuilder(MutableBoolIndex);

impl FieldIndexBuilderTrait for MutableBoolIndexBuilder {
    type FieldIndexType = MutableBoolIndex;

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
        Ok(self.0)
    }
}

impl ValueIndexer for MutableBoolIndex {
    type ValueType = bool;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        let has_true = values.iter().any(|v| *v);
        let has_false = values.iter().any(|v| !*v);

        self.set_or_insert(id, has_true, has_false);

        Ok(())
    }

    fn get_value(value: &serde_json::Value) -> Option<Self::ValueType> {
        value.as_bool()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.set_or_insert(id, false, false);
        Ok(())
    }
}

impl PayloadFieldIndex for MutableBoolIndex {
    fn wipe(self) -> OperationResult<()> {
        let base_dir = self.base_dir.clone();
        // drop mmap handles before deleting files
        drop(self);
        if base_dir.is_dir() {
            fs::remove_dir_all(&base_dir)?;
        };

        Ok(())
    }

    fn flusher(&self) -> crate::common::Flusher {
        let Self {
            base_dir: _,
            indexed_count: _,
            trues_count: _,
            falses_count: _,
            storage,
        } = self;
        let Storage {
            trues_flags,
            falses_flags,
        } = storage;

        let trues_flusher = trues_flags.flusher();
        let falses_flusher = falses_flags.flusher();

        Box::new(move || {
            trues_flusher()?;
            falses_flusher()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        BoolIndexRead::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new() // everything is mutable
    }
}

impl PayloadFieldIndexRead for MutableBoolIndex {
    fn count_indexed_points(&self) -> usize {
        self.indexed_count()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        Ok(read_ops::filter(self, condition, hw_counter))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(read_ops::estimate_cardinality(self, condition, hw_counter))
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        read_ops::for_each_payload_block(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        read_ops::condition_checker(self, condition, hw_acc)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tempfile::TempDir;
    use walkdir::WalkDir;

    use super::MutableBoolIndex;
    use crate::index::field_index::PayloadFieldIndex;

    #[test]
    fn test_files() {
        let dir = TempDir::with_prefix("test_mmap_bool_index").unwrap();
        let index = MutableBoolIndex::open(dir.path(), true).unwrap().unwrap();

        let reported = index.files().into_iter().collect::<HashSet<_>>();

        let actual = WalkDir::new(dir.path())
            .into_iter()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                entry.path().is_file().then_some(entry.into_path())
            })
            .collect::<HashSet<_>>();

        assert_eq!(reported, actual);
    }
}
