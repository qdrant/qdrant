use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::types::PointOffsetType;
use fs_err as fs;
use roaring::RoaringBitmap;

use super::BoolIndex;
use crate::common::Flusher;
use crate::common::flags::dynamic_mmap_flags::DynamicMmapFlags;
use crate::common::flags::roaring_flags::RoaringFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::map_index::IdIter;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    PrimaryCondition, ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, MatchValue, PayloadKeyType, ValueVariants};

const TRUES_DIRNAME: &str = "trues";
const FALSES_DIRNAME: &str = "falses";

/// Payload index for boolean values, in-memory via roaring bitmaps, stored in memory-mapped bitslices.
pub struct MutableBoolIndex {
    base_dir: PathBuf,
    indexed_count: usize,
    trues_count: usize,
    falses_count: usize,
    storage: Storage,
}

struct Storage {
    trues_flags: RoaringFlags,
    falses_flags: RoaringFlags,
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
        let trues_slice = DynamicMmapFlags::open(&trues_path, false)?;
        let trues_flags = RoaringFlags::new(trues_slice);

        // Falses bitslice
        let falses_path = path.join(FALSES_DIRNAME);
        let falses_slice = DynamicMmapFlags::open(&falses_path, false)?;
        let falses_flags = RoaringFlags::new(falses_slice);

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

    fn get_bitmap_for(&self, value: bool) -> &RoaringBitmap {
        if value {
            self.storage.trues_flags.get_bitmap()
        } else {
            self.storage.falses_flags.get_bitmap()
        }
    }

    fn get_count_for(&self, value: bool) -> usize {
        if value {
            self.trues_count
        } else {
            self.falses_count
        }
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.indexed_count,
            points_values_count: (self.trues_count + self.falses_count),
            histogram_bucket_size: None,
            index_type: "mmap_bool",
        }
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        let has_true = self.storage.trues_flags.get(point_id);
        let has_false = self.storage.falses_flags.get(point_id);
        usize::from(has_true) + usize::from(has_false)
    }

    pub fn check_values_any(&self, point_id: PointOffsetType, is_true: bool) -> bool {
        if is_true {
            self.storage.trues_flags.get(point_id)
        } else {
            self.storage.falses_flags.get(point_id)
        }
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        !self.storage.trues_flags.get(point_id) && !self.storage.falses_flags.get(point_id)
    }

    pub fn iter_values_map<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = (bool, IdIter<'a>)> + 'a {
        [
            (
                false,
                Box::new(self.storage.falses_flags.iter_trues()) as IdIter,
            ),
            (
                true,
                Box::new(self.storage.trues_flags.iter_trues()) as IdIter,
            ),
        ]
        .into_iter()
        .measure_hw_with_acc(hw_counter.new_accumulator(), u8::BITS as usize, |i| {
            i.payload_index_io_read_counter()
        })
    }

    pub fn iter_values(&self) -> impl Iterator<Item = bool> + '_ {
        [
            self.storage.falses_flags.iter_trues().next().map(|_| false),
            self.storage.trues_flags.iter_trues().next().map(|_| true),
        ]
        .into_iter()
        .flatten()
    }

    pub fn iter_counts_per_value(&self) -> impl Iterator<Item = (bool, usize)> + '_ {
        [
            (false, self.storage.falses_flags.count_trues()),
            (true, self.storage.trues_flags.count_trues()),
        ]
        .into_iter()
    }

    pub(crate) fn get_point_values(&self, point_id: u32) -> Vec<bool> {
        [
            self.storage.trues_flags.get(point_id).then_some(true),
            self.storage.falses_flags.get(point_id).then_some(false),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    pub fn is_on_disk(&self) -> bool {
        false
    }

    pub fn populate(&self) -> OperationResult<()> {
        // The true and false flags are always in memory
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.trues_flags.clear_cache()?;
        self.storage.falses_flags.clear_cache()
    }
}

pub struct MutableBoolIndexBuilder(MutableBoolIndex);

impl FieldIndexBuilderTrait for MutableBoolIndexBuilder {
    type FieldIndexType = BoolIndex;

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
        Ok(BoolIndex::Mmap(self.0))
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
    fn count_indexed_points(&self) -> usize {
        self.indexed_count
    }

    fn cleanup(self) -> OperationResult<()> {
        if self.base_dir.is_dir() {
            fs::remove_dir_all(self.base_dir)?;
        };

        Ok(())
    }

    fn flusher(&self) -> (Flusher, Flusher) {
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

        let stage_2_flusher = Box::new(move || {
            trues_flusher()?;
            falses_flusher()?;
            Ok(())
        });

        (Box::new(|| Ok(())), stage_2_flusher)
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        let mut files = self.storage.trues_flags.files();
        files.extend(self.storage.falses_flags.files());
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new() // everything is mutable
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Bool(value),
            })) => {
                let iter = self
                    .get_bitmap_for(*value)
                    .iter()
                    .map(|x| x as PointOffsetType)
                    .measure_hw_with_acc_and_fraction(
                        hw_counter.new_accumulator(),
                        u8::BITS as usize,
                        |i| i.payload_index_io_read_counter(),
                    );
                Some(Box::new(iter))
            }
            _ => None,
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Bool(value),
            })) => {
                let count = self.get_count_for(*value);

                hw_counter
                    .payload_index_io_read_counter()
                    .incr_delta(size_of::<usize>());

                let estimation = CardinalityEstimation::exact(count)
                    .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone())));

                Some(estimation)
            }
            _ => None,
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        let make_block = |count, value, key: PayloadKeyType| {
            if count > threshold {
                Some(PayloadBlockCondition {
                    condition: FieldCondition::new_match(
                        key,
                        Match::Value(MatchValue {
                            value: ValueVariants::Bool(value),
                        }),
                    ),
                    cardinality: count,
                })
            } else {
                None
            }
        };

        // just two possible blocks: true and false
        let iter = [
            make_block(self.trues_count, true, key.clone()),
            make_block(self.falses_count, false, key),
        ]
        .into_iter()
        .flatten();

        Box::new(iter)
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
