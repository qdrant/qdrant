use std::path::{Path, PathBuf};

use bitvec::slice::BitSlice;
use common::counter::conditioned_counter::ConditionedCounter;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::types::PointOffsetType;
use itertools::Itertools;

use super::BoolIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::map_index::IdIter;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    PrimaryCondition, ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, MatchValue, PayloadKeyType, ValueVariants};
use crate::vector_storage::common::PAGE_SIZE_BYTES;
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;

const TRUES_DIRNAME: &str = "trues";
const FALSES_DIRNAME: &str = "falses";

/// Payload index for boolean values, stored in memory-mapped files.
pub struct MmapBoolIndex {
    base_dir: PathBuf,
    indexed_count: usize,
    trues_count: usize,
    falses_count: usize,
    trues_slice: DynamicMmapFlags,
    falses_slice: DynamicMmapFlags,
    populated: bool,
}

impl MmapBoolIndex {
    pub fn builder(path: &Path, is_on_disk: bool) -> OperationResult<MmapBoolIndexBuilder> {
        Ok(MmapBoolIndexBuilder(Self::open_or_create(
            path, is_on_disk,
        )?))
    }

    /// Creates a new boolean index at the given path. If it already exists, loads the index.
    ///
    /// # Arguments
    /// - `path` - The directory where the index files should live, must be exclusive to this index.
    /// - `is_on_disk` - If the index should be kept on disk. Memory will be populated if false.
    pub fn open_or_create(path: &Path, is_on_disk: bool) -> OperationResult<Self> {
        let falses_dir = path.join(FALSES_DIRNAME);
        if falses_dir.is_dir() {
            Self::open(path, is_on_disk)
        } else {
            std::fs::create_dir_all(path).map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to create mmap bool index directory: {err}"
                ))
            })?;
            Self::open(path, is_on_disk)
        }
    }

    fn open(path: &Path, is_on_disk: bool) -> OperationResult<Self> {
        if !path.is_dir() {
            return Err(OperationError::service_error("Path is not a directory"));
        }

        let populate = !is_on_disk;
        // Trues bitslice
        let trues_path = path.join(TRUES_DIRNAME);
        let trues_slice = DynamicMmapFlags::open(&trues_path, populate)?;

        // Falses bitslice
        let falses_path = path.join(FALSES_DIRNAME);
        let falses_slice = DynamicMmapFlags::open(&falses_path, populate)?;

        Ok(Self {
            base_dir: path.to_path_buf(),
            trues_slice,
            falses_slice,
            // loading is done after opening during `PayloadFieldIndex::load()`
            indexed_count: 0,
            trues_count: 0,
            falses_count: 0,
            populated: populate,
        })
    }

    fn make_conditioned_counter<'a>(
        &self,
        hw_counter: &'a HardwareCounterCell,
    ) -> ConditionedCounter<'a> {
        let on_disk = !self.populated; // Measure if on disk.
        ConditionedCounter::new(on_disk, hw_counter)
    }

    fn set_or_insert(
        &mut self,
        id: u32,
        has_true: bool,
        has_false: bool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let hw_counter = self.make_conditioned_counter(hw_counter);

        // Set or insert the flags
        let prev_true =
            set_or_insert_flag(&mut self.trues_slice, id as usize, has_true, &hw_counter)?;
        let prev_false =
            set_or_insert_flag(&mut self.falses_slice, id as usize, has_false, &hw_counter)?;

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

        Ok(())
    }

    fn get_slice_for(&self, value: bool) -> &BitSlice {
        if value {
            self.trues_slice.get_bitslice()
        } else {
            self.falses_slice.get_bitslice()
        }
    }

    fn get_count_for(&self, value: bool) -> usize {
        if value {
            self.trues_count
        } else {
            self.falses_count
        }
    }

    fn bitslice_usize_view(bitslice: &BitSlice) -> &[usize] {
        let (head, body, tail) = bitslice
            .domain()
            .region()
            .expect("BitSlice should be bigger than one usize");

        debug_assert!(head.is_none(), "BitSlice should be aligned to usize");
        debug_assert!(tail.is_none(), "BitSlice should be aligned to usize");

        body
    }

    /// Calculates the count of true values of the union of both slices.
    fn calculate_indexed_count(&self) -> u32 {
        // indexed_count would be trues_bitslice.union_count(falses_bitslice), but there is no such operation yet.
        // So we do it manually.
        let trues = Self::bitslice_usize_view(self.get_slice_for(true));
        let falses = Self::bitslice_usize_view(self.get_slice_for(false));

        let mut indexed_count = 0;
        for elem in trues.iter().zip_longest(falses) {
            match elem {
                itertools::EitherOrBoth::Both(trues_elem, falses_elem) => {
                    let union = trues_elem | falses_elem;
                    indexed_count += union.count_ones();
                }
                itertools::EitherOrBoth::Left(trues_elem) => {
                    indexed_count += trues_elem.count_ones();
                }
                itertools::EitherOrBoth::Right(falses_elem) => {
                    indexed_count += falses_elem.count_ones();
                }
            }
        }

        indexed_count
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
        let has_true = self.trues_slice.get(point_id as usize);
        let has_false = self.falses_slice.get(point_id as usize);
        usize::from(has_true) + usize::from(has_false)
    }

    pub fn check_values_any(
        &self,
        point_id: PointOffsetType,
        is_true: bool,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        let hw_counter = self.make_conditioned_counter(hw_counter);

        hw_counter
            .payload_index_io_read_counter()
            .incr_delta(size_of::<bool>());
        if is_true {
            self.trues_slice.get(point_id as usize)
        } else {
            self.falses_slice.get(point_id as usize)
        }
    }

    pub fn values_is_empty(&self, point_id: u32) -> bool {
        !self.trues_slice.get(point_id as usize) && !self.falses_slice.get(point_id as usize)
    }

    pub fn iter_values_map<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = (bool, IdIter<'a>)> + 'a {
        let hw_counter = self.make_conditioned_counter(hw_counter);

        [
            (false, Box::new(self.falses_slice.iter_trues()) as IdIter),
            (true, Box::new(self.trues_slice.iter_trues()) as IdIter),
        ]
        .into_iter()
        .measure_hw_with_acc(hw_counter.new_accumulator(), u8::BITS as usize, |i| {
            i.payload_index_io_read_counter()
        })
    }

    pub fn iter_values(&self) -> impl Iterator<Item = bool> + '_ {
        [
            self.falses_slice.iter_trues().next().map(|_| false),
            self.trues_slice.iter_trues().next().map(|_| true),
        ]
        .into_iter()
        .flatten()
    }

    pub fn iter_counts_per_value(&self) -> impl Iterator<Item = (bool, usize)> + '_ {
        [
            (false, self.falses_slice.count_flags()),
            (true, self.trues_slice.count_flags()),
        ]
        .into_iter()
    }

    pub(crate) fn get_point_values(&self, point_id: u32) -> Vec<bool> {
        [
            self.trues_slice.get(point_id as usize).then_some(true),
            self.falses_slice.get(point_id as usize).then_some(false),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    pub fn is_on_disk(&self) -> bool {
        !self.populated
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.trues_slice.populate()?;
        self.falses_slice.populate()?;

        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.trues_slice.clear_cache()?;
        self.falses_slice.clear_cache()?;

        Ok(())
    }
}

/// Set or insert a flag in the given flags. Returns previous value.
///
/// Resizes if needed to set to true.
fn set_or_insert_flag(
    flags: &mut DynamicMmapFlags,
    key: usize,
    value: bool,
    hw_counter: &ConditionedCounter,
) -> OperationResult<bool> {
    let counter = hw_counter.payload_index_io_write_counter();

    // Measure writing bool.
    counter.incr_delta(size_of::<bool>());

    // Set to true
    if value {
        // Make sure the key fits
        if key >= flags.len() {
            let new_len = (key + 1).next_multiple_of(PAGE_SIZE_BYTES);
            counter.incr_delta(new_len - flags.len());
            flags.set_len(new_len)?;
        }
        return Ok(flags.set(key, value));
    }

    // Set to false
    if key < flags.len() {
        return Ok(flags.set(key, value));
    }

    Ok(false)
}

pub struct MmapBoolIndexBuilder(MmapBoolIndex);

impl FieldIndexBuilderTrait for MmapBoolIndexBuilder {
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

impl ValueIndexer for MmapBoolIndex {
    type ValueType = bool;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        let has_true = values.iter().any(|v| *v);
        let has_false = values.iter().any(|v| !*v);

        self.set_or_insert(id, has_true, has_false, hw_counter)?;

        Ok(())
    }

    fn get_value(value: &serde_json::Value) -> Option<Self::ValueType> {
        value.as_bool()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        let disposable_hw = HardwareCounterCell::disposable();
        self.set_or_insert(id, false, false, &disposable_hw)?;
        Ok(())
    }
}
impl PayloadFieldIndex for MmapBoolIndex {
    fn count_indexed_points(&self) -> usize {
        self.indexed_count
    }

    fn load(&mut self) -> OperationResult<bool> {
        let calculated_indexed_count = self.calculate_indexed_count();

        // Destructure to not forget any fields
        let Self {
            base_dir: _,
            indexed_count,
            trues_count,
            falses_count,
            trues_slice,
            falses_slice,
            populated: _,
        } = self;

        *indexed_count = calculated_indexed_count as usize;
        *trues_count = trues_slice.count_flags();
        *falses_count = falses_slice.count_flags();

        Ok(true)
    }

    fn cleanup(self) -> OperationResult<()> {
        std::fs::remove_dir_all(self.base_dir)?;
        Ok(())
    }

    fn flusher(&self) -> crate::common::Flusher {
        let Self {
            base_dir: _,
            indexed_count: _,
            trues_count: _,
            falses_count: _,
            trues_slice,
            falses_slice,
            populated: _,
        } = self;

        let trues_flusher = trues_slice.flusher();
        let falses_flusher = falses_slice.flusher();

        Box::new(move || {
            trues_flusher()?;
            falses_flusher()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        let mut files = self.trues_slice.files();
        files.extend(self.falses_slice.files());

        files
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let hw_counter = self.make_conditioned_counter(hw_counter);

        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Bool(value),
            })) => {
                let iter = self
                    .get_slice_for(*value)
                    .iter_ones()
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
        let hw_counter = self.make_conditioned_counter(hw_counter);

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

    use super::MmapBoolIndex;
    use crate::index::field_index::PayloadFieldIndex;

    #[test]
    fn test_files() {
        let dir = TempDir::with_prefix("test_mmap_bool_index").unwrap();
        let index = MmapBoolIndex::open_or_create(dir.path(), false).unwrap();

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
