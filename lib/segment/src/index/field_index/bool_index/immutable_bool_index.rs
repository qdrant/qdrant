use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs};

use super::mutable_bool_index::MutableBoolIndex;
use super::read_ops::{self, BoolIndexRead};
use crate::common::flags::roaring_flags::RoaringFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexRead, ValueIndexer,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, PayloadKeyType};

pub struct ImmutableBoolIndex(MutableBoolIndex);

impl ImmutableBoolIndex {
    pub fn builder(path: &Path) -> OperationResult<ImmutableBoolIndexBuilder> {
        Ok(ImmutableBoolIndexBuilder(
            MutableBoolIndex::open(path, true)?.ok_or_else(|| {
                OperationError::service_error("Failed to create and open MutableBoolIndex")
            })?,
        ))
    }

    pub fn from_mutable(mutable_index: MutableBoolIndex) -> OperationResult<Self> {
        mutable_index.flusher()()?;
        Ok(Self(mutable_index))
    }

    pub fn open(path: &Path, deleted: &BitSlice) -> OperationResult<Option<Self>> {
        Ok(MutableBoolIndex::open_immutable(path, deleted)?.map(Self))
    }

    #[inline]
    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.0.set_or_insert_immutable(id, false, false);
        Ok(())
    }
}

impl BoolIndexRead for ImmutableBoolIndex {
    type Flags = RoaringFlags<MmapFile, MmapFs>;

    fn trues_flags(&self) -> &Self::Flags {
        self.0.trues_flags()
    }

    fn falses_flags(&self) -> &Self::Flags {
        self.0.falses_flags()
    }

    fn indexed_count(&self) -> usize {
        self.0.indexed_count()
    }

    fn telemetry_index_type(&self) -> &'static str {
        self.0.telemetry_index_type()
    }

    fn trues_count(&self) -> usize {
        self.0.trues_count()
    }

    fn falses_count(&self) -> usize {
        self.0.falses_count()
    }
}

impl PayloadFieldIndexRead for ImmutableBoolIndex {
    #[inline]
    fn count_indexed_points(&self) -> usize {
        self.indexed_count()
    }

    #[inline]
    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        Ok(read_ops::filter(self, condition, hw_counter))
    }

    #[inline]
    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(read_ops::estimate_cardinality(self, condition, hw_counter))
    }

    #[inline]
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

impl PayloadFieldIndex for ImmutableBoolIndex {
    #[inline]
    fn wipe(self) -> OperationResult<()> {
        self.0.wipe()
    }

    #[inline]
    fn files(&self) -> Vec<PathBuf> {
        BoolIndexRead::files(self)
    }

    #[inline]
    fn immutable_files(&self) -> Vec<PathBuf> {
        BoolIndexRead::files(self) // All the files are immutable in this index.
    }

    #[inline]
    fn flusher(&self) -> crate::common::Flusher {
        Box::new(|| Ok(())) // No op for an immutable index.
    }
}

pub struct ImmutableBoolIndexBuilder(MutableBoolIndex);

impl FieldIndexBuilderTrait for ImmutableBoolIndexBuilder {
    type FieldIndexType = ImmutableBoolIndex;

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
        self.0.flusher()()?; // Immutable index has noop flusher, so we have to ensure the data is flushed now.
        Ok(ImmutableBoolIndex(self.0))
    }
}

#[cfg(test)]
mod tests {
    use common::bitvec::BitVec;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_remove_idempotent() {
        let dir = TempDir::with_prefix("test_immutable_bool_index").unwrap();
        let mut builder = ImmutableBoolIndex::builder(dir.path()).unwrap();
        let hw_counter = HardwareCounterCell::new();
        builder.add_point(0, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(1, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(2, &[&json!(false)], &hw_counter).unwrap();

        let mut index = builder.finalize().unwrap();
        assert_eq!(index.get_point_values(1), vec![true; 1]);
        assert_eq!(index.count_indexed_points(), 3);

        index.remove_point(1).unwrap();
        assert_eq!(index.get_point_values(1), vec![true; 0]);
        assert_eq!(index.count_indexed_points(), 2);

        index.remove_point(1).unwrap();
        assert_eq!(index.get_point_values(1), vec![true; 0]);
        assert_eq!(index.count_indexed_points(), 2);
    }

    #[test]
    fn test_remove_reopen() {
        let dir = TempDir::with_prefix("test_immutable_bool_index").unwrap();
        let mut builder = ImmutableBoolIndex::builder(dir.path()).unwrap();
        let hw_counter = HardwareCounterCell::new();
        builder.add_point(0, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(1, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(2, &[&json!(false)], &hw_counter).unwrap();

        let mut index = builder.finalize().unwrap();

        let mut deleted = BitVec::repeat(false, 3);
        deleted.set(1, true);
        index.remove_point(1).unwrap();
        drop(index);

        let opened_index = ImmutableBoolIndex::open(dir.path(), &deleted)
            .unwrap()
            .unwrap();
        assert_eq!(opened_index.get_point_values(1), vec![true; 0]);
        assert_eq!(opened_index.count_indexed_points(), 2);
    }
}
