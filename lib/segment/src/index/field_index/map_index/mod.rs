use std::borrow::{Borrow, Cow};
use std::hash::{BuildHasher, Hash};
use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;
use indexmap::IndexSet;

pub use self::builders::{MapIndexBuilder, MapIndexGridstoreBuilder, MapIndexMmapBuilder};
use self::immutable_map_index::ImmutableMapIndex;
pub use self::key::MapIndexKey;
use self::mmap_map_index::MmapMapIndex;
use self::mutable_map_index::MutableMapIndex;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::CardinalityEstimation;
use crate::index::field_index::stat_tools::number_of_selected_points;
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::telemetry::PayloadIndexTelemetry;

mod builders;
mod facet_index_impl;
pub mod immutable_map_index;
pub mod key;
pub mod mmap_map_index;
pub mod mutable_map_index;
mod payload_index_impl_int;
mod payload_index_impl_str;
mod payload_index_impl_uuid;
mod value_indexer_impl;

/// Block size in Gridstore for keyword map index.
/// Keyword(s) are stored as cbor vector.
/// - "text" - 6 bytes
/// - "some", "text", "here" - 16 bytes
pub(super) const BLOCK_SIZE_KEYWORD: usize = 16;

pub type IdRefIter<'a> = Box<dyn Iterator<Item = &'a PointOffsetType> + 'a>;
pub type IdIter<'a> = Box<dyn Iterator<Item = PointOffsetType> + 'a>;

pub enum MapIndex<N: MapIndexKey + ?Sized>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    Mutable(MutableMapIndex<N>),
    Immutable(ImmutableMapIndex<N>),
    Mmap(Box<MmapMapIndex<N>>),
}

impl<N: MapIndexKey + ?Sized> MapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    /// Load immutable mmap based index, either in RAM or on disk
    pub fn new_mmap(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        // Low-memory mode downgrades the in-RAM `Immutable` wrapper to the
        // pure-mmap `Storage` variant at load time. Files are shared between
        // variants; the persisted `is_on_disk` flag in `mmap_index` is
        // untouched.
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let Some(mmap_index) = MmapMapIndex::open(path, effective_is_on_disk, deleted_points)?
        else {
            return Ok(None);
        };

        let index = if effective_is_on_disk {
            MapIndex::Mmap(Box::new(mmap_index))
        } else {
            // Load into RAM, use mmap as backing storage
            MapIndex::Immutable(ImmutableMapIndex::open_mmap(mmap_index)?)
        };
        Ok(Some(index))
    }

    pub fn new_gridstore(dir: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        let index = MutableMapIndex::open_gridstore(dir, create_if_missing)?;
        Ok(index.map(MapIndex::Mutable))
    }

    pub fn builder_mmap(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> MapIndexMmapBuilder<N> {
        MapIndexMmapBuilder {
            path: path.to_owned(),
            point_to_values: Default::default(),
            values_to_points: Default::default(),
            is_on_disk,
            deleted_points: deleted_points.to_owned(),
        }
    }

    pub fn builder_gridstore(dir: PathBuf) -> MapIndexGridstoreBuilder<N> {
        MapIndexGridstoreBuilder::new(dir)
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> bool {
        match self {
            MapIndex::Mutable(index) => index.check_values_any(idx, check_fn),
            MapIndex::Immutable(index) => index.check_values_any(idx, check_fn),
            // FIXME: don't silently ignore errors. Log error? Update ConditionCheckerFn?
            MapIndex::Mmap(index) => index
                .check_values_any(idx, hw_counter, check_fn)
                .unwrap_or(false),
        }
    }

    pub fn get_values(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = Cow<'_, N>> + '_>> {
        match self {
            MapIndex::Mutable(index) => Some(Box::new(index.get_values(idx)?)),
            MapIndex::Immutable(index) => Some(Box::new(index.get_values(idx)?)),
            MapIndex::Mmap(index) => Some(Box::new(index.get_values(idx, hw_counter)?)),
        }
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            MapIndex::Mutable(index) => index.values_count(idx).unwrap_or_default(),
            MapIndex::Immutable(index) => index.values_count(idx).unwrap_or_default(),
            MapIndex::Mmap(index) => index.values_count(idx).unwrap_or_default(),
        }
    }

    pub(crate) fn get_indexed_points(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_indexed_points(),
            MapIndex::Immutable(index) => index.get_indexed_points(),
            MapIndex::Mmap(index) => index.get_indexed_points(),
        }
    }

    fn get_values_count(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_values_count(),
            MapIndex::Immutable(index) => index.get_values_count(),
            MapIndex::Mmap(index) => index.get_values_count(),
        }
    }

    pub fn get_unique_values_count(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_unique_values_count(),
            MapIndex::Immutable(index) => index.get_unique_values_count(),
            MapIndex::Mmap(index) => index.get_unique_values_count(),
        }
    }

    pub(crate) fn get_count_for_value(
        &self,
        value: &N,
        hw_counter: &HardwareCounterCell,
    ) -> Option<usize> {
        match self {
            MapIndex::Mutable(index) => index.get_count_for_value(value),
            MapIndex::Immutable(index) => index.get_count_for_value(value),
            MapIndex::Mmap(index) => index.get_count_for_value(value, hw_counter),
        }
    }

    pub(crate) fn get_iterator(&self, value: &N, hw_counter: &HardwareCounterCell) -> IdIter<'_> {
        match self {
            MapIndex::Mutable(index) => index.get_iterator(value),
            MapIndex::Immutable(index) => index.get_iterator(value),
            MapIndex::Mmap(index) => index.get_iterator(value, hw_counter),
        }
    }

    pub fn for_each_value(&self, f: impl FnMut(&N) -> OperationResult<()>) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.for_each_value(f),
            MapIndex::Immutable(index) => index.for_each_value(f),
            MapIndex::Mmap(index) => index.for_each_value(f),
        }
    }

    pub fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(&N, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.for_each_count_per_value(deferred_internal_id, f),

            // Two reasons we don't implement deferred filtering here:
            //  - We don't have both deferred points and an immutable index.
            //  - It is not trivial (nor performant) to implement correct filtering for this index variant as
            //    it doesn't work well in combination with the way it handles deletions.
            MapIndex::Immutable(index) => {
                debug_assert!(deferred_internal_id.is_none());
                index.for_each_count_per_value(f)
            }

            MapIndex::Mmap(index) => index.for_each_count_per_value(deferred_internal_id, f),
        }
    }

    pub fn for_each_value_map(
        &self,
        hw_cell: &HardwareCounterCell,
        f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.for_each_value_map(f),
            MapIndex::Immutable(index) => index.for_each_value_map(f),
            MapIndex::Mmap(index) => index.for_each_value_map(hw_cell, f),
        }
    }

    pub(crate) fn flusher(&self) -> Flusher {
        match self {
            MapIndex::Mutable(index) => index.flusher(),
            MapIndex::Immutable(index) => index.flusher(),
            MapIndex::Mmap(index) => index.flusher(),
        }
    }

    pub(crate) fn match_cardinality(
        &self,
        value: &N,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let values_count = self.get_count_for_value(value, hw_counter).unwrap_or(0);

        CardinalityEstimation::exact(values_count)
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.get_indexed_points(),
            points_values_count: self.get_values_count(),
            histogram_bucket_size: None,
            index_type: match self {
                MapIndex::Mutable(_) => "mutable_map",
                MapIndex::Immutable(_) => "immutable_map",
                MapIndex::Mmap(_) => "mmap_map",
            },
        }
    }

    pub fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx) == 0
    }

    pub(crate) fn wipe(self) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.wipe(),
            MapIndex::Immutable(index) => index.wipe(),
            MapIndex::Mmap(index) => index.wipe(),
        }
    }

    pub(crate) fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.remove_point(id),
            MapIndex::Immutable(index) => index.remove_point(id),
            MapIndex::Mmap(index) => {
                index.remove_point(id);
                Ok(())
            }
        }
    }

    pub(crate) fn files(&self) -> Vec<PathBuf> {
        match self {
            MapIndex::Mutable(index) => index.files(),
            MapIndex::Immutable(index) => index.files(),
            MapIndex::Mmap(index) => index.files(),
        }
    }

    pub(crate) fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            MapIndex::Mutable(_) => vec![],
            MapIndex::Immutable(index) => index.immutable_files(),
            MapIndex::Mmap(index) => index.immutable_files(),
        }
    }

    /// Estimates cardinality for `except` clause
    ///
    /// # Arguments
    ///
    /// * 'excluded' - values, which are not considered as matching
    ///
    /// # Returns
    ///
    /// * `CardinalityEstimation` - estimation of cardinality
    pub(crate) fn except_cardinality<'a>(
        &'a self,
        excluded: impl Iterator<Item = &'a N>,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        // Minimal case: we exclude as many points as possible.
        // In this case, excluded points do not have any other values except excluded ones.
        // So the first step - we estimate how many other points is needed to fit unused values.

        // Example:
        // Values: 20, 20
        // Unique values: 5
        // Total points: 100
        // Total values: 110
        // total_excluded_value_count = 40
        // non_excluded_values_count = 110 - 40 = 70
        // max_values_per_point = 5 - 2 = 3
        // min_not_excluded_by_values = 70 / 3 = 24
        // min = max(24, 100 - 40) = 60
        // exp = ...
        // max = min(20, 70) = 20

        // Values: 60, 60
        // Unique values: 5
        // Total points: 100
        // Total values: 200
        // total_excluded_value_count = 120
        // non_excluded_values_count = 200 - 120 = 80
        // max_values_per_point = 5 - 2 = 3
        // min_not_excluded_by_values = 80 / 3 = 27
        // min = max(27, 100 - 120) = 27
        // exp = ...
        // max = min(60, 80) = 60

        // Values: 60, 60, 60
        // Unique values: 5
        // Total points: 100
        // Total values: 200
        // total_excluded_value_count = 180
        // non_excluded_values_count = 200 - 180 = 20
        // max_values_per_point = 5 - 3 = 2
        // min_not_excluded_by_values = 20 / 2 = 10
        // min = max(10, 100 - 180) = 10
        // exp = ...
        // max = min(60, 20) = 20

        let excluded_value_counts: Vec<_> = excluded
            .map(|val| {
                self.get_count_for_value(val.borrow(), hw_counter)
                    .unwrap_or(0)
            })
            .collect();
        let total_excluded_value_count: usize = excluded_value_counts.iter().sum();

        debug_assert!(total_excluded_value_count <= self.get_values_count());

        let non_excluded_values_count = self
            .get_values_count()
            .saturating_sub(total_excluded_value_count);
        let max_values_per_point = self
            .get_unique_values_count()
            .saturating_sub(excluded_value_counts.len());

        if max_values_per_point == 0 {
            debug_assert_eq!(non_excluded_values_count, 0);
            return CardinalityEstimation::exact(0);
        }

        let min_not_excluded_by_values = non_excluded_values_count.div_ceil(max_values_per_point);

        let min = min_not_excluded_by_values.max(
            self.get_indexed_points()
                .saturating_sub(total_excluded_value_count),
        );

        let max_excluded_value_count = excluded_value_counts.iter().max().copied().unwrap_or(0);

        let max = self
            .get_indexed_points()
            .saturating_sub(max_excluded_value_count)
            .min(non_excluded_values_count);

        let exp = number_of_selected_points(self.get_indexed_points(), non_excluded_values_count)
            .max(min)
            .min(max);

        CardinalityEstimation {
            primary_clauses: vec![],
            min,
            exp,
            max,
        }
    }

    pub(crate) fn except_set<'a, K, A>(
        &'a self,
        excluded: &'a IndexSet<K, A>,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>>
    where
        A: BuildHasher,
        K: Borrow<N> + Hash + Eq,
    {
        let mut points = IndexSet::new();
        self.for_each_value(|key| {
            if !excluded.contains(key.borrow()) {
                self.get_iterator(key.borrow(), hw_counter).for_each(|p| {
                    points.insert(p);
                });
            }
            Ok(())
        })?;
        Ok(Box::new(points.into_iter()))
    }

    /// Approximate RAM usage in bytes for in-memory structures.
    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.ram_usage_bytes(),
            MapIndex::Immutable(index) => index.ram_usage_bytes(),
            MapIndex::Mmap(index) => index.ram_usage_bytes(),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            MapIndex::Mutable(_) => false,
            MapIndex::Immutable(_) => false,
            MapIndex::Mmap(index) => index.is_on_disk(),
        }
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(_) => {}
            MapIndex::Immutable(_) => {}
            MapIndex::Mmap(index) => index.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.clear_cache()?,
            MapIndex::Immutable(index) => index.clear_cache()?,
            MapIndex::Mmap(index) => index.clear_cache()?,
        }
        Ok(())
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            Self::Mutable(_) => IndexMutability::Mutable,
            Self::Immutable(_) => IndexMutability::Immutable,
            Self::Mmap(_) => IndexMutability::Immutable,
        }
    }

    pub fn get_storage_type(&self) -> StorageType {
        match self {
            Self::Mutable(index) => index.storage_type(),
            Self::Immutable(index) => index.storage_type(),
            Self::Mmap(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::hint::black_box;
    use std::path::Path;

    use common::bitvec::BitVec;
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::types::PointOffsetType;
    use ecow::EcoString;
    use gridstore::Blob;
    use rstest::rstest;
    use serde_json::Value;
    use tempfile::Builder;

    use super::*;
    use crate::index::field_index::{
        CardinalityEstimation, FieldIndexBuilderTrait, PayloadFieldIndex, PayloadFieldIndexRead,
        ValueIndexer,
    };
    use crate::types::{IntPayloadType, PayloadKeyType, UuidIntType};

    /// Generous default size for the deleted-points bitslice used in tests.
    ///
    /// Must be larger than the stored mmap deletion bitslice for any test in
    /// this file (which is sized to the highest point id, rounded up to a
    /// `usize` boundary). 4096 bits comfortably covers all current tests.
    const TEST_DELETED_BITS: usize = 4096;

    /// All-zero deletion bitslice for tests that don't care about deletions.
    fn empty_deleted() -> BitVec {
        BitVec::repeat(false, TEST_DELETED_BITS)
    }

    /// Deletion bitslice with specific points marked as deleted.
    fn deleted_with(points: &[PointOffsetType]) -> BitVec {
        let mut v = empty_deleted();
        for &p in points {
            v.set(p as usize, true);
        }
        v
    }

    #[derive(Clone, Copy, PartialEq, Debug)]
    enum IndexType {
        MutableGridstore,
        Mmap,
        RamMmap,
    }

    fn save_map_index<N>(
        data: &[Vec<<N as MapIndexKey>::Owned>],
        path: &Path,
        index_type: IndexType,
        into_value: impl Fn(&<N as MapIndexKey>::Owned) -> Value,
    ) where
        N: MapIndexKey + ?Sized,
        Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
        MapIndex<N>: PayloadFieldIndex + ValueIndexer,
        <MapIndex<N> as ValueIndexer>::ValueType: Into<<N as MapIndexKey>::Owned>,
    {
        let hw_counter = HardwareCounterCell::new();

        match index_type {
            IndexType::MutableGridstore => {
                let mut builder = MapIndex::<N>::builder_gridstore(path.to_path_buf());
                builder.init().unwrap();
                for (idx, values) in data.iter().enumerate() {
                    let values: Vec<Value> = values.iter().map(&into_value).collect();
                    let values: Vec<_> = values.iter().collect();
                    builder
                        .add_point(idx as PointOffsetType, &values, &hw_counter)
                        .unwrap();
                }
                builder.finalize().unwrap();
            }
            IndexType::Mmap | IndexType::RamMmap => {
                let mut builder = MapIndex::<N>::builder_mmap(path, false, &empty_deleted());
                builder.init().unwrap();
                for (idx, values) in data.iter().enumerate() {
                    let values: Vec<Value> = values.iter().map(&into_value).collect();
                    let values: Vec<_> = values.iter().collect();
                    builder
                        .add_point(idx as PointOffsetType, &values, &hw_counter)
                        .unwrap();
                }
                builder.finalize().unwrap();
            }
        }
    }

    fn load_map_index<N: MapIndexKey + ?Sized>(
        data: &[Vec<<N as MapIndexKey>::Owned>],
        path: &Path,
        index_type: IndexType,
    ) -> MapIndex<N>
    where
        Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
    {
        let index = match index_type {
            IndexType::MutableGridstore => MapIndex::<N>::new_gridstore(path.to_path_buf(), true)
                .unwrap()
                .unwrap(),
            IndexType::Mmap => MapIndex::<N>::new_mmap(path, true, &empty_deleted())
                .unwrap()
                .unwrap(),
            IndexType::RamMmap => MapIndex::<N>::new_mmap(path, false, &empty_deleted())
                .unwrap()
                .unwrap(),
        };
        let hw_counter = HardwareCounterCell::new();
        for (idx, values) in data.iter().enumerate() {
            let index_values: HashSet<<N as MapIndexKey>::Owned> = index
                .get_values(idx as PointOffsetType, &hw_counter)
                .unwrap()
                .map(|v| MapIndexKey::to_owned(v.as_ref()))
                .collect();
            let index_values: HashSet<&N> = index_values.iter().map(|v| v.borrow()).collect();
            let check_values: HashSet<&N> = values.iter().map(|v| v.borrow()).collect();
            assert_eq!(index_values, check_values);
        }

        index
    }

    #[test]
    fn test_uuid_payload_index() {
        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        let mut builder =
            MapIndex::<UuidIntType>::builder_mmap(temp_dir.path(), false, &empty_deleted());

        builder.init().unwrap();

        let hw_counter = HardwareCounterCell::new();

        let uuid: Value = Value::String("baa56dfc-e746-4ec1-bf50-94822535a46c".to_string());

        for idx in 0..100 {
            builder
                .add_point(idx as PointOffsetType, &[&uuid], &hw_counter)
                .unwrap();
        }

        let index = builder.finalize().unwrap();

        index
            .for_each_payload_block(50, PayloadKeyType::new("test_uuid"), &mut |block| {
                black_box(block);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_index_non_ascending_insertion() {
        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        let mut builder =
            MapIndex::<IntPayloadType>::builder_mmap(temp_dir.path(), false, &empty_deleted());
        builder.init().unwrap();

        let data = [vec![1, 2, 3, 4, 5, 6], vec![25], vec![10, 11]];

        let hw_counter = HardwareCounterCell::new();

        for (idx, values) in data.iter().enumerate().rev() {
            let values: Vec<Value> = values.iter().map(|i| (*i).into()).collect();
            let values: Vec<_> = values.iter().collect();
            builder
                .add_point(idx as PointOffsetType, &values, &hw_counter)
                .unwrap();
        }

        let index = builder.finalize().unwrap();
        let hw_counter = HardwareCounterCell::new();
        for (idx, values) in data.iter().enumerate().rev() {
            let res: Vec<_> = index
                .get_values(idx as u32, &hw_counter)
                .unwrap()
                .map(|i| *i as i32)
                .collect();
            assert_eq!(res, *values);
        }
    }

    #[rstest]
    #[case(IndexType::MutableGridstore)]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn test_int_disk_map_index(#[case] index_type: IndexType) {
        let data = vec![
            vec![1, 2, 3, 4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![13, 14, 15, 16, 17, 18],
            vec![19, 20, 21, 22, 23, 24],
            vec![25],
        ];

        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type, |v| (*v).into());
        let index = load_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type);

        let hw_counter = HardwareCounterCell::new();

        assert!(
            !index
                .except_cardinality(std::iter::empty(), &hw_counter)
                .equals_min_exp_max(&CardinalityEstimation::exact(0))
        );
    }

    #[rstest]
    #[case(IndexType::MutableGridstore)]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn test_string_disk_map_index(#[case] index_type: IndexType) {
        let data = vec![
            vec![
                EcoString::from("AABB"),
                EcoString::from("UUFF"),
                EcoString::from("IIBB"),
            ],
            vec![
                EcoString::from("PPMM"),
                EcoString::from("QQXX"),
                EcoString::from("YYBB"),
            ],
            vec![
                EcoString::from("FFMM"),
                EcoString::from("IICC"),
                EcoString::from("IIBB"),
            ],
            vec![
                EcoString::from("AABB"),
                EcoString::from("UUFF"),
                EcoString::from("IIBB"),
            ],
            vec![EcoString::from("PPGG")],
        ];

        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index::<str>(&data, temp_dir.path(), index_type, |v| v.to_string().into());
        let index = load_map_index::<str>(&data, temp_dir.path(), index_type);

        let hw_counter = HardwareCounterCell::new();

        assert!(
            !index
                .except_cardinality(vec![].into_iter(), &hw_counter)
                .equals_min_exp_max(&CardinalityEstimation::exact(0))
        );
    }

    #[rstest]
    #[case(IndexType::MutableGridstore)]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn test_empty_index(#[case] index_type: IndexType) {
        let data: Vec<Vec<EcoString>> = vec![];

        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index::<str>(&data, temp_dir.path(), index_type, |v| v.to_string().into());
        let index = load_map_index::<str>(&data, temp_dir.path(), index_type);

        let hw_counter = HardwareCounterCell::new();

        assert!(
            index
                .except_cardinality(std::iter::empty(), &hw_counter)
                .equals_min_exp_max(&CardinalityEstimation::exact(0))
        );
    }

    /// Test that `get_values` on an on-disk mmap index actually increments the hardware counter.
    #[test]
    fn test_mmap_get_values_hw_counter() {
        let data = vec![vec![1i64, 2, 3], vec![4, 5], vec![6]];

        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index::<IntPayloadType>(&data, temp_dir.path(), IndexType::Mmap, |v| (*v).into());
        let index = load_map_index::<IntPayloadType>(&data, temp_dir.path(), IndexType::Mmap);

        let hw_counter = HardwareCounterCell::new();
        for idx in 0..data.len() {
            let _values: Vec<_> = index
                .get_values(idx as PointOffsetType, &hw_counter)
                .unwrap()
                .collect();
        }

        assert!(
            hw_counter.payload_index_io_read_counter().get() > 0,
            "Expected on-disk mmap get_values to track payload index IO reads, but counter was 0"
        );

        let temp_dir2 = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index::<IntPayloadType>(&data, temp_dir2.path(), IndexType::RamMmap, |v| {
            (*v).into()
        });
        let index2 = load_map_index::<IntPayloadType>(&data, temp_dir2.path(), IndexType::RamMmap);

        let hw_counter2 = HardwareCounterCell::new();
        for idx in 0..data.len() {
            let _values: Vec<_> = index2
                .get_values(idx as PointOffsetType, &hw_counter2)
                .unwrap()
                .collect();
        }

        assert_eq!(
            hw_counter2.payload_index_io_read_counter().get(),
            0,
            "Expected RAM mmap get_values NOT to track IO reads, but counter was non-zero"
        );
    }

    /// Reload contract: runtime deletions are not persisted by the mmap map
    /// index. Callers must re-supply the deletion bitslice on reload.
    ///
    /// Test data is chosen so that every value retains at least one live
    /// point after deletions — otherwise `ImmutableMapIndex::open_mmap` hits a
    /// pre-existing debug-only assertion when a value's slice becomes empty.
    #[rstest]
    #[case(IndexType::MutableGridstore)]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn test_map_index_reload(#[case] index_type: IndexType) {
        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        let data: Vec<Vec<IntPayloadType>> = vec![
            vec![1, 2], // id 0
            vec![1],    // id 1
            vec![2],    // id 2
            vec![1, 3], // id 3
            vec![2, 3], // id 4
            vec![3],    // id 5
        ];

        {
            save_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type, |v| (*v).into());
            let mut index = load_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type);
            index.remove_point(1).unwrap();
            index.remove_point(2).unwrap();
            index.remove_point(5).unwrap();
            index.flusher()().unwrap();
            assert_eq!(index.get_indexed_points(), 3);
            drop(index);
        }

        let deleted = deleted_with(&[1, 2, 5]);
        let new_index = match index_type {
            IndexType::MutableGridstore => {
                MapIndex::<IntPayloadType>::new_gridstore(temp_dir.path().to_path_buf(), true)
                    .unwrap()
                    .unwrap()
            }
            IndexType::Mmap => {
                MapIndex::<IntPayloadType>::new_mmap(temp_dir.path(), true, &deleted)
                    .unwrap()
                    .unwrap()
            }
            IndexType::RamMmap => {
                MapIndex::<IntPayloadType>::new_mmap(temp_dir.path(), false, &deleted)
                    .unwrap()
                    .unwrap()
            }
        };

        assert_eq!(new_index.get_indexed_points(), 3);

        let hw_counter = HardwareCounterCell::new();
        for id in [1u32, 2, 5] {
            assert_eq!(
                new_index.values_count(id),
                0,
                "deleted point {id} should have no values after reload",
            );
        }
        for id in [0u32, 3, 4] {
            assert!(
                new_index.values_count(id) > 0,
                "live point {id} should have values after reload",
            );
        }

        let mut hits: Vec<PointOffsetType> = new_index.get_iterator(&1, &hw_counter).collect();
        hits.sort();
        assert_eq!(hits, vec![0, 3]);

        let mut hits: Vec<PointOffsetType> = new_index.get_iterator(&2, &hw_counter).collect();
        hits.sort();
        assert_eq!(hits, vec![0, 4]);

        let mut hits: Vec<PointOffsetType> = new_index.get_iterator(&3, &hw_counter).collect();
        hits.sort();
        assert_eq!(hits, vec![3, 4]);
    }

    /// Regression test: when reloading an mmap map index with a `deleted_points`
    /// bitslice shorter than `point_to_values.len()`, missing entries must
    /// default to live, not deleted. Empty-payload bits from the on-disk
    /// `deleted.bin` and any deletions encoded inside the short bitslice must
    /// still be honored.
    #[rstest]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn test_map_index_reload_short_deleted_bitslice(#[case] index_type: IndexType) {
        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();

        let data: Vec<Vec<IntPayloadType>> = vec![
            vec![1],    // id 0
            vec![1, 2], // id 1
            vec![],     // id 2 — empty payload
            vec![2, 3], // id 3
            vec![3],    // id 4
        ];

        save_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type, |v| (*v).into());

        let mut short_deleted = BitVec::repeat(false, 2);
        short_deleted.set(1, true);

        let new_index = match index_type {
            IndexType::Mmap => {
                MapIndex::<IntPayloadType>::new_mmap(temp_dir.path(), true, &short_deleted)
                    .unwrap()
                    .unwrap()
            }
            IndexType::RamMmap => {
                MapIndex::<IntPayloadType>::new_mmap(temp_dir.path(), false, &short_deleted)
                    .unwrap()
                    .unwrap()
            }
            IndexType::MutableGridstore => unreachable!(),
        };

        let hw_counter = HardwareCounterCell::new();

        assert!(new_index.values_count(0) > 0, "id 0 should be live");
        assert_eq!(new_index.values_count(1), 0, "id 1 deleted via bitslice");
        assert_eq!(
            new_index.values_count(2),
            0,
            "id 2 deleted via build-time empty"
        );
        assert!(
            new_index.values_count(3) > 0,
            "id 3 should be live (beyond bitslice)"
        );
        assert!(
            new_index.values_count(4) > 0,
            "id 4 should be live (beyond bitslice)"
        );

        let mut hits: Vec<PointOffsetType> = new_index.get_iterator(&2, &hw_counter).collect();
        hits.sort();
        assert_eq!(hits, vec![3]);
    }
}
