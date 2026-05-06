use std::borrow::{Borrow, Cow};
use std::collections::hash_map::Entry;
use std::fmt::{Debug, Display};
use std::hash::{BuildHasher, Hash};
use std::iter;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use ahash::HashMap;
use common::bitvec::{BitSlice, BitVec};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::persisted_hashmap::Key;
use common::types::PointOffsetType;
use ecow::EcoString;
use gridstore::Blob;
use indexmap::IndexSet;
use itertools::Itertools;
use mmap_map_index::MmapMapIndex;
use serde_json::{Number, Value};
use uuid::Uuid;

use self::immutable_map_index::ImmutableMapIndex;
use self::mutable_map_index::MutableMapIndex;
use super::FieldIndexBuilderTrait;
use super::facet_index::FacetIndex;
use super::stored_point_to_values::StoredValue;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::MultiValue;
use crate::data_types::facets::{FacetHit, FacetValueRef};
use crate::index::field_index::stat_tools::number_of_selected_points;
use crate::index::field_index::utils::value_to_integer;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PayloadFieldIndexRead,
    PrimaryCondition, ValueIndexer,
};
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::index::query_estimator::combine_should_estimations;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::payload_storage::condition_checker::INDEXSET_ITER_THRESHOLD;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    AnyVariants, FieldCondition, IntPayloadType, Match, MatchAny, MatchExcept, MatchValue,
    PayloadKeyType, UuidIntType, UuidPayloadType, ValueVariants,
};

pub mod immutable_map_index;
pub mod mmap_map_index;
pub mod mutable_map_index;

/// Block size in Gridstore for keyword map index.
/// Keyword(s) are stored as cbor vector.
/// - "text" - 6 bytes
/// - "some", "text", "here" - 16 bytes
pub(super) const BLOCK_SIZE_KEYWORD: usize = 16;

pub type IdRefIter<'a> = Box<dyn Iterator<Item = &'a PointOffsetType> + 'a>;
pub type IdIter<'a> = Box<dyn Iterator<Item = PointOffsetType> + 'a>;

pub trait MapIndexKey: Key + StoredValue + Eq + Display + Debug {
    type Owned: Borrow<Self> + Hash + Eq + Clone + FromStr + Default + 'static;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned;

    fn gridstore_block_size() -> usize {
        size_of::<<Self as MapIndexKey>::Owned>()
    }

    /// Extra heap bytes for an owned value beyond `size_of::<Owned>()`.
    /// Override for types with heap allocations (e.g., strings).
    fn owned_heap_bytes(_value: &<Self as MapIndexKey>::Owned) -> usize {
        0
    }
}

impl MapIndexKey for str {
    type Owned = EcoString;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned {
        EcoString::from(self)
    }

    fn gridstore_block_size() -> usize {
        BLOCK_SIZE_KEYWORD
    }

    fn owned_heap_bytes(value: &<Self as MapIndexKey>::Owned) -> usize {
        // EcoString inlines strings up to INLINE_LIMIT bytes.
        // Longer strings are heap-allocated.
        if value.len() > EcoString::INLINE_LIMIT {
            value.len()
        } else {
            0
        }
    }
}

impl MapIndexKey for IntPayloadType {
    type Owned = IntPayloadType;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned {
        *self
    }
}

impl MapIndexKey for UuidIntType {
    type Owned = UuidIntType;

    fn to_owned(&self) -> <Self as MapIndexKey>::Owned {
        *self
    }
}

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
            // Files don't exist, cannot load
            return Ok(None);
        };

        let index = if effective_is_on_disk {
            // Use on mmap directly
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

    fn get_indexed_points(&self) -> usize {
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

    fn get_count_for_value(&self, value: &N, hw_counter: &HardwareCounterCell) -> Option<usize> {
        match self {
            MapIndex::Mutable(index) => index.get_count_for_value(value),
            MapIndex::Immutable(index) => index.get_count_for_value(value),
            MapIndex::Mmap(index) => index.get_count_for_value(value, hw_counter),
        }
    }

    fn get_iterator(&self, value: &N, hw_counter: &HardwareCounterCell) -> IdIter<'_> {
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

    fn flusher(&self) -> Flusher {
        match self {
            MapIndex::Mutable(index) => index.flusher(),
            MapIndex::Immutable(index) => index.flusher(),
            MapIndex::Mmap(index) => index.flusher(),
        }
    }

    fn match_cardinality(
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

    fn wipe(self) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.wipe(),
            MapIndex::Immutable(index) => index.wipe(),
            MapIndex::Mmap(index) => index.wipe(),
        }
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.remove_point(id),
            MapIndex::Immutable(index) => index.remove_point(id),
            MapIndex::Mmap(index) => {
                index.remove_point(id);
                Ok(())
            }
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            MapIndex::Mutable(index) => index.files(),
            MapIndex::Immutable(index) => index.files(),
            MapIndex::Mmap(index) => index.files(),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
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
    fn except_cardinality<'a>(
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
            // All points are excluded, so we can't select any point
            debug_assert_eq!(non_excluded_values_count, 0);
            return CardinalityEstimation::exact(0);
        }

        // Minimal amount of points, required to fit all unused values.
        // Cardinality can't be less than this value.
        let min_not_excluded_by_values = non_excluded_values_count.div_ceil(max_values_per_point);

        let min = min_not_excluded_by_values.max(
            self.get_indexed_points()
                .saturating_sub(total_excluded_value_count),
        );

        // Maximum scenario: selected points overlap as much as possible.
        // From one side, all excluded values should be assigned to the same point
        // => we can take the value with the maximum amount of points.
        // From another side, all other values should be enough to fill all other points.

        let max_excluded_value_count = excluded_value_counts.iter().max().copied().unwrap_or(0);

        let max = self
            .get_indexed_points()
            .saturating_sub(max_excluded_value_count)
            .min(non_excluded_values_count);

        // Expected case: we assume that all points are filled equally.
        // So we can estimate the probability of the point to have non-excluded value.
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

    fn except_set<'a, K, A>(
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
            MapIndex::Mutable(_) => {}   // Not a mmap
            MapIndex::Immutable(_) => {} // Not a mmap
            MapIndex::Mmap(index) => index.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            // Only clears backing mmap storage if used, not in-memory representation
            MapIndex::Mutable(index) => index.clear_cache()?,
            // Only clears backing mmap storage if used, not in-memory representation
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

pub struct MapIndexBuilder<N: MapIndexKey + ?Sized>(MapIndex<N>)
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync;

impl<N: MapIndexKey + ?Sized> FieldIndexBuilderTrait for MapIndexBuilder<N>
where
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    type FieldIndexType = MapIndex<N>;

    fn init(&mut self) -> OperationResult<()> {
        match &mut self.0 {
            MapIndex::Mutable(index) => index.clear(),
            MapIndex::Immutable(_) => unreachable!(),
            MapIndex::Mmap(_) => unreachable!(),
        }
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        values: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.0.add_point(id, values, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(self.0)
    }
}

pub struct MapIndexMmapBuilder<N: MapIndexKey + ?Sized> {
    path: PathBuf,
    point_to_values: Vec<Vec<<N as MapIndexKey>::Owned>>,
    values_to_points: HashMap<<N as MapIndexKey>::Owned, Vec<PointOffsetType>>,
    is_on_disk: bool,
    deleted_points: BitVec,
}

impl<N: MapIndexKey + ?Sized> FieldIndexBuilderTrait for MapIndexMmapBuilder<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
    <MapIndex<N> as ValueIndexer>::ValueType: Into<<N as MapIndexKey>::Owned>,
{
    type FieldIndexType = MapIndex<N>;

    fn init(&mut self) -> OperationResult<()> {
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut flatten_values: Vec<_> = vec![];
        for value in payload {
            let payload_values = <MapIndex<N> as ValueIndexer>::get_values(value);
            flatten_values.extend(payload_values);
        }
        let flatten_values: Vec<<N as MapIndexKey>::Owned> =
            flatten_values.into_iter().map(Into::into).collect();

        if self.point_to_values.len() <= id as usize {
            self.point_to_values.resize_with(id as usize + 1, Vec::new);
        }

        self.point_to_values[id as usize].extend(flatten_values.clone());

        let mut hw_cell_wb = hw_counter
            .payload_index_io_write_counter()
            .write_back_counter();

        for value in flatten_values {
            let entry = self.values_to_points.entry(value);

            if let Entry::Vacant(e) = &entry {
                let size = N::stored_size(e.key().borrow());
                hw_cell_wb.incr_delta(size);
            }

            hw_cell_wb.incr_delta(size_of_val(&id));
            entry.or_default().push(id);
        }

        Ok(())
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(MapIndex::Mmap(Box::new(MmapMapIndex::build(
            &self.path,
            self.point_to_values,
            self.values_to_points,
            self.is_on_disk,
            &self.deleted_points,
        )?)))
    }
}

pub struct MapIndexGridstoreBuilder<N: MapIndexKey + ?Sized>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    dir: PathBuf,
    index: Option<MapIndex<N>>,
}

impl<N: MapIndexKey + ?Sized> MapIndexGridstoreBuilder<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    fn new(dir: PathBuf) -> Self {
        Self { dir, index: None }
    }
}

impl<N: MapIndexKey + ?Sized> FieldIndexBuilderTrait for MapIndexGridstoreBuilder<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
    <MapIndex<N> as ValueIndexer>::ValueType: Into<<N as MapIndexKey>::Owned>,
{
    type FieldIndexType = MapIndex<N>;

    fn init(&mut self) -> OperationResult<()> {
        assert!(
            self.index.is_none(),
            "index must be initialized exactly once",
        );
        self.index.replace(
            MapIndex::new_gridstore(self.dir.clone(), true)?.ok_or_else(|| {
                OperationError::service_error("Failed to create mutable map index")
            })?,
        );
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Some(index) = &mut self.index else {
            return Err(OperationError::service_error(
                "MapIndexGridstoreBuilder: index must be initialized before adding points",
            ));
        };
        index.add_point(id, payload, hw_counter)
    }

    fn finalize(mut self) -> OperationResult<Self::FieldIndexType> {
        let Some(index) = self.index.take() else {
            return Err(OperationError::service_error(
                "MapIndexGridstoreBuilder: index must be initialized to finalize",
            ));
        };
        index.flusher()()?;
        Ok(index)
    }
}

impl PayloadFieldIndex for MapIndex<str> {
    fn wipe(self) -> OperationResult<()> {
        self.wipe()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.immutable_files()
    }
}

impl PayloadFieldIndexRead for MapIndex<str> {
    fn count_indexed_points(&self) -> usize {
        self.get_indexed_points()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        let result: Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> = match &condition
            .r#match
        {
            Some(Match::Value(MatchValue { value })) => match value {
                ValueVariants::String(keyword) => {
                    Some(Box::new(self.get_iterator(keyword.as_str(), hw_counter)))
                }
                ValueVariants::Integer(_) => None,
                ValueVariants::Bool(_) => None,
            },
            Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                AnyVariants::Strings(keywords) => Some(Box::new(
                    keywords
                        .iter()
                        .flat_map(move |keyword| self.get_iterator(keyword.as_str(), hw_counter))
                        .unique(),
                )),
                AnyVariants::Integers(integers) => {
                    if integers.is_empty() {
                        Some(Box::new(iter::empty()))
                    } else {
                        None
                    }
                }
            },
            Some(Match::Except(MatchExcept { except })) => match except {
                AnyVariants::Strings(keywords) => Some(self.except_set(keywords, hw_counter)?),
                AnyVariants::Integers(other) => {
                    if other.is_empty() {
                        Some(Box::new(iter::empty()))
                    } else {
                        None
                    }
                }
            },
            _ => None,
        };

        Ok(result)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(match &condition.r#match {
            Some(Match::Value(MatchValue { value })) => match value {
                ValueVariants::String(keyword) => {
                    let mut estimation = self.match_cardinality(keyword.as_str(), hw_counter);
                    estimation
                        .primary_clauses
                        .push(PrimaryCondition::Condition(Box::new(condition.clone())));
                    Some(estimation)
                }
                ValueVariants::Integer(_) => None,
                ValueVariants::Bool(_) => None,
            },
            Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                AnyVariants::Strings(keywords) => {
                    let estimations = keywords
                        .iter()
                        .map(|keyword| self.match_cardinality(keyword.as_str(), hw_counter))
                        .collect::<Vec<_>>();
                    let estimation = if estimations.is_empty() {
                        CardinalityEstimation::exact(0)
                    } else {
                        combine_should_estimations(&estimations, self.get_indexed_points())
                    };
                    Some(
                        estimation.with_primary_clause(PrimaryCondition::Condition(Box::new(
                            condition.clone(),
                        ))),
                    )
                }
                AnyVariants::Integers(integers) => {
                    if integers.is_empty() {
                        Some(CardinalityEstimation::exact(0).with_primary_clause(
                            PrimaryCondition::Condition(Box::new(condition.clone())),
                        ))
                    } else {
                        None
                    }
                }
            },
            Some(Match::Except(MatchExcept { except })) => match except {
                AnyVariants::Strings(keywords) => {
                    Some(self.except_cardinality(keywords.iter().map(|k| k.as_str()), hw_counter))
                }
                AnyVariants::Integers(others) => {
                    if others.is_empty() {
                        Some(CardinalityEstimation::exact(0).with_primary_clause(
                            PrimaryCondition::Condition(Box::new(condition.clone())),
                        ))
                    } else {
                        None
                    }
                }
            },
            _ => None,
        })
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_value(|value| {
            let count = self
                .get_count_for_value(value, &HardwareCounterCell::disposable()) // Payload_blocks only used in HNSW building, which is unmeasured.
                .unwrap_or(0);
            if count > threshold {
                f(PayloadBlockCondition {
                    condition: FieldCondition::new_match(key.clone(), value.to_string().into()),
                    cardinality: count,
                })?;
            }
            Ok(())
        })
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        // Destructure explicitly (no `..`) so a new field added to
        // `FieldCondition` forces this method to be revisited.
        let FieldCondition {
            key: _,
            r#match,
            range: _,
            geo_radius: _,
            geo_bounding_box: _,
            geo_polygon: _,
            values_count: _,
            is_empty: _,
            is_null: _,
        } = condition;

        let cond_match = r#match.as_ref()?;
        let hw_counter = hw_acc.get_counter_cell();
        match cond_match {
            Match::Value(MatchValue {
                value: ValueVariants::String(keyword),
            }) => {
                let keyword = keyword.clone();
                Some(Box::new(move |point_id: PointOffsetType| {
                    self.check_values_any(point_id, &hw_counter, |value| value == keyword.as_str())
                }))
            }
            Match::Any(MatchAny {
                any: AnyVariants::Strings(list),
            }) => {
                let list = list.clone();
                if list.len() < INDEXSET_ITER_THRESHOLD {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| {
                            list.iter().any(|s| s.as_str() == value)
                        })
                    }))
                } else {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| list.contains(value))
                    }))
                }
            }
            Match::Except(MatchExcept {
                except: AnyVariants::Strings(list),
            }) => {
                let list = list.clone();
                if list.len() < INDEXSET_ITER_THRESHOLD {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| {
                            !list.iter().any(|s| s.as_str() == value)
                        })
                    }))
                } else {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| !list.contains(value))
                    }))
                }
            }
            // Conditions this index can't serve: Match::Text/TextAny/Phrase
            // (handled by FullTextIndex) and value-type mismatches (e.g.
            // Match::Value(Integer) against a string-keyed map).
            Match::Value(MatchValue {
                value: ValueVariants::Integer(_) | ValueVariants::Bool(_),
            })
            | Match::Any(MatchAny {
                any: AnyVariants::Integers(_),
            })
            | Match::Except(MatchExcept {
                except: AnyVariants::Integers(_),
            })
            | Match::Text(_)
            | Match::TextAny(_)
            | Match::Phrase(_) => None,
        }
    }
}

impl PayloadFieldIndex for MapIndex<UuidIntType> {
    fn wipe(self) -> OperationResult<()> {
        self.wipe()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.immutable_files()
    }
}

impl PayloadFieldIndexRead for MapIndex<UuidIntType> {
    fn count_indexed_points(&self) -> usize {
        self.get_indexed_points()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        let result: Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> =
            match &condition.r#match {
                Some(Match::Value(MatchValue { value })) => match value {
                    ValueVariants::String(uuid_string) => {
                        let Ok(uuid) = Uuid::from_str(uuid_string) else {
                            return Ok(None);
                        };
                        Some(Box::new(self.get_iterator(&uuid.as_u128(), hw_counter)))
                    }
                    ValueVariants::Integer(_) => None,
                    ValueVariants::Bool(_) => None,
                },
                Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                    AnyVariants::Strings(uuids_string) => {
                        let Ok(uuids) = uuids_string
                            .iter()
                            .map(|uuid_string| Uuid::from_str(uuid_string).map(|x| x.as_u128()))
                            .collect::<Result<IndexSet<u128>, _>>()
                        else {
                            return Ok(None);
                        };

                        Some(Box::new(
                            uuids
                                .into_iter()
                                .flat_map(move |uuid| self.get_iterator(&uuid, hw_counter))
                                .unique(),
                        ))
                    }
                    AnyVariants::Integers(integers) => {
                        if integers.is_empty() {
                            Some(Box::new(iter::empty()))
                        } else {
                            None
                        }
                    }
                },
                Some(Match::Except(MatchExcept { except })) => match except {
                    AnyVariants::Strings(uuids_string) => {
                        let Ok(excluded_uuids) = uuids_string
                            .iter()
                            .map(|uuid_string| Uuid::from_str(uuid_string).map(|x| x.as_u128()))
                            .collect::<Result<IndexSet<u128>, _>>()
                        else {
                            return Ok(None);
                        };
                        let mut points = IndexSet::new();
                        self.for_each_value(|key| {
                            if !excluded_uuids.contains(key) {
                                self.get_iterator(key, hw_counter).for_each(|p| {
                                    points.insert(p);
                                });
                            }
                            Ok(())
                        })?;
                        Some(Box::new(points.into_iter()))
                    }
                    AnyVariants::Integers(other) => {
                        if other.is_empty() {
                            Some(Box::new(iter::empty()))
                        } else {
                            None
                        }
                    }
                },
                _ => None,
            };

        Ok(result)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(match &condition.r#match {
            Some(Match::Value(MatchValue { value })) => match value {
                ValueVariants::String(uuid_string) => {
                    let Some(uuid) = Uuid::from_str(uuid_string).ok() else {
                        return Ok(None);
                    };
                    let mut estimation = self.match_cardinality(&uuid.as_u128(), hw_counter);
                    estimation
                        .primary_clauses
                        .push(PrimaryCondition::Condition(Box::new(condition.clone())));
                    Some(estimation)
                }
                ValueVariants::Integer(_) => None,
                ValueVariants::Bool(_) => None,
            },
            Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                AnyVariants::Strings(uuids_string) => {
                    let uuids: Result<IndexSet<u128>, _> = uuids_string
                        .iter()
                        .map(|uuid_string| Uuid::from_str(uuid_string).map(|x| x.as_u128()))
                        .collect();

                    let Some(uuids) = uuids.ok() else {
                        return Ok(None);
                    };

                    let estimations = uuids
                        .into_iter()
                        .map(|uuid| self.match_cardinality(&uuid, hw_counter))
                        .collect::<Vec<_>>();
                    let estimation = if estimations.is_empty() {
                        CardinalityEstimation::exact(0)
                    } else {
                        combine_should_estimations(&estimations, self.get_indexed_points())
                    };
                    Some(
                        estimation.with_primary_clause(PrimaryCondition::Condition(Box::new(
                            condition.clone(),
                        ))),
                    )
                }
                AnyVariants::Integers(integers) => {
                    if integers.is_empty() {
                        Some(CardinalityEstimation::exact(0).with_primary_clause(
                            PrimaryCondition::Condition(Box::new(condition.clone())),
                        ))
                    } else {
                        None
                    }
                }
            },
            Some(Match::Except(MatchExcept { except })) => match except {
                AnyVariants::Strings(uuids_string) => {
                    let uuids: Result<IndexSet<u128>, _> = uuids_string
                        .iter()
                        .map(|uuid_string| Uuid::from_str(uuid_string).map(|x| x.as_u128()))
                        .collect();

                    let Some(excluded_uuids) = uuids.ok() else {
                        return Ok(None);
                    };

                    Some(self.except_cardinality(excluded_uuids.iter(), hw_counter))
                }
                AnyVariants::Integers(other) => {
                    if other.is_empty() {
                        Some(CardinalityEstimation::exact(0).with_primary_clause(
                            PrimaryCondition::Condition(Box::new(condition.clone())),
                        ))
                    } else {
                        None
                    }
                }
            },
            _ => None,
        })
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_value(|value| {
            let count = self
                .get_count_for_value(value, &HardwareCounterCell::disposable()) // payload_blocks only used in HNSW building, which is unmeasured.
                .unwrap_or(0);
            if count >= threshold {
                f(PayloadBlockCondition {
                    condition: FieldCondition::new_match(
                        key.clone(),
                        Uuid::from_u128(*value).to_string().into(),
                    ),
                    cardinality: count,
                })?;
            }
            Ok(())
        })
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        // Destructure explicitly (no `..`) so a new field added to
        // `FieldCondition` forces this method to be revisited.
        let FieldCondition {
            key: _,
            r#match,
            range: _,
            geo_radius: _,
            geo_bounding_box: _,
            geo_polygon: _,
            values_count: _,
            is_empty: _,
            is_null: _,
        } = condition;

        let cond_match = r#match.as_ref()?;
        let hw_counter = hw_acc.get_counter_cell();
        match cond_match {
            Match::Value(MatchValue {
                value: ValueVariants::String(keyword),
            }) => {
                let uuid = Uuid::parse_str(keyword).map(|u| u.as_u128()).ok()?;
                Some(Box::new(move |point_id: PointOffsetType| {
                    self.check_values_any(point_id, &hw_counter, |value| value == &uuid)
                }))
            }
            Match::Any(MatchAny {
                any: AnyVariants::Strings(list),
            }) => {
                let list = list
                    .iter()
                    .map(|s| Uuid::parse_str(s).map(|u| u.as_u128()).ok())
                    .collect::<Option<IndexSet<_>>>()?;
                if list.len() < INDEXSET_ITER_THRESHOLD {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| {
                            list.iter().any(|i| i == value)
                        })
                    }))
                } else {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| list.contains(value))
                    }))
                }
            }
            Match::Except(MatchExcept {
                except: AnyVariants::Strings(list),
            }) => {
                let list = list
                    .iter()
                    .map(|s| Uuid::parse_str(s).map(|u| u.as_u128()).ok())
                    .collect::<Option<IndexSet<_>>>()?;
                if list.len() < INDEXSET_ITER_THRESHOLD {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| {
                            !list.iter().any(|i| i == value)
                        })
                    }))
                } else {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| !list.contains(value))
                    }))
                }
            }
            // Conditions this index can't serve.
            Match::Value(MatchValue {
                value: ValueVariants::Integer(_) | ValueVariants::Bool(_),
            })
            | Match::Any(MatchAny {
                any: AnyVariants::Integers(_),
            })
            | Match::Except(MatchExcept {
                except: AnyVariants::Integers(_),
            })
            | Match::Text(_)
            | Match::TextAny(_)
            | Match::Phrase(_) => None,
        }
    }
}

impl PayloadFieldIndex for MapIndex<IntPayloadType> {
    fn wipe(self) -> OperationResult<()> {
        self.wipe()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.immutable_files()
    }
}

impl PayloadFieldIndexRead for MapIndex<IntPayloadType> {
    fn count_indexed_points(&self) -> usize {
        self.get_indexed_points()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        let result: Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> =
            match &condition.r#match {
                Some(Match::Value(MatchValue { value })) => match value {
                    ValueVariants::String(_) => None,
                    ValueVariants::Integer(integer) => {
                        Some(Box::new(self.get_iterator(integer, hw_counter)))
                    }
                    ValueVariants::Bool(_) => None,
                },
                Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                    AnyVariants::Strings(keywords) => {
                        if keywords.is_empty() {
                            Some(Box::new(std::iter::empty()))
                        } else {
                            None
                        }
                    }
                    AnyVariants::Integers(integers) => Some(Box::new(
                        integers
                            .iter()
                            .flat_map(move |integer| self.get_iterator(integer, hw_counter))
                            .unique(),
                    )),
                },
                Some(Match::Except(MatchExcept { except })) => match except {
                    AnyVariants::Strings(other) => {
                        if other.is_empty() {
                            Some(Box::new(iter::empty()))
                        } else {
                            None
                        }
                    }
                    AnyVariants::Integers(integers) => Some(self.except_set(integers, hw_counter)?),
                },
                _ => None,
            };

        Ok(result)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(match &condition.r#match {
            Some(Match::Value(MatchValue { value })) => match value {
                ValueVariants::String(_) => None,
                ValueVariants::Integer(integer) => {
                    let mut estimation = self.match_cardinality(integer, hw_counter);
                    estimation
                        .primary_clauses
                        .push(PrimaryCondition::Condition(Box::new(condition.clone())));
                    Some(estimation)
                }
                ValueVariants::Bool(_) => None,
            },
            Some(Match::Any(MatchAny { any: any_variants })) => match any_variants {
                AnyVariants::Strings(keywords) => {
                    if keywords.is_empty() {
                        Some(CardinalityEstimation::exact(0).with_primary_clause(
                            PrimaryCondition::Condition(Box::new(condition.clone())),
                        ))
                    } else {
                        None
                    }
                }
                AnyVariants::Integers(integers) => {
                    let estimations = integers
                        .iter()
                        .map(|integer| self.match_cardinality(integer, hw_counter))
                        .collect::<Vec<_>>();
                    let estimation = if estimations.is_empty() {
                        CardinalityEstimation::exact(0)
                    } else {
                        combine_should_estimations(&estimations, self.get_indexed_points())
                    };
                    Some(
                        estimation.with_primary_clause(PrimaryCondition::Condition(Box::new(
                            condition.clone(),
                        ))),
                    )
                }
            },
            Some(Match::Except(MatchExcept { except })) => match except {
                AnyVariants::Strings(others) => {
                    if others.is_empty() {
                        Some(CardinalityEstimation::exact(0).with_primary_clause(
                            PrimaryCondition::Condition(Box::new(condition.clone())),
                        ))
                    } else {
                        None
                    }
                }
                AnyVariants::Integers(integers) => {
                    Some(self.except_cardinality(integers.iter(), hw_counter))
                }
            },
            _ => None,
        })
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_value(|value| {
            let count = self
                .get_count_for_value(value, &HardwareCounterCell::disposable()) // Only used in HNSW building so no measurement needed here.
                .unwrap_or(0);
            if count >= threshold {
                f(PayloadBlockCondition {
                    condition: FieldCondition::new_match(key.clone(), (*value).into()),
                    cardinality: count,
                })?;
            }
            Ok(())
        })
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        // Destructure explicitly (no `..`) so a new field added to
        // `FieldCondition` forces this method to be revisited.
        let FieldCondition {
            key: _,
            r#match,
            range: _,
            geo_radius: _,
            geo_bounding_box: _,
            geo_polygon: _,
            values_count: _,
            is_empty: _,
            is_null: _,
        } = condition;

        let cond_match = r#match.as_ref()?;
        let hw_counter = hw_acc.get_counter_cell();
        match cond_match {
            Match::Value(MatchValue {
                value: ValueVariants::Integer(value),
            }) => {
                let value = *value;
                Some(Box::new(move |point_id: PointOffsetType| {
                    self.check_values_any(point_id, &hw_counter, |i| *i == value)
                }))
            }
            Match::Any(MatchAny {
                any: AnyVariants::Integers(list),
            }) => {
                let list = list.clone();
                if list.len() < INDEXSET_ITER_THRESHOLD {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| {
                            list.iter().any(|i| i == value)
                        })
                    }))
                } else {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| list.contains(value))
                    }))
                }
            }
            Match::Except(MatchExcept {
                except: AnyVariants::Integers(list),
            }) => {
                let list = list.clone();
                if list.len() < INDEXSET_ITER_THRESHOLD {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| {
                            !list.iter().any(|i| i == value)
                        })
                    }))
                } else {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| !list.contains(value))
                    }))
                }
            }
            // Conditions this index can't serve.
            Match::Value(MatchValue {
                value: ValueVariants::String(_) | ValueVariants::Bool(_),
            })
            | Match::Any(MatchAny {
                any: AnyVariants::Strings(_),
            })
            | Match::Except(MatchExcept {
                except: AnyVariants::Strings(_),
            })
            | Match::Text(_)
            | Match::TextAny(_)
            | Match::Phrase(_) => None,
        }
    }
}

impl<N: MapIndexKey + ?Sized> FacetIndex for MapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
    for<'a> Cow<'a, N>: Into<FacetValueRef<'a>>,
    for<'a> &'a N: Into<FacetValueRef<'a>>,
{
    fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.for_points_values(points, |idx, slice| {
                f(idx, &mut slice.iter().map(|v| v.borrow().into()));
            }),
            MapIndex::Immutable(index) => index.for_points_values(points, |idx, slice| {
                f(idx, &mut slice.iter().map(|v| v.borrow().into()));
            }),
            MapIndex::Mmap(index) => index.for_points_values(points, hw_counter, |idx, vals| {
                f(idx, &mut vals.map(|v| v.into()));
            })?,
        }
        Ok(())
    }

    fn for_each_value(
        &self,
        mut f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_value(|v| f(v.into()))
    }

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(
            FacetValueRef<'_>,
            &mut dyn Iterator<Item = PointOffsetType>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_value_map(hw_counter, |value, iter| f(value.into(), iter))
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_count_per_value(deferred_internal_id, |value, count| {
            f(FacetHit {
                value: value.into(),
                count,
            })
        })
    }
}

impl ValueIndexer for MapIndex<str> {
    type ValueType = String;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<String>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.add_many_to_map(id, values, hw_counter),
            MapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable map index",
            )),
            MapIndex::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap map index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<String> {
        if let Value::String(keyword) = value {
            return Some(keyword.to_owned());
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}

impl ValueIndexer for MapIndex<IntPayloadType> {
    type ValueType = IntPayloadType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<IntPayloadType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.add_many_to_map(id, values, hw_counter),
            MapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable map index",
            )),
            MapIndex::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap map index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<IntPayloadType> {
        value_to_integer(value)
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}

impl ValueIndexer for MapIndex<UuidIntType> {
    type ValueType = UuidIntType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.add_many_to_map(id, values, hw_counter),
            MapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable map index",
            )),
            MapIndex::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap map index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<Self::ValueType> {
        Some(Uuid::parse_str(value.as_str()?).ok()?.as_u128())
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}

// Per-K value retrievers — produce a closure that maps a point id to
// its indexed keyword/int/uuid values as JSON `Value`s. The conversion
// is K-specific, so each MapIndex variant has its own inherent impl.
// `FieldIndex::value_retriever` dispatches here per variant.

impl MapIndex<str> {
    pub fn value_retriever<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id, hw_counter)
                .into_iter()
                .flatten()
                .filter_map(|v| serde_json::to_value(v).ok())
                .collect()
        })
    }
}

impl MapIndex<IntPayloadType> {
    pub fn value_retriever<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id, hw_counter)
                .into_iter()
                .flatten()
                .map(|v| Value::Number(Number::from(*v)))
                .collect()
        })
    }
}

impl MapIndex<UuidIntType> {
    pub fn value_retriever<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id, hw_counter)
                .into_iter()
                .flatten()
                .map(|value| Value::String(UuidPayloadType::from_u128(*value).to_string()))
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::hint::black_box;
    use std::path::Path;

    use rstest::rstest;
    use tempfile::Builder;

    use super::*;

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

        // Single UUID value
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

        // Ensure cardinality is non-zero
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

        // Ensure cardinality is non-zero
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

        // Ensure cardinality is zero
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

        // Read values with a fresh counter
        let hw_counter = HardwareCounterCell::new();
        for idx in 0..data.len() {
            let _values: Vec<_> = index
                .get_values(idx as PointOffsetType, &hw_counter)
                .unwrap()
                .collect();
        }

        // On-disk mmap variant should have tracked IO reads
        assert!(
            hw_counter.payload_index_io_read_counter().get() > 0,
            "Expected on-disk mmap get_values to track payload index IO reads, but counter was 0"
        );

        // Contrast with RamMmap (is_on_disk=false) — counter should remain zero
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
        // Each value (1, 2, 3) appears at three points — deleting any single
        // point leaves at least two live points per value.
        let data: Vec<Vec<IntPayloadType>> = vec![
            vec![1, 2], // id 0
            vec![1],    // id 1
            vec![2],    // id 2
            vec![1, 3], // id 3
            vec![2, 3], // id 4
            vec![3],    // id 5
        ];

        // Build, remove some points, drop.
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

        // Reload, re-supplying the deletion set for the mmap variants. For
        // gridstore the argument is ignored.
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

        // Lookup-by-value path: consistent across variants.
        // Value 1: originally at ids {0, 1, 3} → after deletions {0, 3}.
        let mut hits: Vec<PointOffsetType> = new_index.get_iterator(&1, &hw_counter).collect();
        hits.sort();
        assert_eq!(hits, vec![0, 3]);

        // Value 2: originally at ids {0, 2, 4} → after deletions {0, 4}.
        let mut hits: Vec<PointOffsetType> = new_index.get_iterator(&2, &hw_counter).collect();
        hits.sort();
        assert_eq!(hits, vec![0, 4]);

        // Value 3: originally at ids {3, 4, 5} → after deletions {3, 4}.
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

        // Point at id 2 has an empty payload, so build-time `deleted.bin`
        // will mark it. Every value retains at least one live point after
        // deleting id 1 — see note on `test_map_index_reload`.
        let data: Vec<Vec<IntPayloadType>> = vec![
            vec![1],    // id 0
            vec![1, 2], // id 1
            vec![],     // id 2 — empty payload
            vec![2, 3], // id 3
            vec![3],    // id 4
        ];

        save_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type, |v| (*v).into());

        // Reload with a bitslice shorter than `point_to_values.len()` (which
        // is 5) marking only point 1 as deleted. Models an id-tracker whose
        // internal range hasn't yet caught up to the index's highest id.
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

        // id 1 deleted (from short bitslice), id 2 deleted (from build-time
        // empty payload), ids 0, 3, 4 live (3 and 4 are beyond the short
        // bitslice and must default to live).
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

        // Value `2`: originally at ids 1, 3 — after id 1 is deleted, only id 3.
        let mut hits: Vec<PointOffsetType> = new_index.get_iterator(&2, &hw_counter).collect();
        hits.sort();
        assert_eq!(hits, vec![3]);
    }
}
