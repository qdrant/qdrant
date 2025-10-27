use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::fmt::{Debug, Display};
use std::hash::{BuildHasher, Hash};
use std::iter;
use std::path::{Path, PathBuf};
use std::str::FromStr;
#[cfg(feature = "rocksdb")]
use std::sync::Arc;

use ahash::HashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap_hashmap::Key;
use common::types::PointOffsetType;
use ecow::EcoString;
use gridstore::Blob;
use indexmap::IndexSet;
use itertools::Itertools;
use mmap_map_index::MmapMapIndex;
#[cfg(feature = "rocksdb")]
use parking_lot::RwLock;
#[cfg(feature = "rocksdb")]
use rocksdb::DB;
use serde_json::Value;
use uuid::Uuid;

use self::immutable_map_index::ImmutableMapIndex;
use self::mutable_map_index::MutableMapIndex;
use super::FieldIndexBuilderTrait;
use super::facet_index::FacetIndex;
use super::mmap_point_to_values::MmapValue;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::facets::{FacetHit, FacetValueRef};
use crate::index::field_index::stat_tools::number_of_selected_points;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PrimaryCondition, ValueIndexer,
};
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::index::query_estimator::combine_should_estimations;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    AnyVariants, FieldCondition, IntPayloadType, Match, MatchAny, MatchExcept, MatchValue,
    PayloadKeyType, UuidIntType, ValueVariants,
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

pub trait MapIndexKey: Key + MmapValue + Eq + Display + Debug {
    type Owned: Borrow<Self> + Hash + Eq + Clone + FromStr + Default + 'static;

    fn to_owned(&self) -> Self::Owned;

    fn gridstore_block_size() -> usize {
        size_of::<Self::Owned>()
    }
}

impl MapIndexKey for str {
    type Owned = EcoString;

    fn to_owned(&self) -> Self::Owned {
        EcoString::from(self)
    }

    fn gridstore_block_size() -> usize {
        BLOCK_SIZE_KEYWORD
    }
}

impl MapIndexKey for IntPayloadType {
    type Owned = IntPayloadType;

    fn to_owned(&self) -> Self::Owned {
        *self
    }
}

impl MapIndexKey for UuidIntType {
    type Owned = UuidIntType;

    fn to_owned(&self) -> Self::Owned {
        *self
    }
}

pub enum MapIndex<N: MapIndexKey + ?Sized>
where
    Vec<N::Owned>: Blob + Send + Sync,
{
    Mutable(MutableMapIndex<N>),
    Immutable(ImmutableMapIndex<N>),
    Mmap(Box<MmapMapIndex<N>>),
}

impl<N: MapIndexKey + ?Sized> MapIndex<N>
where
    Vec<N::Owned>: Blob + Send + Sync,
{
    #[cfg(feature = "rocksdb")]
    pub fn new_rocksdb(
        db: Arc<RwLock<DB>>,
        field_name: &str,
        is_appendable: bool,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        let index = if is_appendable {
            MutableMapIndex::open_rocksdb(db, field_name, create_if_missing)?.map(MapIndex::Mutable)
        } else {
            ImmutableMapIndex::open_rocksdb(db, field_name)?.map(MapIndex::Immutable)
        };
        Ok(index)
    }

    /// Load immutable mmap based index, either in RAM or on disk
    pub fn new_mmap(path: &Path, is_on_disk: bool) -> OperationResult<Option<Self>> {
        let Some(mmap_index) = MmapMapIndex::open(path, is_on_disk)? else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        let index = if is_on_disk {
            // Use on mmap directly
            MapIndex::Mmap(Box::new(mmap_index))
        } else {
            // Load into RAM, use mmap as backing storage
            MapIndex::Immutable(ImmutableMapIndex::open_mmap(mmap_index))
        };
        Ok(Some(index))
    }

    pub fn new_gridstore(dir: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        let index = MutableMapIndex::open_gridstore(dir, create_if_missing)?;
        Ok(index.map(MapIndex::Mutable))
    }

    #[cfg(feature = "rocksdb")]
    pub fn builder_rocksdb(
        db: Arc<RwLock<DB>>,
        field_name: &str,
    ) -> OperationResult<MapIndexBuilder<N>> {
        Ok(MapIndexBuilder(MapIndex::Mutable(
            MutableMapIndex::open_rocksdb(db, field_name, true)?.ok_or_else(|| {
                OperationError::service_error(format!(
                    "Failed to create and load mutable map index builder for field '{field_name}'",
                ))
            })?,
        )))
    }

    pub fn builder_mmap(path: &Path, is_on_disk: bool) -> MapIndexMmapBuilder<N> {
        MapIndexMmapBuilder {
            path: path.to_owned(),
            point_to_values: Default::default(),
            values_to_points: Default::default(),
            is_on_disk,
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
            MapIndex::Mmap(index) => index.check_values_any(idx, hw_counter, check_fn),
        }
    }

    pub fn get_values(
        &self,
        idx: PointOffsetType,
    ) -> Option<Box<dyn Iterator<Item = N::Referenced<'_>> + '_>> {
        match self {
            MapIndex::Mutable(index) => Some(Box::new(
                index.get_values(idx)?.map(|v| N::as_referenced(v)),
            )),
            MapIndex::Immutable(index) => Some(Box::new(
                index.get_values(idx)?.map(|v| N::as_referenced(v)),
            )),
            MapIndex::Mmap(index) => Some(Box::new(index.get_values(idx)?)),
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

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        match self {
            MapIndex::Mutable(index) => index.iter_values(),
            MapIndex::Immutable(index) => index.iter_values(),
            MapIndex::Mmap(index) => Box::new(index.iter_values()),
        }
    }

    pub fn iter_counts_per_value(&self) -> Box<dyn Iterator<Item = (&N, usize)> + '_> {
        match self {
            MapIndex::Mutable(index) => Box::new(index.iter_counts_per_value()),
            MapIndex::Immutable(index) => Box::new(index.iter_counts_per_value()),
            MapIndex::Mmap(index) => Box::new(index.iter_counts_per_value()),
        }
    }

    pub fn iter_values_map<'a>(
        &'a self,
        hw_cell: &'a HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = (&'a N, IdIter<'a>)> + 'a> {
        match self {
            MapIndex::Mutable(index) => Box::new(index.iter_values_map()),
            MapIndex::Immutable(index) => Box::new(index.iter_values_map()),
            MapIndex::Mmap(index) => Box::new(index.iter_values_map(hw_cell)),
        }
    }

    pub fn storage_cf_name(field: &str) -> String {
        format!("{field}_map")
    }

    fn flusher(&self) -> (Flusher, Flusher) {
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

    pub fn encode_db_record(value: &N, idx: PointOffsetType) -> String {
        format!("{value}/{idx}")
    }

    pub fn decode_db_record(s: &str) -> OperationResult<(N::Owned, PointOffsetType)> {
        const DECODE_ERR: &str = "Index db parsing error: wrong data format";
        let separator_pos = s
            .rfind('/')
            .ok_or_else(|| OperationError::service_error(DECODE_ERR))?;
        if separator_pos == s.len() - 1 {
            return Err(OperationError::service_error(DECODE_ERR));
        }
        let value_str = &s[..separator_pos];
        let value =
            N::Owned::from_str(value_str).map_err(|_| OperationError::service_error(DECODE_ERR))?;
        let idx_str = &s[separator_pos + 1..];
        let idx = PointOffsetType::from_str(idx_str)
            .map_err(|_| OperationError::service_error(DECODE_ERR))?;
        Ok((value, idx))
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
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a>
    where
        A: BuildHasher,
        K: Borrow<N> + Hash + Eq,
    {
        Box::new(
            self.iter_values()
                .filter(|key| !excluded.contains((*key).borrow()))
                .flat_map(move |key| self.get_iterator(key.borrow(), hw_counter))
                .unique(),
        )
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            MapIndex::Mutable(_) => false,
            MapIndex::Immutable(_) => false,
            MapIndex::Mmap(index) => index.is_on_disk(),
        }
    }

    #[cfg(feature = "rocksdb")]
    pub fn is_rocksdb(&self) -> bool {
        match self {
            MapIndex::Mutable(index) => index.is_rocksdb(),
            MapIndex::Immutable(index) => index.is_rocksdb(),
            MapIndex::Mmap(_) => false,
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
    Vec<N::Owned>: Blob + Send + Sync;

impl<N: MapIndexKey + ?Sized> FieldIndexBuilderTrait for MapIndexBuilder<N>
where
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
    Vec<N::Owned>: Blob + Send + Sync,
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
    point_to_values: Vec<Vec<N::Owned>>,
    values_to_points: HashMap<N::Owned, Vec<PointOffsetType>>,
    is_on_disk: bool,
}

impl<N: MapIndexKey + ?Sized> FieldIndexBuilderTrait for MapIndexMmapBuilder<N>
where
    Vec<N::Owned>: Blob + Send + Sync,
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
    <MapIndex<N> as ValueIndexer>::ValueType: Into<N::Owned>,
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
        for value in payload.iter() {
            let payload_values = <MapIndex<N> as ValueIndexer>::get_values(value);
            flatten_values.extend(payload_values);
        }
        let flatten_values: Vec<N::Owned> = flatten_values.into_iter().map(Into::into).collect();

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
                let size = N::mmapped_size(N::as_referenced(e.key().borrow()));
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
        )?)))
    }
}

pub struct MapIndexGridstoreBuilder<N: MapIndexKey + ?Sized>
where
    Vec<N::Owned>: Blob + Send + Sync,
{
    dir: PathBuf,
    index: Option<MapIndex<N>>,
}

impl<N: MapIndexKey + ?Sized> MapIndexGridstoreBuilder<N>
where
    Vec<N::Owned>: Blob + Send + Sync,
{
    fn new(dir: PathBuf) -> Self {
        Self { dir, index: None }
    }
}

impl<N: MapIndexKey + ?Sized> FieldIndexBuilderTrait for MapIndexGridstoreBuilder<N>
where
    Vec<N::Owned>: Blob + Send + Sync,
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
    <MapIndex<N> as ValueIndexer>::ValueType: Into<N::Owned>,
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
        index.flush_all()?;
        Ok(index)
    }
}

impl PayloadFieldIndex for MapIndex<str> {
    fn count_indexed_points(&self) -> usize {
        self.get_indexed_points()
    }

    fn cleanup(self) -> OperationResult<()> {
        self.wipe()
    }

    fn flusher(&self) -> (Flusher, Flusher) {
        MapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.immutable_files()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
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
                AnyVariants::Strings(keywords) => Some(self.except_set(keywords, hw_counter)),
                AnyVariants::Integers(other) => {
                    if other.is_empty() {
                        Some(Box::new(iter::empty()))
                    } else {
                        None
                    }
                }
            },
            _ => None,
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CardinalityEstimation> {
        match &condition.r#match {
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
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        Box::new(
            self.iter_values()
                .map(|value| {
                    (
                        value,
                        self.get_count_for_value(value, &HardwareCounterCell::disposable()) // Payload_blocks only used in HNSW building, which is unmeasured.
                            .unwrap_or(0),
                    )
                })
                .filter(move |(_value, count)| *count > threshold)
                .map(move |(value, count)| PayloadBlockCondition {
                    condition: FieldCondition::new_match(key.clone(), value.to_string().into()),
                    cardinality: count,
                }),
        )
    }
}

impl PayloadFieldIndex for MapIndex<UuidIntType> {
    fn count_indexed_points(&self) -> usize {
        self.get_indexed_points()
    }

    fn cleanup(self) -> OperationResult<()> {
        self.wipe()
    }

    fn flusher(&self) -> (Flusher, Flusher) {
        MapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.immutable_files()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue { value })) => match value {
                ValueVariants::String(uuid_string) => {
                    let uuid = Uuid::from_str(uuid_string).ok()?;
                    Some(Box::new(self.get_iterator(&uuid.as_u128(), hw_counter)))
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

                    let uuids = uuids.ok()?;

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
                    let uuids: Result<IndexSet<u128>, _> = uuids_string
                        .iter()
                        .map(|uuid_string| Uuid::from_str(uuid_string).map(|x| x.as_u128()))
                        .collect();

                    let excluded_uuids = uuids.ok()?;
                    let exclude_iter = self
                        .iter_values()
                        .filter(move |key| !excluded_uuids.contains(*key))
                        .flat_map(move |key| self.get_iterator(key, hw_counter))
                        .unique();
                    Some(Box::new(exclude_iter))
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
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Value(MatchValue { value })) => match value {
                ValueVariants::String(uuid_string) => {
                    let uuid = Uuid::from_str(uuid_string).ok()?;
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

                    let uuids = uuids.ok()?;

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

                    let excluded_uuids = uuids.ok()?;

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
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        Box::new(
            self.iter_values()
                .map(move |value| {
                    (
                        value,
                        self.get_count_for_value(value, &HardwareCounterCell::disposable()) // payload_blocks only used in HNSW building, which is unmeasured.
                            .unwrap_or(0),
                    )
                })
                .filter(move |(_value, count)| *count >= threshold)
                .map(move |(value, count)| PayloadBlockCondition {
                    condition: FieldCondition::new_match(
                        key.clone(),
                        Uuid::from_u128(*value).to_string().into(),
                    ),
                    cardinality: count,
                }),
        )
    }
}

impl PayloadFieldIndex for MapIndex<IntPayloadType> {
    fn count_indexed_points(&self) -> usize {
        self.get_indexed_points()
    }

    fn cleanup(self) -> OperationResult<()> {
        self.wipe()
    }

    fn flusher(&self) -> (Flusher, Flusher) {
        MapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.immutable_files()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
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
                AnyVariants::Integers(integers) => Some(self.except_set(integers, hw_counter)),
            },
            _ => None,
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CardinalityEstimation> {
        match &condition.r#match {
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
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        Box::new(
            self.iter_values()
                .map(move |value| {
                    (
                        value,
                        self.get_count_for_value(value, &HardwareCounterCell::disposable()) // Only used in HNSW building so no measurement needed here.
                            .unwrap_or(0),
                    )
                })
                .filter(move |(_value, count)| *count >= threshold)
                .map(move |(value, count)| PayloadBlockCondition {
                    condition: FieldCondition::new_match(key.clone(), (*value).into()),
                    cardinality: count,
                }),
        )
    }
}

impl<N: MapIndexKey + ?Sized> FacetIndex for MapIndex<N>
where
    Vec<N::Owned>: Blob + Send + Sync,
    for<'a> N::Referenced<'a>: Into<FacetValueRef<'a>>,
    for<'a> &'a N: Into<FacetValueRef<'a>>,
{
    fn get_point_values(
        &self,
        point_id: PointOffsetType,
    ) -> impl Iterator<Item = FacetValueRef<'_>> + '_ {
        MapIndex::get_values(self, point_id)
            .into_iter()
            .flatten()
            .map(Into::into)
    }

    fn iter_values(&self) -> impl Iterator<Item = FacetValueRef<'_>> + '_ {
        self.iter_values().map(Into::into)
    }

    fn iter_values_map<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = (FacetValueRef<'a>, IdIter<'a>)> + 'a {
        self.iter_values_map(hw_counter)
            .map(|(k, iter)| (k.into(), iter))
    }

    fn iter_counts_per_value(&self) -> impl Iterator<Item = FacetHit<FacetValueRef<'_>>> + '_ {
        self.iter_counts_per_value().map(|(value, count)| FacetHit {
            value: value.into(),
            count,
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
        if let Value::Number(num) = value {
            return num.as_i64();
        }
        None
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::hint::black_box;
    use std::path::Path;

    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    #[cfg(feature = "rocksdb")]
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;

    #[cfg(feature = "rocksdb")]
    const FIELD_NAME: &str = "test";

    #[derive(Clone, Copy)]
    enum IndexType {
        #[cfg(feature = "rocksdb")]
        Mutable,
        MutableGridstore,
        #[cfg(feature = "rocksdb")]
        Immutable,
        Mmap,
        RamMmap,
    }

    fn save_map_index<N>(
        data: &[Vec<N::Owned>],
        path: &Path,
        index_type: IndexType,
        into_value: impl Fn(&N::Owned) -> Value,
    ) where
        N: MapIndexKey + ?Sized,
        Vec<N::Owned>: Blob + Send + Sync,
        MapIndex<N>: PayloadFieldIndex + ValueIndexer,
        <MapIndex<N> as ValueIndexer>::ValueType: Into<N::Owned>,
    {
        let hw_counter = HardwareCounterCell::new();

        match index_type {
            #[cfg(feature = "rocksdb")]
            IndexType::Mutable | IndexType::Immutable => {
                let mut builder = MapIndex::<N>::builder_rocksdb(
                    open_db_with_existing_cf(path).unwrap(),
                    FIELD_NAME,
                )
                .unwrap();
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
                let mut builder = MapIndex::<N>::builder_mmap(path, false);
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
        data: &[Vec<N::Owned>],
        path: &Path,
        index_type: IndexType,
    ) -> MapIndex<N>
    where
        Vec<N::Owned>: Blob + Send + Sync,
    {
        let index = match index_type {
            #[cfg(feature = "rocksdb")]
            IndexType::Mutable => MapIndex::<N>::new_rocksdb(
                open_db_with_existing_cf(path).unwrap(),
                FIELD_NAME,
                true,
                true,
            )
            .unwrap()
            .unwrap(),
            IndexType::MutableGridstore => MapIndex::<N>::new_gridstore(path.to_path_buf(), true)
                .unwrap()
                .unwrap(),
            #[cfg(feature = "rocksdb")]
            IndexType::Immutable => MapIndex::<N>::new_rocksdb(
                open_db_with_existing_cf(path).unwrap(),
                FIELD_NAME,
                false,
                true,
            )
            .unwrap()
            .unwrap(),
            IndexType::Mmap => MapIndex::<N>::new_mmap(path, true).unwrap().unwrap(),
            IndexType::RamMmap => MapIndex::<N>::new_mmap(path, false).unwrap().unwrap(),
        };
        for (idx, values) in data.iter().enumerate() {
            let index_values: HashSet<N::Owned> = index
                .get_values(idx as PointOffsetType)
                .unwrap()
                .map(|v| N::to_owned(N::from_referenced(&v)))
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
        let mut builder = MapIndex::<UuidIntType>::builder_mmap(temp_dir.path(), false);

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

        for block in index.payload_blocks(50, PayloadKeyType::new("test_uuid")) {
            black_box(block);
        }
    }

    #[test]
    fn test_index_non_ascending_insertion() {
        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        let mut builder = MapIndex::<IntPayloadType>::builder_mmap(temp_dir.path(), false);
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
        for (idx, values) in data.iter().enumerate().rev() {
            let res: Vec<_> = index
                .get_values(idx as u32)
                .unwrap()
                .map(|i| *i as i32)
                .collect();
            assert_eq!(res, *values);
        }
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
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
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
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
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
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
}
