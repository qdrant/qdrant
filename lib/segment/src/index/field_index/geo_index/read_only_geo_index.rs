use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use serde_json::Value;

use super::mmap_geo_index::StoredGeoMapIndex;
use super::read_ops::{self, GeoMapIndexRead};
use crate::common::operation_error::OperationResult;
use crate::common::utils::MultiValue;
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::payload_config::StorageType;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::types::{FieldCondition, GeoPoint, PayloadKeyType};

/// Read-only counterpart of [`MutableGeoMapIndex`][1] / [`ImmutableGeoMapIndex`][2].
///
/// Thin wrapper over the already-immutable [`StoredGeoMapIndex<S>`][3] backing
/// format, parameterised by a [`UniversalRead`] storage. Read logic (filter /
/// cardinality / payload blocks / condition checker) is shared with the
/// writable variants via [`read_ops`][4]; the wrapper just re-tags telemetry
/// and surfaces the `S`-generic version of the same data on the read-only side
/// of `ReadOnlyFieldIndex`.
///
/// [1]: super::mutable_geo_index::MutableGeoMapIndex
/// [2]: super::immutable_geo_index::ImmutableGeoMapIndex
/// [3]: super::mmap_geo_index::StoredGeoMapIndex
/// [4]: super::read_ops
pub struct ReadOnlyGeoMapIndex<S: UniversalRead> {
    inner: StoredGeoMapIndex<S>,
}

impl<S: UniversalRead> ReadOnlyGeoMapIndex<S> {
    /// Open a read-only geo index at the given path. Returns `None` if no
    /// index exists on disk.
    pub fn open(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        Ok(StoredGeoMapIndex::open(path, is_on_disk, deleted_points)?.map(|inner| Self { inner }))
    }

    /// Produce a closure that maps a point id to its indexed geo values as
    /// JSON `Value`s. Mirrors `GeoMapIndex::value_retriever`.
    pub fn value_retriever<'a>(
        &'a self,
        _hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            GeoMapIndexRead::get_values(self, point_id)
                .into_iter()
                .flatten()
                .filter_map(|v| serde_json::to_value(v).ok())
                .collect()
        })
    }
}

impl<S: UniversalRead> GeoMapIndexRead for ReadOnlyGeoMapIndex<S> {
    fn points_count(&self) -> usize {
        GeoMapIndexRead::points_count(&self.inner)
    }

    fn points_values_count(&self) -> usize {
        GeoMapIndexRead::points_values_count(&self.inner)
    }

    fn max_values_per_point(&self) -> usize {
        GeoMapIndexRead::max_values_per_point(&self.inner)
    }

    fn points_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        GeoMapIndexRead::points_of_hash(&self.inner, hash, hw_counter)
    }

    fn values_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        GeoMapIndexRead::values_of_hash(&self.inner, hash, hw_counter)
    }

    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: &dyn Fn(&GeoPoint) -> bool,
    ) -> bool {
        GeoMapIndexRead::check_values_any(&self.inner, idx, hw_counter, check_fn)
    }

    fn values_count(&self, idx: PointOffsetType) -> usize {
        GeoMapIndexRead::values_count(&self.inner, idx)
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        GeoMapIndexRead::get_values(&self.inner, idx)
    }

    fn iterator(
        &self,
        values: Vec<GeoHash>,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        GeoMapIndexRead::iterator(&self.inner, values)
    }

    fn points_per_hash_filtered(
        &self,
        filter: &dyn Fn(&(GeoHash, usize)) -> bool,
    ) -> OperationResult<Vec<(GeoHash, usize)>> {
        GeoMapIndexRead::points_per_hash_filtered(&self.inner, filter)
    }

    fn get_storage_type(&self) -> StorageType {
        GeoMapIndexRead::get_storage_type(&self.inner)
    }

    fn ram_usage_bytes(&self) -> usize {
        GeoMapIndexRead::ram_usage_bytes(&self.inner)
    }

    fn is_on_disk(&self) -> bool {
        GeoMapIndexRead::is_on_disk(&self.inner)
    }

    fn populate(&self) -> OperationResult<()> {
        GeoMapIndexRead::populate(&self.inner)
    }

    fn clear_cache(&self) -> OperationResult<()> {
        GeoMapIndexRead::clear_cache(&self.inner)
    }

    fn files(&self) -> Vec<PathBuf> {
        GeoMapIndexRead::files(&self.inner)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        GeoMapIndexRead::immutable_files(&self.inner)
    }

    fn telemetry_index_type(&self) -> &'static str {
        "read_only_geo"
    }
}

impl<S: UniversalRead> PayloadFieldIndexRead for ReadOnlyGeoMapIndex<S> {
    fn count_indexed_points(&self) -> usize {
        self.points_count()
    }

    fn filter<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        read_ops::filter(self, condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        read_ops::estimate_cardinality(self, condition, hw_counter)
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
