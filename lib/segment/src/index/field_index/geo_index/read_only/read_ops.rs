use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::super::read_ops::{self, GeoIndexRead};
use super::ReadOnlyGeoIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::payload_config::StorageType;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, GeoPoint, PayloadKeyType};

/// Dispatcher impl: forwards every [`GeoIndexRead`] method to the active
/// variant. Helper methods (`match_cardinality`, `large_hashes`, `get_telemetry_data`,
/// `values_is_empty`) are picked up from the trait's default impls — they
/// only depend on the required methods, so no per-variant dispatch is needed.
impl<S: UniversalRead> GeoIndexRead for ReadOnlyGeoIndex<S> {
    fn points_count(&self) -> usize {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::points_count(index),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::points_count(index),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::points_count(index),
        }
    }

    fn points_values_count(&self) -> usize {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::points_values_count(index),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::points_values_count(index),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::points_values_count(index),
        }
    }

    fn max_values_per_point(&self) -> usize {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::max_values_per_point(index),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::max_values_per_point(index),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::max_values_per_point(index),
        }
    }

    fn points_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => {
                GeoIndexRead::points_of_hash(index, hash, hw_counter)
            }
            ReadOnlyGeoIndex::Immutable(index) => {
                GeoIndexRead::points_of_hash(index, hash, hw_counter)
            }
            ReadOnlyGeoIndex::OnDisk(index) => {
                GeoIndexRead::points_of_hash(index, hash, hw_counter)
            }
        }
    }

    fn values_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => {
                GeoIndexRead::values_of_hash(index, hash, hw_counter)
            }
            ReadOnlyGeoIndex::Immutable(index) => {
                GeoIndexRead::values_of_hash(index, hash, hw_counter)
            }
            ReadOnlyGeoIndex::OnDisk(index) => {
                GeoIndexRead::values_of_hash(index, hash, hw_counter)
            }
        }
    }

    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: &dyn Fn(&GeoPoint) -> bool,
    ) -> OperationResult<bool> {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => {
                GeoIndexRead::check_values_any(index, idx, hw_counter, check_fn)
            }
            ReadOnlyGeoIndex::Immutable(index) => {
                GeoIndexRead::check_values_any(index, idx, hw_counter, check_fn)
            }
            ReadOnlyGeoIndex::OnDisk(index) => {
                GeoIndexRead::check_values_any(index, idx, hw_counter, check_fn)
            }
        }
    }

    fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::values_count(index, idx),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::values_count(index, idx),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::values_count(index, idx),
        }
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::get_values(index, idx),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::get_values(index, idx),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::get_values(index, idx),
        }
    }

    fn iterator(
        &self,
        values: Vec<GeoHash>,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::iterator(index, values),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::iterator(index, values),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::iterator(index, values),
        }
    }

    fn points_per_hash_filtered(
        &self,
        filter: &dyn Fn(&(GeoHash, usize)) -> bool,
    ) -> OperationResult<Vec<(GeoHash, usize)>> {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => {
                GeoIndexRead::points_per_hash_filtered(index, filter)
            }
            ReadOnlyGeoIndex::Immutable(index) => {
                GeoIndexRead::points_per_hash_filtered(index, filter)
            }
            ReadOnlyGeoIndex::OnDisk(index) => {
                GeoIndexRead::points_per_hash_filtered(index, filter)
            }
        }
    }

    fn get_storage_type(&self) -> StorageType {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::get_storage_type(index),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::get_storage_type(index),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::get_storage_type(index),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::ram_usage_bytes(index),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::ram_usage_bytes(index),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::ram_usage_bytes(index),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::is_on_disk(index),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::is_on_disk(index),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::is_on_disk(index),
        }
    }

    fn populate(&self) -> OperationResult<()> {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::populate(index),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::populate(index),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::populate(index),
        }
    }

    fn clear_cache(&self) -> OperationResult<()> {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::clear_cache(index),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::clear_cache(index),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::clear_cache(index),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::files(index),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::files(index),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::files(index),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::immutable_files(index),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::immutable_files(index),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::immutable_files(index),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            ReadOnlyGeoIndex::Appendable(index) => GeoIndexRead::telemetry_index_type(index),
            ReadOnlyGeoIndex::Immutable(index) => GeoIndexRead::telemetry_index_type(index),
            ReadOnlyGeoIndex::OnDisk(index) => GeoIndexRead::telemetry_index_type(index),
        }
    }
}

impl<S: UniversalRead> PayloadFieldIndexRead for ReadOnlyGeoIndex<S> {
    fn count_indexed_points(&self) -> usize {
        GeoIndexRead::points_count(self)
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
    ) -> OperationResult<Option<ConditionCheckerFn<'a>>> {
        Ok(read_ops::condition_checker(self, condition, hw_acc))
    }
}
