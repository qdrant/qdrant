use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::super::read_ops::{self, GeoMapIndexRead};
use super::ReadOnlyGeoMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::payload_config::StorageType;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, GeoPoint, PayloadKeyType};

/// Dispatcher impl: forwards every [`GeoMapIndexRead`] method to the active
/// variant. Helper methods (`match_cardinality`, `large_hashes`, `get_telemetry_data`,
/// `values_is_empty`) are picked up from the trait's default impls — they
/// only depend on the required methods, so no per-variant dispatch is needed.
impl<S: UniversalRead> GeoMapIndexRead for ReadOnlyGeoMapIndex<S> {
    fn points_count(&self) -> usize {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::points_count(index),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::points_count(index),
        }
    }

    fn points_values_count(&self) -> usize {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::points_values_count(index),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::points_values_count(index),
        }
    }

    fn max_values_per_point(&self) -> usize {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::max_values_per_point(index),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::max_values_per_point(index),
        }
    }

    fn points_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => {
                GeoMapIndexRead::points_of_hash(index, hash, hw_counter)
            }
            ReadOnlyGeoMapIndex::Immutable(index) => {
                GeoMapIndexRead::points_of_hash(index, hash, hw_counter)
            }
        }
    }

    fn values_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => {
                GeoMapIndexRead::values_of_hash(index, hash, hw_counter)
            }
            ReadOnlyGeoMapIndex::Immutable(index) => {
                GeoMapIndexRead::values_of_hash(index, hash, hw_counter)
            }
        }
    }

    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: &dyn Fn(&GeoPoint) -> bool,
    ) -> bool {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => {
                GeoMapIndexRead::check_values_any(index, idx, hw_counter, check_fn)
            }
            ReadOnlyGeoMapIndex::Immutable(index) => {
                GeoMapIndexRead::check_values_any(index, idx, hw_counter, check_fn)
            }
        }
    }

    fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::values_count(index, idx),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::values_count(index, idx),
        }
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::get_values(index, idx),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::get_values(index, idx),
        }
    }

    fn iterator(
        &self,
        values: Vec<GeoHash>,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::iterator(index, values),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::iterator(index, values),
        }
    }

    fn points_per_hash_filtered(
        &self,
        filter: &dyn Fn(&(GeoHash, usize)) -> bool,
    ) -> OperationResult<Vec<(GeoHash, usize)>> {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => {
                GeoMapIndexRead::points_per_hash_filtered(index, filter)
            }
            ReadOnlyGeoMapIndex::Immutable(index) => {
                GeoMapIndexRead::points_per_hash_filtered(index, filter)
            }
        }
    }

    fn get_storage_type(&self) -> StorageType {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::get_storage_type(index),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::get_storage_type(index),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::ram_usage_bytes(index),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::ram_usage_bytes(index),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::is_on_disk(index),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::is_on_disk(index),
        }
    }

    fn populate(&self) -> OperationResult<()> {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::populate(index),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::populate(index),
        }
    }

    fn clear_cache(&self) -> OperationResult<()> {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::clear_cache(index),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::clear_cache(index),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::files(index),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::files(index),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::immutable_files(index),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::immutable_files(index),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            ReadOnlyGeoMapIndex::Appendable(index) => GeoMapIndexRead::telemetry_index_type(index),
            ReadOnlyGeoMapIndex::Immutable(index) => GeoMapIndexRead::telemetry_index_type(index),
        }
    }
}

impl<S: UniversalRead> PayloadFieldIndexRead for ReadOnlyGeoMapIndex<S> {
    fn count_indexed_points(&self) -> usize {
        GeoMapIndexRead::points_count(self)
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
