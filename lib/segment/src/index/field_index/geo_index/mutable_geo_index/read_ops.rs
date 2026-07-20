use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::super::read_ops::GeoMapIndexRead;
use super::MutableGeoMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::payload_config::StorageType;
use crate::types::GeoPoint;

impl GeoMapIndexRead for MutableGeoMapIndex {
    fn points_count(&self) -> usize {
        self.in_memory_index.points_count()
    }

    fn points_values_count(&self) -> usize {
        self.in_memory_index.points_values_count()
    }

    fn max_values_per_point(&self) -> usize {
        self.in_memory_index.max_values_per_point()
    }

    fn points_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        self.in_memory_index.points_of_hash(hash, hw_counter)
    }

    fn values_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        self.in_memory_index.values_of_hash(hash, hw_counter)
    }

    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: &dyn Fn(&GeoPoint) -> bool,
    ) -> OperationResult<bool> {
        self.in_memory_index
            .check_values_any(idx, hw_counter, check_fn)
    }

    fn values_count(&self, idx: PointOffsetType) -> usize {
        self.in_memory_index.values_count(idx)
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        self.in_memory_index.get_values(idx)
    }

    fn iterator(
        &self,
        values: Vec<GeoHash>,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        self.in_memory_index.iterator(values)
    }

    fn points_per_hash_filtered(
        &self,
        filter: &dyn Fn(&(GeoHash, usize)) -> bool,
    ) -> OperationResult<Vec<(GeoHash, usize)>> {
        self.in_memory_index.points_per_hash_filtered(filter)
    }

    fn get_storage_type(&self) -> StorageType {
        StorageType::Gridstore
    }

    fn ram_usage_bytes(&self) -> usize {
        self.in_memory_index.ram_usage_bytes()
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn populate(&self) -> OperationResult<()> {
        Ok(())
    }

    fn clear_cache(&self) -> OperationResult<()> {
        MutableGeoMapIndex::clear_cache(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        MutableGeoMapIndex::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn telemetry_index_type(&self) -> &'static str {
        "mutable_geo"
    }
}
