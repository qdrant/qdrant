use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::super::read_ops::GeoMapIndexRead;
use super::StoredGeoMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::payload_config::StorageType;
use crate::types::GeoPoint;

impl<S: UniversalRead> GeoMapIndexRead for StoredGeoMapIndex<S> {
    fn points_count(&self) -> usize {
        StoredGeoMapIndex::points_count(self)
    }

    fn points_values_count(&self) -> usize {
        StoredGeoMapIndex::points_values_count(self)
    }

    fn max_values_per_point(&self) -> usize {
        StoredGeoMapIndex::max_values_per_point(self)
    }

    fn points_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        StoredGeoMapIndex::points_of_hash(self, hash, hw_counter)
    }

    fn values_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        StoredGeoMapIndex::values_of_hash(self, hash, hw_counter)
    }

    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: &dyn Fn(&GeoPoint) -> bool,
    ) -> OperationResult<bool> {
        StoredGeoMapIndex::check_values_any(self, idx, hw_counter, |p| check_fn(p))
    }

    fn values_count(&self, idx: PointOffsetType) -> usize {
        StoredGeoMapIndex::values_count(self, idx)
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        StoredGeoMapIndex::get_values(self, idx)
            .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = GeoPoint> + '_>)
    }

    fn iterator(
        &self,
        values: Vec<GeoHash>,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        let points = self.all_points(values)?;
        Ok(Box::new(points.into_iter()))
    }

    fn points_per_hash_filtered(
        &self,
        filter: &dyn Fn(&(GeoHash, usize)) -> bool,
    ) -> OperationResult<Vec<(GeoHash, usize)>> {
        StoredGeoMapIndex::points_per_hash(self, |pair| filter(pair))
    }

    fn get_storage_type(&self) -> StorageType {
        StorageType::Mmap {
            is_on_disk: self.is_on_disk(),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        StoredGeoMapIndex::ram_usage_bytes(self)
    }

    fn is_on_disk(&self) -> bool {
        StoredGeoMapIndex::is_on_disk(self)
    }

    fn populate(&self) -> OperationResult<()> {
        StoredGeoMapIndex::populate(self)
    }

    fn clear_cache(&self) -> OperationResult<()> {
        StoredGeoMapIndex::clear_cache(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        StoredGeoMapIndex::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        StoredGeoMapIndex::immutable_files(self)
    }

    fn telemetry_index_type(&self) -> &'static str {
        "mmap_geo"
    }
}
