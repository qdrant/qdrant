use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use itertools::Itertools;

use super::super::super::read_ops::GeoMapIndexRead;
use super::ReadOnlyAppendableGeoMapIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::payload_config::StorageType;
use crate::types::GeoPoint;

impl<S: UniversalRead> GeoMapIndexRead for ReadOnlyAppendableGeoMapIndex<S> {
    fn points_count(&self) -> usize {
        self.in_memory_index.points_count
    }

    fn points_values_count(&self) -> usize {
        self.in_memory_index.points_values_count
    }

    fn max_values_per_point(&self) -> usize {
        self.in_memory_index.max_values_per_point
    }

    fn points_of_hash(
        &self,
        hash: GeoHash,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        Ok(self.in_memory_index.points_of_hash(hash))
    }

    fn values_of_hash(
        &self,
        hash: GeoHash,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        Ok(self.in_memory_index.values_of_hash(hash))
    }

    fn check_values_any(
        &self,
        idx: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
        check_fn: &dyn Fn(&GeoPoint) -> bool,
    ) -> bool {
        self.in_memory_index.check_values_any(idx, |p| check_fn(p))
    }

    fn values_count(&self, idx: PointOffsetType) -> usize {
        self.in_memory_index.values_count(idx)
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        self.in_memory_index
            .point_to_values
            .get(idx as usize)
            .map(|v| Box::new(v.iter().copied()) as Box<dyn Iterator<Item = GeoPoint> + '_>)
    }

    fn iterator(
        &self,
        values: Vec<GeoHash>,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        Ok(Box::new(
            values
                .into_iter()
                .flat_map(|top_geo_hash| self.in_memory_index.stored_sub_regions(top_geo_hash))
                .unique(),
        ))
    }

    fn points_per_hash_filtered(
        &self,
        filter: &dyn Fn(&(GeoHash, usize)) -> bool,
    ) -> OperationResult<Vec<(GeoHash, usize)>> {
        Ok(self
            .in_memory_index
            .points_per_hash()
            .filter(|pair| filter(pair))
            .collect())
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
        self.storage.clear_cache().map_err(|err| {
            OperationError::service_error(format!(
                "Failed to clear read-only appendable geo index gridstore cache: {err}"
            ))
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn telemetry_index_type(&self) -> &'static str {
        "read_only_appendable_geo"
    }
}
