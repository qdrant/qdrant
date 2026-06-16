use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use itertools::Itertools;

use super::super::read_ops::GeoIndexRead;
use super::ImmutableGeoIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::payload_config::StorageType;
use crate::types::GeoPoint;

impl<S: UniversalRead> GeoIndexRead for ImmutableGeoIndex<S> {
    fn points_count(&self) -> usize {
        ImmutableGeoIndex::points_count(self)
    }

    fn points_values_count(&self) -> usize {
        ImmutableGeoIndex::points_values_count(self)
    }

    fn max_values_per_point(&self) -> usize {
        ImmutableGeoIndex::max_values_per_point(self)
    }

    fn points_of_hash(
        &self,
        hash: GeoHash,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        Ok(ImmutableGeoIndex::points_of_hash(self, hash))
    }

    fn values_of_hash(
        &self,
        hash: GeoHash,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        Ok(ImmutableGeoIndex::values_of_hash(self, hash))
    }

    fn check_values_any(
        &self,
        idx: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
        check_fn: &dyn Fn(&GeoPoint) -> bool,
    ) -> OperationResult<bool> {
        Ok(ImmutableGeoIndex::check_values_any(self, idx, check_fn))
    }

    fn values_count(&self, idx: PointOffsetType) -> usize {
        ImmutableGeoIndex::values_count(self, idx)
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        ImmutableGeoIndex::get_values(self, idx)
            .map(|iter| Box::new(iter.copied()) as Box<dyn Iterator<Item = GeoPoint> + '_>)
    }

    fn iterator(
        &self,
        values: Vec<GeoHash>,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        Ok(Box::new(
            values
                .into_iter()
                .flat_map(|top_geo_hash| self.stored_sub_regions(top_geo_hash))
                .unique(),
        ))
    }

    fn points_per_hash_filtered(
        &self,
        filter: &dyn Fn(&(GeoHash, usize)) -> bool,
    ) -> OperationResult<Vec<(GeoHash, usize)>> {
        Ok(ImmutableGeoIndex::points_per_hash(self)
            .filter(|pair| filter(pair))
            .collect())
    }

    fn get_storage_type(&self) -> StorageType {
        ImmutableGeoIndex::storage_type(self)
    }

    fn ram_usage_bytes(&self) -> usize {
        ImmutableGeoIndex::ram_usage_bytes(self)
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn populate(&self) -> OperationResult<()> {
        Ok(())
    }

    fn clear_cache(&self) -> OperationResult<()> {
        ImmutableGeoIndex::clear_cache(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        ImmutableGeoIndex::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        ImmutableGeoIndex::immutable_files(self)
    }

    fn telemetry_index_type(&self) -> &'static str {
        "immutable_geo"
    }
}
