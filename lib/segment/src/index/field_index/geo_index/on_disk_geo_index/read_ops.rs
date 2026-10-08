use std::path::PathBuf;

use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UserData};

use super::super::read_ops::GeoIndexRead;
use super::OnDiskGeoIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::payload_config::StorageType;
use crate::types::GeoPoint;

impl<S: UniversalRead> GeoIndexRead for OnDiskGeoIndex<S> {
    fn points_count(&self) -> usize {
        OnDiskGeoIndex::points_count(self)
    }

    fn points_values_count(&self) -> usize {
        OnDiskGeoIndex::points_values_count(self)
    }

    fn max_values_per_point(&self) -> usize {
        OnDiskGeoIndex::max_values_per_point(self)
    }

    fn points_of_hash(&self, hash: GeoHash) -> OperationResult<usize> {
        OnDiskGeoIndex::points_of_hash(self, hash)
    }

    fn values_of_hash(&self, hash: GeoHash) -> OperationResult<usize> {
        OnDiskGeoIndex::values_of_hash(self, hash)
    }

    fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: &dyn Fn(&GeoPoint) -> bool,
    ) -> OperationResult<bool> {
        OnDiskGeoIndex::check_values_any(self, idx, |p| check_fn(p))
    }

    fn for_each_matching_value<I, F, M, U>(
        &self,
        items: I,
        check_fn: F,
        on_match: M,
    ) -> OperationResult<()>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffsetType)>,
        F: Fn(&GeoPoint) -> bool,
        M: FnMut(U, bool),
    {
        OnDiskGeoIndex::for_each_matching_value(self, items, check_fn, on_match)
    }

    fn values_count(&self, idx: PointOffsetType) -> usize {
        OnDiskGeoIndex::values_count(self, idx)
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        OnDiskGeoIndex::get_values(self, idx)
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
        OnDiskGeoIndex::points_per_hash(self, |pair| filter(pair))
    }

    fn get_storage_type(&self) -> StorageType {
        StorageType::Mmap {
            on_disk_variant: true,
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        OnDiskGeoIndex::ram_usage_bytes(self)
    }

    fn is_cold(&self) -> bool {
        self.cold
    }

    fn populate(&self) -> OperationResult<()> {
        OnDiskGeoIndex::populate(self)
    }

    fn clear_cache(&self) -> OperationResult<()> {
        OnDiskGeoIndex::clear_cache(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        OnDiskGeoIndex::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        OnDiskGeoIndex::immutable_files(self)
    }

    fn telemetry_index_type(&self) -> &'static str {
        "mmap_geo"
    }
}
