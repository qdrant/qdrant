mod builders;
pub mod immutable_geo_index;
pub mod mutable_geo_index;
pub mod on_disk_geo_index;
mod payload_index;
pub mod read_only;
pub mod read_ops;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs, Populate};
use mutable_geo_index::InMemoryGeoIndex;

pub use self::builders::{GeoIndexGridstoreBuilder, GeoIndexMmapBuilder};
use self::immutable_geo_index::ImmutableGeoIndex;
use self::mutable_geo_index::MutableGeoIndex;
use self::on_disk_geo_index::OnDiskGeoIndex;
pub use self::read_only::ReadOnlyGeoIndex;
pub use self::read_ops::{GeoConditionChecker, GeoIndexRead};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::types::GeoPoint;

/// Max number of sub-regions computed for an input geo query
// TODO discuss value, should it be dynamically computed?
const GEO_QUERY_MAX_REGION: usize = 12;

pub enum GeoIndex {
    /// Loads into RAM from mutable Gridstore storage format.
    Mutable(MutableGeoIndex),
    /// Loads into RAM in immutable format.
    Immutable(ImmutableGeoIndex),
    /// Directly reads from mmap in immutable format.
    OnDisk(OnDiskGeoIndex<MmapFile>),
}

impl GeoIndex {
    pub fn new_immutable(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let populate = Populate::from(!effective_is_on_disk);
        let Some(on_disk_index) = OnDiskGeoIndex::open(&MmapFs, path, populate, deleted_points)?
        else {
            return Ok(None);
        };

        let index = if effective_is_on_disk {
            GeoIndex::OnDisk(on_disk_index)
        } else {
            GeoIndex::Immutable(ImmutableGeoIndex::load_from_on_disk(on_disk_index)?)
        };

        Ok(Some(index))
    }

    pub fn new_mutable(dir: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        Ok(MutableGeoIndex::open(dir, create_if_missing)?.map(GeoIndex::Mutable))
    }

    pub fn builder_mmap(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> GeoIndexMmapBuilder {
        GeoIndexMmapBuilder {
            path: path.to_owned(),
            in_memory_index: InMemoryGeoIndex::new(),
            is_on_disk,
            deleted_points: deleted_points.to_owned(),
        }
    }

    pub fn builder_gridstore(dir: PathBuf) -> GeoIndexGridstoreBuilder {
        GeoIndexGridstoreBuilder::new(dir)
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            Self::Mutable(_) => IndexMutability::Mutable,
            Self::Immutable(_) => IndexMutability::Immutable,
            Self::OnDisk(_) => IndexMutability::Immutable,
        }
    }
}

impl GeoIndexRead for GeoIndex {
    fn points_count(&self) -> usize {
        match self {
            GeoIndex::Mutable(index) => index.points_count(),
            GeoIndex::Immutable(index) => index.points_count(),
            GeoIndex::OnDisk(index) => index.points_count(),
        }
    }

    fn points_values_count(&self) -> usize {
        match self {
            GeoIndex::Mutable(index) => index.points_values_count(),
            GeoIndex::Immutable(index) => index.points_values_count(),
            GeoIndex::OnDisk(index) => index.points_values_count(),
        }
    }

    fn max_values_per_point(&self) -> usize {
        match self {
            GeoIndex::Mutable(index) => index.max_values_per_point(),
            GeoIndex::Immutable(index) => index.max_values_per_point(),
            GeoIndex::OnDisk(index) => index.max_values_per_point(),
        }
    }

    fn points_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        match self {
            GeoIndex::Mutable(index) => GeoIndexRead::points_of_hash(index, hash, hw_counter),
            GeoIndex::Immutable(index) => GeoIndexRead::points_of_hash(index, hash, hw_counter),
            GeoIndex::OnDisk(index) => index.points_of_hash(hash, hw_counter),
        }
    }

    fn values_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        match self {
            GeoIndex::Mutable(index) => GeoIndexRead::values_of_hash(index, hash, hw_counter),
            GeoIndex::Immutable(index) => GeoIndexRead::values_of_hash(index, hash, hw_counter),
            GeoIndex::OnDisk(index) => index.values_of_hash(hash, hw_counter),
        }
    }

    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: &dyn Fn(&GeoPoint) -> bool,
    ) -> OperationResult<bool> {
        match self {
            GeoIndex::Mutable(index) => {
                GeoIndexRead::check_values_any(index, idx, hw_counter, check_fn)
            }
            GeoIndex::Immutable(index) => {
                GeoIndexRead::check_values_any(index, idx, hw_counter, check_fn)
            }
            GeoIndex::OnDisk(index) => index.check_values_any(idx, hw_counter, check_fn),
        }
    }

    fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            GeoIndex::Mutable(index) => GeoIndexRead::values_count(index, idx),
            GeoIndex::Immutable(index) => GeoIndexRead::values_count(index, idx),
            GeoIndex::OnDisk(index) => index.values_count(idx),
        }
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        match self {
            GeoIndex::Mutable(index) => GeoIndexRead::get_values(index, idx),
            GeoIndex::Immutable(index) => GeoIndexRead::get_values(index, idx),
            GeoIndex::OnDisk(index) => GeoIndexRead::get_values(index, idx),
        }
    }

    fn iterator(
        &self,
        values: Vec<GeoHash>,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        match self {
            GeoIndex::Mutable(index) => GeoIndexRead::iterator(index, values),
            GeoIndex::Immutable(index) => GeoIndexRead::iterator(index, values),
            GeoIndex::OnDisk(index) => GeoIndexRead::iterator(index, values),
        }
    }

    fn points_per_hash_filtered(
        &self,
        filter: &dyn Fn(&(GeoHash, usize)) -> bool,
    ) -> OperationResult<Vec<(GeoHash, usize)>> {
        match self {
            GeoIndex::Mutable(index) => index.points_per_hash_filtered(filter),
            GeoIndex::Immutable(index) => index.points_per_hash_filtered(filter),
            GeoIndex::OnDisk(index) => index.points_per_hash_filtered(filter),
        }
    }

    fn get_storage_type(&self) -> StorageType {
        match self {
            GeoIndex::Mutable(index) => GeoIndexRead::get_storage_type(index),
            GeoIndex::Immutable(index) => GeoIndexRead::get_storage_type(index),
            GeoIndex::OnDisk(index) => GeoIndexRead::get_storage_type(index),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            GeoIndex::Mutable(index) => GeoIndexRead::ram_usage_bytes(index),
            GeoIndex::Immutable(index) => GeoIndexRead::ram_usage_bytes(index),
            GeoIndex::OnDisk(index) => GeoIndexRead::ram_usage_bytes(index),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            GeoIndex::Mutable(_) => false,
            GeoIndex::Immutable(_) => false,
            GeoIndex::OnDisk(_) => true,
        }
    }

    fn populate(&self) -> OperationResult<()> {
        match self {
            GeoIndex::Mutable(_) | GeoIndex::Immutable(_) => Ok(()),
            GeoIndex::OnDisk(index) => index.populate(),
        }
    }

    fn clear_cache(&self) -> OperationResult<()> {
        match self {
            GeoIndex::Mutable(index) => GeoIndexRead::clear_cache(index),
            GeoIndex::Immutable(index) => GeoIndexRead::clear_cache(index),
            GeoIndex::OnDisk(index) => index.clear_cache(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            GeoIndex::Mutable(index) => GeoIndexRead::files(index),
            GeoIndex::Immutable(index) => GeoIndexRead::files(index),
            GeoIndex::OnDisk(index) => GeoIndexRead::files(index),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            GeoIndex::Mutable(_) => vec![],
            GeoIndex::Immutable(index) => GeoIndexRead::immutable_files(index),
            GeoIndex::OnDisk(index) => GeoIndexRead::immutable_files(index),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            GeoIndex::Mutable(_) => "mutable_geo",
            GeoIndex::Immutable(_) => "immutable_geo",
            GeoIndex::OnDisk(_) => "mmap_geo",
        }
    }
}
