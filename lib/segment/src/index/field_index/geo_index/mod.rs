mod builders;
pub mod immutable_geo_index;
pub mod mmap_geo_index;
pub mod mutable_geo_index;
mod payload_index;
pub mod read_only;
pub mod read_ops;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs};
use mutable_geo_index::InMemoryGeoMapIndex;

pub use self::builders::{GeoMapIndexGridstoreBuilder, GeoMapIndexMmapBuilder};
use self::immutable_geo_index::ImmutableGeoMapIndex;
use self::mmap_geo_index::StoredGeoMapIndex;
use self::mutable_geo_index::MutableGeoMapIndex;
pub use self::read_only::ReadOnlyGeoMapIndex;
pub use self::read_ops::GeoMapIndexRead;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::types::GeoPoint;

/// Max number of sub-regions computed for an input geo query
// TODO discuss value, should it be dynamically computed?
const GEO_QUERY_MAX_REGION: usize = 12;

pub enum GeoMapIndex {
    Mutable(MutableGeoMapIndex),
    Immutable(ImmutableGeoMapIndex),
    Storage(Box<StoredGeoMapIndex<MmapFile>>),
}

impl GeoMapIndex {
    pub fn new_mmap(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let Some(mmap_index) =
            StoredGeoMapIndex::open(&MmapFs, path, effective_is_on_disk, deleted_points)?
        else {
            return Ok(None);
        };

        let index = if effective_is_on_disk {
            GeoMapIndex::Storage(Box::new(mmap_index))
        } else {
            GeoMapIndex::Immutable(ImmutableGeoMapIndex::open_mmap(mmap_index)?)
        };

        Ok(Some(index))
    }

    pub fn new_gridstore(dir: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        Ok(MutableGeoMapIndex::open_gridstore(dir, create_if_missing)?.map(GeoMapIndex::Mutable))
    }

    pub fn builder_mmap(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> GeoMapIndexMmapBuilder {
        GeoMapIndexMmapBuilder {
            path: path.to_owned(),
            in_memory_index: InMemoryGeoMapIndex::new(),
            is_on_disk,
            deleted_points: deleted_points.to_owned(),
        }
    }

    pub fn builder_gridstore(dir: PathBuf) -> GeoMapIndexGridstoreBuilder {
        GeoMapIndexGridstoreBuilder::new(dir)
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            Self::Mutable(_) => IndexMutability::Mutable,
            Self::Immutable(_) => IndexMutability::Immutable,
            Self::Storage(_) => IndexMutability::Immutable,
        }
    }
}

impl GeoMapIndexRead for GeoMapIndex {
    fn points_count(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.points_count(),
            GeoMapIndex::Immutable(index) => index.points_count(),
            GeoMapIndex::Storage(index) => index.points_count(),
        }
    }

    fn points_values_count(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.points_values_count(),
            GeoMapIndex::Immutable(index) => index.points_values_count(),
            GeoMapIndex::Storage(index) => index.points_values_count(),
        }
    }

    fn max_values_per_point(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.max_values_per_point(),
            GeoMapIndex::Immutable(index) => index.max_values_per_point(),
            GeoMapIndex::Storage(index) => index.max_values_per_point(),
        }
    }

    fn points_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        match self {
            GeoMapIndex::Mutable(index) => GeoMapIndexRead::points_of_hash(index, hash, hw_counter),
            GeoMapIndex::Immutable(index) => {
                GeoMapIndexRead::points_of_hash(index, hash, hw_counter)
            }
            GeoMapIndex::Storage(index) => index.points_of_hash(hash, hw_counter),
        }
    }

    fn values_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        match self {
            GeoMapIndex::Mutable(index) => GeoMapIndexRead::values_of_hash(index, hash, hw_counter),
            GeoMapIndex::Immutable(index) => {
                GeoMapIndexRead::values_of_hash(index, hash, hw_counter)
            }
            GeoMapIndex::Storage(index) => index.values_of_hash(hash, hw_counter),
        }
    }

    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: &dyn Fn(&GeoPoint) -> bool,
    ) -> bool {
        match self {
            GeoMapIndex::Mutable(index) => {
                GeoMapIndexRead::check_values_any(index, idx, hw_counter, check_fn)
            }
            GeoMapIndex::Immutable(index) => {
                GeoMapIndexRead::check_values_any(index, idx, hw_counter, check_fn)
            }
            GeoMapIndex::Storage(index) => index.check_values_any(idx, hw_counter, check_fn),
        }
    }

    fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => GeoMapIndexRead::values_count(index, idx),
            GeoMapIndex::Immutable(index) => GeoMapIndexRead::values_count(index, idx),
            GeoMapIndex::Storage(index) => index.values_count(idx),
        }
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        match self {
            GeoMapIndex::Mutable(index) => GeoMapIndexRead::get_values(index, idx),
            GeoMapIndex::Immutable(index) => GeoMapIndexRead::get_values(index, idx),
            GeoMapIndex::Storage(index) => GeoMapIndexRead::get_values(index.as_ref(), idx),
        }
    }

    fn iterator(
        &self,
        values: Vec<GeoHash>,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        match self {
            GeoMapIndex::Mutable(index) => GeoMapIndexRead::iterator(index, values),
            GeoMapIndex::Immutable(index) => GeoMapIndexRead::iterator(index, values),
            GeoMapIndex::Storage(index) => GeoMapIndexRead::iterator(index.as_ref(), values),
        }
    }

    fn points_per_hash_filtered(
        &self,
        filter: &dyn Fn(&(GeoHash, usize)) -> bool,
    ) -> OperationResult<Vec<(GeoHash, usize)>> {
        match self {
            GeoMapIndex::Mutable(index) => index.points_per_hash_filtered(filter),
            GeoMapIndex::Immutable(index) => index.points_per_hash_filtered(filter),
            GeoMapIndex::Storage(index) => index.points_per_hash_filtered(filter),
        }
    }

    fn get_storage_type(&self) -> StorageType {
        match self {
            GeoMapIndex::Mutable(index) => GeoMapIndexRead::get_storage_type(index),
            GeoMapIndex::Immutable(index) => GeoMapIndexRead::get_storage_type(index),
            GeoMapIndex::Storage(index) => GeoMapIndexRead::get_storage_type(index.as_ref()),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => GeoMapIndexRead::ram_usage_bytes(index),
            GeoMapIndex::Immutable(index) => GeoMapIndexRead::ram_usage_bytes(index),
            GeoMapIndex::Storage(index) => GeoMapIndexRead::ram_usage_bytes(index.as_ref()),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            GeoMapIndex::Mutable(_) => false,
            GeoMapIndex::Immutable(_) => false,
            GeoMapIndex::Storage(index) => index.is_on_disk(),
        }
    }

    fn populate(&self) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(_) | GeoMapIndex::Immutable(_) => Ok(()),
            GeoMapIndex::Storage(index) => index.populate(),
        }
    }

    fn clear_cache(&self) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => GeoMapIndexRead::clear_cache(index),
            GeoMapIndex::Immutable(index) => GeoMapIndexRead::clear_cache(index),
            GeoMapIndex::Storage(index) => index.clear_cache(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            GeoMapIndex::Mutable(index) => GeoMapIndexRead::files(index),
            GeoMapIndex::Immutable(index) => GeoMapIndexRead::files(index),
            GeoMapIndex::Storage(index) => GeoMapIndexRead::files(index.as_ref()),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            GeoMapIndex::Mutable(_) => vec![],
            GeoMapIndex::Immutable(index) => GeoMapIndexRead::immutable_files(index),
            GeoMapIndex::Storage(index) => GeoMapIndexRead::immutable_files(index.as_ref()),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            GeoMapIndex::Mutable(_) => "mutable_geo",
            GeoMapIndex::Immutable(_) => "immutable_geo",
            GeoMapIndex::Storage(_) => "mmap_geo",
        }
    }
}
