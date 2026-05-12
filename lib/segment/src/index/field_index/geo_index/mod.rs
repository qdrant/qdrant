mod builders;
pub mod immutable_geo_index;
pub mod mmap_geo_index;
pub mod mutable_geo_index;
mod payload_index;
#[cfg(test)]
mod tests;

use std::cmp::{max, min};
use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
use itertools::{Either, Itertools};
use mutable_geo_index::InMemoryGeoMapIndex;

pub use self::builders::{GeoMapIndexGridstoreBuilder, GeoMapIndexMmapBuilder};
use self::immutable_geo_index::ImmutableGeoMapIndex;
use self::mmap_geo_index::StoredGeoMapIndex;
use self::mutable_geo_index::MutableGeoMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::CardinalityEstimation;
use crate::index::field_index::geo_hash::{GeoHash, common_hash_prefix};
use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::telemetry::PayloadIndexTelemetry;
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

        let Some(mmap_index) = StoredGeoMapIndex::open(path, effective_is_on_disk, deleted_points)?
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

    pub(crate) fn points_count(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.points_count(),
            GeoMapIndex::Immutable(index) => index.points_count(),
            GeoMapIndex::Storage(index) => index.points_count(),
        }
    }

    pub(crate) fn points_values_count(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.points_values_count(),
            GeoMapIndex::Immutable(index) => index.points_values_count(),
            GeoMapIndex::Storage(index) => index.points_values_count(),
        }
    }

    /// Maximum number of values per point
    ///
    /// # Warning
    ///
    /// Zero if the index is empty.
    pub(crate) fn max_values_per_point(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.max_values_per_point(),
            GeoMapIndex::Immutable(index) => index.max_values_per_point(),
            GeoMapIndex::Storage(index) => index.max_values_per_point(),
        }
    }

    pub(crate) fn points_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        Ok(match self {
            GeoMapIndex::Mutable(index) => index.points_of_hash(hash),
            GeoMapIndex::Immutable(index) => index.points_of_hash(hash),
            GeoMapIndex::Storage(index) => index.points_of_hash(hash, hw_counter)?,
        })
    }

    pub(crate) fn values_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        Ok(match self {
            GeoMapIndex::Mutable(index) => index.values_of_hash(hash),
            GeoMapIndex::Immutable(index) => index.values_of_hash(hash),
            GeoMapIndex::Storage(index) => index.values_of_hash(hash, hw_counter)?,
        })
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&GeoPoint) -> bool,
    ) -> bool {
        match self {
            GeoMapIndex::Mutable(index) => index.check_values_any(idx, check_fn),
            GeoMapIndex::Immutable(index) => index.check_values_any(idx, check_fn),
            GeoMapIndex::Storage(index) => index.check_values_any(idx, hw_counter, check_fn),
        }
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.values_count(idx),
            GeoMapIndex::Immutable(index) => index.values_count(idx),
            GeoMapIndex::Storage(index) => index.values_count(idx),
        }
    }

    pub fn get_values(
        &self,
        idx: PointOffsetType,
    ) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        match self {
            GeoMapIndex::Mutable(index) => index.get_values(idx).map(|x| Box::new(x.cloned()) as _),
            GeoMapIndex::Immutable(index) => {
                index.get_values(idx).map(|x| Box::new(x.cloned()) as _)
            }
            GeoMapIndex::Storage(index) => index.get_values(idx).map(|x| Box::new(x) as _),
        }
    }

    pub fn match_cardinality(
        &self,
        values: &[GeoHash],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        let max_values_per_point = self.max_values_per_point();
        if max_values_per_point == 0 {
            return Ok(CardinalityEstimation::exact(0));
        }

        let Some(common_hash) = common_hash_prefix(values) else {
            return Ok(CardinalityEstimation::exact(0));
        };

        let total_points = self.points_of_hash(common_hash, hw_counter)?;
        let total_values = self.values_of_hash(common_hash, hw_counter)?;

        let (sum, maximum_per_hash) = values
            .iter()
            .map(|&region| self.points_of_hash(region, hw_counter))
            .try_fold((0, 0), |(sum, maximum), count| {
                let count = count?;
                OperationResult::Ok((sum + count, max(maximum, count)))
            })?;

        // Assume all selected points have `max_values_per_point` value hits.
        // Therefore number of points can't be less than `total_hits / max_values_per_point`
        // Note: max_values_per_point is never zero here because we check it above
        let min_hits_by_value_groups = sum / max_values_per_point;

        // Assume that we have selected all possible duplications of the points
        let point_duplications = total_values - total_points;
        let possible_non_duplicated = sum.saturating_sub(point_duplications);

        let estimation_min = max(
            max(min_hits_by_value_groups, possible_non_duplicated),
            maximum_per_hash,
        );
        let estimation_max = min(sum, total_points);

        // estimate_multi_value_selection_cardinality might overflow at some corner cases
        // so it is better to limit its value with min and max
        let estimation_exp =
            estimate_multi_value_selection_cardinality(total_points, total_values, sum).round()
                as usize;

        Ok(CardinalityEstimation {
            primary_clauses: vec![],
            min: estimation_min,
            exp: min(estimation_max, max(estimation_min, estimation_exp)),
            max: estimation_max,
        })
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.points_count(),
            points_values_count: self.points_values_count(),
            histogram_bucket_size: None,
            index_type: match self {
                GeoMapIndex::Mutable(_) => "mutable_geo",
                GeoMapIndex::Immutable(_) => "immutable_geo",
                GeoMapIndex::Storage(_) => "mmap_geo",
            },
        }
    }

    pub(crate) fn iterator(
        &self,
        values: Vec<GeoHash>,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + '_> {
        match self {
            GeoMapIndex::Mutable(index) => Ok(Either::Left(Either::Right(
                values
                    .into_iter()
                    .flat_map(|top_geo_hash| index.stored_sub_regions(top_geo_hash))
                    .unique(),
            ))),
            GeoMapIndex::Immutable(index) => Ok(Either::Left(Either::Left(
                values
                    .into_iter()
                    .flat_map(|top_geo_hash| index.stored_sub_regions(top_geo_hash))
                    .unique(),
            ))),
            GeoMapIndex::Storage(index) => {
                let result = index.all_points(values)?;
                Ok(Either::Right(result.into_iter()))
            }
        }
    }

    /// Get iterator over smallest geo-hash regions larger than `threshold` points
    pub(crate) fn large_hashes(
        &self,
        threshold: usize,
    ) -> OperationResult<impl Iterator<Item = (GeoHash, usize)> + '_> {
        let filter_condition =
            |(hash, size): &(GeoHash, usize)| *size > threshold && !hash.is_empty();
        let mut large_regions = match self {
            GeoMapIndex::Mutable(index) => index
                .points_per_hash()
                .filter(filter_condition)
                .collect_vec(),
            GeoMapIndex::Immutable(index) => index
                .points_per_hash()
                .filter(filter_condition)
                .collect_vec(),
            GeoMapIndex::Storage(index) => index.points_per_hash(filter_condition)?,
        };

        // smallest regions first
        large_regions.sort_by(|a, b| b.cmp(a));

        let mut edge_region = vec![];

        let mut current_region = GeoHash::default();

        for (region, size) in large_regions {
            if !current_region.starts_with(region) {
                current_region = region;
                edge_region.push((region, size));
            }
        }

        Ok(edge_region.into_iter())
    }

    pub fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx) == 0
    }

    /// Approximate RAM usage in bytes for in-memory structures.
    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.ram_usage_bytes(),
            GeoMapIndex::Immutable(index) => index.ram_usage_bytes(),
            GeoMapIndex::Storage(index) => index.ram_usage_bytes(),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            GeoMapIndex::Mutable(_) => false,
            GeoMapIndex::Immutable(_) => false,
            GeoMapIndex::Storage(index) => index.is_on_disk(),
        }
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(_) => {}
            GeoMapIndex::Immutable(_) => {}
            GeoMapIndex::Storage(index) => index.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.clear_cache(),
            GeoMapIndex::Immutable(index) => index.clear_cache(),
            GeoMapIndex::Storage(index) => index.clear_cache(),
        }
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            Self::Mutable(_) => IndexMutability::Mutable,
            Self::Immutable(_) => IndexMutability::Immutable,
            Self::Storage(_) => IndexMutability::Immutable,
        }
    }

    pub fn get_storage_type(&self) -> StorageType {
        match self {
            Self::Mutable(index) => index.storage_type(),
            Self::Immutable(index) => index.storage_type(),
            Self::Storage(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
        }
    }
}
