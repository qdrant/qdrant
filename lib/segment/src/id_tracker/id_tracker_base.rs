use std::fmt;
use std::path::PathBuf;

use bitvec::prelude::BitSlice;
use common::ext::BitSliceExt as _;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::in_memory_id_tracker::InMemoryIdTracker;
use super::mutable_id_tracker::MutableIdTracker;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::ImmutableIdTracker;
#[cfg(feature = "rocksdb")]
use crate::id_tracker::simple_id_tracker::SimpleIdTracker;
use crate::types::{PointIdType, SeqNumberType};

/// Sampling randomness seed
///
/// Using seeded randomness so search results don't show randomness or 'inconsistencies' which
/// would otherwise be introduced by HNSW/ID tracker point sampling.
const SEED: u64 = 0b1011000011011110001110010101001010001011001101001010010001111010;

/// This version should be assigned to a point when it is deleted.
/// It does not mean a point with this version is always deleted.
pub const DELETED_POINT_VERSION: SeqNumberType = 0;

/// Trait for point ids tracker.
///
/// This tracker is used to convert external (i.e. user-facing) point id into internal point id
/// as well as for keeping track on point version
/// Internal ids are useful for contiguous-ness
pub trait IdTracker: fmt::Debug {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType>;

    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()>;

    /// Returns internal ID of the point, which is used inside this segment
    ///
    /// Excludes soft deleted points.
    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType>;

    /// Return external ID for internal point, defined by user
    ///
    /// Excludes soft deleted points.
    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType>;

    /// Set mapping
    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()>;

    /// Drop mapping
    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()>;

    /// Same as `drop`, but by internal ID
    /// If mapping doesn't exist, still removes( unsets ) version.
    fn drop_internal(&mut self, internal_id: PointOffsetType) -> OperationResult<()>;

    /// Iterate over all external IDs
    ///
    /// Count should match `available_point_count`, excludes soft deleted points.
    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_>;

    /// Iterate over internal IDs (offsets)
    ///
    /// Count should match `total_point_count`, excludes soft deleted points.
    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_>;

    /// Iterate over internal IDs (offsets)
    ///
    /// - excludes soft deleted points
    /// - excludes flagged items from `exclude_bitslice`
    fn iter_internal_excluding<'a>(
        &'a self,
        exclude_bitslice: &'a BitSlice,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        Box::new(
            self.iter_internal()
                .filter(|point| !exclude_bitslice.get_bit(*point as usize).unwrap_or(false)),
        )
    }

    /// Iterate starting from a given ID
    ///
    /// Excludes soft deleted points.
    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_>;

    /// Iterate over internal IDs in a random order
    ///
    /// Excludes soft deleted points.
    fn iter_random(&self) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_>;

    /// Flush id mapping to disk
    fn mapping_flusher(&self) -> Flusher;

    /// Flush points versions to disk
    fn versions_flusher(&self) -> Flusher;

    /// Number of total points
    ///
    /// - includes soft deleted points
    fn total_point_count(&self) -> usize;

    /// Number of available points
    ///
    /// - excludes soft deleted points
    fn available_point_count(&self) -> usize {
        self.total_point_count() - self.deleted_point_count()
    }

    /// Number of deleted points
    fn deleted_point_count(&self) -> usize;

    /// Get [`BitSlice`] representation for deleted points with deletion flags
    ///
    /// The size of this slice is not guaranteed. It may be smaller/larger than the number of
    /// vectors in this segment.
    fn deleted_point_bitslice(&self) -> &BitSlice;

    /// Check whether the given point is soft deleted
    fn is_deleted_point(&self, internal_id: PointOffsetType) -> bool;

    fn name(&self) -> &'static str;

    /// Iterator over `n` random IDs which are not deleted
    ///
    /// A [`BitSlice`] of deleted vectors may optionally be given to also consider deleted named
    /// vectors.
    fn sample_ids<'a>(
        &'a self,
        deleted_vector_bitslice: Option<&'a BitSlice>,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        // Use seeded randomness, prevents 'inconsistencies' in search results with sampling
        let mut rng = StdRng::seed_from_u64(SEED);

        let total = self.total_point_count() as PointOffsetType;
        Box::new(
            (0..total)
                .map(move |_| rng.random_range(0..total))
                .filter(move |&x| {
                    // Check for deleted vector first, as that is more likely
                    !deleted_vector_bitslice
                        .and_then(|d| d.get_bit(x as usize))
                        .unwrap_or(false)
                    // Also check point deletion for integrity
                    && !self.is_deleted_point(x)
                }),
        )
    }

    /// Iterate over all stored internal versions, even if they were deleted
    /// Required for cleanup on segment open
    fn iter_internal_versions(
        &self,
    ) -> Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>;

    /// Finds inconsistencies between id mapping and versions storage.
    /// It might happen that point doesn't have version due to un-flushed WAL.
    /// This method makes those points usable again.
    ///
    /// Returns a list of internal ids, that have non-zero versions, but are missing in the id mapping.
    /// Those points should be removed from all other parts of the segment.
    fn fix_inconsistencies(&mut self) -> OperationResult<Vec<PointOffsetType>> {
        // Points with mapping, but no version.
        // Can happen if insertion didn't complete.
        // We need to remove mapping and assume the point is going to be re-inserted by WAL.
        let mut to_remove = Vec::new();

        // Points with version, but no mapping.
        // Can happen if point was deleted, but version (and likely the storage) wasn't cleaned up.
        // We return those points to the caller to clean up the storage.
        let mut to_return = Vec::new();

        for (internal_id, version) in self.iter_internal_versions() {
            if version != DELETED_POINT_VERSION && self.external_id(internal_id).is_none() {
                to_return.push(internal_id);
            }
        }

        for internal_id in self.iter_internal() {
            if self.internal_version(internal_id).is_none() {
                if let Some(external_id) = self.external_id(internal_id) {
                    to_remove.push(external_id);
                    to_return.push(internal_id);
                } else {
                    debug_assert!(false, "internal id {internal_id} has no external id");
                }
            }
        }
        for external_id in to_remove {
            self.drop(external_id)?;
            #[cfg(debug_assertions)]
            {
                log::debug!("dropped version for point {external_id} without version");
            }
        }
        Ok(to_return)
    }

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new()
    }
}

pub type IdTrackerSS = dyn IdTracker + Sync + Send;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum IdTrackerEnum {
    MutableIdTracker(MutableIdTracker),
    ImmutableIdTracker(ImmutableIdTracker),
    InMemoryIdTracker(InMemoryIdTracker),

    // Deprecated since Qdrant 1.14
    #[cfg(feature = "rocksdb")]
    RocksDbIdTracker(SimpleIdTracker),
}

impl IdTracker for IdTrackerEnum {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.internal_version(internal_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => {
                id_tracker.internal_version(internal_id)
            }
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => {
                id_tracker.internal_version(internal_id)
            }
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.internal_version(internal_id),
        }
    }

    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => {
                id_tracker.set_internal_version(internal_id, version)
            }
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => {
                id_tracker.set_internal_version(internal_id, version)
            }
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => {
                id_tracker.set_internal_version(internal_id, version)
            }
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => {
                id_tracker.set_internal_version(internal_id, version)
            }
        }
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.internal_id(external_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.internal_id(external_id),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.internal_id(external_id),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.internal_id(external_id),
        }
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.external_id(internal_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.external_id(internal_id),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.external_id(internal_id),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.external_id(internal_id),
        }
    }

    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => {
                id_tracker.set_link(external_id, internal_id)
            }
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => {
                id_tracker.set_link(external_id, internal_id)
            }
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => {
                id_tracker.set_link(external_id, internal_id)
            }
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => {
                id_tracker.set_link(external_id, internal_id)
            }
        }
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.drop(external_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.drop(external_id),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.drop(external_id),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.drop(external_id),
        }
    }

    fn drop_internal(&mut self, internal_id: PointOffsetType) -> OperationResult<()> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.drop_internal(internal_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.drop_internal(internal_id),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.drop_internal(internal_id),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.drop_internal(internal_id),
        }
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.iter_external(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.iter_external(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.iter_external(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.iter_external(),
        }
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.iter_internal(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.iter_internal(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.iter_internal(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.iter_internal(),
        }
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.iter_from(external_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.iter_from(external_id),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.iter_from(external_id),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.iter_from(external_id),
        }
    }

    fn iter_random(&self) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.iter_random(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.iter_random(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.iter_random(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.iter_random(),
        }
    }

    fn mapping_flusher(&self) -> Flusher {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.mapping_flusher(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.mapping_flusher(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.mapping_flusher(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.mapping_flusher(),
        }
    }

    fn versions_flusher(&self) -> Flusher {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.versions_flusher(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.versions_flusher(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.versions_flusher(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.versions_flusher(),
        }
    }

    fn total_point_count(&self) -> usize {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.total_point_count(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.total_point_count(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.total_point_count(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.total_point_count(),
        }
    }

    fn deleted_point_count(&self) -> usize {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.deleted_point_count(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.deleted_point_count(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.deleted_point_count(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.deleted_point_count(),
        }
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.deleted_point_bitslice(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.deleted_point_bitslice(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.deleted_point_bitslice(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.deleted_point_bitslice(),
        }
    }

    fn is_deleted_point(&self, internal_id: PointOffsetType) -> bool {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.is_deleted_point(internal_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => {
                id_tracker.is_deleted_point(internal_id)
            }
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => {
                id_tracker.is_deleted_point(internal_id)
            }
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.is_deleted_point(internal_id),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.name(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.name(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.name(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.name(),
        }
    }

    fn iter_internal_versions(
        &self,
    ) -> Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.iter_internal_versions(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.iter_internal_versions(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.iter_internal_versions(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.iter_internal_versions(),
        }
    }

    fn fix_inconsistencies(&mut self) -> OperationResult<Vec<PointOffsetType>> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.fix_inconsistencies(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.fix_inconsistencies(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.fix_inconsistencies(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.fix_inconsistencies(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.files(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.files(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.files(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.files(),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.immutable_files(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.immutable_files(),
            IdTrackerEnum::InMemoryIdTracker(id_tracker) => id_tracker.immutable_files(),
            #[cfg(feature = "rocksdb")]
            IdTrackerEnum::RocksDbIdTracker(id_tracker) => id_tracker.immutable_files(),
        }
    }
}
