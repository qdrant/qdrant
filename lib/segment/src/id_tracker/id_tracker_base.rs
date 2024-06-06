use std::path::Path;

use bitvec::prelude::BitSlice;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::id_tracker::immutable_id_tracker::ImmutableIdTracker;
use crate::id_tracker::simple_id_tracker::SimpleIdTracker;
use crate::types::{PointIdType, SeqNumberType};

/// Sampling randomness seed
///
/// Using seeded randomness so search results don't show randomness or 'inconsistencies' which
/// would otherwise be introduced by HNSW/ID tracker point sampling.
const SEED: u64 = 0b1011000011011110001110010101001010001011001101001010010001111010;

/// Trait for point ids tracker.
///
/// This tracker is used to convert external (i.e. user-facing) point id into internal point id
/// as well as for keeping track on point version
/// Internal ids are useful for contiguous-ness
pub trait IdTracker {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType>;

    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()>;

    /// Returns internal ID of the point, which is used inside this segment
    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType>;

    /// Return external ID for internal point, defined by user
    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType>;

    /// Set mapping
    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()>;

    /// Drop mapping
    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()>;

    /// Iterate over all external IDs
    ///
    /// Count should match `available_point_count`.
    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_>;

    /// Iterate over all internal IDs
    ///
    /// Count should match `total_point_count`.
    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_>;

    /// Iterate starting from a given ID
    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_>;

    /// Iterate over internal IDs (offsets)
    ///
    /// - excludes removed points
    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_>;

    /// Iterate over internal IDs (offsets)
    ///
    /// - excludes removed points
    /// - excludes flagged items from `exclude_bitslice`
    fn iter_ids_excluding<'a>(
        &'a self,
        exclude_bitslice: &'a BitSlice,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(self.iter_ids().filter(|point| {
            !exclude_bitslice
                .get(*point as usize)
                .as_deref()
                .copied()
                .unwrap_or(false)
        }))
    }

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

    fn make_immutable(&self, save_path: &Path) -> OperationResult<IdTrackerEnum>;

    fn name(&self) -> &'static str;

    /// Iterator over `n` random IDs which are not deleted
    ///
    /// A [`BitSlice`] of deleted vectors may optionally be given to also consider deleted named
    /// vectors.
    fn sample_ids<'a>(
        &'a self,
        deleted_vector_bitslice: Option<&'a BitSlice>,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        // Use seeded randomness, prevents 'inconsistencies' in search results with sampling
        let mut rng = StdRng::seed_from_u64(SEED);

        let total = self.total_point_count() as PointOffsetType;
        Box::new(
            (0..total)
                .map(move |_| rng.gen_range(0..total))
                .filter(move |x| {
                    // Check for deleted vector first, as that is more likely
                    !deleted_vector_bitslice
                        .and_then(|d| d.get(*x as usize).as_deref().copied())
                        .unwrap_or(false)
                    // Also check point deletion for integrity
                    && !self.is_deleted_point(*x)
                }),
        )
    }

    /// Finds inconsistencies between id mapping and versions storage.
    /// It might happen that point doesn't have version due to un-flushed WAL.
    /// This method makes those points usable again.
    fn cleanup_versions(&mut self) -> OperationResult<()>;
}

pub type IdTrackerSS = dyn IdTracker + Sync + Send;

pub enum IdTrackerEnum {
    MutableIdTracker(SimpleIdTracker),
    ImmutableIdTracker(ImmutableIdTracker),
}

impl IdTracker for IdTrackerEnum {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.internal_version(internal_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => {
                id_tracker.internal_version(internal_id)
            }
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
        }
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.internal_id(external_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.internal_id(external_id),
        }
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.external_id(internal_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.external_id(internal_id),
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
        }
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.drop(external_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.drop(external_id),
        }
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.iter_external(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.iter_external(),
        }
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.iter_internal(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.iter_internal(),
        }
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.iter_from(external_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.iter_from(external_id),
        }
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.iter_ids(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.iter_ids(),
        }
    }

    fn mapping_flusher(&self) -> Flusher {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.mapping_flusher(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.mapping_flusher(),
        }
    }

    fn versions_flusher(&self) -> Flusher {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.versions_flusher(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.versions_flusher(),
        }
    }

    fn total_point_count(&self) -> usize {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.total_point_count(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.total_point_count(),
        }
    }

    fn deleted_point_count(&self) -> usize {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.deleted_point_count(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.deleted_point_count(),
        }
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.deleted_point_bitslice(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.deleted_point_bitslice(),
        }
    }

    fn is_deleted_point(&self, internal_id: PointOffsetType) -> bool {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.is_deleted_point(internal_id),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => {
                id_tracker.is_deleted_point(internal_id)
            }
        }
    }

    fn make_immutable(&self, save_path: &Path) -> OperationResult<IdTrackerEnum> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.make_immutable(save_path),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => {
                Ok(id_tracker.make_immutable(save_path)?)
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.name(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.name(),
        }
    }

    fn cleanup_versions(&mut self) -> OperationResult<()> {
        match self {
            IdTrackerEnum::MutableIdTracker(id_tracker) => id_tracker.cleanup_versions(),
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker.cleanup_versions(),
        }
    }
}
