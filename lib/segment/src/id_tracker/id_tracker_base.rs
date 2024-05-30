use bitvec::prelude::BitSlice;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
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
    SimpleIdTracker(SimpleIdTracker),
    ImmutableIdTracker(SimpleIdTracker),
}

impl IdTracker for IdTrackerEnum {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.internal_version(internal_id),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.internal_version(internal_id),
        }
    }

    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => {
                simple.set_internal_version(internal_id, version)
            }
            IdTrackerEnum::ImmutableIdTracker(immutable) => {
                immutable.set_internal_version(internal_id, version)
            }
        }
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.internal_id(external_id),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.internal_id(external_id),
        }
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.external_id(internal_id),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.external_id(internal_id),
        }
    }

    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.set_link(external_id, internal_id),
            IdTrackerEnum::ImmutableIdTracker(immutable) => {
                immutable.set_link(external_id, internal_id)
            }
        }
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.drop(external_id),
            IdTrackerEnum::ImmutableIdTracker(internal) => internal.drop(external_id),
        }
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.iter_external(),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.iter_external(),
        }
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.iter_internal(),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.iter_internal(),
        }
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.iter_from(external_id),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.iter_from(external_id),
        }
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.iter_ids(),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.iter_ids(),
        }
    }

    fn mapping_flusher(&self) -> Flusher {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.mapping_flusher(),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.mapping_flusher(),
        }
    }

    fn versions_flusher(&self) -> Flusher {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.versions_flusher(),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.versions_flusher(),
        }
    }

    fn total_point_count(&self) -> usize {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.total_point_count(),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.total_point_count(),
        }
    }

    fn deleted_point_count(&self) -> usize {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.deleted_point_count(),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.deleted_point_count(),
        }
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.deleted_point_bitslice(),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.deleted_point_bitslice(),
        }
    }

    fn is_deleted_point(&self, internal_id: PointOffsetType) -> bool {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.is_deleted_point(internal_id),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.is_deleted_point(internal_id),
        }
    }

    fn cleanup_versions(&mut self) -> OperationResult<()> {
        match self {
            IdTrackerEnum::SimpleIdTracker(simple) => simple.cleanup_versions(),
            IdTrackerEnum::ImmutableIdTracker(immutable) => immutable.cleanup_versions(),
        }
    }
}
