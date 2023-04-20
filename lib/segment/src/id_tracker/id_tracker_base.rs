use bitvec::prelude::BitSlice;
use rand::Rng;

use crate::common::Flusher;
use crate::entry::entry_point::OperationResult;
use crate::types::{PointIdType, PointOffsetType, SeqNumberType};

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
    fn iter_ids_exluding<'a>(
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
        deleted_vec_bitslice: Option<&'a BitSlice>,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let total = self.total_point_count() as PointOffsetType;
        let mut rng = rand::thread_rng();
        Box::new(
            (0..total)
                .map(move |_| rng.gen_range(0..total))
                .filter(move |x| {
                    // Check for deleted vector first, as that is more likely
                    !deleted_vec_bitslice
                        .and_then(|d| d.get(*x as usize).as_deref().copied())
                        .unwrap_or(false)
                    // Also check point deletion for integrity
                    && !self.is_deleted_point(*x)
                }),
        )
    }
}

pub type IdTrackerSS = dyn IdTracker + Sync + Send;
