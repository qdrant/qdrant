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

    /// Iterate over all external ids
    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_>;

    /// Iterate over all internal ids
    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_>;

    /// Iterate starting from a given ID
    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_>;

    /// Number of unique records in the segment
    fn points_count(&self) -> usize;

    /// Iterate over all non-removed internal ids (offsets)
    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_>;

    /// Total number of internal ids (offsets), including removed ones
    fn internal_size(&self) -> usize;

    /// Flush id mapping to disk
    fn mapping_flusher(&self) -> Flusher;

    /// Flush points versions to disk
    fn versions_flusher(&self) -> Flusher;

    fn deleted_bitslice(&self) -> &BitSlice;

    fn is_deleted(&self, internal_id: PointOffsetType) -> bool;

    // Number of deleted points
    fn deleted_count(&self) -> usize {
        self.internal_size() - self.points_count()
    }

    /// Iterator over `n` random ids which are not deleted
    fn sample_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let total = self.internal_size() as PointOffsetType;
        let mut rng = rand::thread_rng();
        Box::new(
            (0..total)
                .map(move |_| rng.gen_range(0..total))
                .filter(move |x| !self.is_deleted(*x)),
        )
    }
}

pub type IdTrackerSS = dyn IdTracker + Sync + Send;
