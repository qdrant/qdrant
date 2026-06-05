use std::fmt;
use std::path::PathBuf;

use common::bitvec::{BitSlice, BitSliceExt as _};
use common::types::{DeferredBehavior, PointOffsetType};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::point_mappings_ref::PointMappingsRefEnum;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
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
pub trait IdTracker: IdTrackerRead + fmt::Debug {
    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()>;

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

    /// Flush id mapping to disk
    fn mapping_flusher(&self) -> Flusher;

    /// Flush points versions to disk
    fn versions_flusher(&self) -> Flusher;

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

        for internal_id in self.point_mappings().iter_internal() {
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
            log::debug!("dropped mapping for point {external_id} without version");
        }
        Ok(to_return)
    }

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new()
    }

    /// Hint to the OS that the page cache backing this tracker's on-disk files
    /// can be reclaimed.
    ///
    /// Used after building/optimizing a segment to avoid leaving cold id-tracker
    /// pages resident in the page cache. The default implementation is a no-op
    /// for fully in-memory trackers without backing files.
    fn clear_cache(&self) -> OperationResult<()> {
        Ok(())
    }

    /// Clear the page cache for this tracker's on-disk files, but only if the
    /// tracker data is expected to live on disk rather than in RAM.
    ///
    /// The id tracker currently always loads its data into RAM and has no
    /// on-disk mode, so for now this unconditionally clears the cache. Once an
    /// on-disk mode is added, this should be gated on that configuration,
    /// mirroring the vector and payload storages.
    fn clear_cache_if_on_disk(&self) -> OperationResult<()> {
        // TODO: gate on an on-disk flag once the id tracker supports it.
        self.clear_cache()
    }
}

pub trait IdTrackerRead {
    /// Get a reference to the point mappings, which provides iteration methods.
    fn point_mappings(&self) -> PointMappingsRefEnum<'_>;

    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType>;

    /// Returns the internal ID of the point under explicit deferred
    /// semantics — see [`PointMappings::internal_id_with_behavior`].
    ///
    /// Excludes soft deleted points. Trackers that never carry deferred
    /// mutations (immutable / compressed) ignore the behavior argument.
    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        deferred_behavior: DeferredBehavior,
    ) -> Option<PointOffsetType>;

    /// Return external ID for internal point, defined by user
    ///
    /// Excludes soft deleted points.
    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType>;

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

    /// Internal-id threshold above which points are hidden from reads.
    ///
    /// Only appendable trackers can carry a non-`None` value.
    fn deferred_internal_id(&self) -> Option<PointOffsetType> {
        None
    }

    /// Number of soft-deleted points at or above the deferred threshold.
    fn deferred_deleted_count(&self) -> usize {
        0
    }

    /// Translate external point ids into two parallel vectors of `(ids,
    /// offsets)` in a single pass.
    ///
    /// Applies deferred filtering according to `deferred_behavior` inline
    /// (no separate `point_is_deferred` lookup), missing points will be ignored
    ///
    /// The parallel-vector return shape lets downstream batched fetchers
    /// consume `&offsets` directly.
    ///
    /// Centralising this here keeps the deferred threshold from leaking out
    /// of the id tracker — callers go through this entry point instead of
    /// reading `deferred_internal_id()` themselves.
    fn resolve_external_ids(
        &self,
        point_ids: &[PointIdType],
        deferred_behavior: DeferredBehavior,
    ) -> (Vec<PointIdType>, Vec<PointOffsetType>) {
        // For VisibleOnly, the deferred-aware lookup returns the active head
        // only — there's no need for the post-lookup cutoff filter the
        // old impl carried. For WithDeferred, the lookup prefers the
        // deferred head over a shadowed active so each ext yields its
        // latest version exactly once.
        let mut ids = Vec::with_capacity(point_ids.len());
        let mut offsets = Vec::with_capacity(point_ids.len());
        for &point_id in point_ids {
            let Some(internal_id) = self.internal_id_with_behavior(point_id, deferred_behavior)
            else {
                continue;
            };
            ids.push(point_id);
            offsets.push(internal_id);
        }
        (ids, offsets)
    }
}
