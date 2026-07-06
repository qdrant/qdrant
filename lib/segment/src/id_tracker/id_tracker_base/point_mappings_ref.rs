use atomic_refcell::AtomicRef;
use common::bitvec::{BitSlice, BitSliceExt as _};
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{MmapFile, UniversalRead};
use itertools::Either;
use self_cell::self_cell;

use super::tracker_enum::IdTrackerEnum;
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::disk_id_tracker::mappings::DiskMappingsRef;
use crate::id_tracker::point_mappings::PointMappings;
use crate::types::PointIdType;

/// Enum holding a reference to point mappings from an ID tracker.
///
/// Provides iteration methods over external/internal IDs without requiring
/// the `IdTracker` trait to return boxed iterators.
///
/// Generic over the disk-resident tracker's read backend `S`
/// ([`IdTrackerRead::Backend`](super::trait_def::IdTrackerRead::Backend)); the
/// [`Disk`](Self::Disk) variant holds a concrete [`DiskMappingsRef`] rather than a
/// trait object, so the mapping is reached by static dispatch. `S` is unused by
/// the `Plain`/`Compressed` variants.
pub enum PointMappingsRefEnum<'a, S: UniversalRead> {
    Plain(&'a PointMappings),
    Compressed(&'a CompressedPointMappings),
    Disk(DiskMappingsRef<'a, S>),
}

// Hand-written so `Copy` doesn't require `S: Copy` (every variant is references).
impl<S: UniversalRead> Clone for PointMappingsRefEnum<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<S: UniversalRead> Copy for PointMappingsRefEnum<'_, S> {}

impl<'a, S: UniversalRead> PointMappingsRefEnum<'a, S> {
    /// Iterate over all external IDs.
    ///
    /// Excludes soft deleted points.
    pub fn iter_external(self) -> impl Iterator<Item = PointIdType> + 'a {
        match self {
            PointMappingsRefEnum::Plain(m) => Either::Left(m.iter_external()),
            PointMappingsRefEnum::Compressed(m) => Either::Right(Either::Left(m.iter_external())),
            PointMappingsRefEnum::Disk(m) => Either::Right(Either::Right(m.iter_external())),
        }
    }

    /// Iterate over internal IDs (offsets).
    ///
    /// Excludes soft deleted points.
    pub fn iter_internal(self) -> impl Iterator<Item = PointOffsetType> + 'a {
        match self {
            PointMappingsRefEnum::Plain(m) => Either::Left(m.iter_internal()),
            PointMappingsRefEnum::Compressed(m) => Either::Right(Either::Left(m.iter_internal())),
            PointMappingsRefEnum::Disk(m) => Either::Right(Either::Right(m.iter_internal())),
        }
    }

    /// Iterate starting from a given ID.
    ///
    /// Excludes soft deleted points.
    pub fn iter_from(
        self,
        external_id: Option<PointIdType>,
    ) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + 'a {
        match self {
            PointMappingsRefEnum::Plain(m) => Either::Left(m.iter_from(external_id)),
            PointMappingsRefEnum::Compressed(m) => {
                Either::Right(Either::Left(m.iter_from(external_id)))
            }
            PointMappingsRefEnum::Disk(m) => Either::Right(Either::Right(m.iter_from(external_id))),
        }
    }

    /// Iterate over internal IDs (offsets), excluding soft deleted points
    /// and flagged items from `exclude_bitslice`.
    pub fn iter_internal_excluding(
        self,
        exclude_bitslice: &'a BitSlice,
    ) -> impl Iterator<Item = PointOffsetType> + 'a {
        self.iter_internal()
            .filter(move |point| !exclude_bitslice.get_bit(*point as usize).unwrap_or(false))
    }

    /// Iterate over all internal IDs, filtering deferred points using the
    /// mapping's own threshold.
    pub fn iter_internal_visible(self) -> impl Iterator<Item = PointOffsetType> + 'a {
        match self.deferred_internal_id() {
            None => Either::Left(self.iter_internal()),
            Some(deferred_internal_id) => Either::Right(
                self.iter_internal()
                    .take_while(move |&id| id < deferred_internal_id),
            ),
        }
    }

    /// Iterate over all internal IDs, with deferred filtering selected by
    /// `deferred_behavior`. See [`PointMappings::iter_internal_with_behavior`]
    /// for the per-mode contract.
    pub fn iter_internal_with_behavior(
        self,
        deferred_behavior: DeferredBehavior,
    ) -> impl Iterator<Item = PointOffsetType> + 'a {
        match self {
            PointMappingsRefEnum::Plain(m) => {
                Either::Left(m.iter_internal_with_behavior(deferred_behavior))
            }
            PointMappingsRefEnum::Compressed(m) => {
                // Compressed mappings are immutable,
                // they can't have deferred points,
                // so we can only pull visible points
                // and ignore the parameter
                Either::Right(Either::Left(m.iter_internal()))
            }
            // Disk mappings are immutable too; ignore the deferred parameter.
            PointMappingsRefEnum::Disk(m) => Either::Right(Either::Right(m.iter_internal())),
        }
    }

    /// Wrap an iterator of internal IDs so that soft-deleted points and (when
    /// requested) points at or above the mapping's deferred threshold are
    /// excluded.
    ///
    /// Intended for iterators sourced outside the mapping (e.g. field-index
    /// outputs) where neither the deleted bitslice nor the deferred threshold
    /// are applied implicitly. The bitslice check guards against stale
    /// postings for tombstoned internal IDs (a single bit test per element,
    /// negligible overhead).
    ///
    /// For [`DeferredBehavior::VisibleOnly`] — points at or above the cutoff are
    /// dropped on top of the deleted check.
    /// For [`DeferredBehavior::WithDeferred`] — every non-deleted point is
    /// yielded, except shadowed actives (an active whose external id has
    /// been overridden by a deferred mutation). Skipping shadowed actives
    /// is what gives the WithDeferred consumer a one-yield-per-external
    /// guarantee in the presence of append-only mutations into a deferred
    /// segment.
    pub fn filter_deferred_and_deleted<I>(
        self,
        iter: I,
        deferred_behavior: DeferredBehavior,
    ) -> impl Iterator<Item = PointOffsetType>
    where
        I: Iterator<Item = PointOffsetType>,
    {
        let deleted = self.deleted();
        match deferred_behavior.apply(self.deferred_internal_id()) {
            None => {
                let shadowed = self.shadowed();
                Either::Left(iter.filter(move |&id| {
                    !deleted.get_bit(id as usize).unwrap_or(false)
                        && !shadowed.get_bit(id as usize).unwrap_or(false)
                }))
            }
            Some(cutoff) => {
                Either::Right(iter.filter(move |&id| {
                    id < cutoff && !deleted.get_bit(id as usize).unwrap_or(false)
                }))
            }
        }
    }

    /// Iterate starting from a given ID, with deferred filtering selected by
    /// `deferred_behavior`. See [`PointMappings::iter_from_with_behavior`] for
    /// the per-mode contract. Compressed mappings ignore the parameter (they
    /// can't hold deferred entries).
    pub fn iter_from_with_behavior(
        self,
        external_id: Option<PointIdType>,
        deferred_behavior: DeferredBehavior,
    ) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + 'a {
        match self {
            PointMappingsRefEnum::Plain(m) => {
                Either::Left(m.iter_from_with_behavior(external_id, deferred_behavior))
            }
            PointMappingsRefEnum::Compressed(m) => {
                Either::Right(Either::Left(m.iter_from(external_id)))
            }
            PointMappingsRefEnum::Disk(m) => Either::Right(Either::Right(m.iter_from(external_id))),
        }
    }

    /// Iterate over internal IDs in random order, with deferred filtering
    /// selected by `deferred_behavior`. See
    /// [`PointMappings::iter_random_with_behavior`] for the per-mode contract.
    /// Compressed mappings ignore the parameter (they can't hold deferred
    /// entries).
    pub fn iter_random_with_behavior(
        self,
        deferred_behavior: DeferredBehavior,
    ) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + 'a {
        match self {
            PointMappingsRefEnum::Plain(m) => {
                Either::Left(m.iter_random_with_behavior(deferred_behavior))
            }
            PointMappingsRefEnum::Compressed(m) => Either::Right(Either::Left(m.iter_random())),
            PointMappingsRefEnum::Disk(m) => Either::Right(Either::Right(m.iter_random())),
        }
    }

    /// Iterate starting from a given ID, filtering deferred points using the
    /// mapping's own threshold. Shorthand for
    /// [`Self::iter_from_with_behavior`] with [`DeferredBehavior::VisibleOnly`].
    pub fn iter_from_visible(
        self,
        external_id: Option<PointIdType>,
    ) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + 'a {
        self.iter_from_with_behavior(external_id, DeferredBehavior::VisibleOnly)
    }

    /// Iterate over internal IDs in random order, filtering deferred points
    /// using the mapping's own threshold. Shorthand for
    /// [`Self::iter_random_with_behavior`] with [`DeferredBehavior::VisibleOnly`].
    pub fn iter_random_visible(self) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + 'a {
        self.iter_random_with_behavior(DeferredBehavior::VisibleOnly)
    }

    /// Deferred threshold attached to this mapping, if any.
    ///
    /// Compressed mappings (used by immutable / read-only-immutable id trackers)
    /// never carry a deferred threshold.
    ///
    /// Kept private so callers go through the dispatch helpers
    /// ([`Self::iter_internal_with_behavior`], [`Self::external_iter_cutoff`])
    /// instead of leaking the raw threshold.
    fn deferred_internal_id(self) -> Option<PointOffsetType> {
        match self {
            PointMappingsRefEnum::Plain(m) => m.deferred_internal_id(),
            PointMappingsRefEnum::Compressed(_) | PointMappingsRefEnum::Disk(_) => None,
        }
    }

    /// Soft-deleted point bitslice for this mapping.
    fn deleted(self) -> &'a BitSlice {
        match self {
            PointMappingsRefEnum::Plain(m) => m.deleted(),
            PointMappingsRefEnum::Compressed(m) => m.deleted(),
            PointMappingsRefEnum::Disk(m) => m.deleted(),
        }
    }

    /// Shadowed-active bitslice for this mapping. Empty for compressed and disk
    /// mappings (immutable trackers can't carry deferred mutations).
    fn shadowed(self) -> &'a BitSlice {
        match self {
            PointMappingsRefEnum::Plain(m) => m.shadowed_bitslice(),
            PointMappingsRefEnum::Compressed(_) | PointMappingsRefEnum::Disk(_) => {
                BitSlice::empty()
            }
        }
    }
}

/// The `PointMappingsRefEnum` produced by an [`IdTrackerEnum`], whose backend is
/// always [`MmapFile`] (all its variants are local, `MmapFile`-backed trackers).
type IdTrackerEnumMappingsRef<'a> = PointMappingsRefEnum<'a, MmapFile>;

self_cell! {
    /// Wrapper around `PointMappingsRefEnum` that only exposes external ID iteration.
    ///
    /// Used by `NonAppendableSegmentEntry::iter_points()` to return an iterator
    /// over external point IDs without creating the iterator inside the unsafe block.
    pub struct PointMappingsGuard<'a> {
        owner: AtomicRef<'a, IdTrackerEnum>,

        #[covariant]
        dependent: IdTrackerEnumMappingsRef,
    }
}
