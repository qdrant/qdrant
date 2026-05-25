use atomic_refcell::AtomicRef;
use common::bitvec::{BitSlice, BitSliceExt as _};
use common::types::{DeferredBehavior, PointOffsetType};
use itertools::Either;
use self_cell::self_cell;

use super::tracker_enum::IdTrackerEnum;
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::point_mappings::PointMappings;
use crate::types::PointIdType;

/// Enum holding a reference to point mappings from an ID tracker.
///
/// Provides iteration methods over external/internal IDs without requiring
/// the `IdTracker` trait to return boxed iterators.
#[derive(Clone, Copy)]
pub enum PointMappingsRefEnum<'a> {
    Plain(&'a PointMappings),
    Compressed(&'a CompressedPointMappings),
}

impl<'a> PointMappingsRefEnum<'a> {
    /// Iterate over all external IDs.
    ///
    /// Excludes soft deleted points.
    pub fn iter_external(self) -> Box<dyn Iterator<Item = PointIdType> + 'a> {
        match self {
            PointMappingsRefEnum::Plain(m) => m.iter_external(),
            PointMappingsRefEnum::Compressed(m) => m.iter_external(),
        }
    }

    /// Iterate over internal IDs (offsets).
    ///
    /// Excludes soft deleted points.
    pub fn iter_internal(self) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        match self {
            PointMappingsRefEnum::Plain(m) => m.iter_internal(),
            PointMappingsRefEnum::Compressed(m) => m.iter_internal(),
        }
    }

    /// Iterate starting from a given ID.
    ///
    /// Excludes soft deleted points.
    pub fn iter_from(
        self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + 'a> {
        match self {
            PointMappingsRefEnum::Plain(m) => m.iter_from(external_id),
            PointMappingsRefEnum::Compressed(m) => m.iter_from(external_id),
        }
    }

    /// Iterate over internal IDs in a random order.
    ///
    /// Excludes soft deleted points.
    pub fn iter_random(self) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + 'a> {
        match self {
            PointMappingsRefEnum::Plain(m) => m.iter_random(),
            PointMappingsRefEnum::Compressed(m) => m.iter_random(),
        }
    }

    /// Iterate over internal IDs (offsets), excluding soft deleted points
    /// and flagged items from `exclude_bitslice`.
    pub fn iter_internal_excluding(
        self,
        exclude_bitslice: &'a BitSlice,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        let iter: Box<dyn Iterator<Item = PointOffsetType> + 'a> = match self {
            PointMappingsRefEnum::Plain(m) => m.iter_internal(),
            PointMappingsRefEnum::Compressed(m) => m.iter_internal(),
        };
        Box::new(
            iter.filter(move |point| !exclude_bitslice.get_bit(*point as usize).unwrap_or(false)),
        )
    }

    /// Iterate over all internal IDs, filtering deferred points using the
    /// mapping's own threshold.
    pub fn iter_internal_visible(self) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        match self.deferred_internal_id() {
            None => self.iter_internal(),
            Some(deferred_internal_id) => Box::new(
                self.iter_internal()
                    .take_while(move |&id| id < deferred_internal_id),
            ),
        }
    }

    /// Iterate over all internal IDs, with deferred filtering selected by
    /// `deferred_behavior`:
    /// - [`DeferredBehavior::Exclude`] applies the mapping's own threshold;
    /// - [`DeferredBehavior::IncludeAll`] yields every point regardless of the
    ///   threshold.
    pub fn iter_internal_with_behavior(
        self,
        deferred_behavior: DeferredBehavior,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        if deferred_behavior.include_all_points() {
            self.iter_internal()
        } else {
            self.iter_internal_visible()
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
    /// For [`DeferredBehavior::IncludeAll`] — or when the mapping has no
    /// deferred threshold — the threshold cutoff is skipped, but the
    /// deleted-bitslice filter is always applied.
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
                Either::Left(iter.filter(move |&id| !deleted.get_bit(id as usize).unwrap_or(false)))
            }
            Some(cutoff) => {
                Either::Right(iter.filter(move |&id| {
                    id < cutoff && !deleted.get_bit(id as usize).unwrap_or(false)
                }))
            }
        }
    }

    /// Iterate starting from a given ID, filtering deferred points using the
    /// mapping's own threshold.
    pub fn iter_from_visible(
        self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + 'a> {
        match self.deferred_internal_id() {
            None => self.iter_from(external_id),
            Some(deferred_internal_id) => Box::new(
                self.iter_from(external_id)
                    .filter(move |&(_, iid)| iid < deferred_internal_id),
            ),
        }
    }

    /// Iterate over internal IDs in random order, filtering deferred points
    /// using the mapping's own threshold.
    pub fn iter_random_visible(
        self,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + 'a> {
        match self.deferred_internal_id() {
            None => self.iter_random(),
            Some(deferred_internal_id) => Box::new(
                self.iter_random()
                    // We _can_ prevent iterating over all points by going down into `iter_random()` and set
                    // the `max_internal_id` to `deferred_internal_id`.
                    .filter(move |&(_, iid)| iid < deferred_internal_id),
            ),
        }
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
            PointMappingsRefEnum::Compressed(_) => None,
        }
    }

    /// Soft-deleted point bitslice for this mapping.
    fn deleted(self) -> &'a BitSlice {
        match self {
            PointMappingsRefEnum::Plain(m) => m.deleted(),
            PointMappingsRefEnum::Compressed(m) => m.deleted(),
        }
    }
}

self_cell! {
    /// Wrapper around `PointMappingsRefEnum` that only exposes external ID iteration.
    ///
    /// Used by `NonAppendableSegmentEntry::iter_points()` to return an iterator
    /// over external point IDs without creating the iterator inside the unsafe block.
    pub struct PointMappingsGuard<'a> {
        owner: AtomicRef<'a, IdTrackerEnum>,

        #[covariant]
        dependent: PointMappingsRefEnum,
    }
}
