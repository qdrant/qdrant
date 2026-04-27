use atomic_refcell::AtomicRef;
use common::bitvec::{BitSlice, BitSliceExt as _};
use common::types::PointOffsetType;
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

    /// Iterate over all internal IDs. Optionally filter all deferred points.
    pub fn iter_internal_visible(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match deferred_internal_id {
            None => self.iter_internal(),
            Some(deferred_internal_id) => Box::new(
                self.iter_internal()
                    .take_while(move |&id| id < deferred_internal_id),
            ),
        }
    }

    /// Iterate starting from a given ID. Optionally filter all deferred points.
    pub fn iter_from_visible(
        &self,
        external_id: Option<PointIdType>,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        match deferred_internal_id {
            None => self.iter_from(external_id),
            Some(deferred_internal_id) => Box::new(
                self.iter_from(external_id)
                    .filter(move |&(_, iid)| iid < deferred_internal_id),
            ),
        }
    }

    pub fn iter_random_visible(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        match deferred_internal_id {
            None => self.iter_random(),
            Some(deferred_internal_id) => Box::new(
                self.iter_random()
                    // We _can_ prevent iterating over all points by going down into `iter_random()` and set
                    // the `max_internal_id` to `deferred_internal_id`.
                    .filter(move |&(_, iid)| iid < deferred_internal_id),
            ),
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
