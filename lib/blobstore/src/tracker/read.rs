use std::ops::Range;

use common::generic_consts::AccessPattern;
use common::universal_io::UserData;

use crate::Result;
use crate::tracker::{PointOffset, PointerItem, ValuePointer};

/// Read-side interface over the pointer tracker of either storage mode.
///
/// Implemented by the writable trackers, whose reads see pending in-memory updates, and by
/// read-only trackers, which serve the persisted state. The views and readers of both modes
/// are generic over this trait, so the same read logic works with any tracker.
pub trait TrackerRead {
    /// Exclusive upper bound of point offsets that may have a mapping.
    ///
    /// Maintained by the writer: exact and in memory for the writable trackers, taken from the
    /// persisted state for the read-only ones.
    fn max_point_offset(&self) -> Result<PointOffset>;

    /// Get the mapping at the given point offset.
    ///
    /// Point offsets without a mapping yield `None`, including those at or past
    /// [`max_point_offset`](Self::max_point_offset).
    fn get<P: AccessPattern>(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>>;

    /// Get the mappings for a contiguous range of point offsets.
    ///
    /// The result holds one entry per requested point offset, so callers should bound the range
    /// they ask for.
    fn get_range<P: AccessPattern>(
        &self,
        point_offsets: Range<PointOffset>,
    ) -> Result<Vec<Option<ValuePointer>>>;

    /// Iterate the mappings for the given point offsets.
    ///
    /// Issues batched reads against the underlying storage, so async backends can fetch entries
    /// in parallel. Yields one [`PointerItem`] per requested point offset, possibly in a
    /// different order.
    fn iter<U, I>(
        &self,
        point_offsets: I,
    ) -> Result<impl Iterator<Item = Result<(U, PointerItem)>>>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffset)>;
}
