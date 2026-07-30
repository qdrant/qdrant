//! The write half of an [`UpdateOnlySegment`]: storing resolved points and
//! tombstoning the slots they replace.
//!
//! Not implemented in this iteration: every method needs append-only
//! components that do not exist yet — an appendable `DynamicStoredFlags`
//! (deleted-points bitmask), appendable `ChunkedVectors`, an appendable
//! payload blobstore and field indexes. Today's equivalents all mutate at an
//! offset, which an object store cannot do. The signatures fix the shape of
//! the write; the bodies are `todo!()`.

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::UpdateOnlySegment;
use crate::common::operation_error::OperationResult;
use crate::data_types::fully_qualified_point::FullyQualifiedPoint;

impl<S: UniversalRead + 'static> UpdateOnlySegment<S> {
    /// Append `points` to this segment, each into a fresh slot, and repoint
    /// the id tracker at those slots. A point that already exists here is
    /// never rewritten in place: it is written anew and its previous slot is
    /// tombstoned.
    ///
    /// Requires [`is_appendable`](UpdateOnlySegment::is_appendable).
    pub fn store_points(
        &mut self,
        points: &[FullyQualifiedPoint],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        debug_assert!(self.is_appendable());
        let _ = (points, hw_counter);
        todo!("needs the append-only storages and field indexes of the write target")
    }

    /// Mark `internal_ids` deleted in this segment. Nothing but the
    /// deleted-points bitmask is written: the payload row, the vectors and the
    /// field indexes at those slots are left untouched.
    pub fn tombstone_points(&mut self, internal_ids: &[PointOffsetType]) -> OperationResult<()> {
        let _ = internal_ids;
        todo!("needs an appendable deleted-points bitmask")
    }

    /// Persist everything written since the last flush. There is no WAL:
    /// writes are durable only once this returns.
    pub fn flush(&self) -> OperationResult<()> {
        todo!("needs the append-only storages")
    }
}
