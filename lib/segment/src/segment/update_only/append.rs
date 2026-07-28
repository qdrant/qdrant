//! The write half of an [`UpdateOnlySegment`]: storing resolved points and
//! tombstoning the slots they replace.
//!
//! Not implemented in this iteration. Every method here needs append-only
//! components that do not exist yet:
//!
//! * an appendable `DynamicStoredFlags` — the deleted-points bitmask, which
//!   both a tombstone and a copy-on-write replacement have to grow;
//! * appendable `ChunkedVectors` — the dense/multi vector storages, which must
//!   grow by appending a slot rather than by writing at an offset;
//! * an appendable payload blobstore and the appendable field indexes of the
//!   write target (an appendable segment does maintain a payload index, unlike
//!   the immutable segments the writer only reads from).
//!
//! What they have in common is that today's implementations mutate at an
//! offset, which an object store cannot do. Until the append-only variants
//! land, the shape of the write is fixed here — take fully qualified points,
//! append them, repoint the id tracker, tombstone what they replaced — and the
//! bodies are `todo!()`.

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::UpdateOnlySegment;
use crate::common::operation_error::OperationResult;
use crate::data_types::fully_qualified_point::FullyQualifiedPoint;

impl<S: UniversalRead + 'static> UpdateOnlySegment<S> {
    /// Append `points` to this segment, each into a fresh slot, and repoint the
    /// id tracker at those slots.
    ///
    /// Copy-on-write is unconditional: a point that already exists here is
    /// never rewritten in place, it is written anew and its previous slot is
    /// tombstoned. That is what makes the storages append-only, and it is why
    /// the caller resolves points in full first — a slot is written once, from
    /// data that is already complete.
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

    /// Mark `internal_ids` deleted in this segment.
    ///
    /// This is the *only* mutation a non-appendable segment ever receives, and
    /// it is deliberately tombstone-only: the payload row, the vectors and the
    /// field indexes at those slots are left untouched, so nothing but the
    /// deleted-points bitmask is written — and nothing but that bitmask has to
    /// be fetched to write it.
    pub fn tombstone_points(&mut self, internal_ids: &[PointOffsetType]) -> OperationResult<()> {
        let _ = internal_ids;
        todo!("needs an appendable deleted-points bitmask")
    }

    /// Persist everything written since the last flush.
    ///
    /// A batch is durable when this returns: the writer has no WAL, so the
    /// operations it applied exist nowhere else until the storages are
    /// flushed. Ordering is the same constraint the per-point path has —
    /// a point's new slot must be durable before the tombstone that retires
    /// its old one.
    pub fn flush(&self) -> OperationResult<()> {
        todo!("needs the append-only storages")
    }
}
