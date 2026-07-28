//! Point representations used by the batch update path.
//!
//! The update-only writer never mutates a stored point in place: it resolves
//! what the point must become, then appends it whole. That resolution has two
//! ends — [`StoredPoint`], read out of the segment that currently owns the
//! point, and [`FullyQualifiedPoint`], the finished point handed to a segment
//! for storing.

use common::types::PointOffsetType;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::segment_record::NamedVectorBytesOwned;
use crate::types::{Payload, PointIdType, SeqNumberType};

/// The stored form of a point, as read out of the segment that currently owns
/// it: the base a batch of mutations is folded onto.
///
/// Vectors are storage-native bytes rather than decoded vectors, so names the
/// batch does not touch travel to the new slot verbatim — the same reason the
/// copy-on-write move path reads them raw (see
/// [`SegmentEntry::upsert_moved_point`]). Only the vectors the batch actually
/// replaces are ever decoded.
///
/// [`SegmentEntry::upsert_moved_point`]: crate::entry::entry_point::SegmentEntry::upsert_moved_point
/// Carries no version: deciding which segment holds the newest copy of a point
/// happens before this is read, so the version is already in the caller's hand
/// (see `SegmentUpdateView::point_versions`) and reading it again would cost a
/// second id-tracker pass on a disk-resident tracker.
#[derive(Debug, Clone)]
pub struct StoredPoint {
    /// Slot the point occupies in the segment it was read from. The writer
    /// tombstones it once the rewritten point is durable.
    pub internal_id: PointOffsetType,
    /// Every named vector the point has, in storage-native bytes.
    pub vectors: NamedVectorBytesOwned,
    /// The point's complete payload; empty when it has none.
    pub payload: Payload,
}

/// A point resolved to everything a segment needs in order to store it — its
/// external id, the version to record, every named vector, and the complete
/// payload.
///
/// "Fully qualified" is the writer's central invariant. An update-only segment
/// only ever appends whole points, so every operation is resolved against the
/// currently stored point *before* any write happens; from there on, storing a
/// point reads nothing back.
///
/// Vectors come in two halves, mirroring [`SegmentEntry::upsert_moved_point`]:
/// `stored_vectors` are carried over verbatim from the previous slot, while
/// `updated_vectors` are the ones the batch supplied and therefore had to
/// decode. On conflict `updated_vectors` wins — a name present in both was
/// replaced by the batch.
///
/// [`SegmentEntry::upsert_moved_point`]: crate::entry::entry_point::SegmentEntry::upsert_moved_point
#[derive(Debug, Clone)]
pub struct FullyQualifiedPoint {
    pub id: PointIdType,
    /// Operation number to record as the point's version — the highest one
    /// among the batch operations folded into it.
    pub version: SeqNumberType,
    /// Vectors carried over from the point's previous slot, in storage-native
    /// bytes. Empty for a point the batch creates from scratch.
    pub stored_vectors: NamedVectorBytesOwned,
    /// Vectors supplied by the batch, overriding `stored_vectors` by name.
    pub updated_vectors: NamedVectors<'static>,
    /// The point's complete payload, already merged.
    pub payload: Payload,
}
