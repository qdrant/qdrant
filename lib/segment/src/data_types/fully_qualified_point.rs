//! Point representations of the batch update path: [`StoredPoint`] is a point
//! as read out of the segment that owns it, [`FullyQualifiedPoint`] is a point
//! resolved in full, ready to be appended.

use common::types::PointOffsetType;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::segment_record::NamedVectorBytesOwned;
use crate::types::{Payload, PointIdType, SeqNumberType};

/// The stored form of a point: the base a batch of mutations is folded onto.
///
/// Vectors are storage-native bytes, never decoded, so they can move to a new
/// slot without the lossy decode/re-encode round-trip — the same contract as
/// [`SegmentEntry::upsert_moved_point`].
///
/// Carries no version: versions are resolved separately, before the point is
/// read (see `UpdateOnlySegment::point_versions`).
///
/// [`SegmentEntry::upsert_moved_point`]: crate::entry::entry_point::SegmentEntry::upsert_moved_point
#[derive(Debug, Clone)]
pub struct StoredPoint {
    /// Slot the point occupies in the segment it was read from.
    pub internal_id: PointOffsetType,
    /// Every named vector the point has, in storage-native bytes.
    pub vectors: NamedVectorBytesOwned,
    /// The point's complete payload; empty when it has none.
    pub payload: Payload,
}

/// A point resolved to everything a segment needs in order to store it:
/// storing a fully qualified point reads nothing back.
///
/// Vectors come in two halves: `stored_vectors` carried over verbatim as
/// storage-native bytes, `updated_vectors` supplied by the batch and therefore
/// decoded. A name present in both is taken from `updated_vectors`.
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
