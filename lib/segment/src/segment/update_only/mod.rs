//! Update-only segments: the write-side counterpart of
//! [`ReadOnlySegment`](crate::segment::read_only::ReadOnlySegment).
//!
//! Applying a batch of updates runs in two phases, and each has its own type
//! here because they agree on almost nothing — components, lifetime, backend
//! capability, and how many segments they touch:
//!
//! * [`LookupSegment`] is the read phase. Every segment of a shard is opened
//!   as one, cold and read-only, and the phase above them aggregates: a point
//!   is located in whichever segments hold it and read from the newest copy.
//!   It opens exactly what resolving an update requires — the id tracker, the
//!   payload storage and the vector storages. No vector index, no quantized
//!   vectors, no payload index: on a remote backend those files are never
//!   fetched.
//! * [`DeleteOnlySegment`] and [`AppendableSegment`] are the write phase, one
//!   segment each, [`UpdateOnlySegmentEnum`] over the two. They are opened for
//!   one batch and dropped with it, since the append-only components behind
//!   them hold nothing across calls.
//!
//! The two phases meet at [`WriterIdTrackerState`]: what a writer must know
//! about the segment it resumes, taken from the read the lookup phase already
//! did. See [`LookupSegment::writer_state`].

mod appendable;
mod delete_only;
mod lookup;
mod segment_enum;

use common::bitvec::BitVec;
use common::types::PointOffsetType;

pub use self::appendable::AppendableSegment;
pub use self::delete_only::DeleteOnlySegment;
pub use self::lookup::LookupSegment;
pub use self::segment_enum::UpdateOnlySegmentEnum;
use crate::types::PointIdType;

/// Id-tracker state the read phase hands to a segment's writer; the variant
/// decides the writer's kind.
pub enum WriterIdTrackerState {
    Appendable(AppendableIdTrackerState),
    DeleteOnly(DeleteOnlyIdTrackerState),
}

/// The tail of an appendable segment's mappings log, as the read phase saw it.
///
/// The fields are the arguments of [`UpdateOnlyAppendableIdTracker::new`],
/// which documents each and requires that all three come from one and the same
/// read of that log — which is why they travel together rather than being
/// re-read by the writer.
///
/// [`UpdateOnlyAppendableIdTracker::new`]:
///     crate::id_tracker::mutable_id_tracker::update_only::UpdateOnlyAppendableIdTracker::new
pub struct AppendableIdTrackerState {
    pub max_claimed_internal_id: Option<PointOffsetType>,
    pub pending_inserts: Vec<PointIdType>,
    pub mappings_end: u64,
}

/// Which immutable id tracker the read phase found — deciding which
/// update-only tracker the writer resumes with — carrying the deleted-points
/// mask when the read phase held it in memory, sparing the writer the read of
/// the mask file. `None` means the writer's tracker reads it itself.
pub enum DeleteOnlyIdTrackerState {
    Immutable(Option<BitVec>),
    DiskResident(Option<BitVec>),
}
