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
//! The two phases meet at [`SegmentWriterState`]: what a writer must know
//! about the segment it resumes, taken from the read the lookup phase already
//! did. See [`LookupSegment::writer_state`].

mod appendable;
mod delete_only;
mod lookup;
mod segment_enum;

use common::types::PointOffsetType;

pub use self::appendable::AppendableSegment;
pub use self::delete_only::DeleteOnlySegment;
pub use self::lookup::{LookupSegment, LookupVectorData};
pub use self::segment_enum::UpdateOnlySegmentEnum;
use crate::types::PointIdType;

/// What a writer needs to know about the segment it is about to resume,
/// produced by [`LookupSegment::writer_state`] and consumed by
/// [`UpdateOnlySegmentEnum::open`].
///
/// Which variant a segment yields is decided by the id-tracker format it was
/// loaded with, not by its config: the format is what dictates how a point is
/// retired.
pub enum SegmentWriterState {
    /// An immutable segment: its mappings cannot grow, so the only write it
    /// accepts is retiring points that are already in it.
    DeleteOnly,
    /// An appendable segment, resuming from the state of its mappings log.
    Appendable(AppendableIdTrackerState),
}

/// The tail of an appendable segment's mappings log, as the read phase saw it.
///
/// All three fields must come from one and the same read of that log — see
/// [`UpdateOnlyAppendableIdTracker::new`], which is why they travel together
/// rather than being re-read by the writer.
///
/// [`UpdateOnlyAppendableIdTracker::new`]:
///     crate::id_tracker::mutable_id_tracker::update_only::UpdateOnlyAppendableIdTracker::new
pub struct AppendableIdTrackerState {
    /// Highest slot the log has ever claimed; the writer resumes above it.
    pub max_claimed_internal_id: Option<PointOffsetType>,
    /// External ids the log inserted whose versions were never committed. The
    /// writer retires them before accepting anything new.
    pub pending_inserts: Vec<PointIdType>,
    /// Byte offset just past the last complete entry, where the next batch is
    /// appended.
    pub mappings_end: u64,
}
