//! Where each candidate of a query came from.
//!
//! Candidates are selected globally but addressed per segment: without
//! provenance, a later stage hands its candidate ids to *every* segment, and
//! each one resolves the full list through its own `e2i` mapping — on a disk id
//! tracker, one block read per id, nearly all of them misses. Remembering the
//! producing segment turns that into one lookup in one segment per id.

use std::collections::hash_map::Entry;

use ahash::AHashMap;
use segment::types::{PointIdType, ScoredPoint, SeqNumberType};
use shard::segment_holder::SegmentId;

/// Candidate ids grouped by the segment that produced them.
pub type Routes = AHashMap<SegmentId, Vec<PointIdType>>;

/// The segment that held the highest version of each candidate when it was
/// collected.
///
/// Keys are holder ids, not pinned segments: a segment that gets proxied for an
/// optimization keeps its id, and looking the id up again in the live holder is
/// what reaches the proxy — and so the deletes that landed on it. A finished
/// optimization does mint a new id, which is why a route can go missing and
/// every stage must be able to fall back to a broadcast.
#[derive(Debug, Default)]
pub struct Provenance(AHashMap<PointIdType, (SegmentId, SeqNumberType)>);

impl Provenance {
    /// Record the points one segment contributed.
    ///
    /// Same rule as `BatchResultAggregator`: the highest version wins, the
    /// first segment seen wins a tie. A tie means both segments hold the same
    /// version of the point, i.e. the same content, so either is a correct
    /// choice.
    pub fn record<'a>(
        &mut self,
        segment_id: SegmentId,
        points: impl IntoIterator<Item = &'a ScoredPoint>,
    ) {
        for point in points {
            match self.0.entry(point.id) {
                Entry::Vacant(entry) => {
                    entry.insert((segment_id, point.version));
                }
                Entry::Occupied(mut entry) => {
                    if entry.get().1 < point.version {
                        entry.insert((segment_id, point.version));
                    }
                }
            }
        }
    }

    /// Fold another map in, keeping the higher version per point.
    pub fn merge(&mut self, other: Self) {
        for (id, (segment_id, version)) in other.0 {
            match self.0.entry(id) {
                Entry::Vacant(entry) => {
                    entry.insert((segment_id, version));
                }
                Entry::Occupied(mut entry) => {
                    if entry.get().1 < version {
                        entry.insert((segment_id, version));
                    }
                }
            }
        }
    }

    /// Group `ids` by the segment that produced them, plus the ids that have no
    /// recorded segment (from a source that does not track provenance, or from
    /// another shard).
    ///
    /// A stage that must be routed all-or-nothing — a search, whose results are
    /// merged by a single aggregator — falls back to a broadcast unless the
    /// unrouted list is empty.
    pub fn routes(&self, ids: impl IntoIterator<Item = PointIdType>) -> (Routes, Vec<PointIdType>) {
        let mut routes = Routes::new();
        let mut unrouted = Vec::new();
        for id in ids {
            match self.0.get(&id) {
                Some(&(segment_id, _version)) => routes.entry(segment_id).or_default().push(id),
                None => unrouted.push(id),
            }
        }
        (routes, unrouted)
    }
}
