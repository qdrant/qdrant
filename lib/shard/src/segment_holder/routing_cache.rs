//! Which segment holds the newest visible version of a point.
//!
//! Candidates of a multi-stage query are selected globally but addressed per
//! segment: a `has_id` filter is resolved by every segment through its own
//! `e2i` mapping, and on a disk id tracker each miss costs a block read. The
//! cache remembers where a point was last seen, so a `has_id`-bounded read
//! asks each segment only about the ids it holds.

use ahash::AHashMap;
use crossbeam_utils::atomic::AtomicCell;
use segment::types::{PointIdType, ScoredPoint, SeqNumberType};

use crate::segment_holder::SegmentId;

/// Direct-mapped slots; a colliding insert overwrites. Together ~768 KiB per
/// shard: enough to bridge the stages of the queries in flight, which carry up
/// to a few thousand candidates each. A lost slot costs one lookup in every
/// segment for that id, never a wrong answer.
const SLOTS: usize = 16 * 1024;
const _: () = assert!(SLOTS.is_power_of_two());

/// A point's last observation: the segment a read found it in, or `None` when
/// an update invalidated it, with the version of either.
type Slot = Option<(PointIdType, Option<SegmentId>, SeqNumberType)>;

/// Point ids grouped by the segment that holds them.
pub type Routes = AHashMap<SegmentId, Vec<PointIdType>>;

/// Segment where a read last found each point, filled from search results and
/// invalidated by updates.
///
/// Keys are holder ids, never pinned segments: a proxied segment keeps its id,
/// so the entry still reaches the proxy and the deletes that landed on it. An
/// id the holder no longer has counts as no entry for readers, and the next
/// observation of the point replaces it, so nothing is ever removed.
///
/// Every write carries a version and a newer one wins, the latest write among
/// equals. An update invalidates with its operation number, which is newer
/// than any copy a read could have seen before it, so a read that saw the old
/// copy cannot put the route back after the update: only a copy the update
/// itself produced, or something newer, fills the slot again. A plain
/// invalidation without the version would leave that race open.
#[derive(Debug)]
pub struct RoutingCache {
    /// A 48-byte slot exceeds the native atomics, so `AtomicCell` works under
    /// a striped seqlock: readers never block, writers racing on the same
    /// stripe spin briefly.
    slots: Box<[AtomicCell<Slot>]>,
}

impl Default for RoutingCache {
    fn default() -> Self {
        Self {
            slots: (0..SLOTS).map(|_| AtomicCell::new(None)).collect(),
        }
    }
}

impl RoutingCache {
    /// A read found `point_id` in `segment_id` at `version`.
    pub fn record(&self, point_id: PointIdType, segment_id: SegmentId, version: SeqNumberType) {
        self.write(point_id, Some(segment_id), version);
    }

    /// [`record`](Self::record) every point one segment returned.
    pub fn record_points<'a>(
        &self,
        segment_id: SegmentId,
        points: impl IntoIterator<Item = &'a ScoredPoint>,
    ) {
        for point in points {
            self.record(point.id, segment_id, point.version);
        }
    }

    /// An update at `version` touched `point_id`: wherever it was last seen no
    /// longer holds, and only a copy at least this new may be recorded again.
    pub fn invalidate(&self, point_id: PointIdType, version: SeqNumberType) {
        self.write(point_id, None, version);
    }

    /// Overwrite the slot unless it holds a newer observation of the same
    /// point. A colliding point is overwritten.
    fn write(&self, point_id: PointIdType, segment_id: Option<SegmentId>, version: SeqNumberType) {
        let slot = &self.slots[Self::slot(point_id)];
        let mut current = slot.load();
        loop {
            if let Some((id, _, recorded)) = current
                && id == point_id
                && recorded > version
            {
                return;
            }
            match slot.compare_exchange(current, Some((point_id, segment_id, version))) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Group `ids` by the segment recorded for them. Ids without a segment,
    /// invalidated or never seen, come back separately: they have to be looked
    /// up in every segment.
    pub fn routes(&self, ids: impl IntoIterator<Item = PointIdType>) -> (Routes, Vec<PointIdType>) {
        let mut routes = Routes::new();
        let mut uncached = Vec::new();
        for id in ids {
            match self.slots[Self::slot(id)].load() {
                Some((recorded, Some(segment_id), _version)) if recorded == id => {
                    routes.entry(segment_id).or_default().push(id);
                }
                Some(_) | None => uncached.push(id),
            }
        }
        (routes, uncached)
    }

    /// Forget everything: every id is looked up in every segment until it is
    /// seen again.
    pub fn clear(&self) {
        for slot in self.slots.iter() {
            slot.store(None);
        }
    }

    /// Fibonacci hash of the raw id, so strided numeric ids spread over the
    /// slots too.
    fn slot(point_id: PointIdType) -> usize {
        let raw = match point_id {
            PointIdType::NumId(num) => num,
            PointIdType::Uuid(uuid) => {
                let value = uuid.as_u128();
                (value as u64) ^ ((value >> 64) as u64)
            }
        };
        (raw.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - SLOTS.trailing_zeros())) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POINT: PointIdType = PointIdType::NumId(1);

    fn route_of(cache: &RoutingCache, point: PointIdType) -> Option<SegmentId> {
        let (routes, _) = cache.routes([point]);
        routes.into_keys().next()
    }

    #[test]
    fn invalidation_outlives_reads_of_the_old_copy() {
        let cache = RoutingCache::default();
        cache.record(POINT, 7, 3);
        assert_eq!(route_of(&cache, POINT), Some(7));

        // The update lands; a read that saw the old copy lands after it.
        cache.invalidate(POINT, 5);
        cache.record(POINT, 7, 3);
        assert_eq!(route_of(&cache, POINT), None);

        // The copy the update produced, or anything newer, fills the slot.
        cache.record(POINT, 9, 5);
        assert_eq!(route_of(&cache, POINT), Some(9));
        cache.record(POINT, 8, 6);
        assert_eq!(route_of(&cache, POINT), Some(8));

        // An out-of-order older operation changes nothing.
        cache.invalidate(POINT, 4);
        assert_eq!(route_of(&cache, POINT), Some(8));
    }

    #[test]
    fn same_version_elsewhere_takes_over() {
        // E.g. the segment came back under a new id after an optimization.
        let cache = RoutingCache::default();
        cache.record(POINT, 7, 3);
        cache.record(POINT, 9, 3);
        assert_eq!(route_of(&cache, POINT), Some(9));
    }

    #[test]
    fn routes_split_recorded_from_unknown() {
        let cache = RoutingCache::default();
        let ids: Vec<_> = (1..=4).map(PointIdType::NumId).collect();
        cache.record(ids[0], 1, 1);
        cache.record(ids[1], 2, 1);
        cache.record(ids[2], 1, 1);
        let (routes, uncached) = cache.routes(ids.iter().copied());
        assert_eq!(routes[&1], vec![ids[0], ids[2]]);
        assert_eq!(routes[&2], vec![ids[1]]);
        assert_eq!(uncached, vec![ids[3]]);
    }

    #[test]
    fn colliding_point_evicts_not_misroutes() {
        let cache = RoutingCache::default();
        let colliding = (2..)
            .map(PointIdType::NumId)
            .find(|id| RoutingCache::slot(*id) == RoutingCache::slot(POINT))
            .unwrap();
        cache.record(POINT, 1, 1);
        cache.record(colliding, 2, 1);
        let (routes, uncached) = cache.routes([POINT, colliding]);
        assert_eq!(routes[&2], vec![colliding]);
        assert_eq!(uncached, vec![POINT]);
    }
}
