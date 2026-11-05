//! Access to the shard's live segments, for serverless only.

use segment::entry::ReadSegmentEntry as _;
use segment::segment::Segment;
use uuid::Uuid;

use crate::EdgeShard;

impl EdgeShard {
    /// Run `f` on the live segment `uuid` under its read lock.
    ///
    /// Lets the caller read a segment the shard just built while its pages are still cached:
    /// dropping the shard evicts them. `None` if the shard holds no such segment, or only a proxy
    /// of it.
    pub fn with_segment<R>(&self, uuid: Uuid, f: impl FnOnce(&Segment) -> R) -> Option<R> {
        let segment = self
            .segments
            .read()
            .iter_original()
            .find(|(_, segment)| segment.read().segment_uuid() == uuid)
            .map(|(_, segment)| segment.clone())?;
        let segment = segment.read();
        Some(f(&segment))
    }
}

#[cfg(test)]
mod tests {
    use segment::entry::ReadSegmentEntry as _;
    use tempfile::Builder;
    use uuid::Uuid;

    use crate::EdgeShard;
    use crate::test_helpers::{point, test_config, upsert};

    #[test]
    fn finds_a_live_segment_by_uuid() {
        let dir = Builder::new()
            .prefix("edge-with-segment")
            .tempdir()
            .unwrap();
        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
        upsert(&shard, vec![point(1), point(2)]);

        let uuids: Vec<Uuid> = shard
            .segments
            .read()
            .iter_original()
            .map(|(_, segment)| segment.read().segment_uuid())
            .collect();
        assert!(!uuids.is_empty());
        let points: usize = uuids
            .iter()
            .map(|uuid| {
                shard
                    .with_segment(*uuid, |segment| segment.available_point_count())
                    .unwrap()
            })
            .sum();
        assert_eq!(points, 2);

        assert!(shard.with_segment(Uuid::new_v4(), |_| ()).is_none());
    }
}
