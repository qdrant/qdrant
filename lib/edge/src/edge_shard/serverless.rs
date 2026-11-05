//! Access to the shard's live segments, for serverless only.

use segment::segment::Segment;

use crate::EdgeShard;

impl EdgeShard {
    /// Run `f` on every live segment of the shard, each under its own read lock.
    ///
    /// Lets the caller read the segments the shard holds while their pages are still cached:
    /// dropping the shard evicts them. The caller decides which segments it cares about, e.g. by
    /// `segment_uuid()` or `is_appendable()`. Segments currently wrapped in a proxy (under
    /// optimization) are skipped.
    pub fn for_each_segment(&self, mut f: impl FnMut(&Segment)) {
        // Snapshot the segment set and release the holder lock before running `f`, so a slow
        // callback does not block updates or optimizer swaps.
        let segments: Vec<_> = self
            .segments
            .read()
            .iter_original()
            .map(|(_, segment)| segment.clone())
            .collect();
        for segment in segments {
            f(&segment.read());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use segment::entry::ReadSegmentEntry as _;
    use tempfile::Builder;
    use uuid::Uuid;

    use crate::EdgeShard;
    use crate::test_helpers::{point, test_config, upsert};

    #[test]
    fn visits_every_live_segment() {
        let dir = Builder::new()
            .prefix("edge-for-each-segment")
            .tempdir()
            .unwrap();
        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
        upsert(&shard, vec![point(1), point(2)]);

        let expected: HashSet<Uuid> = shard
            .segments
            .read()
            .iter_original()
            .map(|(_, segment)| segment.read().segment_uuid())
            .collect();
        assert!(!expected.is_empty());

        let mut visited = HashSet::new();
        let mut points = 0;
        shard.for_each_segment(|segment| {
            assert!(visited.insert(segment.segment_uuid()));
            points += segment.available_point_count();
        });

        assert_eq!(visited, expected);
        assert_eq!(points, 2);
    }
}
