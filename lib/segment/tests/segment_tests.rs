mod fixtures;

#[cfg(test)]
mod tests {
    use crate::fixtures::segment::build_segment_1;
    use segment::entry::entry_point::SegmentEntry;
    use segment::types::{Condition, Filter};
    use std::collections::HashSet;
    use tempdir::TempDir;

    #[test]
    fn test_point_exclusion() {
        let dir = TempDir::new("segment_dir").unwrap();

        let segment = build_segment_1(dir.path());

        assert!(segment.has_point(3));

        let query_vector = vec![1.0, 1.0, 1.0, 1.0];

        let res = segment.search(&query_vector, None, 1, None).unwrap();

        let best_match = res.get(0).expect("Non-empty result");
        assert_eq!(best_match.id, 3);

        let ids: HashSet<_> = vec![3].into_iter().collect();

        let frt = Filter {
            should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(ids.into())]),
        };

        let res = segment.search(&query_vector, Some(&frt), 1, None).unwrap();

        let best_match = res.get(0).expect("Non-empty result");
        assert_ne!(best_match.id, 3);

        let point_ids1: Vec<_> = segment.iter_points().collect();
        let point_ids2: Vec<_> = segment.iter_points().collect();

        assert!(!point_ids1.is_empty());
        assert!(!point_ids2.is_empty());

        assert_eq!(&point_ids1, &point_ids2)
    }
}
