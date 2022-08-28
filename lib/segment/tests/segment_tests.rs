mod fixtures;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use segment::entry::entry_point::{AllVectors, OperationError, SegmentEntry};
    use segment::segment::DEFAULT_VECTOR_NAME;
    use segment::types::{Condition, Filter, WithPayload};
    use tempfile::Builder;

    use crate::fixtures::segment::{build_segment_1, build_segment_3};

    #[test]
    fn test_point_exclusion() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segment = build_segment_1(dir.path());

        assert!(segment.has_point(3.into()));

        let query_vector = vec![1.0, 1.0, 1.0, 1.0];

        let res = segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                false,
                None,
                1,
                None,
            )
            .unwrap();

        let best_match = res.get(0).expect("Non-empty result");
        assert_eq!(best_match.id, 3.into());

        let ids: HashSet<_> = HashSet::from_iter([3.into()]);

        let frt = Filter {
            should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(ids.into())]),
        };

        let res = segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                false,
                Some(&frt),
                1,
                None,
            )
            .unwrap();

        let best_match = res.get(0).expect("Non-empty result");
        assert_ne!(best_match.id, 3.into());

        let point_ids1: Vec<_> = segment.iter_points().collect();
        let point_ids2: Vec<_> = segment.iter_points().collect();

        assert!(!point_ids1.is_empty());
        assert!(!point_ids2.is_empty());

        assert_eq!(&point_ids1, &point_ids2)
    }

    #[test]
    fn test_named_vector_search() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segment = build_segment_3(dir.path());

        assert!(segment.has_point(3.into()));

        let query_vector = vec![1.0, 1.0, 1.0, 1.0];

        let res = segment
            .search(
                "vector1",
                &query_vector,
                &WithPayload::default(),
                false,
                None,
                1,
                None,
            )
            .unwrap();

        let best_match = res.get(0).expect("Non-empty result");
        assert_eq!(best_match.id, 3.into());

        let ids: HashSet<_> = HashSet::from_iter([3.into()]);

        let frt = Filter {
            should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(ids.into())]),
        };

        let res = segment
            .search(
                "vector1",
                &query_vector,
                &WithPayload::default(),
                false,
                Some(&frt),
                1,
                None,
            )
            .unwrap();

        let best_match = res.get(0).expect("Non-empty result");
        assert_ne!(best_match.id, 3.into());

        let point_ids1: Vec<_> = segment.iter_points().collect();
        let point_ids2: Vec<_> = segment.iter_points().collect();

        assert!(!point_ids1.is_empty());
        assert!(!point_ids2.is_empty());

        assert_eq!(&point_ids1, &point_ids2)
    }

    #[test]
    fn test_missed_vector_name() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut segment = build_segment_3(dir.path());

        let result = segment.upsert_point(
            6,
            6.into(),
            &AllVectors::from([
                ("vector2".to_owned(), vec![10.].clone()),
                ("vector3".to_owned(), vec![5., 6., 7., 8.].clone()),
            ]),
        );

        if let Err(OperationError::MissedVectorName { received_name }) = result {
            assert!(received_name == "vector1");
        } else {
            panic!("wrong upsert result")
        }
    }

    #[test]
    fn test_vector_name_not_exists() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut segment = build_segment_3(dir.path());

        let result = segment.upsert_point(
            6,
            6.into(),
            &AllVectors::from([
                ("vector1".to_owned(), vec![5., 6., 7., 8.].clone()),
                ("vector2".to_owned(), vec![10.].clone()),
                ("vector3".to_owned(), vec![5., 6., 7., 8.].clone()),
                ("vector4".to_owned(), vec![5., 6., 7., 8.].clone()),
            ]),
        );

        if let Err(OperationError::VectorNameNotExists { received_name }) = result {
            assert!(received_name == "vector4");
        } else {
            panic!("wrong upsert result")
        }
    }
}
