mod fixtures;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use segment::data_types::named_vectors::NamedVectors;
    use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
    use segment::entry::entry_point::{OperationError, SegmentEntry};
    use segment::segment_constructor::load_segment;
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
                &false.into(),
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
                &false.into(),
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
                &false.into(),
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
                &false.into(),
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

        let exists = segment
            .upsert_vector(
                7,
                1.into(),
                &NamedVectors::from([
                    ("vector2".to_owned(), vec![10.]),
                    ("vector3".to_owned(), vec![5., 6., 7., 8.]),
                ]),
            )
            .unwrap();
        assert!(exists, "this partial vector should overwrite existing");

        let exists = segment
            .upsert_vector(
                8,
                6.into(),
                &NamedVectors::from([
                    ("vector2".to_owned(), vec![10.]),
                    ("vector3".to_owned(), vec![5., 6., 7., 8.]),
                ]),
            )
            .unwrap();
        assert!(!exists, "this partial vector should not existing");
    }

    #[test]
    fn test_vector_name_not_exists() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut segment = build_segment_3(dir.path());

        let result = segment.upsert_vector(
            6,
            6.into(),
            &NamedVectors::from([
                ("vector1".to_owned(), vec![5., 6., 7., 8.]),
                ("vector2".to_owned(), vec![10.]),
                ("vector3".to_owned(), vec![5., 6., 7., 8.]),
                ("vector4".to_owned(), vec![5., 6., 7., 8.]),
            ]),
        );

        if let Err(OperationError::VectorNameNotExists { received_name }) = result {
            assert!(received_name == "vector4");
        } else {
            panic!("wrong upsert result")
        }
    }

    #[test]
    fn ordered_deletion_test() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let path = {
            let mut segment = build_segment_1(dir.path());
            segment.delete_point(6, 5.into()).unwrap();
            segment.delete_point(6, 4.into()).unwrap();
            segment.flush(true).unwrap();
            segment.current_path.clone()
        };

        let segment = load_segment(&path).unwrap().unwrap();
        let query_vector = vec![1.0, 1.0, 1.0, 1.0];
        let res = segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                None,
                1,
                None,
            )
            .unwrap();
        let best_match = res.get(0).expect("Non-empty result");
        assert_eq!(best_match.id, 3.into());
    }
}
