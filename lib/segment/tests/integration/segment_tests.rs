use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::atomic::AtomicBool;

use itertools::Itertools;
use segment::common::operation_error::OperationError;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{
    only_default_vector, VectorRef, VectorStruct, DEFAULT_VECTOR_NAME,
};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::index_fixtures::random_vector;
use segment::segment_constructor::load_segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Condition, Distance, Filter, SearchParams, WithPayload};
use tempfile::Builder;

use crate::fixtures::segment::{build_segment_1, build_segment_3};

#[test]
fn test_point_exclusion() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let segment = build_segment_1(dir.path());

    assert!(segment.has_point(3.into()));

    let query_vector = [1.0, 1.0, 1.0, 1.0].into();

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

    let best_match = res.first().expect("Non-empty result");
    assert_eq!(best_match.id, 3.into());

    let ids: HashSet<_> = HashSet::from_iter([3.into()]);

    let frt = Filter::new_must_not(Condition::HasId(ids.into()));

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

    let best_match = res.first().expect("Non-empty result");
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

    let query_vector = [1.0, 1.0, 1.0, 1.0].into();

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

    let best_match = res.first().expect("Non-empty result");
    assert_eq!(best_match.id, 3.into());

    let ids: HashSet<_> = HashSet::from_iter([3.into()]);

    let frt = Filter {
        should: None,
        min_should: None,
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

    let best_match = res.first().expect("Non-empty result");
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
        .upsert_point(
            7,
            1.into(),
            NamedVectors::from([
                ("vector2".to_owned(), vec![10.]),
                ("vector3".to_owned(), vec![5., 6., 7., 8.]),
            ]),
        )
        .unwrap();
    assert!(exists, "this partial vector should overwrite existing");

    let exists = segment
        .upsert_point(
            8,
            6.into(),
            NamedVectors::from([
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

    let result = segment.upsert_point(
        6,
        6.into(),
        NamedVectors::from([
            ("vector1".to_owned(), vec![5., 6., 7., 8.]),
            ("vector2".to_owned(), vec![10.]),
            ("vector3".to_owned(), vec![5., 6., 7., 8.]),
            ("vector4".to_owned(), vec![5., 6., 7., 8.]),
        ]),
    );

    if let Err(OperationError::VectorNameNotExists { received_name }) = result {
        assert_eq!(received_name, "vector4");
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

    let segment = load_segment(&path, &AtomicBool::new(false))
        .unwrap()
        .unwrap();
    let query_vector = [1.0, 1.0, 1.0, 1.0].into();

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
    let best_match = res.first().expect("Non-empty result");
    assert_eq!(best_match.id, 3.into());
}

#[test]
fn skip_deleted_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let path = {
        let mut segment = build_segment_1(dir.path());
        segment.delete_point(6, 5.into()).unwrap();
        segment.delete_point(6, 4.into()).unwrap();
        segment.flush(true).unwrap();
        segment.current_path.clone()
    };

    let new_path = path.with_extension("deleted");
    std::fs::rename(&path, new_path).unwrap();

    let segment = load_segment(&path, &AtomicBool::new(false)).unwrap();

    assert!(segment.is_none());
}

#[test]
fn test_update_named_vector() {
    let num_points = 25;
    let dim = 4;
    let mut rng = rand::thread_rng();
    let distance = Distance::Cosine;
    let vectors = (0..num_points)
        .map(|_| random_vector(&mut rng, dim))
        .collect_vec();

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_simple_segment(dir.path(), dim, distance).unwrap();

    for (i, vec) in vectors.iter().enumerate() {
        let i = i as u64;
        segment
            .upsert_point(i, i.into(), only_default_vector(vec))
            .unwrap();
    }

    let query_vector = random_vector(&mut rng, dim).into();

    // do exact search
    let search_params = SearchParams {
        hnsw_ef: None,
        exact: true,
        quantization: None,
        indexed_only: false,
    };
    let nearest_upsert = segment
        .search(
            DEFAULT_VECTOR_NAME,
            &query_vector,
            &false.into(),
            &true.into(),
            None,
            1,
            Some(&search_params),
        )
        .unwrap();
    let nearest_upsert = nearest_upsert.first().unwrap();

    let sqrt_distance = |v: &[f32]| -> f32 { v.iter().map(|x| x * x).sum::<f32>().sqrt() };

    // check if nearest_upsert is normalized
    match &nearest_upsert.vector {
        Some(VectorStruct::Single(v)) => {
            assert!((sqrt_distance(v) - 1.).abs() < 1e-5);
        }
        Some(VectorStruct::Multi(v)) => {
            let v: VectorRef = (&v[DEFAULT_VECTOR_NAME]).into();
            let v: &[_] = v.try_into().unwrap();
            assert!((sqrt_distance(v) - 1.).abs() < 1e-5);
        }
        _ => panic!("unexpected vector type"),
    }

    // update vector using the same values
    for (i, vec) in vectors.iter().enumerate() {
        let i = i as u64;
        segment
            .update_vectors(i + num_points as u64, i.into(), only_default_vector(vec))
            .unwrap();
    }

    // do search after update
    let nearest_update = segment
        .search(
            DEFAULT_VECTOR_NAME,
            &query_vector,
            &false.into(),
            &true.into(),
            None,
            1,
            Some(&search_params),
        )
        .unwrap();
    let nearest_update = nearest_update.first().unwrap();

    // check that nearest_upsert is normalized
    match &nearest_update.vector {
        Some(VectorStruct::Single(v)) => {
            assert!((sqrt_distance(v) - 1.).abs() < 1e-5);
        }
        Some(VectorStruct::Multi(v)) => {
            let v: VectorRef = (&v[DEFAULT_VECTOR_NAME]).into();
            let v: &[_] = v.try_into().unwrap();
            assert!((sqrt_distance(v) - 1.).abs() < 1e-5);
        }
        _ => panic!("unexpected vector type"),
    }

    // check that nearests are the same
    assert_eq!(nearest_upsert.id, nearest_update.id);
}
