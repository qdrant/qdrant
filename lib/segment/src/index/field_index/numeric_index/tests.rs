use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use tempfile::{Builder, TempDir};

use super::*;
use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
use crate::common::utils::MultiValue;

const COLUMN_NAME: &str = "test";

fn get_index() -> (TempDir, NumericIndex<f64>) {
    let temp_dir = Builder::new()
        .prefix("test_numeric_index")
        .tempdir()
        .unwrap();
    let db = open_db_with_existing_cf(temp_dir.path()).unwrap();
    let index: NumericIndex<_> = NumericIndex::new(db, COLUMN_NAME, true);
    index.recreate().unwrap();
    (temp_dir, index)
}

fn random_index(
    num_points: usize,
    values_per_point: usize,
    immutable: bool,
) -> (TempDir, NumericIndex<f64>) {
    let mut rng = StdRng::seed_from_u64(42);
    let (temp_dir, mut index) = get_index();

    for i in 0..num_points {
        let values = (0..values_per_point).map(|_| rng.gen_range(0.0..100.0));
        match &mut index {
            NumericIndex::Mutable(index) => index
                .add_many_to_list(i as PointOffsetType, values)
                .unwrap(),
            NumericIndex::Immutable(_) => unreachable!("index is mutable"),
        }
    }

    index.flusher()().unwrap();

    // if immutable, we have to reload the index
    if immutable {
        let db_ref = index.get_db_wrapper().database.clone();
        let mut new_index: NumericIndex<f64> = NumericIndex::new(db_ref, COLUMN_NAME, false);
        new_index.load().unwrap();
        (temp_dir, new_index)
    } else {
        (temp_dir, index)
    }
}

fn cardinality_request(index: &NumericIndex<f64>, query: Range) -> CardinalityEstimation {
    let estimation = index.range_cardinality(&query);

    let result = index
        .filter(&FieldCondition::new_range("".to_string(), query))
        .unwrap()
        .unique()
        .collect_vec();

    eprintln!("estimation = {estimation:#?}");
    eprintln!("result.len() = {:#?}", result.len());
    assert!(estimation.min <= result.len());
    assert!(estimation.max >= result.len());
    estimation
}

#[test]
fn test_set_empty_payload() {
    let (_temp_dir, mut index) = random_index(1000, 1, false);

    let point_id = 42;

    let value = index.get_values(point_id).unwrap();

    assert!(!value.is_empty());

    let payload = serde_json::json!(null);
    index
        .add_point(point_id, &MultiValue::one(&payload))
        .unwrap();

    let value = index.get_values(point_id).unwrap();

    assert!(value.is_empty());
}

#[rstest]
#[case(true)]
#[case(false)]
fn test_cardinality_exp(#[case] immutable: bool) {
    let (_temp_dir, index) = random_index(1000, 1, immutable);

    cardinality_request(
        &index,
        Range {
            lt: Some(20.0),
            gt: None,
            gte: Some(10.0),
            lte: None,
        },
    );
    cardinality_request(
        &index,
        Range {
            lt: Some(60.0),
            gt: None,
            gte: Some(10.0),
            lte: None,
        },
    );

    let (_temp_dir, index) = random_index(1000, 2, immutable);
    cardinality_request(
        &index,
        Range {
            lt: Some(20.0),
            gt: None,
            gte: Some(10.0),
            lte: None,
        },
    );
    cardinality_request(
        &index,
        Range {
            lt: Some(60.0),
            gt: None,
            gte: Some(10.0),
            lte: None,
        },
    );

    cardinality_request(
        &index,
        Range {
            lt: None,
            gt: None,
            gte: Some(10.0),
            lte: None,
        },
    );

    cardinality_request(
        &index,
        Range {
            lt: None,
            gt: None,
            gte: Some(110.0),
            lte: None,
        },
    );
}

#[rstest]
#[case(true)]
#[case(false)]
fn test_payload_blocks(#[case] immutable: bool) {
    let (_temp_dir, index) = random_index(1000, 2, immutable);
    let threshold = 100;
    let blocks = index
        .payload_blocks(threshold, "test".to_owned())
        .collect_vec();
    assert!(!blocks.is_empty());
    eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());

    let threshold = 500;
    let blocks = index
        .payload_blocks(threshold, "test".to_owned())
        .collect_vec();
    assert!(!blocks.is_empty());
    eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());

    let threshold = 1000;
    let blocks = index
        .payload_blocks(threshold, "test".to_owned())
        .collect_vec();
    assert!(!blocks.is_empty());
    eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());

    let threshold = 10000;
    let blocks = index
        .payload_blocks(threshold, "test".to_owned())
        .collect_vec();
    assert!(!blocks.is_empty());
    eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());
}

#[rstest]
#[case(true)]
#[case(false)]
fn test_payload_blocks_small(#[case] immutable: bool) {
    let (_temp_dir, mut index) = get_index();
    let threshold = 4;
    let values = vec![
        vec![1.0],
        vec![1.0],
        vec![1.0],
        vec![1.0],
        vec![1.0],
        vec![2.0],
        vec![2.0],
        vec![2.0],
        vec![2.0],
    ];

    values
        .into_iter()
        .enumerate()
        .for_each(|(idx, values)| match &mut index {
            NumericIndex::Mutable(index) => index
                .add_many_to_list(idx as PointOffsetType + 1, values)
                .unwrap(),
            NumericIndex::Immutable(_) => unreachable!("index is mutable"),
        });

    index.flusher()().unwrap();

    // if immutable, we have to reload the index
    let index = if immutable {
        let db_ref = index.get_db_wrapper().database.clone();
        let mut new_index: NumericIndex<f64> = NumericIndex::new(db_ref, COLUMN_NAME, false);
        new_index.load().unwrap();
        new_index
    } else {
        index
    };

    let blocks = index
        .payload_blocks(threshold, "test".to_owned())
        .collect_vec();
    assert!(!blocks.is_empty());
}

#[rstest]
#[case(true)]
#[case(false)]
fn test_numeric_index_load_from_disk(#[case] immutable: bool) {
    let (_temp_dir, mut index) = get_index();

    let values = vec![
        vec![1.0],
        vec![1.0],
        vec![1.0],
        vec![1.0],
        vec![1.0],
        vec![2.0],
        vec![2.5],
        vec![2.6],
        vec![3.0],
    ];

    values
        .into_iter()
        .enumerate()
        .for_each(|(idx, values)| match &mut index {
            NumericIndex::Mutable(index) => index
                .add_many_to_list(idx as PointOffsetType + 1, values)
                .unwrap(),
            NumericIndex::Immutable(_) => unreachable!("index is mutable"),
        });

    index.flusher()().unwrap();

    let db_ref = index.get_db_wrapper().database.clone();
    let mut new_index: NumericIndex<f64> = NumericIndex::new(db_ref, COLUMN_NAME, !immutable);
    new_index.load().unwrap();

    test_cond(
        &new_index,
        Range {
            gt: None,
            gte: None,
            lt: None,
            lte: Some(2.6),
        },
        vec![1, 2, 3, 4, 5, 6, 7, 8],
    );
}

#[rstest]
#[case(true)]
#[case(false)]
fn test_numeric_index(#[case] immutable: bool) {
    let (_temp_dir, mut index) = get_index();

    let values = vec![
        vec![1.0],
        vec![1.0],
        vec![1.0],
        vec![1.0],
        vec![1.0],
        vec![2.0],
        vec![2.5],
        vec![2.6],
        vec![3.0],
    ];

    values
        .into_iter()
        .enumerate()
        .for_each(|(idx, values)| match &mut index {
            NumericIndex::Mutable(index) => index
                .add_many_to_list(idx as PointOffsetType + 1, values)
                .unwrap(),
            NumericIndex::Immutable(_) => unreachable!("index is mutable"),
        });

    index.flusher()().unwrap();

    // if immutable, we have to reload the index
    let index = if immutable {
        let db_ref = index.get_db_wrapper().database.clone();
        let mut new_index: NumericIndex<f64> = NumericIndex::new(db_ref, COLUMN_NAME, false);
        new_index.load().unwrap();
        new_index
    } else {
        index
    };

    test_cond(
        &index,
        Range {
            gt: Some(1.0),
            gte: None,
            lt: None,
            lte: None,
        },
        vec![6, 7, 8, 9],
    );

    test_cond(
        &index,
        Range {
            gt: None,
            gte: Some(1.0),
            lt: None,
            lte: None,
        },
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
    );

    test_cond(
        &index,
        Range {
            gt: None,
            gte: None,
            lt: Some(2.6),
            lte: None,
        },
        vec![1, 2, 3, 4, 5, 6, 7],
    );

    test_cond(
        &index,
        Range {
            gt: None,
            gte: None,
            lt: None,
            lte: Some(2.6),
        },
        vec![1, 2, 3, 4, 5, 6, 7, 8],
    );

    test_cond(
        &index,
        Range {
            gt: None,
            gte: Some(2.0),
            lt: None,
            lte: Some(2.6),
        },
        vec![6, 7, 8],
    );
}

fn test_cond<T: Encodable + Numericable + PartialOrd + Clone>(
    index: &NumericIndex<T>,
    rng: Range,
    result: Vec<u32>,
) {
    let condition = FieldCondition {
        key: "".to_string(),
        r#match: None,
        range: Some(rng),
        geo_bounding_box: None,
        geo_radius: None,
        values_count: None,
        geo_polygon: None,
    };

    let offsets = index.filter(&condition).unwrap().collect_vec();

    assert_eq!(offsets, result);
}

// Check we don't panic on an empty index. See <https://github.com/qdrant/qdrant/pull/2933>.
#[rstest]
#[case(true)]
#[case(false)]
fn test_empty_cardinality(#[case] immutable: bool) {
    let (_temp_dir, index) = random_index(0, 1, immutable);
    cardinality_request(
        &index,
        Range {
            lt: Some(20.0),
            gt: None,
            gte: Some(10.0),
            lte: None,
        },
    );

    let (_temp_dir, index) = random_index(0, 0, immutable);
    cardinality_request(
        &index,
        Range {
            lt: Some(20.0),
            gt: None,
            gte: Some(10.0),
            lte: None,
        },
    );
}
