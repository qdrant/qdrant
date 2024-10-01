use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use tempfile::{Builder, TempDir};

use super::*;
use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
use crate::json_path::JsonPath;

const COLUMN_NAME: &str = "test";

#[derive(Clone, Copy)]
enum IndexType {
    Mutable,
    Immutable,
    Mmap,
}

enum IndexBuilder {
    Mutable(NumericIndexBuilder<FloatPayloadType, FloatPayloadType>),
    Immutable(NumericIndexImmutableBuilder<FloatPayloadType, FloatPayloadType>),
    Mmap(NumericIndexMmapBuilder<FloatPayloadType, FloatPayloadType>),
}

impl IndexBuilder {
    fn finalize(self) -> OperationResult<NumericIndex<FloatPayloadType, FloatPayloadType>> {
        match self {
            IndexBuilder::Mutable(builder) => builder.finalize(),
            IndexBuilder::Immutable(builder) => builder.finalize(),
            IndexBuilder::Mmap(builder) => builder.finalize(),
        }
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        match self {
            IndexBuilder::Mutable(builder) => builder.add_point(id, payload),
            IndexBuilder::Immutable(builder) => builder.add_point(id, payload),
            IndexBuilder::Mmap(builder) => builder.add_point(id, payload),
        }
    }
}

fn get_index_builder(index_type: IndexType) -> (TempDir, IndexBuilder) {
    let temp_dir = Builder::new()
        .prefix("test_numeric_index")
        .tempdir()
        .unwrap();
    let db = open_db_with_existing_cf(temp_dir.path()).unwrap();
    let mut builder = match index_type {
        IndexType::Mutable => IndexBuilder::Mutable(NumericIndex::<
            FloatPayloadType,
            FloatPayloadType,
        >::builder(db, COLUMN_NAME)),
        IndexType::Immutable => IndexBuilder::Immutable(NumericIndex::<
            FloatPayloadType,
            FloatPayloadType,
        >::builder_immutable(
            db, COLUMN_NAME
        )),
        IndexType::Mmap => IndexBuilder::Mmap(
            NumericIndex::<FloatPayloadType, FloatPayloadType>::builder_mmap(temp_dir.path()),
        ),
    };
    match &mut builder {
        IndexBuilder::Mutable(builder) => builder.init().unwrap(),
        IndexBuilder::Immutable(builder) => builder.init().unwrap(),
        IndexBuilder::Mmap(builder) => builder.init().unwrap(),
    }
    (temp_dir, builder)
}

fn random_index(
    num_points: usize,
    values_per_point: usize,
    index_type: IndexType,
) -> (TempDir, NumericIndex<FloatPayloadType, FloatPayloadType>) {
    let mut rng = StdRng::seed_from_u64(42);
    let (temp_dir, mut index_builder) = get_index_builder(index_type);

    for i in 0..num_points {
        let values = (0..values_per_point)
            .map(|_| Value::from(rng.gen_range(0.0..100.0)))
            .collect_vec();
        let values = values.iter().collect_vec();
        index_builder
            .add_point(i as PointOffsetType, &values)
            .unwrap();
    }

    let index = index_builder.finalize().unwrap();
    (temp_dir, index)
}

fn cardinality_request(
    index: &NumericIndex<FloatPayloadType, FloatPayloadType>,
    query: Range<FloatPayloadType>,
) -> CardinalityEstimation {
    let estimation = index
        .inner()
        .range_cardinality(&RangeInterface::Float(query.clone()));

    let result = index
        .inner()
        .filter(&FieldCondition::new_range(JsonPath::new("unused"), query))
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
    let (_temp_dir, mut index) = random_index(1000, 1, IndexType::Mutable);

    let point_id = 42;

    let values_count = index.inner().get_values(point_id).unwrap().count();

    assert_ne!(values_count, 0);

    let payload = serde_json::json!(null);
    index.add_point(point_id, &[&payload]).unwrap();

    let values_count = index.inner().get_values(point_id).unwrap().count();

    assert_eq!(values_count, 0);
}

#[rstest]
#[case(IndexType::Mutable)]
#[case(IndexType::Immutable)]
#[case(IndexType::Mmap)]
fn test_cardinality_exp(#[case] index_type: IndexType) {
    let (_temp_dir, index) = random_index(1000, 1, index_type);

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

    let (_temp_dir, index) = random_index(1000, 2, index_type);
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
#[case(IndexType::Mutable)]
#[case(IndexType::Immutable)]
#[case(IndexType::Mmap)]
fn test_payload_blocks(#[case] index_type: IndexType) {
    let (_temp_dir, index) = random_index(1000, 2, index_type);
    let threshold = 100;
    let blocks = index
        .inner()
        .payload_blocks(threshold, JsonPath::new("test"))
        .collect_vec();
    assert!(!blocks.is_empty());
    eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());

    let threshold = 500;
    let blocks = index
        .inner()
        .payload_blocks(threshold, JsonPath::new("test"))
        .collect_vec();
    assert!(!blocks.is_empty());
    eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());

    let threshold = 1000;
    let blocks = index
        .inner()
        .payload_blocks(threshold, JsonPath::new("test"))
        .collect_vec();
    assert!(!blocks.is_empty());
    eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());

    let threshold = 10000;
    let blocks = index
        .inner()
        .payload_blocks(threshold, JsonPath::new("test"))
        .collect_vec();
    assert!(!blocks.is_empty());
    eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());
}

#[rstest]
#[case(IndexType::Mutable)]
#[case(IndexType::Immutable)]
#[case(IndexType::Mmap)]
fn test_payload_blocks_small(#[case] index_type: IndexType) {
    let (_temp_dir, mut index_builder) = get_index_builder(index_type);
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

    values.into_iter().enumerate().for_each(|(idx, values)| {
        let values = values.iter().map(|v| Value::from(*v)).collect_vec();
        let values = values.iter().collect_vec();
        index_builder
            .add_point(idx as PointOffsetType + 1, &values)
            .unwrap();
    });
    let index = index_builder.finalize().unwrap();

    let blocks = index
        .inner()
        .payload_blocks(threshold, JsonPath::new("test"))
        .collect_vec();
    assert!(!blocks.is_empty());
}

#[rstest]
#[case(IndexType::Mutable)]
#[case(IndexType::Immutable)]
#[case(IndexType::Mmap)]
fn test_numeric_index_load_from_disk(#[case] index_type: IndexType) {
    let (temp_dir, mut index_builder) = get_index_builder(index_type);

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

    values.into_iter().enumerate().for_each(|(idx, values)| {
        let values = values.iter().map(|v| Value::from(*v)).collect_vec();
        let values = values.iter().collect_vec();
        index_builder
            .add_point(idx as PointOffsetType + 1, &values)
            .unwrap();
    });
    let index = index_builder.finalize().unwrap();

    let db = match index.inner() {
        NumericIndexInner::Mutable(index) => Some(index.get_db_wrapper().get_database()),
        NumericIndexInner::Immutable(index) => Some(index.get_db_wrapper().get_database()),
        NumericIndexInner::Mmap(_) => None,
    };
    drop(index);

    let mut new_index = match index_type {
        IndexType::Mutable => {
            NumericIndexInner::<FloatPayloadType>::new_memory(db.unwrap(), COLUMN_NAME, false)
        }
        IndexType::Immutable => {
            NumericIndexInner::<FloatPayloadType>::new_memory(db.unwrap(), COLUMN_NAME, true)
        }
        IndexType::Mmap => {
            NumericIndexInner::<FloatPayloadType>::new_mmap(temp_dir.path()).unwrap()
        }
    };
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
#[case(IndexType::Mutable)]
#[case(IndexType::Immutable)]
#[case(IndexType::Mmap)]
fn test_numeric_index(#[case] index_type: IndexType) {
    let (_temp_dir, mut index_builder) = get_index_builder(index_type);

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

    values.into_iter().enumerate().for_each(|(idx, values)| {
        let values = values.iter().map(|v| Value::from(*v)).collect_vec();
        let values = values.iter().collect_vec();
        index_builder
            .add_point(idx as PointOffsetType + 1, &values)
            .unwrap();
    });
    let index = index_builder.finalize().unwrap();

    test_cond(
        index.inner(),
        Range {
            gt: Some(1.0),
            gte: None,
            lt: None,
            lte: None,
        },
        vec![6, 7, 8, 9],
    );

    test_cond(
        index.inner(),
        Range {
            gt: None,
            gte: Some(1.0),
            lt: None,
            lte: None,
        },
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
    );

    test_cond(
        index.inner(),
        Range {
            gt: None,
            gte: None,
            lt: Some(2.6),
            lte: None,
        },
        vec![1, 2, 3, 4, 5, 6, 7],
    );

    test_cond(
        index.inner(),
        Range {
            gt: None,
            gte: None,
            lt: None,
            lte: Some(2.6),
        },
        vec![1, 2, 3, 4, 5, 6, 7, 8],
    );

    test_cond(
        index.inner(),
        Range {
            gt: None,
            gte: Some(2.0),
            lt: None,
            lte: Some(2.6),
        },
        vec![6, 7, 8],
    );
}

fn test_cond<T: Encodable + Numericable + PartialOrd + Clone + MmapValue + Default + 'static>(
    index: &NumericIndexInner<T>,
    rng: Range<FloatPayloadType>,
    result: Vec<u32>,
) {
    let condition = FieldCondition::new_range(JsonPath::new("unused"), rng);
    let offsets = index.filter(&condition).unwrap().collect_vec();
    assert_eq!(offsets, result);
}

// Check we don't panic on an empty index. See <https://github.com/qdrant/qdrant/pull/2933>.
#[rstest]
#[case(IndexType::Mutable)]
#[case(IndexType::Immutable)]
#[case(IndexType::Mmap)]
fn test_empty_cardinality(#[case] index_type: IndexType) {
    let (_temp_dir, index) = random_index(0, 1, index_type);
    cardinality_request(
        &index,
        Range {
            lt: Some(20.0),
            gt: None,
            gte: Some(10.0),
            lte: None,
        },
    );

    let (_temp_dir, index) = random_index(0, 0, index_type);
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
