use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use tempfile::{Builder, TempDir};

use super::*;
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
use crate::json_path::JsonPath;

#[cfg(feature = "rocksdb")]
const COLUMN_NAME: &str = "test";

#[derive(Clone, Copy)]
enum IndexType {
    #[cfg(feature = "rocksdb")]
    Mutable,
    MutableGridstore,
    #[cfg(feature = "rocksdb")]
    Immutable,
    Mmap,
    RamMmap,
}

enum IndexBuilder {
    #[cfg(feature = "rocksdb")]
    Mutable(NumericIndexBuilder<FloatPayloadType, FloatPayloadType>),
    MutableGridstore(NumericIndexGridstoreBuilder<FloatPayloadType, FloatPayloadType>),
    #[cfg(feature = "rocksdb")]
    Immutable(NumericIndexImmutableBuilder<FloatPayloadType, FloatPayloadType>),
    Mmap(NumericIndexMmapBuilder<FloatPayloadType, FloatPayloadType>),
}

impl IndexBuilder {
    fn finalize(self) -> OperationResult<NumericIndex<FloatPayloadType, FloatPayloadType>> {
        match self {
            #[cfg(feature = "rocksdb")]
            IndexBuilder::Mutable(builder) => builder.finalize(),
            IndexBuilder::MutableGridstore(builder) => builder.finalize(),
            #[cfg(feature = "rocksdb")]
            IndexBuilder::Immutable(builder) => builder.finalize(),
            IndexBuilder::Mmap(builder) => builder.finalize(),
        }
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            #[cfg(feature = "rocksdb")]
            IndexBuilder::Mutable(builder) => builder.add_point(id, payload, hw_counter),
            IndexBuilder::MutableGridstore(builder) => builder.add_point(id, payload, hw_counter),
            #[cfg(feature = "rocksdb")]
            IndexBuilder::Immutable(builder) => builder.add_point(id, payload, hw_counter),
            IndexBuilder::Mmap(builder) => builder.add_point(id, payload, hw_counter),
        }
    }
}

fn get_index_builder(index_type: IndexType) -> (TempDir, IndexBuilder) {
    let temp_dir = Builder::new()
        .prefix("test_numeric_index")
        .tempdir()
        .unwrap();
    #[cfg(feature = "rocksdb")]
    let db = open_db_with_existing_cf(temp_dir.path()).unwrap();
    let mut builder = match index_type {
        #[cfg(feature = "rocksdb")]
        IndexType::Mutable => IndexBuilder::Mutable(
            NumericIndex::<FloatPayloadType, FloatPayloadType>::builder_rocksdb(db, COLUMN_NAME)
                .unwrap(),
        ),
        IndexType::MutableGridstore => IndexBuilder::MutableGridstore(NumericIndex::<
            FloatPayloadType,
            FloatPayloadType,
        >::builder_gridstore(
            temp_dir.path().to_path_buf(),
        )),
        #[cfg(feature = "rocksdb")]
        IndexType::Immutable => IndexBuilder::Immutable(NumericIndex::<
            FloatPayloadType,
            FloatPayloadType,
        >::builder_rocksdb_immutable(
            db, COLUMN_NAME
        )),
        IndexType::Mmap | IndexType::RamMmap => IndexBuilder::Mmap(NumericIndex::<
            FloatPayloadType,
            FloatPayloadType,
        >::builder_mmap(
            temp_dir.path(), false
        )),
    };
    match &mut builder {
        #[cfg(feature = "rocksdb")]
        IndexBuilder::Mutable(builder) => builder.init().unwrap(),
        IndexBuilder::MutableGridstore(builder) => builder.init().unwrap(),
        #[cfg(feature = "rocksdb")]
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

    let hw_counter = HardwareCounterCell::new();

    for i in 0..num_points {
        let values = (0..values_per_point)
            .map(|_| Value::from(rng.random_range(0.0..100.0)))
            .collect_vec();
        let values = values.iter().collect_vec();
        index_builder
            .add_point(i as PointOffsetType, &values, &hw_counter)
            .unwrap();
    }

    let mut index = index_builder.finalize().unwrap();

    if matches!(index_type, IndexType::RamMmap) {
        let NumericIndexInner::Mmap(mmap_index) = index.inner else {
            panic!("Expected mmap index");
        };
        index = NumericIndex {
            inner: NumericIndexInner::Immutable(ImmutableNumericIndex::open_mmap(mmap_index)),
            _phantom: Default::default(),
        };
    }

    (temp_dir, index)
}

fn cardinality_request(
    index: &NumericIndex<FloatPayloadType, FloatPayloadType>,
    query: Range<FloatPayloadType>,
    hw_acc: HwMeasurementAcc,
) -> CardinalityEstimation {
    let hw_counter = hw_acc.get_counter_cell();

    let ordered_range = Range {
        lt: query.lt.map(OrderedFloat::from),
        gt: query.gt.map(OrderedFloat::from),
        gte: query.gte.map(OrderedFloat::from),
        lte: query.lte.map(OrderedFloat::from),
    };

    let estimation = index
        .inner()
        .range_cardinality(&RangeInterface::Float(ordered_range.clone()));

    let result = index
        .inner()
        .filter(
            &FieldCondition::new_range(JsonPath::new("unused"), ordered_range),
            &hw_counter,
        )
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
    let (_temp_dir, mut index) = random_index(1000, 1, IndexType::MutableGridstore);

    let point_id = 42;

    let values_count = index.inner().get_values(point_id).unwrap().count();

    assert_ne!(values_count, 0);

    let hw_counter = HardwareCounterCell::new();

    let payload = serde_json::json!(null);
    index.add_point(point_id, &[&payload], &hw_counter).unwrap();

    let values_count = index.inner().get_values(point_id).unwrap().count();

    assert_eq!(values_count, 0);
}

#[rstest]
#[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
#[case(IndexType::MutableGridstore)]
#[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
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
        HwMeasurementAcc::new(),
    );
    cardinality_request(
        &index,
        Range {
            lt: Some(60.0),
            gt: None,
            gte: Some(10.0),
            lte: None,
        },
        HwMeasurementAcc::new(),
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
        HwMeasurementAcc::new(),
    );
    cardinality_request(
        &index,
        Range {
            lt: Some(60.0),
            gt: None,
            gte: Some(10.0),
            lte: None,
        },
        HwMeasurementAcc::new(),
    );

    cardinality_request(
        &index,
        Range {
            lt: None,
            gt: None,
            gte: Some(10.0),
            lte: None,
        },
        HwMeasurementAcc::new(),
    );

    cardinality_request(
        &index,
        Range {
            lt: None,
            gt: None,
            gte: Some(110.0),
            lte: None,
        },
        HwMeasurementAcc::new(),
    );
}

#[rstest]
#[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
#[case(IndexType::MutableGridstore)]
#[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
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
#[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
#[case(IndexType::MutableGridstore)]
#[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
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

    let hw_counter = HardwareCounterCell::new();

    values.into_iter().enumerate().for_each(|(idx, values)| {
        let values = values.iter().map(|v| Value::from(*v)).collect_vec();
        let values = values.iter().collect_vec();
        index_builder
            .add_point(idx as PointOffsetType + 1, &values, &hw_counter)
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
#[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
#[case(IndexType::MutableGridstore)]
#[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
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

    let hw_counter = HardwareCounterCell::new();

    values.into_iter().enumerate().for_each(|(idx, values)| {
        let values = values.iter().map(|v| Value::from(*v)).collect_vec();
        let values = values.iter().collect_vec();
        index_builder
            .add_point(idx as PointOffsetType + 1, &values, &hw_counter)
            .unwrap();
    });
    let index = index_builder.finalize().unwrap();

    #[cfg(feature = "rocksdb")]
    let db = match index.inner() {
        NumericIndexInner::Mutable(index) => index.db_wrapper().map(|db| db.get_database()),
        NumericIndexInner::Immutable(index) => index.db_wrapper().map(|db| db.get_database()),
        NumericIndexInner::Mmap(_) => None,
    };
    drop(index);

    let new_index = match index_type {
        #[cfg(feature = "rocksdb")]
        IndexType::Mutable => {
            NumericIndexInner::<FloatPayloadType>::new_rocksdb(db.unwrap(), COLUMN_NAME, true, true)
                .unwrap()
                .unwrap()
        }
        IndexType::MutableGridstore => NumericIndexInner::<FloatPayloadType>::new_gridstore(
            temp_dir.path().to_path_buf(),
            true,
        )
        .unwrap()
        .unwrap(),
        #[cfg(feature = "rocksdb")]
        IndexType::Immutable => NumericIndexInner::<FloatPayloadType>::new_rocksdb(
            db.unwrap(),
            COLUMN_NAME,
            false,
            true,
        )
        .unwrap()
        .unwrap(),
        IndexType::Mmap => NumericIndexInner::<FloatPayloadType>::new_mmap(temp_dir.path(), true)
            .unwrap()
            .unwrap(),
        IndexType::RamMmap => {
            NumericIndexInner::<FloatPayloadType>::new_mmap(temp_dir.path(), false)
                .unwrap()
                .unwrap()
        }
    };

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
#[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
#[case(IndexType::MutableGridstore)]
#[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
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

    let hw_counter = HardwareCounterCell::new();

    values.into_iter().enumerate().for_each(|(idx, values)| {
        let values = values.iter().map(|v| Value::from(*v)).collect_vec();
        let values = values.iter().collect_vec();
        index_builder
            .add_point(idx as PointOffsetType + 1, &values, &hw_counter)
            .unwrap();
    });
    let mut index = index_builder.finalize().unwrap();

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

    // Remove some points
    index.remove_point(1).unwrap();
    index.remove_point(2).unwrap();
    index.remove_point(5).unwrap();

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
        vec![3, 4, 6, 7, 8, 9],
    );

    test_cond(
        index.inner(),
        Range {
            gt: None,
            gte: None,
            lt: Some(2.6),
            lte: None,
        },
        vec![3, 4, 6, 7],
    );

    test_cond(
        index.inner(),
        Range {
            gt: None,
            gte: None,
            lt: None,
            lte: Some(2.6),
        },
        vec![3, 4, 6, 7, 8],
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

fn test_cond<
    T: Encodable + Numericable + PartialOrd + Clone + MmapValue + Send + Sync + Default + 'static,
>(
    index: &NumericIndexInner<T>,
    rng: Range<FloatPayloadType>,
    result: Vec<u32>,
) where
    Vec<T>: Blob,
{
    let ordered_range = Range {
        lt: rng.lt.map(OrderedFloat::from),
        gt: rng.gt.map(OrderedFloat::from),
        gte: rng.gte.map(OrderedFloat::from),
        lte: rng.lte.map(OrderedFloat::from),
    };

    let condition = FieldCondition::new_range(JsonPath::new("unused"), ordered_range);
    let hw_acc = HwMeasurementAcc::new();
    let hw_counter = hw_acc.get_counter_cell();
    let offsets = index.filter(&condition, &hw_counter).unwrap().collect_vec();
    assert_eq!(offsets, result);
}

// Check we don't panic on an empty index. See <https://github.com/qdrant/qdrant/pull/2933>.
#[rstest]
#[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
#[case(IndexType::MutableGridstore)]
#[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
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
        HwMeasurementAcc::new(),
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
        HwMeasurementAcc::new(),
    );
}
