use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context, Result};
use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use fnv::FnvBuildHasher;
use fs_err as fs;
use indexmap::IndexSet;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use segment::data_types::facets::{FacetParams, FacetValue};
use segment::data_types::index::{
    FloatIndexParams, FloatIndexType, IntegerIndexParams, IntegerIndexType, KeywordIndexParams,
    KeywordIndexType, TextIndexParams, TextIndexType,
};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_context_fixture::FixtureIdTracker;
use segment::fixtures::payload_fixtures::{
    FLICKING_KEY, FLT_KEY, GEO_KEY, INT_KEY, INT_KEY_2, INT_KEY_3, LAT_RANGE, LON_RANGE, STR_KEY,
    STR_PROJ_KEY, STR_ROOT_PROJ_KEY, TEXT_KEY, generate_diverse_nested_payload,
    generate_diverse_payload, random_filter, random_nested_filter, random_vector,
};
use segment::index::PayloadIndex;
use segment::index::field_index::{FieldIndex, PrimaryCondition};
use segment::index::struct_payload_index::StructPayloadIndex;
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::payload_storage::PayloadStorage;
use segment::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::PayloadFieldSchema::{FieldParams, FieldType};
use segment::types::PayloadSchemaType::{Integer, Keyword};
use segment::types::{
    AnyVariants, Condition, Distance, FieldCondition, Filter, GeoBoundingBox, GeoLineString,
    GeoPoint, GeoPolygon, GeoRadius, HnswConfig, HnswGlobalConfig, Indexes, IsEmptyCondition,
    Match, Payload, PayloadField, PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType,
    Range, SegmentConfig, ValueVariants, VectorDataConfig, VectorStorageType, WithPayload,
};
use segment::utils::scored_point_ties::ScoredPointTies;
use tempfile::{Builder, TempDir};

macro_rules! here {
    () => {
        format!("at {}:{}", file!(), line!())
    };
}

/// `anyhow::ensure!` but with location, as what `assert!` would do
macro_rules! ensure {
    ($($arg:tt)*) => {
        (|| Ok(anyhow::ensure!($($arg)*)))().map_err(|e| {
            e.context(here!())
        })?
    };
}

const DIM: usize = 5;
const ATTEMPTS: usize = 20;

struct TestSegments {
    _base_dir: TempDir,
    struct_segment: Segment,
    plain_segment: Segment,
    mmap_segment: Segment,
}

impl TestSegments {
    fn new() -> Self {
        let base_dir = Builder::new().prefix("test_segments").tempdir().unwrap();

        let hw_counter = HardwareCounterCell::new();

        let mut rng = StdRng::seed_from_u64(42);

        let config = Self::make_simple_config(true);

        let mut plain_segment =
            build_segment(&base_dir.path().join("plain"), &config, true).unwrap();
        let mut struct_segment =
            build_segment(&base_dir.path().join("struct"), &config, true).unwrap();

        let num_points = 3000;
        let points_to_delete = 500;
        let points_to_clear = 500;

        let mut opnum = 0;
        struct_segment
            .create_field_index(
                opnum,
                &JsonPath::new(INT_KEY_2),
                Some(&Integer.into()),
                &hw_counter,
            )
            .unwrap();

        opnum += 1;
        for n in 0..num_points {
            let idx = n.into();
            let vector = random_vector(&mut rng, DIM);
            let payload: Payload = generate_diverse_payload(&mut rng);

            plain_segment
                .upsert_point(opnum, idx, only_default_vector(&vector), &hw_counter)
                .unwrap();
            struct_segment
                .upsert_point(opnum, idx, only_default_vector(&vector), &hw_counter)
                .unwrap();
            plain_segment
                .set_full_payload(opnum, idx, &payload, &hw_counter)
                .unwrap();
            struct_segment
                .set_full_payload(opnum, idx, &payload, &hw_counter)
                .unwrap();

            opnum += 1;
        }

        struct_segment
            .create_field_index(
                opnum,
                &JsonPath::new(STR_KEY),
                Some(&Keyword.into()),
                &hw_counter,
            )
            .unwrap();
        let int_payload_schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Integer);
        struct_segment
            .create_field_index(
                opnum,
                &JsonPath::new(INT_KEY),
                Some(&int_payload_schema),
                &hw_counter,
            )
            .unwrap();
        struct_segment
            .create_field_index(
                opnum,
                &JsonPath::new(INT_KEY_2),
                Some(&FieldParams(PayloadSchemaParams::Integer(
                    IntegerIndexParams {
                        r#type: IntegerIndexType::Integer,
                        lookup: Some(true),
                        range: Some(false),
                        is_principal: None,
                        on_disk: None,
                    },
                ))),
                &hw_counter,
            )
            .unwrap();
        struct_segment
            .create_field_index(
                opnum,
                &JsonPath::new(INT_KEY_3),
                Some(&FieldParams(PayloadSchemaParams::Integer(
                    IntegerIndexParams {
                        r#type: IntegerIndexType::Integer,
                        lookup: Some(false),
                        range: Some(true),
                        is_principal: None,
                        on_disk: None,
                    },
                ))),
                &hw_counter,
            )
            .unwrap();
        struct_segment
            .create_field_index(
                opnum,
                &JsonPath::new(GEO_KEY),
                Some(&PayloadSchemaType::Geo.into()),
                &hw_counter,
            )
            .unwrap();
        struct_segment
            .create_field_index(
                opnum,
                &JsonPath::new(TEXT_KEY),
                Some(&PayloadSchemaType::Text.into()),
                &hw_counter,
            )
            .unwrap();
        struct_segment
            .create_field_index(
                opnum,
                &JsonPath::new(FLICKING_KEY),
                Some(&Integer.into()),
                &hw_counter,
            )
            .unwrap();

        // Make mmap segment after inserting the points, but before deleting some of them
        let mut mmap_segment =
            Self::make_mmap_segment(&mut rng, &base_dir.path().join("mmap"), &plain_segment);

        for _ in 0..points_to_clear {
            opnum += 1;
            let idx_to_remove = rng.random_range(0..num_points);
            plain_segment
                .clear_payload(opnum, idx_to_remove.into(), &hw_counter)
                .unwrap();
            struct_segment
                .clear_payload(opnum, idx_to_remove.into(), &hw_counter)
                .unwrap();
            mmap_segment
                .clear_payload(opnum, idx_to_remove.into(), &hw_counter)
                .unwrap();
        }

        for _ in 0..points_to_delete {
            opnum += 1;
            let idx_to_remove = rng.random_range(0..num_points);
            plain_segment
                .delete_point(opnum, idx_to_remove.into(), &hw_counter)
                .unwrap();
            struct_segment
                .delete_point(opnum, idx_to_remove.into(), &hw_counter)
                .unwrap();
            mmap_segment
                .delete_point(opnum, idx_to_remove.into(), &hw_counter)
                .unwrap();
        }

        for (field, indexes) in struct_segment.payload_index.borrow().field_indexes.iter() {
            for index in indexes {
                assert!(index.count_indexed_points() <= num_points as usize);
                if field.to_string() != FLICKING_KEY {
                    assert!(
                        index.count_indexed_points()
                            >= (num_points as usize - points_to_delete - points_to_clear)
                    );
                }
            }
        }

        Self {
            _base_dir: base_dir,
            struct_segment,
            plain_segment,
            mmap_segment,
        }
    }

    fn make_simple_config(appendable: bool) -> SegmentConfig {
        let conf = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: DIM,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::default(),
                    index: if appendable {
                        Indexes::Plain {}
                    } else {
                        Indexes::Hnsw(HnswConfig::default())
                    },
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };
        assert_eq!(conf.is_appendable(), appendable);
        conf
    }

    fn make_mmap_segment(rng: &mut StdRng, path: &Path, plain_segment: &Segment) -> Segment {
        let stopped = AtomicBool::new(false);
        fs::create_dir(path).unwrap();

        let mut builder = SegmentBuilder::new(
            path,
            &path.with_extension("tmp"),
            &Self::make_simple_config(false),
            &HnswGlobalConfig::default(),
        )
        .unwrap();

        builder.update(&[plain_segment], &stopped).unwrap();
        let permit = ResourcePermit::dummy(1);
        let hw_counter = HardwareCounterCell::new();

        let mut segment = builder.build(permit, &stopped, rng, &hw_counter).unwrap();
        let opnum = segment.version() + 1;

        segment
            .create_field_index(
                opnum,
                &JsonPath::new(STR_KEY),
                Some(&FieldParams(PayloadSchemaParams::Keyword(
                    KeywordIndexParams {
                        r#type: KeywordIndexType::Keyword,
                        is_tenant: None,
                        on_disk: Some(true),
                    },
                ))),
                &hw_counter,
            )
            .unwrap();
        segment
            .create_field_index(
                opnum,
                &JsonPath::new(INT_KEY),
                Some(&FieldParams(PayloadSchemaParams::Integer(
                    IntegerIndexParams {
                        r#type: IntegerIndexType::Integer,
                        lookup: Some(true),
                        range: Some(true),
                        is_principal: None,
                        on_disk: Some(true),
                    },
                ))),
                &hw_counter,
            )
            .unwrap();
        segment
            .create_field_index(
                opnum,
                &JsonPath::new(INT_KEY_2),
                Some(&FieldParams(PayloadSchemaParams::Integer(
                    IntegerIndexParams {
                        r#type: IntegerIndexType::Integer,
                        lookup: Some(true),
                        range: Some(false),
                        is_principal: None,
                        on_disk: Some(true),
                    },
                ))),
                &hw_counter,
            )
            .unwrap();
        segment
            .create_field_index(
                opnum,
                &JsonPath::new(INT_KEY_3),
                Some(&FieldParams(PayloadSchemaParams::Integer(
                    IntegerIndexParams {
                        r#type: IntegerIndexType::Integer,
                        lookup: Some(false),
                        range: Some(true),
                        is_principal: None,
                        on_disk: Some(true),
                    },
                ))),
                &hw_counter,
            )
            .unwrap();
        segment
            .create_field_index(
                opnum,
                &JsonPath::new(FLT_KEY),
                Some(&FieldParams(PayloadSchemaParams::Float(FloatIndexParams {
                    r#type: FloatIndexType::Float,
                    is_principal: None,
                    on_disk: Some(true),
                }))),
                &hw_counter,
            )
            .unwrap();
        segment
            .create_field_index(
                opnum,
                &JsonPath::new(TEXT_KEY),
                Some(&FieldParams(PayloadSchemaParams::Text(TextIndexParams {
                    r#type: TextIndexType::Text,
                    on_disk: Some(true),
                    ..Default::default()
                }))),
                &hw_counter,
            )
            .unwrap();

        segment
    }
}

fn build_test_segments_nested_payload(path_struct: &Path, path_plain: &Path) -> (Segment, Segment) {
    let mut rng = StdRng::seed_from_u64(42);

    let mut plain_segment = build_simple_segment(path_plain, DIM, Distance::Dot).unwrap();
    let mut struct_segment = build_simple_segment(path_struct, DIM, Distance::Dot).unwrap();

    let num_points = 3000;
    let points_to_delete = 500;
    let points_to_clear = 500;

    // Nested payload keys
    let nested_str_key = JsonPath::new(&format!("{}.{}.{}", STR_KEY, "nested_1", "nested_2"));
    let nested_str_proj_key =
        JsonPath::new(&format!("{}.{}[].{}", STR_PROJ_KEY, "nested_1", "nested_2"));
    let deep_nested_str_proj_key = JsonPath::new(&format!(
        "{}[].{}[].{}",
        STR_ROOT_PROJ_KEY, "nested_1", "nested_2"
    ));

    let hw_counter = HardwareCounterCell::new();

    let mut opnum = 0;
    struct_segment
        .create_field_index(opnum, &nested_str_key, Some(&Keyword.into()), &hw_counter)
        .unwrap();

    struct_segment
        .create_field_index(
            opnum,
            &nested_str_proj_key,
            Some(&Keyword.into()),
            &hw_counter,
        )
        .unwrap();

    struct_segment
        .create_field_index(
            opnum,
            &deep_nested_str_proj_key,
            Some(&Keyword.into()),
            &hw_counter,
        )
        .unwrap();

    eprintln!("{deep_nested_str_proj_key}");

    opnum += 1;
    for n in 0..num_points {
        let idx = n.into();
        let vector = random_vector(&mut rng, DIM);
        let payload: Payload = generate_diverse_nested_payload(&mut rng);

        plain_segment
            .upsert_point(opnum, idx, only_default_vector(&vector), &hw_counter)
            .unwrap();
        struct_segment
            .upsert_point(opnum, idx, only_default_vector(&vector), &hw_counter)
            .unwrap();
        plain_segment
            .set_full_payload(opnum, idx, &payload, &hw_counter)
            .unwrap();
        struct_segment
            .set_full_payload(opnum, idx, &payload, &hw_counter)
            .unwrap();

        opnum += 1;
    }

    for _ in 0..points_to_clear {
        opnum += 1;
        let idx_to_remove = rng.random_range(0..num_points);
        plain_segment
            .clear_payload(opnum, idx_to_remove.into(), &hw_counter)
            .unwrap();
        struct_segment
            .clear_payload(opnum, idx_to_remove.into(), &hw_counter)
            .unwrap();
    }

    for _ in 0..points_to_delete {
        opnum += 1;
        let idx_to_remove = rng.random_range(0..num_points);
        plain_segment
            .delete_point(opnum, idx_to_remove.into(), &hw_counter)
            .unwrap();
        struct_segment
            .delete_point(opnum, idx_to_remove.into(), &hw_counter)
            .unwrap();
    }

    for (_field, indexes) in struct_segment.payload_index.borrow().field_indexes.iter() {
        for index in indexes {
            assert!(index.count_indexed_points() <= num_points as usize);
            assert!(
                index.count_indexed_points()
                    > (num_points as usize - points_to_delete - points_to_clear)
            );
        }
    }

    (struct_segment, plain_segment)
}

fn validate_geo_filter(test_segments: &TestSegments, query_filter: Filter) -> Result<()> {
    let mut rng = rand::rng();

    for _i in 0..ATTEMPTS {
        let query = random_vector(&mut rng, DIM).into();
        let plain_result = test_segments
            .plain_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query,
                &WithPayload::default(),
                &false.into(),
                Some(&query_filter),
                5,
                None,
            )
            .unwrap();

        let hw_counter = HardwareCounterCell::new();
        let estimation = test_segments
            .plain_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&query_filter, &hw_counter);

        ensure!(estimation.min <= estimation.exp, "{estimation:#?}");
        ensure!(estimation.exp <= estimation.max, "{estimation:#?}");
        ensure!(
            estimation.max
                <= test_segments
                    .struct_segment
                    .id_tracker
                    .borrow()
                    .available_point_count(),
            "{estimation:#?}",
        );

        let struct_result = test_segments
            .struct_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query,
                &WithPayload::default(),
                &false.into(),
                Some(&query_filter),
                5,
                None,
            )
            .unwrap();

        let estimation = test_segments
            .struct_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&query_filter, &hw_counter);

        ensure!(estimation.min <= estimation.exp, "{estimation:#?}");
        ensure!(estimation.exp <= estimation.max, "{estimation:#?}");
        ensure!(
            estimation.max
                <= test_segments
                    .struct_segment
                    .id_tracker
                    .borrow()
                    .available_point_count(),
            "{estimation:#?}",
        );

        for (r1, r2) in plain_result.iter().zip(struct_result.iter()) {
            ensure!(r1.id == r2.id);
            ensure!((r1.score - r2.score) < 0.0001)
        }
    }

    Ok(())
}

/// Test read operations on segments.
/// The segments fixtures are created only once to improve test speed.
#[test]
fn test_read_operations() -> Result<()> {
    let test_segments = Arc::new(TestSegments::new());
    let mut handles = vec![];

    for test_fn in [
        test_is_empty_conditions,
        test_integer_index_types,
        test_cardinality_estimation,
        test_struct_payload_index,
        test_struct_payload_geo_boundingbox_index,
        test_struct_payload_geo_radius_index,
        test_struct_payload_geo_polygon_index,
        test_any_matcher_cardinality_estimation,
        test_struct_keyword_facet,
        test_mmap_keyword_facet,
        test_struct_keyword_facet_filtered,
        test_mmap_keyword_facet_filtered,
    ] {
        let segments = Arc::clone(&test_segments);
        handles.push(std::thread::spawn(move || test_fn(&segments)));
    }

    for handle in handles {
        handle.join().unwrap()?;
    }

    Ok(())
}

fn test_is_empty_conditions(test_segments: &TestSegments) -> Result<()> {
    let filter = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
        is_empty: PayloadField {
            key: JsonPath::new(FLICKING_KEY),
        },
    }));

    let hw_counter = HardwareCounterCell::new();

    let estimation_struct = test_segments
        .struct_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter, &hw_counter);

    let estimation_plain = test_segments
        .plain_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter, &hw_counter);

    let plain_result = test_segments
        .plain_segment
        .payload_index
        .borrow()
        .query_points(&filter, &hw_counter);

    let real_number = plain_result.len();

    let id_tracker = test_segments.struct_segment.id_tracker.borrow();
    let struct_result = test_segments
        .struct_segment
        .payload_index
        .borrow()
        .query_points(&filter, &hw_counter)
        .into_iter()
        // null index does not track deleted points, so we need to filter them out here. In callsites,
        // the deleted check is done externally anyway
        .filter(|id| !id_tracker.is_deleted_point(*id))
        .collect::<Vec<_>>();

    ensure!(plain_result == struct_result);

    eprintln!("estimation_plain = {estimation_plain:#?}");
    eprintln!("estimation_struct = {estimation_struct:#?}");
    eprintln!("real_number = {real_number:#?}");

    ensure!(estimation_plain.max >= real_number);
    ensure!(estimation_plain.min <= real_number);

    ensure!(estimation_struct.max >= real_number);
    ensure!(estimation_struct.min <= real_number);

    ensure!(
        (estimation_struct.exp as f64 - real_number as f64).abs()
            <= (estimation_plain.exp as f64 - real_number as f64).abs()
    );

    Ok(())
}

fn test_integer_index_types(test_segments: &TestSegments) -> Result<()> {
    for (kind, indexes) in [
        (
            "struct",
            &test_segments.struct_segment.payload_index.borrow(),
        ),
        ("mmap", &test_segments.mmap_segment.payload_index.borrow()),
    ] {
        eprintln!("Checking {kind}_segment");
        let field_indexes = indexes.field_indexes.get(&JsonPath::new(INT_KEY)).unwrap();

        let has_map_index = field_indexes
            .iter()
            .any(|index| matches!(index, FieldIndex::IntMapIndex(_)));
        let has_int_index = field_indexes
            .iter()
            .any(|index| matches!(index, FieldIndex::IntIndex(_)));

        ensure!(has_map_index);
        ensure!(has_int_index);

        let field_indexes = indexes
            .field_indexes
            .get(&JsonPath::new(INT_KEY_2))
            .unwrap();

        let has_map_index = field_indexes
            .iter()
            .any(|index| matches!(index, FieldIndex::IntMapIndex(_)));
        let has_int_index = field_indexes
            .iter()
            .any(|index| matches!(index, FieldIndex::IntIndex(_)));

        ensure!(has_map_index);
        ensure!(!has_int_index);

        let field_indexes = indexes
            .field_indexes
            .get(&JsonPath::new(INT_KEY_3))
            .unwrap();

        let has_map_index = field_indexes
            .iter()
            .any(|index| matches!(index, FieldIndex::IntMapIndex(_)));
        let has_int_index = field_indexes
            .iter()
            .any(|index| matches!(index, FieldIndex::IntIndex(_)));

        ensure!(!has_map_index);
        ensure!(has_int_index);
    }
    Ok(())
}

fn test_cardinality_estimation(test_segments: &TestSegments) -> Result<()> {
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
        JsonPath::new(INT_KEY),
        Range {
            lt: None,
            gt: None,
            gte: Some(OrderedFloat(50.)),
            lte: Some(OrderedFloat(100.)),
        },
    )));

    let hw_counter = HardwareCounterCell::new();

    let estimation = test_segments
        .struct_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter, &hw_counter);

    let hw_counter = HardwareCounterCell::new();

    let payload_index = test_segments.struct_segment.payload_index.borrow();
    let filter_context = payload_index.filter_context(&filter, &hw_counter);
    let exact = test_segments
        .struct_segment
        .id_tracker
        .borrow()
        .iter_ids()
        .filter(|x| filter_context.check(*x))
        .collect_vec()
        .len();

    eprintln!("exact = {exact:#?}");
    eprintln!("estimation = {estimation:#?}");

    ensure!(exact <= estimation.max);
    ensure!(exact >= estimation.min);

    Ok(())
}

#[test]
fn test_root_nested_array_filter_cardinality_estimation() {
    let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

    let (struct_segment, _) = build_test_segments_nested_payload(dir1.path(), dir2.path());

    // rely on test data from `build_test_segments_nested_payload`
    let nested_key = "nested_1[].nested_2";
    let nested_match =
        FieldCondition::new_match(JsonPath::new(nested_key), "some value".to_owned().into());
    let filter = Filter::new_must(Condition::new_nested(
        JsonPath::new(STR_ROOT_PROJ_KEY),
        Filter::new_must(Condition::Field(nested_match)),
    ));

    let hw_counter = HardwareCounterCell::new();

    let estimation = struct_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter, &hw_counter);

    // not empty primary clauses
    assert_eq!(estimation.primary_clauses.len(), 1);
    eprintln!("primary_clauses = {:#?}", estimation.primary_clauses);
    let primary_clause = estimation.primary_clauses.first().unwrap();

    let expected_primary_clause = FieldCondition::new_match(
        JsonPath::new(&format!("{STR_ROOT_PROJ_KEY}[].{nested_key}")), // full key expected
        "some value".to_owned().into(),
    );

    match primary_clause {
        PrimaryCondition::Condition(field_condition) => {
            assert_eq!(*field_condition, Box::new(expected_primary_clause));
        }
        o => panic!("unexpected primary clause: {o:?}"),
    }

    let hw_counter = HardwareCounterCell::new();

    let payload_index = struct_segment.payload_index.borrow();
    let filter_context = payload_index.filter_context(&filter, &hw_counter);
    let exact = struct_segment
        .id_tracker
        .borrow()
        .iter_ids()
        .filter(|x| filter_context.check(*x))
        .collect_vec()
        .len();

    eprintln!("exact = {exact:#?}");
    eprintln!("estimation = {estimation:#?}");

    assert!(exact <= estimation.max);
    assert!(exact >= estimation.min);
}

#[test]
fn test_nesting_nested_array_filter_cardinality_estimation() {
    let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

    let (struct_segment, _) = build_test_segments_nested_payload(dir1.path(), dir2.path());

    // rely on test data from `build_test_segments_nested_payload`
    let nested_match_key = "nested_2";
    let nested_match = FieldCondition::new_match(
        JsonPath::new(nested_match_key),
        "some value".to_owned().into(),
    );
    let filter = Filter::new_must(Condition::new_nested(
        JsonPath::new(STR_ROOT_PROJ_KEY),
        Filter::new_must(Condition::new_nested(
            JsonPath::new("nested_1"),
            Filter::new_must(Condition::Field(nested_match)),
        )),
    ));

    let hw_counter = HardwareCounterCell::new();

    let estimation = struct_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter, &hw_counter);

    // not empty primary clauses
    assert_eq!(estimation.primary_clauses.len(), 1);
    eprintln!("primary_clauses = {:#?}", estimation.primary_clauses);
    let primary_clause = estimation.primary_clauses.first().unwrap();

    let expected_primary_clause = FieldCondition::new_match(
        // full key expected
        JsonPath::new(&format!(
            "{STR_ROOT_PROJ_KEY}[].nested_1[].{nested_match_key}"
        )),
        "some value".to_owned().into(),
    );

    match primary_clause {
        PrimaryCondition::Condition(field_condition) => {
            assert_eq!(*field_condition, Box::new(expected_primary_clause));
        }
        o => panic!("unexpected primary clause: {o:?}"),
    }

    let hw_counter = HardwareCounterCell::new();

    let payload_index = struct_segment.payload_index.borrow();
    let filter_context = payload_index.filter_context(&filter, &hw_counter);
    let exact = struct_segment
        .id_tracker
        .borrow()
        .iter_ids()
        .filter(|x| filter_context.check(*x))
        .collect_vec()
        .len();

    eprintln!("exact = {exact:#?}");
    eprintln!("estimation = {estimation:#?}");

    assert!(exact <= estimation.max);
    assert!(exact >= estimation.min);
}

/// Compare search with plain, struct, and mmap indices.
fn test_struct_payload_index(test_segments: &TestSegments) -> Result<()> {
    let mut rng = rand::rng();

    for _i in 0..ATTEMPTS {
        let query_vector = random_vector(&mut rng, DIM).into();
        let query_filter = random_filter(&mut rng, 3);

        let plain_result = test_segments
            .plain_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                Some(&query_filter),
                5,
                None,
            )
            .unwrap();
        let struct_result = test_segments
            .struct_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                Some(&query_filter),
                5,
                None,
            )
            .unwrap();
        let mmap_result = test_segments
            .mmap_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                Some(&query_filter),
                5,
                None,
            )
            .unwrap();

        let hw_counter = HardwareCounterCell::new();

        let estimation = test_segments
            .struct_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&query_filter, &hw_counter);

        ensure!(estimation.min <= estimation.exp, "{estimation:#?}");
        ensure!(estimation.exp <= estimation.max, "{estimation:#?}");
        ensure!(
            estimation.max
                <= test_segments
                    .struct_segment
                    .id_tracker
                    .borrow()
                    .available_point_count(),
            "{estimation:#?}",
        );

        // Perform additional sort to break ties by score
        let mut plain_result_sorted_ties: Vec<ScoredPointTies> =
            plain_result.iter().map(|x| x.into()).collect_vec();
        plain_result_sorted_ties.sort();

        let mut struct_result_sorted_ties: Vec<ScoredPointTies> =
            struct_result.iter().map(|x| x.into()).collect_vec();
        struct_result_sorted_ties.sort();

        let mut mmap_result_sorted_ties: Vec<ScoredPointTies> =
            mmap_result.iter().map(|x| x.into()).collect_vec();
        mmap_result_sorted_ties.sort();

        ensure!(
            plain_result_sorted_ties.len() == struct_result_sorted_ties.len(),
            "query vector {query_vector:?}\n\
            query filter {query_filter:?}\n\
            plain result {plain_result:?}\n\
            struct result{struct_result:?}",
        );
        ensure!(
            plain_result_sorted_ties.len() == mmap_result_sorted_ties.len(),
            "query vector {query_vector:?}\n\
            query filter {query_filter:?}\n\
            plain result {plain_result:?}\n\
            mmap result  {mmap_result:?}",
        );

        for (r1, r2, r3) in itertools::izip!(
            plain_result_sorted_ties,
            struct_result_sorted_ties,
            mmap_result_sorted_ties,
        )
        .map(|(r1, r2, r3)| (r1.0, r2.0, r3.0))
        {
            ensure!(
                r1.id == r2.id,
                "got different ScoredPoint {r1:?} and {r2:?} for\n\
                query vector {query_vector:?}\n\
                query filter {query_filter:?}\n\
                plain result {plain_result:?}\n\
                struct result{struct_result:?}"
            );
            ensure!((r1.score - r2.score) < 0.0001);
            ensure!(
                r1.id == r3.id,
                "got different ScoredPoint {r1:?} and {r3:?} for\n\
                query vector {query_vector:?}\n\
                query filter {query_filter:?}\n\
                plain result {plain_result:?}\n\
                mmap result  {mmap_result:?}",
            );
            ensure!((r1.score - r3.score) < 0.0001);
        }
    }
    Ok(())
}

fn test_struct_payload_geo_boundingbox_index(test_segments: &TestSegments) -> Result<()> {
    let mut rng = rand::rng();

    let geo_bbox = GeoBoundingBox {
        top_left: GeoPoint::new_unchecked(rng.random_range(LON_RANGE), rng.random_range(LAT_RANGE)),
        bottom_right: GeoPoint::new_unchecked(
            rng.random_range(LON_RANGE),
            rng.random_range(LAT_RANGE),
        ),
    };

    let condition = Condition::Field(FieldCondition::new_geo_bounding_box(
        JsonPath::new("geo_key"),
        geo_bbox,
    ));

    let query_filter = Filter::new_must(condition);

    validate_geo_filter(test_segments, query_filter).context(here!())
}

fn test_struct_payload_geo_radius_index(test_segments: &TestSegments) -> Result<()> {
    let mut rng = rand::rng();

    let r_meters = rng.random_range(1.0..10000.0);
    let geo_radius = GeoRadius {
        center: GeoPoint::new_unchecked(rng.random_range(LON_RANGE), rng.random_range(LAT_RANGE)),
        radius: OrderedFloat(r_meters),
    };

    let condition = Condition::Field(FieldCondition::new_geo_radius(
        JsonPath::new("geo_key"),
        geo_radius,
    ));

    let query_filter = Filter::new_must(condition);

    validate_geo_filter(test_segments, query_filter).context(here!())
}

fn test_struct_payload_geo_polygon_index(test_segments: &TestSegments) -> Result<()> {
    let polygon_edge = 5;
    let interiors_num = 3;

    fn generate_ring(polygon_edge: i32) -> GeoLineString {
        let mut rng = rand::rng();
        let mut line = GeoLineString {
            points: (0..polygon_edge)
                .map(|_| {
                    GeoPoint::new_unchecked(
                        rng.random_range(LON_RANGE),
                        rng.random_range(LAT_RANGE),
                    )
                })
                .collect(),
        };
        line.points.push(line.points[0]); // add last point that is identical to the first
        line
    }

    let exterior = generate_ring(polygon_edge);
    let interiors = Some(
        std::iter::repeat_with(|| generate_ring(polygon_edge))
            .take(interiors_num)
            .collect(),
    );

    let geo_polygon = GeoPolygon {
        exterior,
        interiors,
    };

    let condition = Condition::Field(FieldCondition::new_geo_polygon(
        JsonPath::new("geo_key"),
        geo_polygon,
    ));

    let query_filter = Filter::new_must(condition);

    validate_geo_filter(test_segments, query_filter).context(here!())
}

#[test]
fn test_struct_payload_index_nested_fields() {
    // Compare search with plain and struct indexes
    let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

    let mut rng = rand::rng();

    let (struct_segment, plain_segment) =
        build_test_segments_nested_payload(dir1.path(), dir2.path());

    let attempts = 100;
    for _i in 0..attempts {
        let query_vector = random_vector(&mut rng, DIM).into();
        let query_filter = random_nested_filter(&mut rng);
        let plain_result = plain_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload {
                    enable: true,
                    payload_selector: None,
                },
                &false.into(),
                Some(&query_filter),
                5,
                None,
            )
            .unwrap();
        let struct_result = struct_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload {
                    enable: true,
                    payload_selector: None,
                },
                &false.into(),
                Some(&query_filter),
                5,
                None,
            )
            .unwrap();

        let hw_counter = HardwareCounterCell::new();

        let estimation = struct_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&query_filter, &hw_counter);

        assert!(estimation.min <= estimation.exp, "{estimation:#?}");
        assert!(estimation.exp <= estimation.max, "{estimation:#?}");
        assert!(
            estimation.max <= struct_segment.id_tracker.borrow().available_point_count(),
            "{estimation:#?}",
        );

        // warning: report flakiness at https://github.com/qdrant/qdrant/issues/534
        plain_result
            .iter()
            .zip(struct_result.iter())
            .for_each(|(r1, r2)| {
                assert_eq!(
                    r1.id, r2.id,
                    "got different ScoredPoint {r1:?} and {r2:?} for\n\
                    query vector {query_vector:?}\n\
                    query filter {query_filter:?}\n\
                    plain result {plain_result:?}\n\
                    struct result{struct_result:?}"
                );
                assert!((r1.score - r2.score) < 0.0001)
            });
    }
}

#[test]
fn test_update_payload_index_type() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut payload_storage = InMemoryPayloadStorage::default();

    let point_num = 10;
    let mut points = HashMap::new();

    let mut payloads: Vec<Payload> = vec![];
    for i in 0..point_num {
        payloads.push(payload_json! {"field": i});
    }

    let hw_counter = HardwareCounterCell::new();

    for (idx, payload) in payloads.into_iter().enumerate() {
        points.insert(idx, payload.clone());
        payload_storage
            .set(idx as PointOffsetType, &payload, &hw_counter)
            .unwrap();
    }

    let wrapped_payload_storage = Arc::new(AtomicRefCell::new(payload_storage.into()));
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(point_num)));

    let mut index = StructPayloadIndex::open(
        wrapped_payload_storage,
        id_tracker,
        HashMap::new(),
        dir.path(),
        true,
        true,
    )
    .unwrap();

    let field = JsonPath::new("field");

    // set field to Integer type
    index.set_indexed(&field, Integer, &hw_counter).unwrap();
    assert_eq!(
        *index.indexed_fields().get(&field).unwrap(),
        FieldType(Integer)
    );
    let field_index = index.field_indexes.get(&field).unwrap();
    assert_eq!(field_index[0].count_indexed_points(), point_num);
    assert_eq!(field_index[1].count_indexed_points(), point_num);

    // update field to Keyword type
    index.set_indexed(&field, Keyword, &hw_counter).unwrap();
    assert_eq!(
        *index.indexed_fields().get(&field).unwrap(),
        FieldType(Keyword)
    );
    let field_index = index.field_indexes.get(&field).unwrap();
    assert_eq!(field_index[0].count_indexed_points(), 0); // only one field index for Keyword

    // set field to Integer type (again)
    index.set_indexed(&field, Integer, &hw_counter).unwrap();
    assert_eq!(
        *index.indexed_fields().get(&field).unwrap(),
        FieldType(Integer)
    );
    let field_index = index.field_indexes.get(&field).unwrap();
    assert_eq!(field_index[0].count_indexed_points(), point_num);
    assert_eq!(field_index[1].count_indexed_points(), point_num);
}

fn test_any_matcher_cardinality_estimation(test_segments: &TestSegments) -> Result<()> {
    let keywords: IndexSet<String, FnvBuildHasher> = ["value1", "value2"]
        .iter()
        .map(|&i| i.to_string())
        .collect();
    let any_match = FieldCondition::new_match(
        JsonPath::new(STR_KEY),
        Match::new_any(AnyVariants::Strings(keywords)),
    );

    let filter = Filter::new_must(Condition::Field(any_match.clone()));

    let hw_counter = HardwareCounterCell::new();

    let estimation = test_segments
        .struct_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter, &hw_counter);

    ensure!(estimation.primary_clauses.len() == 1);
    for clause in estimation.primary_clauses.iter() {
        let expected_primary_clause = any_match.clone();

        match clause {
            PrimaryCondition::Condition(field_condition) => {
                ensure!(*field_condition == Box::new(expected_primary_clause));
            }
            o => panic!("unexpected primary clause: {o:?}"),
        }
    }

    let hw_counter = HardwareCounterCell::new();

    let payload_index = test_segments.struct_segment.payload_index.borrow();
    let filter_context = payload_index.filter_context(&filter, &hw_counter);
    let exact = test_segments
        .struct_segment
        .id_tracker
        .borrow()
        .iter_ids()
        .filter(|x| filter_context.check(*x))
        .collect_vec()
        .len();

    eprintln!("exact = {exact:#?}");
    eprintln!("estimation = {estimation:#?}");

    ensure!(exact <= estimation.max);
    ensure!(exact >= estimation.min);

    Ok(())
}

/// FacetParams fixture without a filter
fn keyword_facet_request() -> FacetParams {
    let limit = 1000;
    let key: JsonPath = STR_KEY.try_into().unwrap();
    let exact = false; // This is only used at local shard level

    // *** Without filter ***
    FacetParams {
        key: key.clone(),
        limit,
        filter: None,
        exact,
    }
}

/// Checks that the counts are the same as counting each value exactly.
fn validate_facet_result(
    segment: &Segment,
    facet_hits: HashMap<FacetValue, usize>,
    filter: Option<Filter>,
) -> Result<()> {
    let hw_counter = HardwareCounterCell::new();

    for (value, count) in facet_hits.iter() {
        // Compare against exact count
        let value = ValueVariants::from(value.clone());

        let count_filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new(STR_KEY),
            Match::from(value.clone()),
        )));
        let count_filter = Filter::merge_opts(Some(count_filter), filter.clone());

        let exact = segment
            .read_filtered(
                None,
                None,
                count_filter.as_ref(),
                &Default::default(),
                &hw_counter,
            )
            .len();

        ensure!(*count == exact, "Facet value: {value:?}");
    }

    Ok(())
}

fn test_struct_keyword_facet(test_segments: &TestSegments) -> Result<()> {
    let request = keyword_facet_request();

    // Plain segment should fail, as it does not have a keyword index
    assert!(
        test_segments
            .plain_segment
            .facet(&request, &Default::default(), &Default::default())
            .is_err(),
    );

    // Struct segment
    let facet_hits = test_segments
        .struct_segment
        .facet(&request, &Default::default(), &Default::default())
        .unwrap();

    validate_facet_result(&test_segments.struct_segment, facet_hits, None).context(here!())
}

fn test_mmap_keyword_facet(test_segments: &TestSegments) -> Result<()> {
    let request = keyword_facet_request();

    let facet_hits = test_segments
        .mmap_segment
        .facet(&request, &Default::default(), &Default::default())
        .unwrap();

    validate_facet_result(&test_segments.mmap_segment, facet_hits, None).context(here!())
}

fn test_struct_keyword_facet_filtered(test_segments: &TestSegments) -> Result<()> {
    let mut request = keyword_facet_request();

    for _ in 0..ATTEMPTS {
        let filter = random_filter(&mut rand::rng(), 3);
        request.filter = Some(filter.clone());

        let facet_hits = test_segments
            .struct_segment
            .facet(&request, &Default::default(), &Default::default())
            .unwrap();

        validate_facet_result(&test_segments.struct_segment, facet_hits, Some(filter))
            .context(here!())?
    }
    Ok(())
}

fn test_mmap_keyword_facet_filtered(test_segments: &TestSegments) -> Result<()> {
    let mut request = keyword_facet_request();

    for _ in 0..ATTEMPTS {
        let filter = random_filter(&mut rand::rng(), 3);
        request.filter = Some(filter.clone());

        let facet_hits = test_segments
            .mmap_segment
            .facet(&request, &Default::default(), &Default::default())
            .unwrap();

        validate_facet_result(&test_segments.mmap_segment, facet_hits, Some(filter))
            .context(here!())?
    }
    Ok(())
}
