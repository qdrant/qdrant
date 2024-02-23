use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;
use fnv::FnvBuildHasher;
use indexmap::IndexSet;
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use segment::data_types::integer_index::{IntegerIndexParams, IntegerIndexType};
use segment::data_types::vectors::{only_default_vector, DEFAULT_VECTOR_NAME};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_context_fixture::FixtureIdTracker;
use segment::fixtures::payload_fixtures::{
    generate_diverse_nested_payload, generate_diverse_payload, random_filter, random_nested_filter,
    random_vector, FLICKING_KEY, GEO_KEY, INT_KEY, INT_KEY_2, INT_KEY_3, LAT_RANGE, LON_RANGE,
    STR_KEY, STR_PROJ_KEY, STR_ROOT_PROJ_KEY, TEXT_KEY,
};
use segment::index::field_index::{FieldIndex, PrimaryCondition};
use segment::index::struct_payload_index::StructPayloadIndex;
use segment::index::PayloadIndex;
use segment::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use segment::payload_storage::PayloadStorage;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::types::PayloadFieldSchema::{FieldParams, FieldType};
use segment::types::PayloadSchemaType::{Integer, Keyword};
use segment::types::{
    AnyVariants, Condition, Distance, FieldCondition, Filter, GeoBoundingBox, GeoLineString,
    GeoPoint, GeoPolygon, GeoRadius, Indexes, IsEmptyCondition, Match, Payload, PayloadField,
    PayloadSchemaParams, PayloadSchemaType, Range, SegmentConfig, VectorDataConfig,
    VectorStorageType, WithPayload,
};
use serde_json::json;
use tempfile::Builder;

use crate::utils::path;
use crate::utils::scored_point_ties::ScoredPointTies;

const DIM: usize = 5;
const ATTEMPTS: usize = 100;

fn build_test_segments(path_struct: &Path, path_plain: &Path) -> (Segment, Segment) {
    let mut rnd = StdRng::seed_from_u64(42);

    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Dot,
                storage_type: VectorStorageType::Memory,
                index: Indexes::Plain {},
                quantization_config: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let mut plain_segment = build_segment(path_plain, &config, true).unwrap();
    let mut struct_segment = build_segment(path_struct, &config, true).unwrap();

    let num_points = 3000;
    let points_to_delete = 500;
    let points_to_clear = 500;

    let mut opnum = 0;
    struct_segment
        .create_field_index(opnum, &path(INT_KEY_2), Some(&Integer.into()))
        .unwrap();

    opnum += 1;
    for n in 0..num_points {
        let idx = n.into();
        let vector = random_vector(&mut rnd, DIM);
        let payload: Payload = generate_diverse_payload(&mut rnd);

        plain_segment
            .upsert_point(opnum, idx, only_default_vector(&vector))
            .unwrap();
        struct_segment
            .upsert_point(opnum, idx, only_default_vector(&vector))
            .unwrap();
        plain_segment
            .set_full_payload(opnum, idx, &payload)
            .unwrap();
        struct_segment
            .set_full_payload(opnum, idx, &payload)
            .unwrap();

        opnum += 1;
    }

    struct_segment
        .create_field_index(opnum, &path(STR_KEY), Some(&Keyword.into()))
        .unwrap();
    struct_segment
        .create_field_index(opnum, &path(INT_KEY), None)
        .unwrap();
    struct_segment
        .create_field_index(
            opnum,
            &path(INT_KEY_2),
            Some(&FieldParams(PayloadSchemaParams::Integer(
                IntegerIndexParams {
                    r#type: IntegerIndexType::Integer,
                    lookup: true,
                    range: false,
                },
            ))),
        )
        .unwrap();
    struct_segment
        .create_field_index(
            opnum,
            &path(INT_KEY_3),
            Some(&FieldParams(PayloadSchemaParams::Integer(
                IntegerIndexParams {
                    r#type: IntegerIndexType::Integer,
                    lookup: false,
                    range: true,
                },
            ))),
        )
        .unwrap();
    struct_segment
        .create_field_index(opnum, &path(GEO_KEY), Some(&PayloadSchemaType::Geo.into()))
        .unwrap();
    struct_segment
        .create_field_index(
            opnum,
            &path(TEXT_KEY),
            Some(&PayloadSchemaType::Text.into()),
        )
        .unwrap();
    struct_segment
        .create_field_index(opnum, &path(FLICKING_KEY), Some(&Integer.into()))
        .unwrap();

    for _ in 0..points_to_clear {
        opnum += 1;
        let idx_to_remove = rnd.gen_range(0..num_points);
        plain_segment
            .clear_payload(opnum, idx_to_remove.into())
            .unwrap();
        struct_segment
            .clear_payload(opnum, idx_to_remove.into())
            .unwrap();
    }

    for _ in 0..points_to_delete {
        opnum += 1;
        let idx_to_remove = rnd.gen_range(0..num_points);
        plain_segment
            .delete_point(opnum, idx_to_remove.into())
            .unwrap();
        struct_segment
            .delete_point(opnum, idx_to_remove.into())
            .unwrap();
    }

    for (field, indexes) in struct_segment.payload_index.borrow().field_indexes.iter() {
        for index in indexes {
            assert!(index.count_indexed_points() < num_points as usize);
            if field.to_string() != FLICKING_KEY {
                assert!(
                    index.count_indexed_points()
                        > (num_points as usize - points_to_delete - points_to_clear)
                );
            }
        }
    }

    (struct_segment, plain_segment)
}

fn build_test_segments_nested_payload(path_struct: &Path, path_plain: &Path) -> (Segment, Segment) {
    let mut rnd = StdRng::seed_from_u64(42);

    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Dot,
                storage_type: VectorStorageType::Memory,
                index: Indexes::Plain {},
                quantization_config: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let mut plain_segment = build_segment(path_plain, &config, true).unwrap();
    let mut struct_segment = build_segment(path_struct, &config, true).unwrap();

    let num_points = 3000;
    let points_to_delete = 500;
    let points_to_clear = 500;

    // Nested payload keys
    let nested_str_key = path(&format!("{}.{}.{}", STR_KEY, "nested_1", "nested_2"));
    let nested_str_proj_key = path(&format!("{}.{}[].{}", STR_PROJ_KEY, "nested_1", "nested_2"));
    let deep_nested_str_proj_key = path(&format!(
        "{}[].{}[].{}",
        STR_ROOT_PROJ_KEY, "nested_1", "nested_2"
    ));

    let mut opnum = 0;
    struct_segment
        .create_field_index(opnum, &nested_str_key, Some(&Keyword.into()))
        .unwrap();

    struct_segment
        .create_field_index(opnum, &nested_str_proj_key, Some(&Keyword.into()))
        .unwrap();

    struct_segment
        .create_field_index(opnum, &deep_nested_str_proj_key, Some(&Keyword.into()))
        .unwrap();

    eprintln!("{}", deep_nested_str_proj_key);

    opnum += 1;
    for n in 0..num_points {
        let idx = n.into();
        let vector = random_vector(&mut rnd, DIM);
        let payload: Payload = generate_diverse_nested_payload(&mut rnd);

        plain_segment
            .upsert_point(opnum, idx, only_default_vector(&vector))
            .unwrap();
        struct_segment
            .upsert_point(opnum, idx, only_default_vector(&vector))
            .unwrap();
        plain_segment
            .set_full_payload(opnum, idx, &payload)
            .unwrap();
        struct_segment
            .set_full_payload(opnum, idx, &payload)
            .unwrap();

        opnum += 1;
    }

    for _ in 0..points_to_clear {
        opnum += 1;
        let idx_to_remove = rnd.gen_range(0..num_points);
        plain_segment
            .clear_payload(opnum, idx_to_remove.into())
            .unwrap();
        struct_segment
            .clear_payload(opnum, idx_to_remove.into())
            .unwrap();
    }

    for _ in 0..points_to_delete {
        opnum += 1;
        let idx_to_remove = rnd.gen_range(0..num_points);
        plain_segment
            .delete_point(opnum, idx_to_remove.into())
            .unwrap();
        struct_segment
            .delete_point(opnum, idx_to_remove.into())
            .unwrap();
    }

    for (_field, indexes) in struct_segment.payload_index.borrow().field_indexes.iter() {
        for index in indexes {
            assert!(index.count_indexed_points() < num_points as usize);
            assert!(
                index.count_indexed_points()
                    > (num_points as usize - points_to_delete - points_to_clear)
            );
        }
    }

    (struct_segment, plain_segment)
}

fn validate_geo_filter(query_filter: Filter) {
    let mut rnd = rand::thread_rng();
    let query = random_vector(&mut rnd, DIM).into();
    let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();
    let (struct_segment, plain_segment) = build_test_segments(dir1.path(), dir2.path());

    for _i in 0..ATTEMPTS {
        let plain_result = plain_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query,
                &WithPayload::default(),
                &false.into(),
                Some(&query_filter),
                5,
                None,
                &false.into(),
            )
            .unwrap();

        let estimation = plain_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&query_filter);

        assert!(estimation.min <= estimation.exp, "{estimation:#?}");
        assert!(estimation.exp <= estimation.max, "{estimation:#?}");
        assert!(
            estimation.max <= struct_segment.id_tracker.borrow().available_point_count(),
            "{estimation:#?}",
        );

        let struct_result = struct_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query,
                &WithPayload::default(),
                &false.into(),
                Some(&query_filter),
                5,
                None,
                &false.into(),
            )
            .unwrap();

        let estimation = struct_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&query_filter);

        assert!(estimation.min <= estimation.exp, "{estimation:#?}");
        assert!(estimation.exp <= estimation.max, "{estimation:#?}");
        assert!(
            estimation.max <= struct_segment.id_tracker.borrow().available_point_count(),
            "{estimation:#?}",
        );

        plain_result
            .iter()
            .zip(struct_result.iter())
            .for_each(|(r1, r2)| {
                assert_eq!(r1.id, r2.id);
                assert!((r1.score - r2.score) < 0.0001)
            });
    }
}

#[test]
fn test_is_empty_conditions() {
    let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

    let (struct_segment, plain_segment) = build_test_segments(dir1.path(), dir2.path());

    let filter = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
        is_empty: PayloadField {
            key: path(FLICKING_KEY),
        },
    }));

    let estimation_struct = struct_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter);

    let estimation_plain = plain_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter);

    let plain_result = plain_segment.payload_index.borrow().query_points(&filter);

    let real_number = plain_result.len();

    let struct_result = struct_segment.payload_index.borrow().query_points(&filter);

    assert_eq!(plain_result, struct_result);

    eprintln!("estimation_plain = {estimation_plain:#?}");
    eprintln!("estimation_struct = {estimation_struct:#?}");
    eprintln!("real_number = {real_number:#?}");

    assert!(estimation_plain.max >= real_number);
    assert!(estimation_plain.min <= real_number);

    assert!(estimation_struct.max >= real_number);
    assert!(estimation_struct.min <= real_number);

    assert!(
        (estimation_struct.exp as f64 - real_number as f64).abs()
            <= (estimation_plain.exp as f64 - real_number as f64).abs()
    );
}

#[test]
fn test_integer_index_types() {
    let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

    let (struct_segment, _) = build_test_segments(dir1.path(), dir2.path());

    let indexes = struct_segment.payload_index.borrow();
    assert!(matches!(
        indexes
            .field_indexes
            .get(&path(INT_KEY))
            .unwrap()
            .as_slice(),
        [FieldIndex::IntMapIndex(_), FieldIndex::IntIndex(_)]
    ));
    assert!(matches!(
        indexes
            .field_indexes
            .get(&path(INT_KEY_2))
            .unwrap()
            .as_slice(),
        [FieldIndex::IntMapIndex(_)]
    ));
    assert!(matches!(
        indexes
            .field_indexes
            .get(&path(INT_KEY_3))
            .unwrap()
            .as_slice(),
        [FieldIndex::IntIndex(_)]
    ));
}

#[test]
fn test_cardinality_estimation() {
    let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

    let (struct_segment, _) = build_test_segments(dir1.path(), dir2.path());

    let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
        path(INT_KEY),
        Range {
            lt: None,
            gt: None,
            gte: Some(50.),
            lte: Some(100.),
        },
    )));

    let estimation = struct_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter);

    let payload_index = struct_segment.payload_index.borrow();
    let filter_context = payload_index.filter_context(&filter);
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
fn test_root_nested_array_filter_cardinality_estimation() {
    let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

    let (struct_segment, _) = build_test_segments_nested_payload(dir1.path(), dir2.path());

    // rely on test data from `build_test_segments_nested_payload`
    let nested_key = "nested_1[].nested_2";
    let nested_match = FieldCondition::new_match(path(nested_key), "some value".to_owned().into());
    let filter = Filter::new_must(Condition::new_nested(
        path(STR_ROOT_PROJ_KEY),
        Filter::new_must(Condition::Field(nested_match)),
    ));

    let estimation = struct_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter);

    // not empty primary clauses
    assert_eq!(estimation.primary_clauses.len(), 1);
    eprintln!("primary_clauses = {:#?}", estimation.primary_clauses);
    let primary_clause = estimation.primary_clauses.first().unwrap();

    let expected_primary_clause = FieldCondition::new_match(
        path(&format!("{}[].{}", STR_ROOT_PROJ_KEY, nested_key)), // full key expected
        "some value".to_owned().into(),
    );

    match primary_clause {
        PrimaryCondition::Condition(field_condition) => {
            assert_eq!(field_condition, &expected_primary_clause);
        }
        o => panic!("unexpected primary clause: {:?}", o),
    }

    let payload_index = struct_segment.payload_index.borrow();
    let filter_context = payload_index.filter_context(&filter);
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
    let nested_match =
        FieldCondition::new_match(path(nested_match_key), "some value".to_owned().into());
    let filter = Filter::new_must(Condition::new_nested(
        path(STR_ROOT_PROJ_KEY),
        Filter::new_must(Condition::new_nested(
            path("nested_1"),
            Filter::new_must(Condition::Field(nested_match)),
        )),
    ));

    let estimation = struct_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter);

    // not empty primary clauses
    assert_eq!(estimation.primary_clauses.len(), 1);
    eprintln!("primary_clauses = {:#?}", estimation.primary_clauses);
    let primary_clause = estimation.primary_clauses.first().unwrap();

    let expected_primary_clause = FieldCondition::new_match(
        // full key expected
        path(&format!(
            "{}[].nested_1[].{}",
            STR_ROOT_PROJ_KEY, nested_match_key
        )),
        "some value".to_owned().into(),
    );

    match primary_clause {
        PrimaryCondition::Condition(field_condition) => {
            assert_eq!(field_condition, &expected_primary_clause);
        }
        o => panic!("unexpected primary clause: {:?}", o),
    }

    let payload_index = struct_segment.payload_index.borrow();
    let filter_context = payload_index.filter_context(&filter);
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
fn test_struct_payload_index() {
    // Compare search with plain and struct indexes
    let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

    let mut rnd = rand::thread_rng();

    let (struct_segment, plain_segment) = build_test_segments(dir1.path(), dir2.path());

    for _i in 0..ATTEMPTS {
        let query_vector = random_vector(&mut rnd, DIM).into();
        let query_filter = random_filter(&mut rnd, 3);

        let plain_result = plain_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                Some(&query_filter),
                5,
                None,
                &false.into(),
            )
            .unwrap();
        let struct_result = struct_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                Some(&query_filter),
                5,
                None,
                &false.into(),
            )
            .unwrap();

        let estimation = struct_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&query_filter);

        assert!(estimation.min <= estimation.exp, "{estimation:#?}");
        assert!(estimation.exp <= estimation.max, "{estimation:#?}");
        assert!(
            estimation.max <= struct_segment.id_tracker.borrow().available_point_count(),
            "{estimation:#?}",
        );

        // Perform additional sort to break ties by score
        let mut plain_result_sorted_ties: Vec<ScoredPointTies> =
            plain_result.iter().map(|x| x.clone().into()).collect_vec();
        plain_result_sorted_ties.sort();

        let mut struct_result_sorted_ties: Vec<ScoredPointTies> =
            struct_result.iter().map(|x| x.clone().into()).collect_vec();
        struct_result_sorted_ties.sort();

        plain_result_sorted_ties
            .into_iter()
            .zip(struct_result_sorted_ties.into_iter())
            .map(|(r1, r2)| (r1.scored_point, r2.scored_point))
            .for_each(|(r1, r2)| {
                assert_eq!(r1.id, r2.id, "got different ScoredPoint {r1:?} and {r2:?} for\nquery vector {query_vector:?}\nquery filter {query_filter:?}\nplain result {plain_result:?}\nstruct result{struct_result:?}");
                assert!((r1.score - r2.score) < 0.0001)
            });
    }
}

#[test]
fn test_struct_payload_geo_boundingbox_index() {
    let mut rnd = rand::thread_rng();

    let geo_bbox = GeoBoundingBox {
        top_left: GeoPoint {
            lon: rnd.gen_range(LON_RANGE),
            lat: rnd.gen_range(LAT_RANGE),
        },
        bottom_right: GeoPoint {
            lon: rnd.gen_range(LON_RANGE),
            lat: rnd.gen_range(LAT_RANGE),
        },
    };

    let condition = Condition::Field(FieldCondition::new_geo_bounding_box(
        path("geo_key"),
        geo_bbox,
    ));

    let query_filter = Filter::new_must(condition);

    validate_geo_filter(query_filter)
}

#[test]
fn test_struct_payload_geo_radius_index() {
    let mut rnd = rand::thread_rng();

    let r_meters = rnd.gen_range(1.0..10000.0);
    let geo_radius = GeoRadius {
        center: GeoPoint {
            lon: rnd.gen_range(LON_RANGE),
            lat: rnd.gen_range(LAT_RANGE),
        },
        radius: r_meters,
    };

    let condition = Condition::Field(FieldCondition::new_geo_radius(path("geo_key"), geo_radius));

    let query_filter = Filter::new_must(condition);

    validate_geo_filter(query_filter)
}

#[test]
fn test_struct_payload_geo_polygon_index() {
    let polygon_edge = 5;
    let interiors_num = 3;

    fn generate_ring(polygon_edge: i32) -> GeoLineString {
        let mut rnd = rand::thread_rng();
        let mut line = GeoLineString {
            points: (0..polygon_edge)
                .map(|_| GeoPoint {
                    lon: rnd.gen_range(LON_RANGE),
                    lat: rnd.gen_range(LAT_RANGE),
                })
                .collect(),
        };
        line.points.push(line.points[0].clone()); // add last point that is identical to the first
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
        path("geo_key"),
        geo_polygon,
    ));

    let query_filter = Filter::new_must(condition);

    validate_geo_filter(query_filter)
}

#[test]
fn test_struct_payload_index_nested_fields() {
    // Compare search with plain and struct indexes
    let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

    let mut rnd = rand::thread_rng();

    let (struct_segment, plain_segment) =
        build_test_segments_nested_payload(dir1.path(), dir2.path());

    let attempts = 100;
    for _i in 0..attempts {
        let query_vector = random_vector(&mut rnd, DIM).into();
        let query_filter = random_nested_filter(&mut rnd);
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
                &false.into(),
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
                &false.into(),
            )
            .unwrap();

        let estimation = struct_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&query_filter);

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
                assert_eq!(r1.id, r2.id, "got different ScoredPoint {r1:?} and {r2:?} for\nquery vector {query_vector:?}\nquery filter {query_filter:?}\nplain result {plain_result:?}\nstruct result{struct_result:?}");
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
        let payload = json!({
            "field": i,
        });
        payloads.push(payload.into());
    }

    for (idx, payload) in payloads.into_iter().enumerate() {
        points.insert(idx, payload.clone());
        payload_storage
            .assign(idx as PointOffsetType, &payload)
            .unwrap();
    }

    let wrapped_payload_storage = Arc::new(AtomicRefCell::new(payload_storage.into()));
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(point_num)));

    let mut index =
        StructPayloadIndex::open(wrapped_payload_storage, id_tracker, dir.path(), true).unwrap();

    let field = path("field");

    // set field to Integer type
    index.set_indexed(&field, Integer.into()).unwrap();
    assert_eq!(
        *index.indexed_fields().get(&field).unwrap(),
        FieldType(Integer)
    );
    let field_index = index.field_indexes.get(&field).unwrap();
    assert_eq!(field_index[0].count_indexed_points(), point_num);
    assert_eq!(field_index[1].count_indexed_points(), point_num);

    // update field to Keyword type
    index.set_indexed(&field, Keyword.into()).unwrap();
    assert_eq!(
        *index.indexed_fields().get(&field).unwrap(),
        FieldType(Keyword)
    );
    let field_index = index.field_indexes.get(&field).unwrap();
    assert_eq!(field_index[0].count_indexed_points(), 0); // only one field index for Keyword

    // set field to Integer type (again)
    index.set_indexed(&field, Integer.into()).unwrap();
    assert_eq!(
        *index.indexed_fields().get(&field).unwrap(),
        FieldType(Integer)
    );
    let field_index = index.field_indexes.get(&field).unwrap();
    assert_eq!(field_index[0].count_indexed_points(), point_num);
    assert_eq!(field_index[1].count_indexed_points(), point_num);
}

#[test]
fn test_any_matcher_cardinality_estimation() {
    let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

    let (struct_segment, _) = build_test_segments(dir1.path(), dir2.path());

    let keywords: IndexSet<String, FnvBuildHasher> =
        ["value1", "value2"].iter().map(|i| i.to_string()).collect();
    let any_match = FieldCondition::new_match(
        path(STR_KEY),
        Match::new_any(AnyVariants::Keywords(keywords)),
    );

    let filter = Filter::new_must(Condition::Field(any_match.clone()));

    let estimation = struct_segment
        .payload_index
        .borrow()
        .estimate_cardinality(&filter);

    assert_eq!(estimation.primary_clauses.len(), 1);
    for clause in estimation.primary_clauses.iter() {
        let expected_primary_clause = any_match.clone();

        match clause {
            PrimaryCondition::Condition(field_condition) => {
                assert_eq!(field_condition, &expected_primary_clause);
            }
            o => panic!("unexpected primary clause: {:?}", o),
        }
    }

    let payload_index = struct_segment.payload_index.borrow();
    let filter_context = payload_index.filter_context(&filter);
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
