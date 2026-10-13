//! A per-element index must be indistinguishable from the flattened index of the same
//! field: the same data and operations go into one segment indexed per element and
//! one indexed flattened, and every read must agree.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::condition_checker::ConditionChecker as _;
use common::flags::FeatureFlags;
use common::types::{DeferredBehavior, PointOffsetType};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde_json::{Value, json};
use tempfile::Builder;
use uuid::Uuid;

use crate::data_types::facets::FacetParams;
use crate::data_types::order_by::{Direction, OrderBy};
use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use crate::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
    StorageSegmentEntry as _,
};
use crate::id_tracker::IdTrackerRead as _;
use crate::index::field_index::{FieldIndex, FieldIndexRead as _, PayloadFieldIndexRead as _};
use crate::json_path::JsonPath;
use crate::segment::Segment;
use crate::segment_constructor::load_segment;
use crate::segment_constructor::segment_builder::SegmentBuilder;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{
    Distance, ExtendedPointId, FieldCondition, Filter, HnswGlobalConfig, Indexes, Memory, Payload,
    PayloadFieldSchema, PayloadSchemaType, SegmentConfig, VectorDataConfig, VectorStorageType,
};

const NUM_POINTS: u64 = 200;

/// Indexed fields inside the `offers` array, with their type.
const FIELDS: &[(&str, PayloadSchemaType)] = &[
    ("offers[].price", PayloadSchemaType::Integer),
    ("offers[].shop", PayloadSchemaType::Keyword),
    ("offers[].rating", PayloadSchemaType::Float),
    ("offers[].available", PayloadSchemaType::Bool),
    ("offers[].tags", PayloadSchemaType::Keyword),
    ("offers[].location", PayloadSchemaType::Geo),
];

fn random_offer(rng: &mut StdRng) -> Value {
    let mut offer = serde_json::Map::new();
    // Each field is sometimes missing, so elements differ in what they have
    if rng.random_bool(0.85) {
        offer.insert("price".into(), json!(rng.random_range(0..40)));
    }
    if rng.random_bool(0.85) {
        offer.insert("shop".into(), json!(format!("s{}", rng.random_range(0..5))));
    }
    // Few decimals: values re-read from payload storage must parse back exactly
    if rng.random_bool(0.8) {
        offer.insert(
            "rating".into(),
            json!(f64::from(rng.random_range(0..500)) / 100.0),
        );
    }
    if rng.random_bool(0.8) {
        offer.insert("available".into(), json!(rng.random_bool(0.5)));
    }
    if rng.random_bool(0.7) {
        let tags: Vec<_> = (0..rng.random_range(0..3))
            .map(|_| format!("t{}", rng.random_range(0..4)))
            .collect();
        offer.insert("tags".into(), json!(tags));
    }
    if rng.random_bool(0.7) {
        offer.insert(
            "location".into(),
            json!({
                "lon": f64::from(rng.random_range(0..1000)) / 1000.0,
                "lat": f64::from(rng.random_range(0..1000)) / 1000.0,
            }),
        );
    }
    Value::Object(offer)
}

fn random_offers(rng: &mut StdRng) -> Value {
    let mut offers: Vec<_> = (0..rng.random_range(0..5))
        .map(|_| random_offer(rng))
        .collect();
    // Values that aren't objects are not elements
    if rng.random_bool(0.1) {
        offers.insert(rng.random_range(0..=offers.len()), json!(7));
    }
    Value::Array(offers)
}

fn random_payload(rng: &mut StdRng) -> Payload {
    let offers = match rng.random_range(0..10) {
        0 => return Payload::default(),
        // Not an array: no elements
        1 => random_offer(rng),
        _ => random_offers(rng),
    };
    let mut payload = Payload::default();
    payload.0.insert("offers".into(), offers);
    payload.0.insert("name".into(), json!("product"));
    payload
}

fn filters() -> Vec<Filter> {
    let conditions = [
        json!({"key": "offers[].shop", "match": {"value": "s1"}}),
        json!({"key": "offers[].shop", "match": {"any": ["s2", "s3"]}}),
        json!({"key": "offers[].shop", "match": {"except": ["s0", "s1"]}}),
        json!({"key": "offers[].price", "match": {"value": 7}}),
        json!({"key": "offers[].price", "match": {"any": [1, 2, 3, 30]}}),
        json!({"key": "offers[].price", "range": {"gte": 10, "lt": 25}}),
        json!({"key": "offers[].rating", "range": {"gt": 3.5}}),
        json!({"key": "offers[].available", "match": {"value": true}}),
        json!({"key": "offers[].tags", "match": {"value": "t2"}}),
        json!({"key": "offers[].location", "geo_radius": {
            "center": {"lon": 0.5, "lat": 0.5}, "radius": 30000.0
        }}),
        json!({"is_empty": {"key": "offers[].price"}}),
        json!({"is_null": {"key": "offers[].shop"}}),
    ];

    let mut filters: Vec<Filter> = conditions
        .iter()
        .map(|condition| serde_json::from_value(json!({"must": [condition]})).unwrap())
        .collect();
    filters.push(
        serde_json::from_value(json!({
            "must": [conditions[0], conditions[5]],
            "must_not": [conditions[7]],
        }))
        .unwrap(),
    );
    filters.push(
        serde_json::from_value(json!({
            "should": [conditions[3], conditions[8], conditions[9]],
        }))
        .unwrap(),
    );
    filters
}

/// Field conditions served by an index, for index-level checks.
fn field_conditions() -> Vec<FieldCondition> {
    filters()
        .into_iter()
        .flat_map(|filter| filter.must.unwrap_or_default())
        .filter_map(|condition| {
            if let crate::types::Condition::Field(condition) = condition {
                Some(condition)
            } else {
                None
            }
        })
        .collect()
}

fn schema(
    field_type: PayloadSchemaType,
    nested: bool,
    memory: Option<Memory>,
) -> PayloadFieldSchema {
    let mut schema = PayloadFieldSchema::FieldType(field_type);
    if let Some(memory) = memory {
        let mut params = schema.expand().into_owned();
        match &mut params {
            crate::types::PayloadSchemaParams::Keyword(p) => p.memory = Some(memory),
            crate::types::PayloadSchemaParams::Integer(p) => p.memory = Some(memory),
            crate::types::PayloadSchemaParams::Float(p) => p.memory = Some(memory),
            crate::types::PayloadSchemaParams::Bool(p) => p.memory = Some(memory),
            crate::types::PayloadSchemaParams::Geo(p) => p.memory = Some(memory),
            crate::types::PayloadSchemaParams::Text(_)
            | crate::types::PayloadSchemaParams::Datetime(_)
            | crate::types::PayloadSchemaParams::Uuid(_) => unreachable!("not in FIELDS"),
        }
        schema = PayloadFieldSchema::FieldParams(params);
    }
    if nested {
        let field = JsonPath::from_str("offers[].x").unwrap();
        schema = schema
            .with_nested(&field, &JsonPath::from_str("offers").unwrap())
            .unwrap();
    }
    schema
}

/// The same segment twice: flattened indexes, and per-element ones.
struct Pair {
    flat: Segment,
    nested: Segment,
    op_num: u64,
}

impl Pair {
    fn new(dir: &Path) -> Self {
        Self {
            flat: build_simple_segment(&dir.join("flat"), 2, Distance::Dot).unwrap(),
            nested: build_simple_segment(&dir.join("nested"), 2, Distance::Dot).unwrap(),
            op_num: 0,
        }
    }

    fn apply(&mut self, f: impl Fn(&mut Segment, u64)) {
        self.op_num += 1;
        f(&mut self.flat, self.op_num);
        f(&mut self.nested, self.op_num);
    }

    fn create_indexes(&mut self, memory: Option<Memory>) {
        for &(field, field_type) in FIELDS {
            let key = JsonPath::from_str(field).unwrap();
            self.op_num += 1;
            let flat_schema = schema(field_type, false, memory);
            let nested_schema = schema(field_type, true, memory);
            self.flat
                .create_field_index(self.op_num, &key, Some(&flat_schema))
                .unwrap();
            self.nested
                .create_field_index(self.op_num, &key, Some(&nested_schema))
                .unwrap();
        }
    }

    fn upsert(&mut self, rng: &mut StdRng, ids: impl Iterator<Item = u64>) {
        for id in ids {
            let payload = random_payload(rng);
            self.apply(|segment, op_num| {
                segment
                    .upsert_point(op_num, id.into(), only_default_vector(&[1.0, 1.0]))
                    .unwrap();
                segment
                    .set_full_payload(op_num, id.into(), &payload)
                    .unwrap();
            });
        }
    }

    fn mutate(&mut self, rng: &mut StdRng, count: usize) {
        for _ in 0..count {
            let id: ExtendedPointId = rng.random_range(0..NUM_POINTS).into();
            if !self.flat.has_point(id, DeferredBehavior::WithDeferred) {
                continue;
            }
            match rng.random_range(0..6) {
                0 => {
                    let payload = random_payload(rng);
                    self.apply(|segment, op_num| {
                        segment.set_full_payload(op_num, id, &payload).unwrap();
                    });
                }
                1 => {
                    let mut payload = Payload::default();
                    payload.0.insert("offers".into(), random_offers(rng));
                    self.apply(|segment, op_num| {
                        segment.set_payload(op_num, id, &payload, &None).unwrap();
                    });
                }
                2 => {
                    // Change one element in place
                    let key = JsonPath::from_str("offers[0]").unwrap();
                    let mut payload = Payload::default();
                    payload
                        .0
                        .insert("price".into(), json!(rng.random_range(0..40)));
                    payload.0.insert("shop".into(), json!("s4"));
                    self.apply(|segment, op_num| {
                        segment
                            .set_payload(op_num, id, &payload, &Some(key.clone()))
                            .unwrap();
                    });
                }
                3 => {
                    let key = JsonPath::from_str("offers[].shop").unwrap();
                    self.apply(|segment, op_num| {
                        segment.delete_payload(op_num, id, &key).unwrap();
                    });
                }
                4 => self.apply(|segment, op_num| {
                    segment.clear_payload(op_num, id).unwrap();
                }),
                _ => self.apply(|segment, op_num| {
                    segment.delete_point(op_num, id).unwrap();
                }),
            }
        }
    }
}

/// Sorted unique external ids; a flattened index repeats a point once per matching value.
fn external_ids(segment: &Segment, offsets: impl Iterator<Item = PointOffsetType>) -> Vec<String> {
    let id_tracker = segment.id_tracker.borrow();
    let mut ids: Vec<_> = offsets
        .filter_map(|offset| id_tracker.external_id(offset))
        .map(|id| id.to_string())
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

/// Index-level reads of one field: what every flattened index of it answers must be
/// what the per-element one answers.
fn assert_field_indexes_equal(flat: &Segment, nested: &Segment, field: &JsonPath) {
    let flat_index = flat.payload_index.borrow();
    let nested_index = nested.payload_index.borrow();
    let flat_indexes = &flat_index.field_indexes[field];
    let nested_indexes = &nested_index.field_indexes[field];
    assert_eq!(flat_indexes.len(), nested_indexes.len(), "{field}");

    let total = flat.id_tracker.borrow().total_point_count() as PointOffsetType;
    assert_eq!(
        total,
        nested.id_tracker.borrow().total_point_count() as PointOffsetType
    );

    for (flat_field_index, nested_field_index) in flat_indexes.iter().zip(nested_indexes) {
        assert_eq!(
            matches!(nested_field_index, FieldIndex::NullIndex(_)),
            matches!(flat_field_index, FieldIndex::NullIndex(_)),
        );
        assert_eq!(
            flat_field_index.count_indexed_points().unwrap(),
            nested_field_index.count_indexed_points().unwrap(),
            "{field}: indexed points of {flat_field_index:?}",
        );

        let non_empty = |index: &FieldIndex, segment: &Segment| {
            external_ids(
                segment,
                (0..total).filter(|&point| !index.values_is_empty(point).unwrap()),
            )
        };
        assert_eq!(
            non_empty(flat_field_index, flat),
            non_empty(nested_field_index, nested),
            "{field}: non-empty points of {flat_field_index:?}",
        );

        for condition in field_conditions().iter().filter(|c| &c.key == field) {
            let flat_points = flat_field_index.filter(condition).unwrap();
            let nested_points = nested_field_index.filter(condition).unwrap();
            assert_eq!(flat_points.is_some(), nested_points.is_some());
            if let (Some(flat_points), Some(nested_points)) = (flat_points, nested_points) {
                assert_eq!(
                    external_ids(flat, flat_points),
                    external_ids(nested, nested_points),
                    "{field}: filter {condition:?} of {flat_field_index:?}",
                );
            }

            let flat_checker = flat_field_index.condition_checker(condition).unwrap();
            let nested_checker = nested_field_index.condition_checker(condition).unwrap();
            assert_eq!(flat_checker.is_some(), nested_checker.is_some());
            if let (Some(flat_checker), Some(nested_checker)) = (flat_checker, nested_checker) {
                let matching = |checker: &dyn Fn(PointOffsetType) -> bool, segment| {
                    external_ids(segment, (0..total).filter(|&point| checker(point)))
                };
                assert_eq!(
                    matching(&|point| flat_checker.check(point).unwrap(), flat),
                    matching(&|point| nested_checker.check(point).unwrap(), nested),
                    "{field}: checker {condition:?} of {flat_field_index:?}",
                );
            }
        }
    }
}

fn assert_equal(flat: &Segment, nested: &Segment) {
    let stopped = AtomicBool::new(false);
    let _scope = ambient::test_guard();

    assert_eq!(flat.available_point_count(), nested.available_point_count());

    let mut matched_any = false;
    for filter in filters() {
        let read = |segment: &Segment| {
            segment
                .read_filtered(
                    None,
                    None,
                    Some(&filter),
                    &stopped,
                    DeferredBehavior::WithDeferred,
                )
                .unwrap()
        };
        let flat_points = read(flat);
        matched_any |= !flat_points.is_empty();
        assert_eq!(flat_points, read(nested), "filter {filter:?}");
    }
    assert!(matched_any, "filters must select something to compare");

    for &(field, _) in FIELDS {
        assert_field_indexes_equal(flat, nested, &JsonPath::from_str(field).unwrap());
    }

    for key in [
        "offers[].shop",
        "offers[].price",
        "offers[].available",
        "offers[].tags",
    ] {
        for filter in [None, Some(filters()[5].clone())] {
            let request = FacetParams {
                key: JsonPath::from_str(key).unwrap(),
                limit: 1000,
                filter,
                exact: true,
            };
            let facet = |segment: &Segment| segment.facet(&request, &stopped).unwrap();
            assert_eq!(facet(flat), facet(nested), "facet {request:?}");
        }
        let key = JsonPath::from_str(key).unwrap();
        let unique = |segment: &Segment| segment.unique_values(&key, None, &stopped).unwrap();
        assert_eq!(unique(flat), unique(nested), "unique values of {key}");
    }

    for key in ["offers[].price", "offers[].rating"] {
        for direction in [Direction::Asc, Direction::Desc] {
            for filter in [None, Some(filters()[0].clone())] {
                let order_by = OrderBy {
                    key: JsonPath::from_str(key).unwrap(),
                    direction: Some(direction),
                    start_from: None,
                };
                let read = |segment: &Segment| {
                    let mut points = segment
                        .read_ordered_filtered(
                            None,
                            filter.as_ref(),
                            &order_by,
                            &stopped,
                            DeferredBehavior::WithDeferred,
                        )
                        .unwrap();
                    // Ties are in no particular order
                    points.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
                    points
                };
                assert_eq!(read(flat), read(nested), "order by {order_by:?}");
            }
        }
    }
}

fn reload(segment: Segment) -> Segment {
    segment.flush(true).unwrap();
    let path = segment.segment_path.clone();
    drop(segment);
    load_segment(&path, Uuid::nil(), None, &AtomicBool::new(false), false).unwrap()
}

/// Optimize into a non-appendable segment, like the optimizer does.
fn optimize(segment: &Segment, dir: &Path) -> Segment {
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: 2,
                distance: Distance::Dot,
                storage_type: VectorStorageType::Mmap,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    };
    fs_err::create_dir_all(dir).unwrap();
    let temp_dir = Builder::new().prefix("temp").tempdir_in(dir).unwrap();
    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();
    builder.update(&[segment], &AtomicBool::new(false)).unwrap();
    let segment = builder.build_for_test(dir);
    assert!(!segment.appendable_flag);
    segment
}

fn run(seed: u64, memory: Option<Memory>) {
    let dir = Builder::new().prefix("nested_index").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(seed);
    let _scope = ambient::test_guard();

    let mut pair = Pair::new(dir.path());

    // Indexes built from existing payloads, then updated with new points
    pair.upsert(&mut rng, 0..NUM_POINTS / 2);
    pair.create_indexes(memory);
    pair.upsert(&mut rng, NUM_POINTS / 2..NUM_POINTS);
    for &(field, _) in FIELDS {
        let field = JsonPath::from_str(field).unwrap();
        let is_nested = |segment: &Segment| {
            segment.payload_index.borrow().field_indexes[&field]
                .iter()
                .filter(|index| index.is_nested())
                .count()
        };
        assert_eq!(is_nested(&pair.flat), 0);
        assert!(
            is_nested(&pair.nested) > 0,
            "{field} must be indexed per element"
        );
    }
    assert_equal(&pair.flat, &pair.nested);

    pair.mutate(&mut rng, NUM_POINTS as usize);
    assert_equal(&pair.flat, &pair.nested);

    // Rebuild one per-element index over the element map, which has moved ranges by now
    let (field, field_type) = FIELDS[1];
    let key = JsonPath::from_str(field).unwrap();
    pair.apply(|segment, op_num| {
        segment.delete_field_index(op_num, &key).unwrap();
    });
    pair.op_num += 1;
    pair.flat
        .create_field_index(pair.op_num, &key, Some(&schema(field_type, false, memory)))
        .unwrap();
    pair.nested
        .create_field_index(pair.op_num, &key, Some(&schema(field_type, true, memory)))
        .unwrap();
    assert_equal(&pair.flat, &pair.nested);

    let Pair {
        flat,
        nested,
        op_num,
    } = pair;
    let mut pair = Pair {
        flat: reload(flat),
        nested: reload(nested),
        op_num,
    };
    assert_equal(&pair.flat, &pair.nested);

    pair.mutate(&mut rng, NUM_POINTS as usize / 2);
    assert_equal(&pair.flat, &pair.nested);

    let mut flat = optimize(&pair.flat, &dir.path().join("flat_optimized"));
    let mut nested = optimize(&pair.nested, &dir.path().join("nested_optimized"));
    assert!(
        nested.payload_index.borrow().field_indexes[&JsonPath::from_str(FIELDS[0].0).unwrap()]
            .iter()
            .any(FieldIndex::is_nested),
        "optimized segment must keep per-element indexes",
    );
    assert_equal(&flat, &nested);

    // Deletions are the only updates of a non-appendable segment
    for id in (0..NUM_POINTS).step_by(7) {
        pair.op_num += 1;
        flat.delete_point(pair.op_num, id.into()).unwrap();
        nested.delete_point(pair.op_num, id.into()).unwrap();
    }
    assert_equal(&flat, &nested);
    assert_equal(&reload(flat), &reload(nested));
}

#[test]
fn nested_index_matches_flattened_index() {
    for seed in 0..3 {
        run(seed, None);
    }
}

#[test]
fn nested_index_matches_flattened_index_on_disk() {
    run(42, Some(Memory::Cold));
}

/// Per-element indexes of one array share its element map, which goes away with the
/// last of them.
#[test]
fn element_map_lifecycle() {
    let dir = Builder::new().prefix("nested_index").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(7);
    let _scope = ambient::test_guard();

    let mut pair = Pair::new(dir.path());
    pair.upsert(&mut rng, 0..NUM_POINTS);
    pair.create_indexes(None);

    let segment = &mut pair.nested;
    let array_key = JsonPath::from_str("offers[]").unwrap();
    let map_dir = crate::index::field_index::nested_index::ElementMap::storage_dir(
        &segment.segment_path.join("payload_index"),
        &array_key,
    );
    assert!(map_dir.exists());
    {
        let payload_index = segment.payload_index.borrow();
        let maps: Vec<_> = payload_index
            .field_indexes
            .values()
            .flatten()
            .filter_map(|index| {
                if let FieldIndex::Nested(index) = index {
                    Some(index.element_map().clone())
                } else {
                    None
                }
            })
            .collect();
        assert!(maps.len() > FIELDS.len());
        assert!(maps.iter().all(|map| std::sync::Arc::ptr_eq(map, &maps[0])));
    }

    let mut op_num = pair.op_num;
    for (i, &(field, _)) in FIELDS.iter().enumerate() {
        op_num += 1;
        segment
            .delete_field_index(op_num, &JsonPath::from_str(field).unwrap())
            .unwrap();
        assert_eq!(
            map_dir.exists(),
            i + 1 < FIELDS.len(),
            "after dropping {field}"
        );
    }
}
