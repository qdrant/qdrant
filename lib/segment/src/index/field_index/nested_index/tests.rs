//! A per-element index must be indistinguishable from the flattened index of the same
//! field: the same data and operations go into one segment indexed per element and
//! one indexed flattened, and every read must agree.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::ambient;
use common::condition_checker::ConditionChecker as _;
use common::flags::FeatureFlags;
use common::types::{DeferredBehavior, PointOffsetType};
use itertools::Itertools as _;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde_json::{Value, json};
use tempfile::Builder;
use uuid::Uuid;

use crate::common::utils::{check_is_empty, check_is_null};
use crate::data_types::facets::FacetParams;
use crate::data_types::order_by::{Direction, OrderBy};
use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use crate::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
    StorageSegmentEntry as _,
};
use crate::id_tracker::IdTrackerRead as _;
use crate::index::PayloadIndexRead as _;
use crate::index::field_index::null_index::NullIndexRead as _;
use crate::index::field_index::{FieldIndex, FieldIndexRead as _, PayloadFieldIndexRead as _};
use crate::index::payload_config::PayloadIndexType;
use crate::json_path::JsonPath;
use crate::payload_storage::query_checker::check_payload;
use crate::segment::Segment;
use crate::segment_constructor::load_segment;
use crate::segment_constructor::segment_builder::SegmentBuilder;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{
    Distance, ExtendedPointId, FieldCondition, Filter, HnswGlobalConfig, Indexes, Memory,
    OwnedPayloadRef, Payload, PayloadFieldSchema, PayloadSchemaType, SegmentConfig,
    VectorDataConfig, VectorStorageType,
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
        // Values of the wrong type are not indexed
        let price = if rng.random_bool(0.1) {
            json!("cheap")
        } else {
            json!(rng.random_range(0..40))
        };
        offer.insert("price".into(), price);
    }
    if rng.random_bool(0.85) {
        let shop = if rng.random_bool(0.1) {
            Value::Null
        } else {
            json!(format!("s{}", rng.random_range(0..5)))
        };
        offer.insert("shop".into(), shop);
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
        // Matches a point if any value is null, so the same whether elements are
        // indexed on their own or not
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
    filters.extend(nested_filters().into_iter().map(|(filter, _)| filter));
    filters
}

/// Filters with a `nested` condition on `offers`, and whether per-element indexes of all
/// fields cover the nested filter.
fn nested_filters() -> Vec<(Filter, bool)> {
    let nested = |filter: Value| json!({"nested": {"key": "offers", "filter": filter}});
    let shop = json!({"key": "shop", "match": {"value": "s1"}});
    let shops = json!({"key": "shop", "match": {"any": ["s2", "s3"]}});
    let not_shops = json!({"key": "shop", "match": {"except": ["s0"]}});
    let price = json!({"key": "price", "range": {"gte": 10, "lt": 25}});
    let cheap = json!({"key": "price", "range": {"lt": 20}});
    let rating = json!({"key": "rating", "range": {"gt": 2.5}});
    let available = json!({"key": "available", "match": {"value": true}});
    let tag = json!({"key": "tags", "match": {"value": "t1"}});
    let near = json!({"key": "location", "geo_radius": {
        "center": {"lon": 0.5, "lat": 0.5}, "radius": 40000.0
    }});

    let covered = [
        nested(json!({"must": [shop, price]})),
        nested(json!({"should": [shops, tag], "must_not": [available]})),
        nested(json!({"min_should": {"conditions": [cheap, rating, tag], "min_count": 2}})),
        nested(json!({"must": [near, not_shops]})),
        nested(json!({"must": [{"should": [shop, shops]}, {"must_not": [cheap]}]})),
        // An empty `should` matches no element
        nested(json!({"should": [], "must": [shop]})),
    ];
    let not_covered = [
        // Values counts, emptiness and unindexed fields are checked in the payload
        nested(json!({"must": [shop, {"key": "tags", "values_count": {"gte": 2}}]})),
        nested(json!({"must": [{"is_empty": {"key": "note"}}, rating]})),
        nested(json!({"should": [shop, {"key": "name", "match": {"value": "x"}}]})),
        // Without field conditions, no index lists the elements of a point
        nested(json!({})),
        nested(json!({"should": []})),
    ];

    let mut filters: Vec<(Filter, bool)> = covered
        .into_iter()
        .map(|condition| (json!({"must": [condition]}), true))
        .chain(
            not_covered
                .into_iter()
                .map(|condition| (json!({"must": [condition]}), false)),
        )
        .map(|(filter, covered)| (serde_json::from_value(filter).unwrap(), covered))
        .collect();

    // Next to other conditions, and negated
    filters.push((
        serde_json::from_value(json!({
            "must": [nested(json!({"must": [shop, price]})), {"key": "offers[].available", "match": {"value": false}}],
            "must_not": [nested(json!({"must": [rating]}))],
        }))
        .unwrap(),
        true,
    ));
    filters
}

/// Nested filters with emptiness conditions, all answered from per-element indexes.
///
/// The flattened segment can't serve as their reference: it estimates a nested `is_empty`
/// with the flattened null index, which selects too few points, see [`payload_reference`].
fn nested_emptiness_filters() -> Vec<Filter> {
    let nested = |filter: Value| json!({"must": [{"nested": {"key": "offers", "filter": filter}}]});
    let shop = json!({"key": "shop", "match": {"value": "s1"}});
    let cheap = json!({"key": "price", "range": {"lt": 20}});
    [
        nested(json!({"must": [{"is_empty": {"key": "price"}}, shop]})),
        nested(json!({"must": [{"is_null": {"key": "shop"}}]})),
        nested(json!({"must": [cheap], "must_not": [{"is_empty": {"key": "tags"}}]})),
        nested(json!({"must": [{"key": "tags", "is_empty": true}]})),
        nested(json!({"should": [{"is_null": {"key": "shop"}}, {"is_empty": {"key": "rating"}}]})),
    ]
    .into_iter()
    .map(|filter| serde_json::from_value(filter).unwrap())
    .collect()
}

/// Points of `segment` matching `filter`, checked on their payload without any index.
fn payload_reference(segment: &Segment, filter: &Filter) -> Vec<ExtendedPointId> {
    let no_indexes = AHashMap::<JsonPath, Vec<FieldIndex>>::new();
    segment
        .read_filtered(
            None,
            None,
            None,
            &AtomicBool::new(false),
            DeferredBehavior::WithDeferred,
        )
        .unwrap()
        .into_iter()
        .filter(|&id| {
            let payload = segment.payload(id).unwrap();
            check_payload(
                Box::new(|| OwnedPayloadRef::from(&payload)),
                None,
                &HashMap::new(),
                filter,
                0,
                &no_indexes,
            )
        })
        .collect()
}

/// Whether the `nested` conditions of `filter` are answered from per-element indexes.
fn nested_from_indexes(segment: &Segment, filter: &Filter) -> Vec<bool> {
    let conditions = filter
        .must
        .iter()
        .chain(&filter.must_not)
        .flatten()
        .filter_map(|condition| match condition {
            crate::types::Condition::Nested(nested) => Some(nested),
            crate::types::Condition::Field(_)
            | crate::types::Condition::IsEmpty(_)
            | crate::types::Condition::IsNull(_)
            | crate::types::Condition::HasId(_)
            | crate::types::Condition::HasVector(_)
            | crate::types::Condition::Slice(_)
            | crate::types::Condition::Filter(_)
            | crate::types::Condition::CustomIdChecker(_) => None,
        });
    let payload_index = segment.payload_index.borrow();
    payload_index.with_view(|view| {
        conditions
            .map(|nested| view.nested_element_checker(nested).unwrap().is_some())
            .collect()
    })
}

#[test]
fn nested_filters_use_per_element_indexes() {
    let dir = Builder::new().prefix("nested_index").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(11);
    let _scope = ambient::test_guard();

    let mut pair = Pair::new(dir.path());
    pair.upsert(&mut rng, 0..NUM_POINTS);
    pair.create_indexes(None);

    let emptiness = nested_emptiness_filters()
        .into_iter()
        .map(|filter| (filter, true));
    for (filter, covered) in nested_filters().into_iter().chain(emptiness) {
        assert!(
            nested_from_indexes(&pair.flat, &filter)
                .iter()
                .all(|used| !used),
            "flattened indexes can't answer {filter:?}",
        );
        assert!(
            nested_from_indexes(&pair.nested, &filter)
                .iter()
                .all(|&used| used == covered),
            "per-element indexes must answer {filter:?}: {covered}",
        );
    }

    // Without per-element indexes of the array, nested filters fall back to the payload
    let key = JsonPath::from_str(FIELDS[0].0).unwrap();
    let mut op_num = pair.op_num;
    for &(field, _) in FIELDS {
        op_num += 1;
        pair.nested
            .delete_field_index(op_num, &JsonPath::from_str(field).unwrap())
            .unwrap();
    }
    assert!(
        !pair
            .nested
            .payload_index
            .borrow()
            .field_indexes
            .contains_key(&key)
    );
    for (filter, _) in nested_filters() {
        assert!(
            nested_from_indexes(&pair.nested, &filter)
                .iter()
                .all(|used| !used)
        );
    }
    assert_equal_filters(&pair.flat, &pair.nested);
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
        schema = schema.nested_in("offers");
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
        let is_null_index =
            flat_field_index.get_full_index_type().index_type == PayloadIndexType::NullIndex;
        assert_eq!(
            nested_field_index.get_full_index_type().index_type == PayloadIndexType::NullIndex,
            is_null_index,
        );
        let flat_count = flat_field_index.count_indexed_points().unwrap();
        let nested_count = nested_field_index.count_indexed_points().unwrap();
        let element_map = nested_field_index
            .as_nested()
            .map(|nested| nested.element_map());
        match element_map {
            // A flattened null index counts every point it covers, with or without values
            Some(_) if is_null_index => {}
            // Opening restores the build-time count without subtracting deletions
            Some(crate::index::field_index::nested_index::ElementMap::Immutable(_)) => {
                let deleted = nested.id_tracker.borrow().deleted_point_count();
                assert!(
                    nested_count >= flat_count && nested_count - flat_count <= deleted,
                    "{field}: indexed points {nested_count} of {flat_field_index:?}, \
                     flattened {flat_count}, deleted {deleted}",
                );
            }
            Some(crate::index::field_index::nested_index::ElementMap::Mutable(_)) | None => {
                assert_eq!(
                    flat_count, nested_count,
                    "{field}: indexed points of {flat_field_index:?}",
                );
            }
        }

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

/// Whether the values of a field in one element match a condition.
type ValuesCheck = fn(&[&Value]) -> bool;

fn assert_equal(flat: &Segment, nested: &Segment) {
    let stopped = AtomicBool::new(false);
    let _scope = ambient::test_guard();

    assert_eq!(flat.available_point_count(), nested.available_point_count());

    assert_equal_filters(flat, nested);

    // A per-element null index treats each element as its own entry: a condition matches
    // a point if one of its elements matches it, checked here on the payload
    let all_points = flat
        .read_filtered(None, None, None, &stopped, DeferredBehavior::WithDeferred)
        .unwrap();
    for field in ["price", "tags"] {
        let relative = JsonPath::from_str(field).unwrap();
        let checks: [(&str, ValuesCheck); 2] = [
            ("is_empty", |values| check_is_empty(values.iter().copied())),
            ("is_null", |values| check_is_null(values.iter().copied())),
        ];
        for (condition, check) in checks {
            let expected: Vec<_> = all_points
                .iter()
                .copied()
                .filter(|&id| {
                    let payload = flat.payload(id).unwrap();
                    payload
                        .0
                        .get("offers")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_object)
                        .any(|element| check(&relative.value_get(element)))
                })
                .collect();
            let filter: Filter = serde_json::from_value(json!({
                "must": [{condition: {"key": format!("offers[].{field}")}}],
            }))
            .unwrap();
            let points = nested
                .read_filtered(
                    None,
                    None,
                    Some(&filter),
                    &stopped,
                    DeferredBehavior::WithDeferred,
                )
                .unwrap();
            assert_eq!(expected, points, "filter {filter:?}");
        }
    }

    for filter in nested_emptiness_filters() {
        let points = nested
            .read_filtered(
                None,
                None,
                Some(&filter),
                &stopped,
                DeferredBehavior::WithDeferred,
            )
            .unwrap();
        assert!(
            !points.is_empty(),
            "filter {filter:?} must select something"
        );
        assert_eq!(
            payload_reference(nested, &filter),
            points,
            "filter {filter:?}"
        );
    }

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

/// Filtered reads, including `nested` conditions, match.
fn assert_equal_filters(flat: &Segment, nested: &Segment) {
    let stopped = AtomicBool::new(false);
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

        // Also checked point by point, without the index-driven candidates of the query plan
        let checked = |segment: &Segment| {
            let total = segment.id_tracker.borrow().total_point_count() as PointOffsetType;
            let payload_index = segment.payload_index.borrow();
            let points = payload_index.with_view(|view| {
                let context = view.filter_context(&filter).unwrap();
                (0..total)
                    .filter(|&point| context.check(point).unwrap())
                    .collect::<Vec<_>>()
            });
            external_ids(segment, points.into_iter())
        };
        assert_eq!(checked(flat), checked(nested), "checked filter {filter:?}");
    }
    assert!(matched_any, "filters must select something to compare");
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

/// Reopening must not subtract deleted points whose elements had no indexed value.
/// The build-time count may overestimate, but must still bound the live matches and
/// allow the remaining indexed points to be deleted without underflowing.
#[test]
fn immutable_nested_count_after_deletions_and_reload() {
    let _scope = ambient::test_guard();
    for memory in [None, Some(Memory::Cold)] {
        let dir = Builder::new().prefix("nested_count").tempdir().unwrap();
        let mut segment =
            build_simple_segment(&dir.path().join("source"), 2, Distance::Dot).unwrap();
        let field = JsonPath::from_str("offers[].shop").unwrap();
        for (id, payload) in [
            (0u64, json!({"offers": [{}]})),
            (1, json!({"offers": [{"shop": "a"}]})),
            (2, json!({"offers": [{"shop": "a"}]})),
        ] {
            segment
                .upsert_point(id + 1, id.into(), only_default_vector(&[1.0, 1.0]))
                .unwrap();
            segment
                .set_full_payload(id + 1, id.into(), &serde_json::from_value(payload).unwrap())
                .unwrap();
        }
        segment
            .create_field_index(
                4,
                &field,
                Some(&schema(PayloadSchemaType::Keyword, true, memory)),
            )
            .unwrap();
        let mut segment = optimize(&segment, &dir.path().join("optimized"));
        segment.delete_point(5, 0u64.into()).unwrap();
        segment.delete_point(6, 2u64.into()).unwrap();
        let mut segment = reload(segment);

        let condition: FieldCondition = serde_json::from_value(json!({
            "key": "offers[].shop", "match": {"value": "a"}
        }))
        .unwrap();
        let check = |segment: &Segment, expected_count: usize, expected_matches: usize| {
            let indexes = segment.payload_index.borrow();
            let index = indexes.field_indexes[&field]
                .iter()
                .find_map(FieldIndex::as_nested)
                .unwrap();
            assert!(matches!(
                index.element_map(),
                super::ElementMap::Immutable(_)
            ));
            assert_eq!(index.count_indexed_points().unwrap(), expected_count);
            let matches = index.filter(&condition).unwrap().unwrap().count();
            assert_eq!(matches, expected_matches);
            let estimation = index.estimate_cardinality(&condition).unwrap().unwrap();
            assert!(
                matches <= estimation.max,
                "{estimation:?}, matches {matches}"
            );
        };

        // Both value-bearing points are counted at build; only one is still live.
        check(&segment, 2, 1);
        segment.delete_point(7, 1u64.into()).unwrap();
        check(&segment, 1, 0);
        // Reopening restores the count but still excludes every deleted point.
        check(&reload(segment), 2, 0);
    }
}

/// Payload blocks of a deduplicating index are sized in points, like the flattened ones.
#[test]
fn nested_payload_blocks_count_points() {
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("nested_blocks").tempdir().unwrap();
    let mut segment = build_simple_segment(dir.path(), 2, Distance::Dot).unwrap();
    let field = JsonPath::from_str("offers[].shop").unwrap();
    for id in 0..10u64 {
        let payload = json!({"offers": [{"shop": "a"}, {"shop": "a"}, {"shop": "a"}]});
        segment
            .upsert_point(id + 1, id.into(), only_default_vector(&[1.0, 1.0]))
            .unwrap();
        segment
            .set_full_payload(id + 1, id.into(), &serde_json::from_value(payload).unwrap())
            .unwrap();
    }
    segment
        .create_field_index(
            11,
            &field,
            Some(&schema(PayloadSchemaType::Keyword, true, None)),
        )
        .unwrap();

    let indexes = segment.payload_index.borrow();
    let index = indexes.field_indexes[&field]
        .iter()
        .find_map(FieldIndex::as_nested)
        .unwrap();
    let blocks = |threshold| {
        let mut cardinalities = Vec::new();
        index
            .for_each_payload_block(threshold, field.clone(), &mut |block| {
                cardinalities.push(block.cardinality);
                Ok(())
            })
            .unwrap();
        cardinalities
    };

    // 30 elements of 10 points hold "a"
    assert_eq!(blocks(5), [10]);
    assert_eq!(blocks(15), Vec::<usize>::new());
}

/// Elements without the field must not dilute the estimation of a condition.
#[test]
fn nested_estimation_ignores_elements_without_field() {
    let _scope = ambient::test_guard();
    let dir = Builder::new()
        .prefix("nested_estimation")
        .tempdir()
        .unwrap();
    let mut segment = build_simple_segment(dir.path(), 2, Distance::Dot).unwrap();
    let field = JsonPath::from_str("offers[].shop").unwrap();
    for id in 0..20u64 {
        let mut offers = vec![json!({}); 9];
        offers.push(json!({"shop": "a"}));
        let payload = json!({ "offers": offers });
        segment
            .upsert_point(id + 1, id.into(), only_default_vector(&[1.0, 1.0]))
            .unwrap();
        segment
            .set_full_payload(id + 1, id.into(), &serde_json::from_value(payload).unwrap())
            .unwrap();
    }
    segment
        .create_field_index(
            21,
            &field,
            Some(&schema(PayloadSchemaType::Keyword, true, None)),
        )
        .unwrap();

    let indexes = segment.payload_index.borrow();
    let index = indexes.field_indexes[&field]
        .iter()
        .find_map(FieldIndex::as_nested)
        .unwrap();
    let condition: FieldCondition = serde_json::from_value(json!({
        "key": "offers[].shop", "match": {"value": "a"}
    }))
    .unwrap();
    let estimation = index.estimate_cardinality(&condition).unwrap().unwrap();
    // All 20 points match; the estimator's approximation may be off by a little
    assert!((18..=20).contains(&estimation.exp), "{estimation:?}");
}

/// Directories of the element maps of the per-element indexes of `field`.
fn element_map_dirs(segment: &Segment, field: &str) -> Vec<std::path::PathBuf> {
    let field = JsonPath::from_str(field).unwrap();
    let payload_index = segment.payload_index.borrow();
    payload_index.field_indexes[&field]
        .iter()
        .filter_map(FieldIndex::as_nested)
        .map(|index| {
            let index_dir = index
                .get_full_index_type()
                .index_type
                .storage_dir(&segment.segment_path.join("payload_index"), &field);
            crate::index::field_index::nested_index::ElementMap::storage_dir(&index_dir)
        })
        .collect()
}

/// Every per-element index keeps an element map of its own next to its inner index: the
/// map goes away with the index, and a lost map rebuilds the field when it is loaded.
#[test]
fn element_map_lifecycle() {
    let dir = Builder::new().prefix("nested_index").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(7);
    let _scope = ambient::test_guard();

    let mut pair = Pair::new(dir.path());
    pair.upsert(&mut rng, 0..NUM_POINTS);
    pair.create_indexes(None);
    pair.mutate(&mut rng, NUM_POINTS as usize);

    // Integers have a lookup, a range and a null index, each with its own map
    let (price, _) = FIELDS[0];
    let price_maps = element_map_dirs(&pair.nested, price);
    assert_eq!(price_maps.len(), 3);
    assert!(price_maps.iter().all_unique());
    assert!(price_maps.iter().all(|map_dir| map_dir.exists()));

    // A lost map rebuilds the field from payload
    let (shop, _) = FIELDS[1];
    let shop_maps = element_map_dirs(&pair.nested, shop);
    pair.nested.flush(true).unwrap();
    fs_err::remove_dir_all(&shop_maps[0]).unwrap();
    let Pair {
        flat,
        nested,
        mut op_num,
    } = pair;
    let flat = reload(flat);
    let mut nested = reload(nested);
    assert!(shop_maps[0].exists());
    assert_equal(&flat, &nested);

    op_num += 1;
    nested
        .delete_field_index(op_num, &JsonPath::from_str(price).unwrap())
        .unwrap();
    assert!(price_maps.iter().all(|map_dir| !map_dir.exists()));
    assert!(shop_maps.iter().all(|map_dir| map_dir.exists()));
}

/// The nested wrapper consumes array elements, while ordinary and null indexes
/// consume the indexed field. All must refresh when their input paths change.
#[rstest::rstest]
fn indexes_follow_input_paths(
    #[values(false, true)] nested: bool,
    #[values(false, true)] build_before_points: bool,
) {
    let dir = Builder::new().prefix("index_input_path").tempdir().unwrap();
    let _scope = ambient::test_guard();
    let mut segment = build_simple_segment(dir.path(), 2, Distance::Dot).unwrap();
    let field = JsonPath::from_str("offers[].price").unwrap();
    let schema = schema(PayloadSchemaType::Integer, nested, None);
    if build_before_points {
        segment
            .create_field_index(1, &field, Some(&schema))
            .unwrap();
    }
    segment
        .upsert_point(2, 1.into(), only_default_vector(&[1.0, 1.0]))
        .unwrap();
    segment
        .set_full_payload(
            2,
            1.into(),
            &crate::payload_json! {
                "offers": [{"price": 10}, {}, null, 7, {"price": null}, {"price": 20}],
            },
        )
        .unwrap();
    if !build_before_points {
        segment
            .create_field_index(3, &field, Some(&schema))
            .unwrap();
    }

    let check = |segment: &Segment, elements: usize, prices: &[i64], is_null: bool| {
        let point = segment.get_internal_id(1.into()).unwrap();
        let payload_index = segment.payload_index.borrow();
        let indexes = &payload_index.field_indexes[&field];
        for index in indexes {
            if let Some(nested) = index.as_nested() {
                assert_eq!(nested.element_map().point_elements(point).count(), elements);
            }
            if let FieldIndex::NullIndex(index) = index {
                assert_eq!(
                    index.values_is_empty(point).unwrap(),
                    prices.is_empty() && !is_null
                );
                assert_eq!(index.values_is_null(point).unwrap(), is_null);
            }
        }
        for price in [10, 20] {
            let condition: FieldCondition = serde_json::from_value(json!({
                "key": field, "match": {"value": price},
            }))
            .unwrap();
            let expected = if prices.contains(&price) {
                vec![point]
            } else {
                vec![]
            };
            let mut checked = false;
            for index in indexes {
                if let Some(points) = index.filter(&condition).unwrap() {
                    assert_eq!(points.collect::<Vec<_>>(), expected, "{condition:?}");
                    checked = true;
                }
            }
            assert!(checked);
        }
    };
    check(&segment, 4, &[10, 20], true);

    segment
        .delete_payload(4, 1.into(), &"offers[0].price".parse().unwrap())
        .unwrap();
    check(&segment, 4, &[20], true);

    // A previously non-object element becomes an object without the indexed
    // field. It still needs an element offset to preserve array positions.
    segment
        .set_payload(
            5,
            1.into(),
            &crate::payload_json! { "shop": "a" },
            &Some("offers[2]".parse().unwrap()),
        )
        .unwrap();
    check(&segment, 5, &[20], true);
    segment
        .delete_payload(6, 1.into(), &"offers[4].price".parse().unwrap())
        .unwrap();
    check(&segment, 5, &[20], false);
    segment = reload(segment);
    check(&segment, 5, &[20], false);

    segment
        .delete_payload(7, 1.into(), &"offers[]".parse().unwrap())
        .unwrap();
    check(&segment, 0, &[], false);
    segment
        .set_payload(8, 1.into(), &crate::payload_json! { "offers": [{}] }, &None)
        .unwrap();
    check(&segment, 1, &[], false);
    segment
        .set_payload(
            9,
            1.into(),
            &crate::payload_json! { "price": 10 },
            &Some("offers[0]".parse().unwrap()),
        )
        .unwrap();
    check(&segment, 1, &[10], false);
    segment
        .delete_payload(10, 1.into(), &"offers".parse().unwrap())
        .unwrap();
    check(&segment, 0, &[], false);
}

/// Words of the per-element text tests, skewed so queries mix common and rare ones.
fn text_word(rng: &mut StdRng) -> String {
    let rank = (rng.random::<f64>().powi(2) * 12.0) as usize;
    format!("w{rank}")
}

fn random_titles(rng: &mut StdRng) -> Vec<String> {
    (0..rng.random_range(0..4))
        .map(|_| {
            (0..rng.random_range(1..6))
                .map(|_| text_word(rng))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

fn titles_payload(titles: &[String]) -> Payload {
    let offers: Vec<_> = titles
        .iter()
        .map(|title| json!({ "title": title }))
        .collect();
    let mut payload = Payload::default();
    payload.0.insert("offers".into(), Value::Array(offers));
    payload
}

fn text_schema(nested: bool) -> PayloadFieldSchema {
    let schema = PayloadFieldSchema::FieldParams(crate::types::PayloadSchemaParams::Text(
        crate::data_types::index::TextIndexParams {
            lowercase: Some(true),
            scoring: Some(Default::default()),
            ..Default::default()
        },
    ));
    if nested {
        schema.nested_in("offers")
    } else {
        schema
    }
}

/// Segment with a text index on `offers[].title` and the given titles per point;
/// points without an entry don't exist.
fn text_segment(dir: &Path, points: &[(u64, Vec<String>)], nested: bool) -> Segment {
    let mut segment = build_simple_segment(dir, 2, Distance::Dot).unwrap();
    let key = JsonPath::from_str("offers[].title").unwrap();
    segment
        .create_field_index(0, &key, Some(&text_schema(nested)))
        .unwrap();
    for (op_num, (id, titles)) in points.iter().enumerate() {
        let op_num = op_num as u64 + 1;
        segment
            .upsert_point(op_num, (*id).into(), only_default_vector(&[1.0, 1.0]))
            .unwrap();
        segment
            .set_full_payload(op_num, (*id).into(), &titles_payload(titles))
            .unwrap();
    }
    segment
}

/// Per element, a text index matches and scores every element as a document of its
/// own: the same as a flattened index over a segment with one point per element.
fn check_text_against_reference(
    nested: &Segment,
    points: &HashMap<u64, Vec<String>>,
    dir: &Path,
    rng: &mut StdRng,
) {
    use crate::data_types::query_context::QueryContext;
    use crate::index::field_index::full_text_index::Bm25Params;
    use crate::index::field_index::full_text_index::tokenizers::Tokenizer;
    use crate::types::{WithPayload, WithVector};

    const LIMIT: usize = 5;

    // One reference point per element, remembering the point that owns it
    let mut owners = Vec::new();
    let mut reference_points = Vec::new();
    let mut ids: Vec<_> = points.keys().copied().collect();
    ids.sort_unstable();
    for id in ids {
        for title in &points[&id] {
            reference_points.push((owners.len() as u64, vec![title.clone()]));
            owners.push(id);
        }
    }
    let reference = text_segment(dir, &reference_points, false);

    let key = JsonPath::from_str("offers[].title").unwrap();
    let crate::types::PayloadSchemaParams::Text(params) = text_schema(false).expand().into_owned()
    else {
        unreachable!()
    };
    let tokenizer = Tokenizer::new_from_text_index_params(&params);
    let id_of = |point: &crate::types::ScoredPoint| match point.id {
        ExtendedPointId::NumId(id) => id,
        ExtendedPointId::Uuid(_) => unreachable!(),
    };
    let ids_filter = |ids: Vec<u64>| -> Filter {
        serde_json::from_value(json!({"must": [{"has_id": ids}]})).unwrap()
    };
    let even = ids_filter(points.keys().copied().filter(|id| id % 2 == 0).collect());
    let reference_even = ids_filter(
        (0..owners.len() as u64)
            .filter(|&element| owners[element as usize] % 2 == 0)
            .collect(),
    );

    let mut matched_any = false;
    let mut scored_any = false;
    for _ in 0..20 {
        let query = (0..rng.random_range(1..=3))
            .map(|_| text_word(rng))
            .collect::<Vec<_>>()
            .join(" ");

        // A point matches when one of its elements has all the words
        let filter: Filter = serde_json::from_value(
            json!({"must": [{"key": "offers[].title", "match": {"text": query}}]}),
        )
        .unwrap();
        let read = |segment: &Segment| {
            segment
                .read_filtered(
                    None,
                    None,
                    Some(&filter),
                    &AtomicBool::new(false),
                    DeferredBehavior::WithDeferred,
                )
                .unwrap()
        };
        let mut expected: Vec<_> = read(&reference)
            .into_iter()
            .map(|element| match element {
                ExtendedPointId::NumId(element) => ExtendedPointId::from(owners[element as usize]),
                ExtendedPointId::Uuid(_) => unreachable!(),
            })
            .collect();
        expected.sort_unstable();
        expected.dedup();
        matched_any |= !expected.is_empty();
        assert_eq!(read(nested), expected, "match text `{query}`");

        let mut terms = Vec::new();
        tokenizer.tokenize_query(&query, |token| terms.push(token.into_owned()));
        for (corpus, reference_corpus, filter, reference_filter) in [
            (None, None, None, None),
            (
                Some(&even),
                Some(&reference_even),
                Some(&even),
                Some(&reference_even),
            ),
        ] {
            let score = |segment: &Segment, corpus: Option<&Filter>, filter, limit| {
                let mut query_context = QueryContext::default();
                query_context.init_text_stats(&key, corpus, terms.iter().cloned());
                segment.fill_query_context(&mut query_context).unwrap();
                segment
                    .score_bm25(
                        &key,
                        &terms,
                        Bm25Params::default(),
                        &WithPayload::from(false),
                        &WithVector::Bool(false),
                        filter,
                        limit,
                        &query_context.get_segment_query_context(),
                    )
                    .unwrap()
            };

            // A point scores as its best element
            let mut best: HashMap<u64, f32> = HashMap::new();
            for element in score(&reference, reference_corpus, reference_filter, usize::MAX) {
                let owner = owners[id_of(&element) as usize];
                let best = best.entry(owner).or_insert(element.score);
                *best = best.max(element.score);
            }
            let mut expected_scores: Vec<_> = best.values().copied().collect();
            expected_scores.sort_by(|a, b| b.total_cmp(a));
            expected_scores.truncate(LIMIT);

            let scored = score(nested, corpus, filter, LIMIT);
            scored_any |= !scored.is_empty();
            let scores: Vec<_> = scored.iter().map(|point| point.score).collect();
            let close = |a: f32, b: f32| (a - b).abs() <= 1e-4 * a.abs().max(1.0);
            assert!(
                scores.len() == expected_scores.len()
                    && scores
                        .iter()
                        .zip(&expected_scores)
                        .all(|(&a, &b)| close(a, b)),
                "BM25 `{query}`: {scores:?} != {expected_scores:?}",
            );
            for point in &scored {
                let id = id_of(point);
                assert!(close(point.score, best[&id]), "score of point {id}");
            }
        }
    }
    assert!(matched_any && scored_any, "queries must hit something");
}

#[test]
fn nested_text_index_scores_elements() {
    let dir = Builder::new().prefix("nested_text").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(3);
    let _scope = ambient::test_guard();

    let mut points: HashMap<u64, Vec<String>> = (0..NUM_POINTS)
        .map(|id| (id, random_titles(&mut rng)))
        .collect();
    let initial: Vec<_> = (0..NUM_POINTS)
        .map(|id| (id, points[&id].clone()))
        .collect();
    let mut nested = text_segment(&dir.path().join("nested"), &initial, true);
    check_text_against_reference(&nested, &points, &dir.path().join("reference_1"), &mut rng);

    // Overwrites move growing points to new element ranges, deletions free them
    let mut op_num = NUM_POINTS + 1;
    for id in (0..NUM_POINTS).step_by(3) {
        op_num += 1;
        if id % 2 == 0 {
            let titles = random_titles(&mut rng);
            nested
                .set_full_payload(op_num, id.into(), &titles_payload(&titles))
                .unwrap();
            points.insert(id, titles);
        } else {
            nested.delete_point(op_num, id.into()).unwrap();
            points.remove(&id);
        }
    }
    check_text_against_reference(&nested, &points, &dir.path().join("reference_2"), &mut rng);

    let nested = reload(nested);
    check_text_against_reference(&nested, &points, &dir.path().join("reference_3"), &mut rng);

    let optimized = optimize(&nested, &dir.path().join("optimized"));
    check_text_against_reference(
        &optimized,
        &points,
        &dir.path().join("reference_4"),
        &mut rng,
    );
}

/// Text conditions in a `nested` filter match per element, from the per-element index
/// or from the payload of each element alike.
#[test]
fn nested_text_filters_match_payload_check() {
    let dir = Builder::new().prefix("nested_text").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(5);
    let _scope = ambient::test_guard();
    let stopped = AtomicBool::new(false);

    let points: Vec<_> = (0..NUM_POINTS)
        .map(|id| (id, random_titles(&mut rng)))
        .collect();
    let flat = text_segment(&dir.path().join("flat"), &points, false);
    let nested = text_segment(&dir.path().join("nested"), &points, true);

    let mut matched_any = false;
    for _ in 0..50 {
        let text = format!("{} {}", text_word(&mut rng), text_word(&mut rng));
        let condition = match rng.random_range(0..3) {
            0 => json!({"key": "title", "match": {"text": text}}),
            1 => json!({"key": "title", "match": {"phrase": text}}),
            _ => json!({"key": "title", "match": {"text_any": text}}),
        };
        let other = json!({"key": "title", "match": {"text": text_word(&mut rng)}});
        let filter: Filter = serde_json::from_value(json!({"must": [{"nested": {
            "key": "offers",
            "filter": {"must": [condition], "must_not": [other]},
        }}]}))
        .unwrap();

        assert_eq!(nested_from_indexes(&flat, &filter), vec![false]);
        assert_eq!(nested_from_indexes(&nested, &filter), vec![true]);

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
        let expected = read(&flat);
        matched_any |= !expected.is_empty();
        assert_eq!(read(&nested), expected, "filter {filter:?}");
    }
    assert!(matched_any);
}

#[test]
fn nested_schema_validation() {
    let schema: PayloadFieldSchema = serde_json::from_value(json!({
        "type": "keyword",
        "nested": "offers",
    }))
    .unwrap();
    let field = |path: &str| JsonPath::from_str(path).unwrap();

    assert!(schema.validate_for_field(&field("offers[].shop")).is_ok());
    // Not inside the elements of the array
    assert!(schema.validate_for_field(&field("shop")).is_err());
    assert!(schema.validate_for_field(&field("offers[]")).is_err());
    // Stored as the array key
    assert_eq!(
        schema.normalized().nested(),
        Some(&JsonPath::from_str("offers[]").unwrap()),
    );

    let two_levels: PayloadFieldSchema =
        serde_json::from_value(json!({"type": "integer", "nested": "offers[].items"})).unwrap();
    assert!(
        two_levels
            .validate_for_field(&field("offers[].items[].size"))
            .is_err()
    );

    let tenant: PayloadFieldSchema = serde_json::from_value(json!({
        "type": "keyword",
        "is_tenant": true,
        "nested": "offers",
    }))
    .unwrap();
    assert!(tenant.validate_for_field(&field("offers[].shop")).is_err());
}
