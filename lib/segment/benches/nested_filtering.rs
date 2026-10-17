//! `nested` filters answered from per-element indexes vs from payload.
//!
//! The same points go into two segments: one with flattened indexes of the nested
//! fields, where a `nested` condition reads the payload of every candidate, and one with
//! per-element indexes, where it is answered from the indexes. Both are measured as
//! appendable segments and after optimization into non-appendable ones.

#[cfg(not(target_os = "windows"))]
mod prof;

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::flags::FeatureFlags;
use common::types::DeferredBehavior;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use segment::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
};
use segment::json_path::JsonPath;
use segment::segment::Segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment_with_payload_storage;
use segment::types::{
    Distance, Filter, HnswGlobalConfig, Indexes, Payload, PayloadFieldSchema, PayloadSchemaType,
    PayloadStorageType, SegmentConfig, VectorDataConfig, VectorStorageType,
};
use serde_json::{Value, json};
use tempfile::Builder;

const NUM_POINTS: u64 = 100_000;
const NUM_SHOPS: usize = 100;
const MAX_PRICE: i64 = 1000;

const FIELDS: &[(&str, PayloadSchemaType)] = &[
    ("offers[].shop", PayloadSchemaType::Keyword),
    ("offers[].price", PayloadSchemaType::Integer),
    ("offers[].available", PayloadSchemaType::Bool),
];

fn random_payload(rng: &mut SmallRng) -> Payload {
    let offers: Vec<Value> = (0..rng.random_range(0..7))
        .map(|_| {
            json!({
                "shop": format!("s{}", rng.random_range(0..NUM_SHOPS)),
                "price": rng.random_range(0..MAX_PRICE),
                "available": rng.random_bool(0.5),
                // Unindexed bulk, like the rest of a real offer
                "title": "Some offer title with a few words in it",
                "url": "https://example.com/some/offer/url",
            })
        })
        .collect();
    let mut payload = Payload::default();
    payload.0.insert("offers".into(), Value::Array(offers));
    payload.0.insert(
        "description".into(),
        json!("Product description ".repeat(10)),
    );
    payload
}

fn build(dir: &Path, nested: bool) -> Segment {
    let mut rng = SmallRng::seed_from_u64(42);
    let mut segment =
        build_simple_segment_with_payload_storage(dir, 2, Distance::Dot, PayloadStorageType::Mmap)
            .unwrap();
    let mut op_num = 0;
    for id in 0..NUM_POINTS {
        op_num += 1;
        segment
            .upsert_point(op_num, id.into(), only_default_vector(&[1.0, 1.0]))
            .unwrap();
        segment
            .set_full_payload(op_num, id.into(), &random_payload(&mut rng))
            .unwrap();
    }
    for &(field, field_type) in FIELDS {
        op_num += 1;
        let key = JsonPath::from_str(field).unwrap();
        let schema: PayloadFieldSchema = if nested {
            // As stored after normalization: the array key
            serde_json::from_value(json!({"type": field_type, "nested": "offers[]"})).unwrap()
        } else {
            PayloadFieldSchema::FieldType(field_type)
        };
        segment
            .create_field_index(op_num, &key, Some(&schema))
            .unwrap();
    }
    segment
}

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
        payload_storage_type: PayloadStorageType::Mmap,
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
    builder.build_for_test(dir)
}

fn nested(filter: Value) -> Value {
    json!({"nested": {"key": "offers", "filter": filter}})
}

/// Makes a random filter of one query kind.
type MakeFilter = fn(&mut SmallRng) -> Filter;

/// Named random filters.
fn queries() -> Vec<(&'static str, MakeFilter)> {
    fn shop(rng: &mut SmallRng) -> Value {
        json!({"key": "shop", "match": {"value": format!("s{}", rng.random_range(0..NUM_SHOPS))}})
    }
    fn price_below(rng: &mut SmallRng, max: i64) -> Value {
        json!({"key": "price", "range": {"lt": rng.random_range(0..max)}})
    }
    fn parse(filter: Value) -> Filter {
        serde_json::from_value(filter).unwrap()
    }
    vec![
        // Selective: candidates from the `shop` index, then the nested check per point
        ("shop-and-price", |rng| {
            parse(
                json!({"must": [nested(json!({"must": [shop(rng), price_below(rng, MAX_PRICE)]}))]}),
            )
        }),
        // Broad: most points are candidates
        ("available-and-price", |rng| {
            let available = json!({"key": "available", "match": {"value": true}});
            parse(
                json!({"must": [nested(json!({"must": [available, price_below(rng, MAX_PRICE)]}))]}),
            )
        }),
        // Negated: no candidates from indexes, every point is checked
        ("not-shop-and-cheap", |rng| {
            parse(
                json!({"must_not": [nested(json!({"must": [shop(rng), price_below(rng, 100)]}))]}),
            )
        }),
    ]
}

fn nested_filtering(c: &mut Criterion) {
    let dir = Builder::new().prefix("nested_filtering").tempdir().unwrap();
    let _scope = ambient::test_guard();
    let stopped = AtomicBool::new(false);

    let payload = build(&dir.path().join("payload"), false);
    let per_element = build(&dir.path().join("per_element"), true);
    let payload_optimized = optimize(&payload, &dir.path().join("payload_optimized"));
    let per_element_optimized = optimize(&per_element, &dir.path().join("per_element_optimized"));

    let variants = [
        ("appendable/payload", &payload),
        ("appendable/per-element", &per_element),
        ("optimized/payload", &payload_optimized),
        ("optimized/per-element", &per_element_optimized),
    ];

    let read = |segment: &Segment, filter: &Filter| {
        segment
            .read_filtered(
                None,
                None,
                Some(filter),
                &stopped,
                DeferredBehavior::WithDeferred,
            )
            .unwrap()
    };

    for (query, make_filter) in queries() {
        // Same answer either way
        let mut rng = SmallRng::seed_from_u64(7);
        for _ in 0..20 {
            let filter = make_filter(&mut rng);
            let expected = read(&payload, &filter);
            for (name, segment) in &variants {
                assert_eq!(read(segment, &filter), expected, "{query} on {name}");
            }
        }

        let mut group = c.benchmark_group(format!("nested-filtering/{query}"));
        for (name, segment) in &variants {
            let mut rng = SmallRng::seed_from_u64(42);
            group.bench_function(*name, |b| {
                b.iter_batched(
                    || make_filter(&mut rng),
                    |filter| read(segment, &filter).len(),
                    BatchSize::SmallInput,
                )
            });
        }
        group.finish();
    }
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = nested_filtering,
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = nested_filtering,
}

criterion_main!(benches);
