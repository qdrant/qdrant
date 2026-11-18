//! BM25 over a text index under filters of different sizes, for a plain and a
//! nested (`offers[].title`) text field, in an appendable and an optimized segment.

#[cfg(not(target_os = "windows"))]
mod prof;

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::flags::FeatureFlags;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use segment::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
};
use segment::index::field_index::full_text_index::Bm25Params;
use segment::json_path::JsonPath;
use segment::segment::Segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment_with_payload_storage;
use segment::types::{
    Distance, Filter, HnswGlobalConfig, Indexes, Payload, PayloadFieldSchema, PayloadStorageType,
    SegmentConfig, VectorDataConfig, VectorStorageType, WithPayload, WithVector,
};
use serde_json::{Value, json};
use tempfile::Builder;

const NUM_POINTS: u64 = 100_000;
const VOCAB: usize = 20_000;
const QUERY: [&str; 4] = ["black", "natural", "hair", "dye"];
/// Share of texts holding each query word.
const PLANTED: [f64; 4] = [0.08, 0.04, 0.03, 0.005];
/// Brands of a fixed size; every other point gets a random one of many.
const BRANDS: [(&str, u64); 3] = [("b5", 5), ("b50", 50), ("b500", 500)];
const LIMIT: usize = 10;

fn text(rng: &mut SmallRng, words: usize, branded: bool) -> String {
    let mut text: Vec<String> = (0..words)
        .map(|_| {
            let rank = (rng.random::<f64>().powi(3) * VOCAB as f64) as usize;
            format!("w{rank}")
        })
        .collect();
    for (i, (word, share)) in QUERY.iter().zip(PLANTED).enumerate() {
        // Every fixed-size brand point matches, so its filter has hits to score
        if (branded && i == 0) || rng.random_bool(share) {
            text.push((*word).to_string());
        }
    }
    text.join(" ")
}

fn payload(rng: &mut SmallRng, id: u64) -> Payload {
    let mut start = 0;
    let mut brand = None;
    for (name, size) in BRANDS {
        if (start..start + size).contains(&id) {
            brand = Some(name);
        }
        start += size;
    }
    let branded = brand.is_some();
    let brand = brand.map_or_else(
        || format!("r{}", rng.random_range(0..20_000)),
        str::to_string,
    );
    let offers: Vec<Value> = (0..rng.random_range(0..7))
        .map(|_| {
            let words = rng.random_range(3..9);
            json!({"title": text(rng, words, branded)})
        })
        .collect();
    let words = rng.random_range(8..25);
    let mut payload = Payload::default();
    payload.0.insert("brand".into(), json!(brand));
    payload
        .0
        .insert("title".into(), json!(text(rng, words, branded)));
    payload.0.insert("offers".into(), Value::Array(offers));
    payload
}

fn build(dir: &Path) -> Segment {
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
            .set_full_payload(op_num, id.into(), &payload(&mut rng, id))
            .unwrap();
    }
    for (field, schema) in [
        ("brand", json!("keyword")),
        (
            "title",
            json!({"type": "text", "scoring": {"type": "bm25"}}),
        ),
        (
            "offers[].title",
            json!({"type": "text", "scoring": {"type": "bm25"}, "nested": "offers[]"}),
        ),
    ] {
        op_num += 1;
        let schema: PayloadFieldSchema = serde_json::from_value(schema).unwrap();
        segment
            .create_field_index(op_num, &JsonPath::from_str(field).unwrap(), Some(&schema))
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

fn filters() -> Vec<(&'static str, Option<Filter>)> {
    let brand = |name: &str| json!({"key": "brand", "match": {"value": name}});
    let parse = |filter: Value| Some(serde_json::from_value(filter).unwrap());
    vec![
        ("none", None),
        ("brand-5", parse(json!({"must": [brand("b5")]}))),
        ("brand-50", parse(json!({"must": [brand("b50")]}))),
        ("brand-500", parse(json!({"must": [brand("b500")]}))),
        // The same 50 points, but without primary clauses
        (
            "not-not-brand-50",
            parse(json!({"must_not": [{"must_not": [brand("b50")]}]})),
        ),
    ]
}

fn bm25_filtering(c: &mut Criterion) {
    let dir = Builder::new().prefix("bm25_filtering").tempdir().unwrap();
    let _scope = ambient::test_guard();

    let appendable = build(&dir.path().join("appendable"));
    let optimized = optimize(&appendable, &dir.path().join("optimized"));
    let terms: Vec<String> = QUERY.iter().map(|word| (*word).to_string()).collect();

    for field in ["title", "offers[].title"] {
        let key = JsonPath::from_str(field).unwrap();
        for (variant, segment) in [("appendable", &appendable), ("optimized", &optimized)] {
            let mut query_context = QueryContext::default();
            query_context.init_text_stats(&key, None, terms.iter().cloned());
            segment.fill_query_context(&mut query_context).unwrap();
            let context = query_context.get_segment_query_context();
            let score = |filter: Option<&Filter>| {
                segment
                    .score_bm25(
                        &key,
                        &terms,
                        Bm25Params::default(),
                        &WithPayload::from(false),
                        &WithVector::Bool(false),
                        filter,
                        LIMIT,
                        &context,
                    )
                    .unwrap()
            };

            let mut group = c.benchmark_group(format!("bm25-filtering/{field}/{variant}"));
            for (name, filter) in filters() {
                assert!(
                    !score(filter.as_ref()).is_empty(),
                    "{field} {variant} {name}"
                );
                group.bench_function(name, |b| b.iter(|| score(filter.as_ref()).len()));
            }
            group.finish();
        }
    }
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = bm25_filtering,
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bm25_filtering,
}

criterion_main!(benches);
