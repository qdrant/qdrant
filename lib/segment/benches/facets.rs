#[cfg(not(target_os = "windows"))]
mod prof;

use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ordered_float::OrderedFloat;
use rand::distr::Distribution;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand_distr::Zipf;
use segment::data_types::facets::FacetParams;
use segment::data_types::vectors::only_default_vector;
use segment::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, Match, PayloadFieldSchema, PayloadSchemaType,
    PointIdType, Range, ValueVariants,
};
use tempfile::Builder;
use uuid::Uuid;

const NUM_POINTS: usize = 1_000_000;
const SEED: u64 = 42;

/// Version checks only skip operations strictly older than the stored version,
/// so a single constant works for every operation here.
const OP_NUM: u64 = 100;

/// Keyword field with [`CATEGORY_CARDINALITY`] unique values, assigned
/// uniformly at random. Low cardinality keeps `approximate_facet` on the
/// full (non-sampling) strategy.
const CATEGORY_KEY: &str = "category";
const CATEGORY_CARDINALITY: usize = 50;
/// UUID field whose values are spread out evenly: each unique value occurs only
/// 1-2 times, so the field has very high cardinality (~`NUM_POINTS` uniques).
const UNIFORM_KEY: &str = "uuid_uniform";
/// UUID field whose values follow a Zipf (`s = 1`) distribution over roughly
/// `NUM_POINTS / 10` unique values: a few values are very common, most are rare.
const ZIPF_KEY: &str = "uuid_zipf";
/// Numeric field with values uniformly distributed in `[0.0, 1.0)`, used for
/// filtering: a `rand < f` range condition matches a fraction `f` of points,
/// so filter restrictiveness is directly tunable.
const RAND_KEY: &str = "rand";

/// Fraction of points matched by the selective filter, a conjunction of a
/// `category` keyword match and a `rand` range
const SELECTIVE_FRACTION: f64 = 0.01;
/// Fraction of points matched by the broad filter
const BROAD_FRACTION: f64 = 0.75;

/// Format an integer as a valid UUID string so it can be indexed as
/// [`PayloadSchemaType::Uuid`].
fn uuid_str(n: u64) -> String {
    Uuid::from_u128(n.into()).to_string()
}

/// Build a single segment holding three facet fields (a low-cardinality
/// keyword field plus two differently-distributed UUID fields) and a
/// uniformly-distributed numeric field to filter on.
///
/// Vectors are irrelevant to faceting, so every point gets the same trivial
/// 1-dimensional vector just to register the point in the segment.
fn build_facet_segment(path: &Path) -> Segment {
    let hw_counter = HardwareCounterCell::new();
    let mut rng = StdRng::seed_from_u64(SEED);

    let mut segment = build_simple_segment(path, 1, Distance::Dot).unwrap();
    let vector = [0.0];

    // Uniform: ~1.5 occurrences per unique value (flat distribution).
    let uniform_cardinality = (NUM_POINTS * 2 / 3).max(1);
    // Zipf (s = 1): heavily skewed over ~NUM_POINTS/10 unique values. Samples
    // land in `1..=zipf_cardinality`.
    let zipf_cardinality = (NUM_POINTS / 10).max(1);
    let zipf = Zipf::new(zipf_cardinality as f64, 1.0).unwrap();

    for i in 0..NUM_POINTS {
        let point_id = PointIdType::from(i as u64 + 1);
        segment
            .upsert_point(OP_NUM, point_id, only_default_vector(&vector), &hw_counter)
            .unwrap();

        let category_idx = rng.random_range(0..CATEGORY_CARDINALITY);
        let uniform_idx = rng.random_range(0..uniform_cardinality) as u64;
        let zipf_idx = zipf.sample(&mut rng) as u64;
        let rand_val = rng.random_range(0.0..1.0);

        let payload = payload_json! {
            CATEGORY_KEY: format!("cat_{category_idx}"),
            UNIFORM_KEY: uuid_str(uniform_idx),
            ZIPF_KEY: uuid_str(zipf_idx),
            RAND_KEY: rand_val,
        };
        segment
            .set_full_payload(OP_NUM, point_id, &payload, &hw_counter)
            .unwrap();
    }

    // Build the indexes in bulk once all points are in place.
    for (key, schema) in [
        (CATEGORY_KEY, PayloadSchemaType::Keyword),
        (UNIFORM_KEY, PayloadSchemaType::Uuid),
        (ZIPF_KEY, PayloadSchemaType::Uuid),
        (RAND_KEY, PayloadSchemaType::Float),
    ] {
        segment
            .create_field_index(
                OP_NUM,
                &JsonPath::new(key),
                Some(&PayloadFieldSchema::FieldType(schema)),
                &hw_counter,
            )
            .unwrap();
    }

    segment
}

/// `rand < fraction`: matches a `fraction` of points, uniformly at random.
fn range_condition(fraction: f64) -> Condition {
    Condition::Field(FieldCondition::new_range(
        JsonPath::new(RAND_KEY),
        Range {
            lt: Some(OrderedFloat(fraction)),
            ..Default::default()
        },
    ))
}

/// Single-clause filter matching a `fraction` of points.
fn range_filter(fraction: f64) -> Filter {
    Filter::new_must(range_condition(fraction))
}

/// Two-clause conjunction matching a `fraction` of points overall: a match on
/// one category (`1 / CATEGORY_CARDINALITY` of points) AND a `rand` range
/// scaled up to compensate. The two fields are independent, so the clause
/// selectivities multiply.
fn category_and_range_filter(fraction: f64) -> Filter {
    let rand_fraction = fraction * CATEGORY_CARDINALITY as f64;
    assert!(
        rand_fraction <= 1.0,
        "fraction can be at most 1/{CATEGORY_CARDINALITY}"
    );

    Filter {
        must: Some(vec![
            Condition::Field(FieldCondition::new_match(
                JsonPath::new(CATEGORY_KEY),
                Match::new_value(ValueVariants::String("cat_0".to_string())),
            )),
            range_condition(rand_fraction),
        ]),
        ..Default::default()
    }
}

/// Facet keys to benchmark, paired with a short label.
const KEYS: [(&str, &str); 3] = [
    ("low-card", CATEGORY_KEY),
    ("uniform", UNIFORM_KEY),
    ("zipf", ZIPF_KEY),
];

/// Filters to benchmark each key against, paired with a short label.
fn filters() -> [(&'static str, Option<Filter>); 3] {
    [
        ("no-filter", None),
        (
            "selective-filter",
            Some(category_and_range_filter(SELECTIVE_FRACTION)),
        ),
        ("broad-filter", Some(range_filter(BROAD_FRACTION))),
    ]
}

/// Covers all six paths of `approximate_facet`.
///
/// The sampling strategy is picked iff the field's unique-value count exceeds
/// `limit * OVERSAMPLE_FACTOR` (10 * 10 = 100 here), so `low-card` (50
/// uniques) takes the full strategy while the UUID fields (>=100k uniques)
/// take the sampling one. Within each strategy, a filtered request iterates
/// the filter when its selectivity is below `ITER_FILTER_INDEX_SELECTIVITY`
/// (30%), and otherwise iterates the facet index probing the filter:
///
/// | facet key      | no-filter        | selective-filter | broad-filter |
/// |----------------|------------------|------------------|--------------|
/// | low-card       | counts per value | filter iter      | facet iter   |
/// | uniform / zipf | sample + counts  | filter iter      | facet iter   |
///
/// The selective filter includes a `category` match, which is also the
/// low-card facet key, so the `low-card/selective-filter` case counts a
/// single surviving value.
fn facet_benchmark(c: &mut Criterion) {
    let dir = Builder::new().prefix("facet_segment").tempdir().unwrap();
    let segment = build_facet_segment(dir.path());

    let hw_counter = HardwareCounterCell::new();
    let is_stopped = AtomicBool::new(false);

    let mut group = c.benchmark_group("facet");
    // A single facet over the whole segment takes a while; keep total runtime
    // sane.
    group.sample_size(10);

    for (key_label, key) in KEYS {
        for (filter_label, filter) in filters() {
            let request = FacetParams {
                key: JsonPath::new(key),
                limit: FacetParams::DEFAULT_LIMIT,
                filter,
                exact: false,
            };

            let bench_id = BenchmarkId::new(key_label, filter_label);
            group.bench_with_input(bench_id, &request, |b, request| {
                b.iter(|| segment.facet(request, &is_stopped, &hw_counter).unwrap());
            });
        }
    }

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = facet_benchmark
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = facet_benchmark
}

criterion_main!(benches);
