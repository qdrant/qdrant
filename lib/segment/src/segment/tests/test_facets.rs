//! Behavioural coverage for `SegmentReadView::approximate_facet`.
//!
//! | test                                   | key    | filter     | intended plan         |
//! |----------------------------------------|--------|------------|-----------------------|
//! | `full_unfiltered_*`                    | colour | none       | full → counts         |
//! | `full_selective_filter_*`              | colour | seq <  5%  | full → filter iter    |
//! | `full_broad_filter_*`                  | colour | seq < 90%  | full → facet iter     |
//! | `sampling_unfiltered_*`                | tag    | none       | sampling → counts     |
//! | `sampling_selective_filter_*`          | tag    | seq <  9%  | sampling → filter iter|
//! | `sampling_broad_filter_*`              | tag    | seq < 95%  | sampling → facet iter |
//!
//! The sampling → filter iter plan needs more points than the others: a filter
//! selective enough to prefer filter iter (< 10% of candidate mass) is also
//! selective enough for the strategy veto to fall back to full, *unless* the
//! segment is much larger than the sampling budget — the veto keeps sampling
//! only while selectivity ≥ √(sample_target / available_points). That window
//! opens around 150k points here (see `N_FILTER_ITER`), so that one test uses a
//! larger fixture.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use ordered_float::OrderedFloat;
use tempfile::{Builder, TempDir};

use crate::data_types::facets::{FacetParams, FacetValue};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorRef};
use crate::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
};
use crate::json_path::JsonPath;
use crate::payload_json;
use crate::segment::Segment;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{
    Condition, Distance, FieldCondition, Filter, Match, PayloadFieldSchema, PayloadSchemaType,
    PointIdType, Range, ValueVariants,
};

const N_POINTS: usize = 2_000;
/// `sample_target = max(LIMIT * 10, 1000) = 1000`, so a field needs > 1000
/// unique values for the sampling strategy to engage.
const LIMIT: usize = 10;

const COLOUR_KEY: &str = "colour";
const TAG_KEY: &str = "tag";
const SEQ_KEY: &str = "seq";
const COLOURS: [&str; 3] = ["red", "blue", "green"];

/// Build the shared fixture with `N_POINTS` points.
fn build_segment() -> (TempDir, Segment) {
    build_segment_n(N_POINTS)
}

/// Build a fixture of `n` points, each with a `colour` (3 uniques), a unique
/// `tag`, and a `seq` equal to its index (for filters of exact selectivity).
fn build_segment_n(n: usize) -> (TempDir, Segment) {
    let hw_counter = HardwareCounterCell::new();
    let dir = Builder::new().prefix("facet_segment").tempdir().unwrap();

    let dim = 2;
    let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();
    let vector: Vec<f32> = (0..dim).map(|i| i as f32 / 10.0).collect();
    let vectors = NamedVectors::from_ref(DEFAULT_VECTOR_NAME, VectorRef::from(&vector));

    let mut op = 0u64;
    for i in 0..n {
        let point_id = PointIdType::from(i as u64 + 1);
        segment
            .insert_new_vectors(point_id, op, &vectors, &hw_counter)
            .unwrap();
        op += 1;

        let payload = payload_json! {
            COLOUR_KEY: COLOURS[i % 3],
            TAG_KEY: format!("tag_{i}"),
            SEQ_KEY: i as f64,
        };
        segment
            .set_full_payload(op, point_id, &payload, &hw_counter)
            .unwrap();
        op += 1;
    }

    for (key, schema) in [
        (COLOUR_KEY, PayloadSchemaType::Keyword),
        (TAG_KEY, PayloadSchemaType::Keyword),
        (SEQ_KEY, PayloadSchemaType::Float),
    ] {
        segment
            .create_field_index(
                op,
                &JsonPath::new(key),
                Some(&PayloadFieldSchema::FieldType(schema)),
                &hw_counter,
            )
            .unwrap();
        op += 1;
    }

    (dir, segment)
}

/// `seq < n`: matches exactly the first `n` points.
fn seq_below(n: usize) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_range(
        JsonPath::new(SEQ_KEY),
        Range {
            lt: Some(OrderedFloat(n as f64)),
            ..Default::default()
        },
    )))
}

fn run_facet(segment: &Segment, key: &str, filter: Option<Filter>) -> HashMap<FacetValue, usize> {
    let request = FacetParams {
        key: JsonPath::new(key),
        limit: LIMIT,
        filter,
        exact: false,
    };
    segment
        .facet(
            &request,
            &AtomicBool::new(false),
            &HardwareCounterCell::new(),
        )
        .unwrap()
}

/// Independently recompute the exact count for every returned value and assert
/// the facet matched it. Works regardless of which values the sampler surfaced:
/// sampling may omit values, but every value it *does* report must be counted
/// exactly.
fn assert_counts_exact(
    segment: &Segment,
    key: &str,
    filter: Option<&Filter>,
    hits: &HashMap<FacetValue, usize>,
) {
    let hw_counter = HardwareCounterCell::new();
    for (value, &count) in hits {
        let value_match = Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new(key),
            Match::from(ValueVariants::from(value.clone())),
        )));
        let exact_filter = Filter::merge_opts(Some(value_match), filter.cloned());
        let exact = segment
            .read_filtered(
                None,
                None,
                exact_filter.as_ref(),
                &AtomicBool::new(false),
                &hw_counter,
                DeferredBehavior::VisibleOnly,
            )
            .unwrap()
            .len();
        assert_eq!(count, exact, "facet count mismatch for {value:?}");
    }
}

/// full → counts. `colour` has 3 uniques (≪ sample_target), no filter.
#[test]
fn full_unfiltered_counts_every_value() {
    let (_dir, segment) = build_segment();

    let hits = run_facet(&segment, COLOUR_KEY, None);

    // Full strategy enumerates every value exactly.
    assert_eq!(hits.len(), COLOURS.len());
    assert_eq!(hits.values().sum::<usize>(), N_POINTS);
    assert_counts_exact(&segment, COLOUR_KEY, None, &hits);
}

/// full → filter iter. `seq < 5%` is below the 10% selectivity threshold, so
/// the terminal op iterates the filtered points and hashes their values.
#[test]
fn full_selective_filter_iterates_filter() {
    let (_dir, segment) = build_segment();
    let filter = seq_below(N_POINTS * 5 / 100);

    let hits = run_facet(&segment, COLOUR_KEY, Some(filter.clone()));

    assert!(!hits.is_empty());
    assert_counts_exact(&segment, COLOUR_KEY, Some(&filter), &hits);
}

/// full → facet iter. `seq < 90%` is above the threshold, so the terminal op
/// walks every value's posting probing a precomputed filter bitmap.
#[test]
fn full_broad_filter_walks_facet_index() {
    let (_dir, segment) = build_segment();
    let filter = seq_below(N_POINTS * 90 / 100);

    let hits = run_facet(&segment, COLOUR_KEY, Some(filter.clone()));

    assert_eq!(hits.len(), COLOURS.len());
    assert_counts_exact(&segment, COLOUR_KEY, Some(&filter), &hits);
}

/// sampling → counts. `tag` has `N_POINTS` uniques (> sample_target), no
/// filter: phase 1 samples candidates, then their counts are read exactly.
#[test]
fn sampling_unfiltered_samples_then_counts() {
    let (_dir, segment) = build_segment();

    let hits = run_facet(&segment, TAG_KEY, None);

    // Sampling returns a strict subset; the full strategy would return all
    // `N_POINTS` values.
    assert!(!hits.is_empty());
    assert!(
        hits.len() < N_POINTS,
        "sampling should not surface every value, got {}",
        hits.len()
    );
    assert_counts_exact(&segment, TAG_KEY, None, &hits);
}

/// sampling → facet iter. `seq < 95%` is broad enough to both avoid the
/// sampling→full veto and drive phase 2 down the posting-walk branch.
#[test]
fn sampling_broad_filter_walks_facet_index() {
    let (_dir, segment) = build_segment();
    let filter = seq_below(N_POINTS * 95 / 100);

    let hits = run_facet(&segment, TAG_KEY, Some(filter.clone()));

    assert!(!hits.is_empty());
    assert!(hits.len() < N_POINTS);
    assert_counts_exact(&segment, TAG_KEY, Some(&filter), &hits);
}

/// Points for the sampling → filter iter test. The veto keeps sampling only
/// while selectivity ≥ √(sample_target / N), so reaching filter iter (< 10%)
/// needs N well above the sampling budget; an `f = 9%` filter lands in the
/// [√(1000/N), 10%) window with margin (boundary ≈ 7.1% here). Verified
/// empirically: the crossover first appears around N = 150k.
const N_FILTER_ITER: usize = 200_000;

/// sampling → filter iter — the plan only the bench otherwise reaches. `seq <
/// 9%` is selective enough for phase 2 to prefer iterating the merged filter,
/// yet broad enough to clear the sampling→full veto at this scale.
#[test]
fn sampling_selective_filter_iterates_filter() {
    let (_dir, segment) = build_segment_n(N_FILTER_ITER);
    let filter = seq_below(N_FILTER_ITER * 9 / 100);

    let hits = run_facet(&segment, TAG_KEY, Some(filter.clone()));

    assert!(!hits.is_empty());
    assert!(hits.len() < N_FILTER_ITER);
    assert_counts_exact(&segment, TAG_KEY, Some(&filter), &hits);
}
