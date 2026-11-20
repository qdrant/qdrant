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
//! | `sampling_filter_matching_nothing_*`   | tag    | unindexed  | sampling → no cands   |
//! | `all_points_deleted_*`                 | tag    | any        | empty segment         |
//!
//! Each test asserts the [`FacetPlan`] it actually executed, not just the
//! counts: every plan returns identical counts, so a threshold change could
//! otherwise silently route a test down a different path.
//!
//! The sampling → filter iter plan needs the segment to be much larger than the
//! sampling budget: a filter selective enough to prefer filter iter (< 10% of
//! candidate mass) is also selective enough for the strategy veto to fall back
//! to full, unless selectivity ≥ √(sample_target / available_points). With the
//! production budget (1000) that window opens around 150k points; that test
//! instead shrinks the budget (see `FILTER_ITER_SAMPLE_TARGET`) so the same
//! window opens on a 10× smaller fixture.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::types::DeferredBehavior;
use ordered_float::OrderedFloat;
use tempfile::{Builder, TempDir};

use crate::data_types::facets::{FacetParams, FacetValue};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorRef};
use crate::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
    StorageSegmentEntry as _,
};
use crate::json_path::JsonPath;
use crate::payload_json;
use crate::segment::Segment;
use crate::segment::read_view::{FacetPlan, default_sample_target};
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
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("facet_segment").tempdir().unwrap();

    let dim = 2;
    let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();
    let vector: Vec<f32> = (0..dim).map(|i| i as f32 / 10.0).collect();
    let vectors = NamedVectors::from_ref(DEFAULT_VECTOR_NAME, VectorRef::from(&vector));

    let mut op = 0u64;
    for i in 0..n {
        let point_id = PointIdType::from(i as u64 + 1);
        segment.insert_new_vectors(point_id, op, &vectors).unwrap();
        op += 1;

        let payload = payload_json! {
            COLOUR_KEY: COLOURS[i % 3],
            TAG_KEY: format!("tag_{i}"),
            SEQ_KEY: i as f64,
        };
        segment.set_full_payload(op, point_id, &payload).unwrap();
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

fn facet_request(key: &str, filter: Option<Filter>) -> FacetParams {
    FacetParams {
        key: JsonPath::new(key),
        limit: LIMIT,
        filter,
        exact: false,
    }
}

/// Run the facet with the production sampling budget and assert it executed
/// the `expected` plan.
fn run_facet(
    segment: &Segment,
    key: &str,
    filter: Option<Filter>,
    expected: FacetPlan,
) -> HashMap<FacetValue, usize> {
    let request = facet_request(key, filter);
    run_planned(segment, &request, default_sample_target(&request), expected)
}

/// Like [`run_facet`], but with an explicit per-segment sampling budget instead
/// of the production `max(LIMIT * 10, 1000)`.
fn run_facet_with_sample_target(
    segment: &Segment,
    key: &str,
    filter: Option<Filter>,
    sample_target: usize,
    expected: FacetPlan,
) -> HashMap<FacetValue, usize> {
    run_planned(
        segment,
        &facet_request(key, filter),
        sample_target,
        expected,
    )
}

fn run_planned(
    segment: &Segment,
    request: &FacetParams,
    sample_target: usize,
    expected: FacetPlan,
) -> HashMap<FacetValue, usize> {
    // Lazy filter probes read payload storage, which bumps ambient hw counters.
    let _scope = ambient::test_guard();
    let (hits, plan) = segment
        .with_view(|view| {
            view.approximate_facet_planned(request, sample_target, &AtomicBool::new(false))
        })
        .unwrap();
    assert_eq!(plan, expected, "unexpected facet plan");
    hits
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

    let hits = run_facet(&segment, COLOUR_KEY, None, FacetPlan::FullCounts);

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

    let hits = run_facet(
        &segment,
        COLOUR_KEY,
        Some(filter.clone()),
        FacetPlan::FullFilterIter,
    );

    assert!(!hits.is_empty());
    assert_counts_exact(&segment, COLOUR_KEY, Some(&filter), &hits);
}

/// full → facet iter. `seq < 90%` is above the threshold, so the terminal op
/// walks every value's posting probing a precomputed filter bitmap.
#[test]
fn full_broad_filter_walks_facet_index() {
    let (_dir, segment) = build_segment();
    let filter = seq_below(N_POINTS * 90 / 100);

    let hits = run_facet(
        &segment,
        COLOUR_KEY,
        Some(filter.clone()),
        FacetPlan::FullFacetIter,
    );

    assert_eq!(hits.len(), COLOURS.len());
    assert_counts_exact(&segment, COLOUR_KEY, Some(&filter), &hits);
}

/// sampling → counts. `tag` has `N_POINTS` uniques (> sample_target), no
/// filter: phase 1 samples candidates, then their counts are read exactly.
#[test]
fn sampling_unfiltered_samples_then_counts() {
    let (_dir, segment) = build_segment();

    let hits = run_facet(&segment, TAG_KEY, None, FacetPlan::SamplingCounts);

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

    let hits = run_facet(
        &segment,
        TAG_KEY,
        Some(filter.clone()),
        FacetPlan::SamplingFacetIter,
    );

    assert!(!hits.is_empty());
    assert!(hits.len() < N_POINTS);
    assert_counts_exact(&segment, TAG_KEY, Some(&filter), &hits);
}

/// Sampling budget for the sampling → filter iter test, 10× below the
/// production minimum (1000). Every decision on that path depends only on
/// `sample_target / N` and on filter selectivity, so shrinking both by the
/// same factor reaches the same plan with the same margins.
const FILTER_ITER_SAMPLE_TARGET: usize = 100;

/// Points for the sampling → filter iter test. The veto keeps sampling only
/// while selectivity ≥ √(sample_target / N), so reaching filter iter (< 10%)
/// needs N well above the sampling budget; an `f = 9%` filter lands in the
/// [√(100/N), 10%) window with margin (boundary ≈ 7.1% here). Equivalent to
/// 200k points with the production budget of 1000.
const N_FILTER_ITER: usize = 20_000;

/// sampling → filter iter — the plan only the bench otherwise reaches. `seq <
/// 9%` is selective enough for phase 2 to prefer iterating the merged filter,
/// yet broad enough to clear the sampling→full veto at this scale.
#[test]
fn sampling_selective_filter_iterates_filter() {
    let (_dir, segment) = build_segment_n(N_FILTER_ITER);
    let filter = seq_below(N_FILTER_ITER * 9 / 100);

    let hits = run_facet_with_sample_target(
        &segment,
        TAG_KEY,
        Some(filter.clone()),
        FILTER_ITER_SAMPLE_TARGET,
        FacetPlan::SamplingFilterIter,
    );

    assert!(!hits.is_empty());
    assert!(hits.len() < N_FILTER_ITER);
    assert_counts_exact(&segment, TAG_KEY, Some(&filter), &hits);
}

/// Unindexed key that no point has: filters on it match nothing, but without
/// an index its cardinality is estimated as unknown (`exp = N/2`, `max = N`).
const ABSENT_KEY: &str = "absent";

/// Sampling budget for [`sampling_filter_matching_nothing_returns_empty`]. The
/// veto keeps sampling while `sample_target / (exp / N) ≤ max`, i.e. here
/// `100 / 0.5 = 200 ≤ 2000`: a 10× margin. With the production budget (1000)
/// it is `2000 ≤ 2000`, too close to the boundary to rely on.
const NO_CANDIDATES_SAMPLE_TARGET: usize = 100;

/// sampling → no candidates. The filter's estimate is broad enough to keep
/// sampling, but no drawn point passes it, so phase 1 returns no candidates
/// and phase 2 is skipped.
#[test]
fn sampling_filter_matching_nothing_returns_empty() {
    let (_dir, segment) = build_segment();
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new(ABSENT_KEY),
        Match::from("x".to_string()),
    )));

    let hits = run_facet_with_sample_target(
        &segment,
        TAG_KEY,
        Some(filter),
        NO_CANDIDATES_SAMPLE_TARGET,
        FacetPlan::SamplingNoCandidates,
    );

    assert!(hits.is_empty());
}

/// Empty segment shortcut: once every point is deleted, nothing is counted
/// even though the facet index still holds (now invisible) postings.
#[test]
fn all_points_deleted_returns_empty() {
    let (_dir, mut segment) = build_segment();
    {
        let _scope = ambient::test_guard();
        let mut op = segment.version();
        for i in 0..N_POINTS {
            op += 1;
            assert!(
                segment
                    .delete_point(op, PointIdType::from(i as u64 + 1))
                    .unwrap()
            );
        }
    }

    for filter in [None, Some(seq_below(N_POINTS))] {
        let hits = run_facet(&segment, TAG_KEY, filter, FacetPlan::EmptySegment);
        assert!(hits.is_empty());
    }
}
