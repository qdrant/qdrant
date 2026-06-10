//! Approximate facet read path.
//!
//! [`SegmentReadView::approximate_facet`] is the entry point; it walks a small
//! decision tree (see the diagram in PR #9208) before producing a
//! `value -> count` map. Two cardinalities drive the choice:
//!
//! - **Facet cardinality** — how many distinct values the field has. When it is
//!   large relative to the requested `limit` we *sample* a candidate set of
//!   values first ([`Self::sampled_approximate_facet`]); otherwise we visit
//!   every value ([`Self::approximate_facet_scan`]).
//! - **Filter cardinality** — how many points the request filter matches. This
//!   picks between the two terminal operations below.
//!
//! ## Terminal operations
//!
//! Both produce *exact* counts; sampling only affects *which* values appear.
//!
//! - [`Self::walk_filter_index`] iterates the filtered points and hashes each
//!   point's facet value(s) into the count map. Fewer filter checks (the
//!   filter's own posting drives the scan) but one value hash per matched
//!   point. Preferred when the filter is selective.
//! - [`Self::walk_facet_index`] iterates the value postings and checks the
//!   filter for each point. More filter checks (one per posting element) but no
//!   value hashing. Preferred when the filter is broad.
//!
//! The filter is applied either *lazily* (a full condition evaluation per
//! checked point) or through a [`BitmapFilterContext`] materialized once and
//! probed per point — see [`FilterProbe`].
//!
//! ## Sampling
//!
//! When sampling, the candidate values restrict both operations:
//!
//! 1. **Iterative novelty sampling** ([`Self::collect_candidate_values`]).
//!    Stream point IDs in random order (filtered, if a filter is given), look
//!    up each point's values, and collect distinct values until `SAMPLE_TARGET`
//!    are found, the stream is exhausted, or too many consecutive draws fail to
//!    contribute a new value.
//! 2. **Exact-count post-pass.** Count each candidate exactly: either one
//!    `walk_facet_index` per candidate posting, or a single `walk_filter_index`
//!    over a `match-any(candidates)` filter (so the filtered points are visited
//!    once instead of once per candidate).
//!
//! The Monte-Carlo simulation in `facet-sampling-mc` (see thread context for
//! PR #9208) suggests `SAMPLE_TARGET = max(request.limit * 10, 1000)` gives
//! ≥90% recall on top-K for Zipf-distributed fields with cardinality up to
//! `10^5`, and trivially-correct results on UUID-style fields (where every
//! value has count 1 and "top-K" is "any K").

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::{DeferredBehavior, PointOffsetType};
use itertools::Itertools;

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::facets::{FacetParams, FacetValue, FacetValueRef};
use crate::id_tracker::IdTrackerRead;
use crate::index::field_index::{CardinalityEstimation, FacetIndex};
use crate::index::{BitmapFilterContext, PayloadIndexRead};
use crate::json_path::JsonPath;
use crate::payload_storage::{FilterContext, PayloadStorageRead};
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{Condition, FieldCondition, Filter, IntPayloadType, Match, ValueVariants};

/// Multiplier from the user-facing `limit` to the threshold above which the
/// per-segment facet read switches from the full-scan strategy
/// ([`SegmentReadView::approximate_facet_scan`]) to the sampling strategy
/// ([`SegmentReadView::sampled_approximate_facet`]).
///
/// Rationale: the full-scan path has to visit every distinct value in the
/// index, so its cost is `O(unique_values_count)`. The sampling path is
/// `O(limit * SAMPLE_OVERSAMPLE_FACTOR)` regardless of cardinality, but it
/// is only accurate when the long tail of unique values is large compared
/// to the requested top-K. The crossover is at
/// `unique_values_count ≈ limit * FACET_FULL_SCAN_FACTOR`; below that, we
/// stay on the full-scan path (we'd need to read most of the values anyway,
/// so iterating once is simpler and avoids the per-candidate post-pass).
const FACET_FULL_SCAN_FACTOR: usize = 4;

/// Filter selectivity below which [`SegmentReadView::walk_filter_index`] beats
/// [`SegmentReadView::walk_facet_index`].
///
/// When the filter is expected to match less than this fraction of the
/// segment, iterating its (few) matches and hashing their values is cheaper
/// than checking the filter against every value posting. Picked from rudimentary
/// benchmarking of two scenarios: a collection with few keys, and a collection
/// with almost a unique key per point.
//
// TODO(facets): define a better estimate for this decision. The question is:
// what is more expensive, to hash the same value excessively, or to check the
// filter too many times?
const WALK_FILTER_INDEX_SELECTIVITY: f64 = 0.3;

/// Multiplier from the user-facing `limit` to the per-segment sampling budget.
///
/// Together with [`MIN_SAMPLE_TARGET`] this determines how many distinct
/// candidate values the sampling phase tries to collect before falling back
/// to the exact-count post-pass. Calibrated from a Monte-Carlo sweep — see
/// the module docs.
const SAMPLE_OVERSAMPLE_FACTOR: usize = 10;

/// Lower bound on the per-segment sampling budget.
///
/// For very small `limit` (e.g. 10) the oversample factor alone leaves too
/// little headroom against per-segment rank variance, so we enforce a floor.
const MIN_SAMPLE_TARGET: usize = 1000;

/// Batch size for the inner random-id iteration loop. Picked so that
/// `for_points_values` is invoked over a meaningful chunk of point IDs at
/// once (amortises any per-call overhead) while still letting us stop early
/// once `SAMPLE_TARGET` distinct values are collected. Setting this to 1
/// recovers the per-point semantics; the Monte-Carlo simulation showed no
/// measurable accuracy difference between B=1 and B=32.
const SAMPLE_BATCH: usize = 32;

/// Maximum number of consecutive batches that may pass without introducing
/// any new candidate value before we give up. This bounds the rejection cost
/// when the long tail of unique values is too thin to make further progress
/// (e.g. very heavily skewed distributions where the top ~K values dominate
/// the total mass).
///
/// With `SAMPLE_BATCH = 32` this caps the wasted work at ~4k point lookups.
const MAX_EMPTY_BATCHES: usize = 128;

/// A sampled candidate value paired with its `field == value` match filter and
/// the filter's cardinality estimate, as produced by
/// [`SegmentReadView::prepare_candidates`] for the per-candidate post-pass.
type PreparedCandidate = (FacetValue, Filter, CardinalityEstimation);

/// Compute the per-segment sampling target from a user-facing `limit`.
fn sample_target_for(limit: usize) -> usize {
    (limit.saturating_mul(SAMPLE_OVERSAMPLE_FACTOR)).max(MIN_SAMPLE_TARGET)
}

/// Whether the filter is selective enough to prefer
/// [`SegmentReadView::walk_filter_index`] over
/// [`SegmentReadView::walk_facet_index`]. See [`WALK_FILTER_INDEX_SELECTIVITY`].
fn prefers_walk_filter_index(cardinality: &CardinalityEstimation, available_points: usize) -> bool {
    (cardinality.exp as f64) < WALK_FILTER_INDEX_SELECTIVITY * available_points as f64
}

/// Build a `field MATCH ANY [candidates]` filter.
///
/// Lets the sampling post-pass visit the filtered points once (over the union
/// of candidate postings) instead of once per candidate. A facet field holds a
/// single value type, so the candidates collapse to one [`AnyVariants`] arm;
/// `Bool` fields never reach here (only two values, far below the sampling
/// threshold), so an all-empty result is unreachable in practice.
///
/// [`AnyVariants`]: crate::types::AnyVariants
fn candidate_match_filter(key: &JsonPath, candidates: &HashSet<FacetValue>) -> Filter {
    let mut strings: Vec<String> = Vec::new();
    let mut integers: Vec<IntPayloadType> = Vec::new();
    for value in candidates {
        match ValueVariants::from(value.clone()) {
            ValueVariants::String(string) => strings.push(string),
            ValueVariants::Integer(integer) => integers.push(integer),
            ValueVariants::Bool(_) => {}
        }
    }

    let any_match = if integers.is_empty() {
        Match::from(strings)
    } else {
        Match::from(integers)
    };

    Filter::new_must(Condition::Field(FieldCondition::new_match(
        key.clone(),
        any_match,
    )))
}

/// How the user filter is applied to individual points.
///
/// The variant is chosen by comparing the expected number of `check` calls
/// against the cost of materializing the filter
/// ([`CardinalityEstimation::full_scan_evals`]): a lazy check costs a full
/// condition evaluation, a probe against a materialized bitmap is nearly free,
/// but the bitmap costs one evaluation pass up front.
pub enum FilterProbe<'a> {
    /// Evaluate the filter lazily on each check.
    Lazy {
        context: Box<dyn FilterContext + 'a>,
        filter: &'a Filter,
        cardinality: CardinalityEstimation,
    },
    /// The filter was materialized into a bitmap.
    Precomputed(BitmapFilterContext),
}

impl<'a> FilterProbe<'a> {
    fn check(&self, point_id: PointOffsetType) -> bool {
        match self {
            FilterProbe::Lazy { context, .. } => context.check(point_id),
            FilterProbe::Precomputed(bitmap) => bitmap.check(point_id),
        }
    }

    /// If this is a lazy probe whose filter is now cheaper to materialize than
    /// to evaluate lazily `expected_checks` times, return the filter and its
    /// cardinality so the caller can build the bitmap. Returns `None` for an
    /// already-materialized probe, or while staying lazy is still cheaper.
    ///
    /// Used to revisit a lazy phase-1 choice once the candidates' summed
    /// posting size makes the post-pass check budget measurable.
    fn upgrade_to_bitmap(
        &self,
        expected_checks: usize,
        available_points: usize,
    ) -> Option<(&'a Filter, &CardinalityEstimation)> {
        match self {
            FilterProbe::Lazy {
                filter,
                cardinality,
                ..
            } if cardinality.full_scan_evals(available_points) <= expected_checks => {
                Some((*filter, cardinality))
            }
            FilterProbe::Lazy { .. } | FilterProbe::Precomputed(_) => None,
        }
    }
}

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    /// Top-level approximate-facet entry point. Picks between the full-scan
    /// and the sampling strategy based on how many distinct values the index
    /// contains relative to the requested `limit`.
    pub fn approximate_facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        // Shortcut if this segment has no points.
        let available_points = self.id_tracker.available_point_count();
        if available_points == 0 {
            return Ok(HashMap::new());
        }

        // Decide strategy based on facet cardinality vs requested limit.
        let unique_count = self.facet_index_for(&request.key)?.unique_values_count();
        let use_sampling = unique_count > request.limit.saturating_mul(FACET_FULL_SCAN_FACTOR);

        if use_sampling {
            self.sampled_approximate_facet(request, is_stopped, hw_counter)
        } else {
            self.full_approximate_facet(request, is_stopped, hw_counter)
        }
    }

    /// Full-scan approximate-facet strategy: aggregate counts over every
    /// distinct value in the index.
    ///
    /// Prefer [`Self::approximate_facet`] for the strategy-aware entry point;
    /// call this directly only when scanning is explicitly required.
    pub fn full_approximate_facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        // Shortcut if this segment has no points; prevent division by zero later.
        let available_points = self.id_tracker.available_point_count();
        if available_points == 0 {
            return Ok(HashMap::new());
        }

        let facet_index = self.facet_index_for(&request.key)?;

        let Some(filter) = &request.filter else {
            // No filter: count how many visible points each value has.
            let mut hits = HashMap::new();
            facet_index.for_each_count_per_value(self.deferred_internal_id(), |hit| {
                check_process_stopped(is_stopped)?;
                if hit.count > 0 {
                    hits.insert(hit.value.to_owned(), hit.count);
                }
                Ok(())
            })?;
            return Ok(hits);
        };

        let cardinality = self
            .payload_index
            .estimate_cardinality(filter, hw_counter)?;

        if prefers_walk_filter_index(&cardinality, available_points) {
            self.walk_filter_index(
                &facet_index,
                filter,
                &cardinality,
                |_| true,
                is_stopped,
                hw_counter,
            )
        } else {
            // The value-posting walk checks the filter once per posting element
            // — at least once per indexed point — so that is its check budget.
            let expected_checks = self.payload_index.indexed_points(&request.key);
            let probe = self.select_probe(
                filter,
                cardinality,
                expected_checks,
                available_points,
                is_stopped,
                hw_counter,
            )?;
            self.walk_facet_index(&facet_index, &probe, is_stopped, hw_counter)
        }
    }

    /// Sampling approximate-facet strategy for high-cardinality facet fields.
    ///
    /// Returns the same shape (`HashMap<FacetValue, usize>`) as the full-scan
    /// variant. Counts are exact (computed via a post-pass that respects the
    /// original filter); only the *set* of returned values is approximate.
    pub(super) fn sampled_approximate_facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let facet_index = self.facet_index_for(&request.key)?;
        let available_points = self.id_tracker.available_point_count();
        let sample_target = sample_target_for(request.limit);

        let Some(filter) = &request.filter else {
            // No filter: sample candidates, then count each candidate's visible
            // posting directly (no filter checks, no hashing).
            let candidates =
                self.collect_candidate_values(request, &facet_index, None, is_stopped, hw_counter)?;
            let (prepared, _) = self.prepare_candidates(request, candidates, hw_counter)?;
            return self.count_candidate_postings(prepared, None, is_stopped, hw_counter);
        };

        let cardinality = self
            .payload_index
            .estimate_cardinality(filter, hw_counter)?;

        // Phase 1: sample candidate values, applying the filter to the random
        // draws via a probe (see [`FilterProbe`]).
        //
        // Rejection sampling evaluates the filter once per drawn id. If phase 1
        // saturates, that's about `sample_target / selectivity` draws; with
        // fewer distinct values than the target it can't saturate and ends up
        // drawing the whole permutation. That expected number of checks is the
        // probe's materialization budget.
        let expected_rejection_evals = if facet_index.unique_values_count() <= sample_target {
            available_points
        } else {
            (sample_target.saturating_mul(available_points) / cardinality.exp.max(1))
                .min(available_points)
        };
        let mut probe = self.select_probe(
            filter,
            cardinality.clone(),
            expected_rejection_evals,
            available_points,
            is_stopped,
            hw_counter,
        )?;
        let candidates = self.collect_candidate_values(
            request,
            &facet_index,
            Some(&probe),
            is_stopped,
            hw_counter,
        )?;
        if candidates.is_empty() {
            return Ok(HashMap::new());
        }

        // Phase 2: exact-count each candidate, respecting the filter.
        if prefers_walk_filter_index(&cardinality, available_points) {
            // Visit the filtered points once, narrowed to the candidates' union
            // posting, and hash candidate values out of the yielded points.
            let match_filter =
                candidate_match_filter(&request.key, &candidates).merge_owned(filter.clone());
            let match_cardinality = self
                .payload_index
                .estimate_cardinality(&match_filter, hw_counter)?;
            self.walk_filter_index(
                &facet_index,
                &match_filter,
                &match_cardinality,
                |value| candidates.contains(value),
                is_stopped,
                hw_counter,
            )
        } else {
            // Walk each candidate's posting and probe the filter. The summed
            // posting size is the post-pass check budget — measurable only now,
            // and the basis for revisiting the lazy phase-1 probe choice
            // (mass-biased sampling on skewed fields surfaces candidates with
            // huge postings, where probing lazily would re-evaluate the filter
            // millions of times).
            let (prepared, total_candidate_points) =
                self.prepare_candidates(request, candidates, hw_counter)?;
            let upgraded = probe
                .upgrade_to_bitmap(total_candidate_points, available_points)
                .map(|(filter, cardinality)| {
                    self.collect_filter_context(filter, cardinality, is_stopped, hw_counter)
                })
                .transpose()?;
            if let Some(bitmap) = upgraded {
                probe = FilterProbe::Precomputed(bitmap);
            }
            self.count_candidate_postings(prepared, Some(&probe), is_stopped, hw_counter)
        }
    }

    /// **Walk filter index.** Iterate the points matching `filter` and hash the
    /// facet value(s) of each into a count map, keeping only values for which
    /// `keep` returns `true` (always, for the full scan; candidate membership,
    /// for sampling).
    ///
    /// `iter_filtered_points` with [`DeferredBehavior::VisibleOnly`] already
    /// excludes deferred and soft-deleted points, so the yielded ids can be
    /// hashed directly.
    fn walk_filter_index<F: FacetIndex>(
        &self,
        facet_index: &F,
        filter: &Filter,
        cardinality: &CardinalityEstimation,
        keep: impl Fn(&FacetValue) -> bool,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let mut hits = HashMap::new();
        let points = self.payload_index.iter_filtered_points(
            filter,
            cardinality,
            hw_counter,
            is_stopped,
            DeferredBehavior::VisibleOnly,
        )?;
        facet_index.for_points_values(points, hw_counter, |_point_id, iter| {
            iter.unique().for_each(|value| {
                let value = value.to_owned();
                if keep(&value) {
                    *hits.entry(value).or_insert(0) += 1;
                }
            });
        })?;
        Ok(hits)
    }

    /// **Walk facet index.** Iterate every value's posting and count the points
    /// passing `probe`. No value hashing: the count is attributed to the value
    /// whose posting is being walked.
    fn walk_facet_index<F: FacetIndex>(
        &self,
        facet_index: &F,
        probe: &FilterProbe<'_>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let max_id = self.deferred_internal_id().unwrap_or(PointOffsetType::MAX);
        let mut hits = HashMap::new();

        facet_index.for_each_value_map(hw_counter, |value, iter| {
            check_process_stopped(is_stopped)?;

            #[cfg(debug_assertions)]
            let iter = {
                let mut prev_id = None;
                iter.inspect(move |&id| {
                    let previous = prev_id.get_or_insert(id);
                    debug_assert!(*previous <= id, "Sorted iter assertion broken");
                    *previous = id;
                })
            };

            let count = iter
                .dedup()
                .take_while(|&point_id| point_id < max_id)
                .filter(|&point_id| probe.check(point_id))
                .count();

            if count > 0 {
                hits.insert(value.to_owned(), count);
            }
            Ok(())
        })?;

        Ok(hits)
    }

    /// Per-candidate variant of [`Self::walk_facet_index`]: walk each
    /// candidate's posting (via its precomputed match filter) and count the
    /// points passing `probe` (or all visible points when `probe` is `None`).
    fn count_candidate_postings(
        &self,
        prepared: Vec<PreparedCandidate>,
        probe: Option<&FilterProbe<'_>>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let mut hits = HashMap::with_capacity(prepared.len());
        for (value, match_filter, cardinality) in prepared {
            check_process_stopped(is_stopped)?;
            // `iter_filtered_points` with `DeferredBehavior::VisibleOnly`
            // already excludes deferred and soft-deleted points, so beyond the
            // filter probe we can simply count the yielded ids.
            let count = self
                .payload_index
                .iter_filtered_points(
                    &match_filter,
                    &cardinality,
                    hw_counter,
                    is_stopped,
                    DeferredBehavior::VisibleOnly,
                )?
                .filter(|&point_id| probe.is_none_or(|probe| probe.check(point_id)))
                .count();
            if count > 0 {
                hits.insert(value, count);
            }
        }
        Ok(hits)
    }

    /// Build the per-candidate `field == value` match filters and estimate each
    /// cardinality, returning them alongside the summed expected posting size
    /// (the post-pass check budget for [`Self::count_candidate_postings`]).
    fn prepare_candidates(
        &self,
        request: &FacetParams,
        candidates: HashSet<FacetValue>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<(Vec<PreparedCandidate>, usize)> {
        let mut total_candidate_points: usize = 0;
        let mut prepared = Vec::with_capacity(candidates.len());
        for value in candidates {
            let match_filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
                request.key.clone(),
                Match::new_value(value.clone().into()),
            )));
            let cardinality = self
                .payload_index
                .estimate_cardinality(&match_filter, hw_counter)?;
            total_candidate_points = total_candidate_points.saturating_add(cardinality.exp);
            prepared.push((value, match_filter, cardinality));
        }
        Ok((prepared, total_candidate_points))
    }

    /// Build a [`FilterProbe`] for `filter`, materializing it into a bitmap when
    /// that costs no more than evaluating it lazily `expected_checks` times, and
    /// staying lazy otherwise.
    fn select_probe<'a>(
        &'a self,
        filter: &'a Filter,
        cardinality: CardinalityEstimation,
        expected_checks: usize,
        available_points: usize,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<FilterProbe<'a>> {
        if cardinality.full_scan_evals(available_points) <= expected_checks {
            let bitmap =
                self.collect_filter_context(filter, &cardinality, is_stopped, hw_counter)?;
            Ok(FilterProbe::Precomputed(bitmap))
        } else {
            let context = self.payload_index.filter_context(filter, hw_counter)?;
            Ok(FilterProbe::Lazy {
                context,
                filter,
                cardinality,
            })
        }
    }

    /// Phase 1 of the sampling algorithm — see module docs.
    fn collect_candidate_values<F: FacetIndex>(
        &self,
        request: &FacetParams,
        facet_index: &F,
        probe: Option<&FilterProbe<'_>>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashSet<FacetValue>> {
        let sample_target = sample_target_for(request.limit);
        let mut seen: HashSet<FacetValue> = HashSet::with_capacity(sample_target);

        // Build a stream of random internal point IDs to draw from.
        let mut id_stream = self.random_internal_id_stream(probe);

        let mut batch_buf: Vec<PointOffsetType> = Vec::with_capacity(SAMPLE_BATCH);
        let mut empty_streak: usize = 0;
        loop {
            check_process_stopped(is_stopped)?;
            if seen.len() >= sample_target {
                break;
            }

            batch_buf.clear();
            for _ in 0..SAMPLE_BATCH {
                match id_stream.next() {
                    Some(id) => batch_buf.push(id),
                    None => break,
                }
            }
            if batch_buf.is_empty() {
                break;
            }

            let mut found_novel = false;
            facet_index.for_points_values(
                batch_buf.iter().copied(),
                hw_counter,
                |_point_id, vals_iter: &mut dyn Iterator<Item = FacetValueRef<'_>>| {
                    if seen.len() >= sample_target {
                        return;
                    }
                    for v in vals_iter {
                        let owned = v.to_owned();
                        if seen.insert(owned) {
                            found_novel = true;
                            if seen.len() >= sample_target {
                                break;
                            }
                        }
                    }
                },
            )?;

            if found_novel {
                empty_streak = 0;
            } else {
                empty_streak += 1;
                if empty_streak >= MAX_EMPTY_BATCHES {
                    break;
                }
            }
        }

        Ok(seen)
    }

    /// Stream of internal point IDs to sample from. Yields IDs in random
    /// order, filtering out soft-deleted and deferred points (and, if a probe
    /// is given, points that don't match the filter it carries).
    ///
    /// The `is_stopped` check is handled by the consumer
    /// (`collect_candidate_values`) on each batch boundary.
    fn random_internal_id_stream<'a>(
        &'a self,
        probe: Option<&'a FilterProbe<'_>>,
    ) -> impl Iterator<Item = PointOffsetType> + 'a {
        self.id_tracker
            .point_mappings()
            .iter_random_visible()
            .map(|(_external_id, internal_id)| internal_id)
            .filter(move |&internal_id| probe.is_none_or(|probe| probe.check(internal_id)))
    }

    /// Resolve the facet index for `request`, erroring if the field is not a
    /// map index.
    fn facet_index_for(&self, key: &JsonPath) -> OperationResult<impl FacetIndex + '_> {
        self.payload_index
            .facet_index_for(key)
            .ok_or_else(|| OperationError::MissingMapIndexForFacet {
                key: key.to_string(),
            })
    }

    /// Materialize `filter` into a [`BitmapFilterContext`]: one filtered
    /// iteration up front (driven by the filter's primary clauses when it has
    /// any), after which every check is a cheap bitmap probe.
    ///
    /// Deferred and soft-deleted points are excluded at construction, so a
    /// probe directly answers "visible point matching the filter".
    ///
    /// Worth it whenever the filter would otherwise be evaluated more than
    /// once per point: the single evaluation pass here costs no more than the
    /// first round of those checks.
    pub(super) fn collect_filter_context(
        &self,
        filter: &Filter,
        filter_cardinality: &CardinalityEstimation,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BitmapFilterContext> {
        let context = self
            .payload_index
            .iter_filtered_points(
                filter,
                filter_cardinality,
                hw_counter,
                is_stopped,
                DeferredBehavior::VisibleOnly,
            )?
            .filter(|&point_id| !self.id_tracker.is_deleted_point(point_id))
            .collect();
        Ok(context)
    }

    pub fn facet_values(
        &self,
        key: &JsonPath,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BTreeSet<FacetValue>> {
        let facet_index = self.payload_index.facet_index_for(key).ok_or_else(|| {
            OperationError::MissingMapIndexForFacet {
                key: key.to_string(),
            }
        })?;
        let mut values = BTreeSet::new();

        if let Some(filter) = filter {
            let filter_cardinality = self
                .payload_index
                .estimate_cardinality(filter, hw_counter)?;

            let points = self
                .payload_index
                .iter_filtered_points(
                    filter,
                    &filter_cardinality,
                    hw_counter,
                    is_stopped,
                    DeferredBehavior::VisibleOnly,
                )?
                .filter(|&point_id| !self.id_tracker.is_deleted_point(point_id));
            facet_index.for_points_values(points, hw_counter, |_point_id, iter| {
                values.extend(iter.map(|v| v.to_owned()));
            })?;
        } else {
            facet_index.for_each_visible_value(
                hw_counter,
                self.deferred_internal_id(),
                |value_ref| {
                    check_process_stopped(is_stopped)?;
                    values.insert(value_ref.to_owned());
                    Ok(())
                },
            )?;
        };

        Ok(values)
    }
}
