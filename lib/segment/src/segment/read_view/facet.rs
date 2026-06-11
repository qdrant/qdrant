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
use std::sync::atomic::{AtomicBool, Ordering};

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
const ITER_FILTER_INDEX_SELECTIVITY: f64 = 0.3;

/// Multiplier from the user-facing `limit` to the per-segment sampling budget.
///
/// Together with [`MIN_SAMPLE_TARGET`] this determines how many distinct
/// candidate values the sampling phase tries to collect before falling back
/// to the exact-count post-pass. Calibrated from a Monte-Carlo sweep — see
/// the module docs.
const OVERSAMPLE_FACTOR: usize = 10;

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
/// Maximum number of consecutive points that may be drawn without introducing
/// any new candidate value before we give up. This bounds the rejection cost
/// when the long tail of unique values is too thin to make further progress
/// (e.g. very heavily skewed distributions where the top ~K values dominate
/// the total mass).
const MAX_EMPTY_POINTS: usize = 4096;

/// Compute the per-segment sampling target from a user-facing `limit`.
fn sample_target_for(limit: usize) -> usize {
    (limit.saturating_mul(OVERSAMPLE_FACTOR)).max(MIN_SAMPLE_TARGET)
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
    // todo: don't precompute, use bloom filter instead
    /// Evaluate the filter lazily on each check.
    Lazy {
        context: Box<dyn FilterContext + 'a>,
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

        let facet_index = self.facet_index_for(&request.key)?;
        // Decide strategy based on facet cardinality vs requested limit.
        let unique_count = facet_index.unique_values_count();
        let use_sampling = unique_count > request.limit.saturating_mul(OVERSAMPLE_FACTOR);

        if use_sampling {
            self.sampled_approximate_facet(&facet_index, request, is_stopped, hw_counter)
        } else {
            self.full_approximate_facet(&facet_index, request, is_stopped, hw_counter)
        }
    }

    /// Full-scan approximate-facet strategy: aggregate counts over every
    /// distinct value in the index.
    ///
    /// Prefer [`Self::approximate_facet`] for the strategy-aware entry point;
    /// call this directly only when scanning is explicitly required.
    pub fn full_approximate_facet(
        &self,
        facet_index: &impl FacetIndex,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let Some(filter) = &request.filter else {
            // No filter: count how many visible points each value has.
            return self.get_facet_counts(facet_index, None);
        };

        let cardinality = self
            .payload_index
            .estimate_cardinality(filter, hw_counter)?;

        let available_points = self.id_tracker.available_point_count();

        // If traversing the entire filter is less than x% the available points, use it.
        // Otherwise, prefer to go over the facet index directly.
        let prefer_filter_iter = (cardinality.full_scan_evals(available_points) as f64)
            < ITER_FILTER_INDEX_SELECTIVITY * available_points as f64;

        if prefer_filter_iter {
            // Iterate the filter, then hash each value to get counts
            self.visit_filter_iter(facet_index, filter, &cardinality, is_stopped, hw_counter)
        } else {
            // Check each values' points against the filter, count total per value
            let probe = FilterProbe::Lazy {
                context: self.payload_index.filter_context(filter, hw_counter)?,
            };
            self.visit_facet_iter(facet_index, &probe, None, is_stopped, hw_counter)
        }
    }

    /// Sampling approximate-facet strategy for high-cardinality facet fields.
    ///
    /// Gets a set of candidates by random sampling, then calculates their counts.
    pub(super) fn sampled_approximate_facet(
        &self,
        facet_index: &impl FacetIndex,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let sample_target = sample_target_for(request.limit);

        let Some(filter) = &request.filter else {
            // No filter: extract each values' count from the index, but sample candidates first
            let candidates = self.collect_candidate_values(
                sample_target,
                facet_index,
                None,
                is_stopped,
                hw_counter,
            )?;

            return self.get_facet_counts(facet_index, Some(candidates));
        };

        let probe = FilterProbe::Lazy {
            context: self.payload_index.filter_context(filter, hw_counter)?,
        };

        // Phase 1: sample candidate values, applying the filter to the random
        // draws via a probe (see [`FilterProbe`]).
        let candidates = self.collect_candidate_values(
            sample_target,
            facet_index,
            Some(&probe),
            is_stopped,
            hw_counter,
        )?;
        if candidates.is_empty() {
            return Ok(HashMap::new());
        }

        let candidate_filter = candidate_match_filter(&request.key, &candidates);
        let candidate_cardinality = self
            .payload_index
            .estimate_cardinality(&candidate_filter, hw_counter)?;

        let new_filter = candidate_filter.merge_owned(filter.clone());
        let new_cardinality = self
            .payload_index
            .estimate_cardinality(&new_filter, hw_counter)?;

        let available_points = self.id_tracker.available_point_count();

        // Phase 2: get candidates' counts

        // If traversing the entire filter is less than x% the cardinality of filtering by candidates, use it.
        // Otherwise, prefer to go over the facet index directly.
        let prefer_filter_iter = (new_cardinality.full_scan_evals(available_points) as f64)
            < ITER_FILTER_INDEX_SELECTIVITY * candidate_cardinality.exp as f64;
        if prefer_filter_iter {
            // Visit the filtered points once, and hash candidate values out of the yielded points.
            self.visit_filter_iter(
                facet_index,
                &new_filter,
                &new_cardinality,
                is_stopped,
                hw_counter,
            )
        } else {
            // Walk each values' posting and check the probe.
            self.visit_facet_iter(
                facet_index,
                &probe,
                Some(candidates),
                is_stopped,
                hw_counter,
            )
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
    fn visit_filter_iter<F: FacetIndex>(
        &self,
        facet_index: &F,
        filter: &Filter,
        cardinality: &CardinalityEstimation,
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
            // todo: maybe we can avoid `unique`, it's approximate after all
            iter.unique().for_each(|value| {
                let value = value.to_owned();
                *hits.entry(value).or_insert(0) += 1;
            });
        })?;
        Ok(hits)
    }

    /// Iterate every value's posting and count the points passing `probe`.
    fn visit_facet_iter<F: FacetIndex>(
        &self,
        facet_index: &F,
        probe: &FilterProbe<'_>,
        candidates: Option<HashSet<FacetValue>>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let mut hits = HashMap::new();

        let max_id = self.deferred_internal_id().unwrap_or(PointOffsetType::MAX);
        // todo(perf): pass candidates list to for_each_value_map and for_each_count_per_value
        facet_index.for_each_value_map(hw_counter, |value, iter| {
            check_process_stopped(is_stopped)?;

            let value = value.to_owned();
            if candidates
                .as_ref()
                .is_some_and(|candidates| !candidates.contains(&value))
            {
                // skip non-candidate
                return Ok(());
            }

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

    fn get_facet_counts(
        &self,
        facet_index: &impl FacetIndex,
        candidates: Option<HashSet<FacetValue>>,
    ) -> Result<HashMap<FacetValue, usize>, OperationError> {
        let mut hits = HashMap::new();
        // todo: accept candidates into for_each_count_per_value
        facet_index.for_each_count_per_value(self.deferred_internal_id(), |hit| {
            let value = hit.value.to_owned();
            if hit.count > 0
                && candidates
                    .as_ref()
                    .is_none_or(|candidates| candidates.contains(&value))
            {
                hits.insert(value, hit.count);
            }
            Ok(())
        })?;
        Ok(hits)
    }

    /// Phase 1 of the sampling algorithm — see module docs.
    fn collect_candidate_values<F: FacetIndex>(
        &self,
        sample_target: usize,
        facet_index: &F,
        probe: Option<&FilterProbe<'_>>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashSet<FacetValue>> {
        let mut seen: HashSet<FacetValue> = HashSet::with_capacity(sample_target);

        // Build a stream of random internal point IDs to draw from.
        let id_stream = self.random_internal_id_stream(probe);

        let mut empty_streak: usize = 0;
        let is_finished = AtomicBool::new(false);
        facet_index.for_points_values(
            id_stream.stop_if(is_stopped).stop_if(&is_finished),
            hw_counter,
            |_point_id, vals_iter: &mut dyn Iterator<Item = FacetValueRef<'_>>| {
                for v in vals_iter {
                    if empty_streak >= MAX_EMPTY_POINTS || seen.len() >= sample_target {
                        is_finished.store(true, Ordering::Relaxed);
                        break;
                    }
                    if seen.insert(v.to_owned()) {
                        empty_streak = 0;
                    } else {
                        empty_streak += 1;
                    }
                }
            },
        )?;

        Ok(seen)
    }

    /// Stream of internal point IDs to sample from. Yields IDs in random
    /// order, filtering out soft-deleted and deferred points (and, if a probe
    /// is given, points that don't match the filter it carries).
    ///
    /// The `is_stopped` check is handled by the consumer
    /// (`collect_candidate_values`) on each drawn point.
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
        self.payload_index.facet_index_for(key).ok_or_else(|| {
            OperationError::MissingMapIndexForFacet {
                key: key.to_string(),
            }
        })
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
