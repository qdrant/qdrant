//! Sampling-based approximate facet strategy.
//!
//! See [`SegmentReadView::approximate_facet`] for the dispatch logic between
//! this path and the full-scan path in [`super::facet`].
//!
//! Algorithm (per segment):
//!
//! 1. **Iterative novelty sampling.** Stream point IDs in random order
//!    (filtered if a filter is given), look up each point's payload values
//!    via the facet index, and collect distinct values into a candidate set.
//!    Stop when either `SAMPLE_TARGET` distinct candidates have been
//!    collected, the IDs stream is exhausted, or too many consecutive
//!    samples failed to contribute a new value (the long tail is too thin
//!    to make progress).
//!
//! 2. **Exact-count post-pass.** For each candidate value, compose the
//!    user filter with `field == value` and count matching points via the
//!    payload index. This yields the same counts the full-scan path would
//!    produce — sampling only affects which values appear in the result,
//!    not the counts attached to those values.
//!
//! The Monte-Carlo simulation in `facet-sampling-mc` (see thread context for
//! PR #9208) suggests `SAMPLE_TARGET = max(request.limit * 10, 1000)` gives
//! ≥90% recall on top-K for Zipf-distributed fields with cardinality up to
//! `10^5`, and trivially-correct results on UUID-style fields (where every
//! value has count 1 and "top-K" is "any K").

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{DeferredBehavior, PointOffsetType};

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::facets::{FacetParams, FacetValue, FacetValueRef};
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::FacetIndex;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{Condition, FieldCondition, Filter, Match, ValueVariants};

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

/// Compute the per-segment sampling target from a user-facing `limit`.
pub(super) fn sample_target_for(limit: usize) -> usize {
    (limit.saturating_mul(SAMPLE_OVERSAMPLE_FACTOR)).max(MIN_SAMPLE_TARGET)
}

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    /// Sampling variant of [`Self::approximate_facet`].
    ///
    /// Returns the same shape (`HashMap<FacetValue, usize>`) as the
    /// full-scan variant. Counts are exact (computed via a per-candidate
    /// post-pass that respects the original filter); only the *set* of
    /// values returned is approximate.
    pub(super) fn sampled_approximate_facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let facet_index = self
            .payload_index
            .facet_index_for(&request.key)
            .ok_or_else(|| OperationError::MissingMapIndexForFacet {
                key: request.key.to_string(),
            })?;

        // --- Phase 1: collect candidate values via iterative novelty sampling.
        let candidates =
            self.collect_candidate_values(request, &facet_index, is_stopped, hw_counter)?;

        // --- Phase 2: exact-count each candidate (respecting the filter).
        let mut hits = HashMap::with_capacity(candidates.len());
        for value in candidates {
            check_process_stopped(is_stopped)?;
            let count = self.exact_count_for_value(
                &request.key,
                &value,
                request.filter.as_ref(),
                is_stopped,
                hw_counter,
            )?;
            if count > 0 {
                hits.insert(value, count);
            }
        }

        Ok(hits)
    }

    /// Phase 1 of the sampling algorithm — see module docs.
    fn collect_candidate_values<F: FacetIndex>(
        &self,
        request: &FacetParams,
        facet_index: &F,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashSet<FacetValue>> {
        let sample_target = sample_target_for(request.limit);
        let mut seen: HashSet<FacetValue> = HashSet::with_capacity(sample_target);

        // Build a stream of random internal point IDs to draw from.
        let mut id_stream = self.random_internal_id_stream(request.filter.as_ref(), hw_counter)?;

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
    /// order, filtering out soft-deleted and deferred points (and, if a
    /// filter is given, points that don't match).
    ///
    /// The `is_stopped` check is handled by the consumer
    /// (`collect_candidate_values`) on each batch boundary; doing it inside
    /// the per-element filter here would be redundant.
    fn random_internal_id_stream<'a>(
        &'a self,
        filter: Option<&'a Filter>,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let unfiltered = self
            .id_tracker
            .point_mappings()
            .iter_random_visible()
            .map(|(_external_id, internal_id)| internal_id);

        match filter {
            None => Ok(Box::new(unfiltered)),
            Some(filter) => {
                let filter_context = self.payload_index.filter_context(filter, hw_counter)?;
                Ok(Box::new(unfiltered.filter(move |internal_id| {
                    filter_context.check(*internal_id)
                })))
            }
        }
    }

    /// Phase 2 of the sampling algorithm — exact-count one candidate value.
    ///
    /// Composes the original user filter (if any) with `field == value` and
    /// counts the matching points using the payload index's filter
    /// machinery — same recipe as `LocalShard::exact_facet`, but at segment
    /// granularity.
    fn exact_count_for_value(
        &self,
        key: &crate::json_path::JsonPath,
        value: &FacetValue,
        user_filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        let value_variant: ValueVariants = value.clone().into();
        let match_value = Filter::new_must(Condition::Field(FieldCondition::new_match(
            key.clone(),
            Match::new_value(value_variant),
        )));
        let merged = Filter::merge_opts(user_filter.cloned(), Some(match_value))
            .expect("merging Some yields Some");

        let cardinality_estimation = self
            .payload_index
            .estimate_cardinality(&merged, hw_counter)?;

        // `iter_filtered_points` with `DeferredBehavior::Exclude` already
        // filters out deferred and soft-deleted points internally, so we
        // can simply count the yielded internal IDs.
        let count = self
            .payload_index
            .iter_filtered_points(
                &merged,
                &cardinality_estimation,
                hw_counter,
                is_stopped,
                DeferredBehavior::Exclude,
            )?
            .count();

        Ok(count)
    }
}
