use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use common::condition_checker::ConditionChecker;
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::{DeferredBehavior, PointOffsetType};
use itertools::Itertools;
use roaring::RoaringBitmap;

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::facets::{FacetParams, FacetValue, FacetValueRef};
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::{CardinalityEstimation, FacetIndex};
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{Condition, FieldCondition, Filter, IntPayloadType, Match, ValueVariants};

/// Filter selectivity below which [`SegmentReadView::visit_filter_iter`] beats
/// [`SegmentReadView::visit_facet_iter`] on the full (non-sampling) path.
///
/// The `visit_filter_iter` cost grows linearly with selectivity, while the posting
/// walk with a precomputed probe is nearly flat.
///
/// If we make the `visit_filter_iter` cheaper, we can increase this value.
const ITER_FILTER_INDEX_SELECTIVITY: f64 = 0.1;

/// Multiplier from the user-facing `limit` to the per-segment sampling budget.
///
/// Together with [`MIN_SAMPLE_TARGET`] this determines how many distinct
/// candidate values the sampling phase tries to collect.
const OVERSAMPLE_FACTOR: usize = 10;

/// Lower bound on the per-segment sampling budget.
const MIN_SAMPLE_TARGET: usize = 1000;

/// Maximum number of consecutive points that may be drawn without introducing
/// any new candidate value before we give up.
const MAX_NO_NEW_POINTS: usize = 4096;

/// Build a `field MATCH ANY [candidates]` filter.
fn candidate_match_filter(key: &JsonPath, candidates: &HashSet<FacetValue>) -> Filter {
    let mut strings: Vec<String> = Vec::new();
    let mut integers: Vec<IntPayloadType> = Vec::new();
    for value in candidates {
        match ValueVariants::from(value.clone()) {
            ValueVariants::String(string) => strings.push(string),
            ValueVariants::Integer(integer) => integers.push(integer),
            ValueVariants::Bool(_) => {
                unreachable!(
                    "this filter is only constructed for value sampling,
                    and bool index can only have 2 possible values"
                )
            }
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

/// How the user filter is use to check individual points.
///
/// It can be "lazy" and check against the index directly,
/// or precomputed and only check against a bitmap.
pub enum FilterProbe<'a> {
    /// Evaluate the filter lazily on each check.
    Lazy {
        context: Box<dyn ConditionChecker<Error = OperationError> + 'a>,
    },
    /// The filter was materialized into a bitmap of matching points.
    Precomputed(RoaringBitmap),
}

impl<'a> FilterProbe<'a> {
    fn check(&self, point_id: PointOffsetType) -> bool {
        match self {
            FilterProbe::Lazy { context, .. } => context.check_infallible(point_id),
            FilterProbe::Precomputed(bitmap) => bitmap.contains(point_id),
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
    /// Top-level approximate-facet entry point. Picks the appropriate
    /// method for calculating facets
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
        let sample_target = MIN_SAMPLE_TARGET.max(request.limit.saturating_mul(OVERSAMPLE_FACTOR));

        // Sampling pays off when there are many more unique values than requested,
        // and the filter (if any) is broad enough for random draws to be cheap.
        let unique_values_count = facet_index.unique_values_count();
        let use_sampling = if unique_values_count > sample_target {
            match &request.filter {
                // Random sampling is too expensive with a restrictive filter,
                // check if we should abort sampling
                Some(filter) => {
                    // Expected number of draws from random id stream to reach `sample_target` values
                    let expected_draws =
                        sample_target.saturating_mul(available_points / unique_values_count.max(1));
                    !self.should_pre_filter(filter, Some(expected_draws), hw_counter)?
                }
                None => true,
            }
        } else {
            false
        };

        if use_sampling {
            self.sampled_approximate_facet(
                &facet_index,
                request,
                sample_target,
                is_stopped,
                hw_counter,
            )
        } else {
            self.full_approximate_facet(&facet_index, request, is_stopped, hw_counter)
        }
    }

    /// Aggregate counts over every distinct value in the index.
    fn full_approximate_facet(
        &self,
        facet_index: &impl FacetIndex,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let Some(filter) = &request.filter else {
            // No filter: count how many visible points each value has.
            return self.get_facet_counts(facet_index, None, hw_counter);
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
            // Every posting element will be checked, so materializing the entire filter
            // is cheaper than that many per-point evaluations.
            let bitmap = self
                .payload_index
                .iter_filtered_points(
                    filter,
                    &cardinality,
                    hw_counter,
                    is_stopped,
                    DeferredBehavior::VisibleOnly,
                )?
                .collect();
            let probe = FilterProbe::Precomputed(bitmap);

            // Check each values' points against the filter, count total per value.
            self.visit_facet_iter(facet_index, &probe, None, is_stopped, hw_counter)
        }
    }

    /// Sampling approximate-facet strategy for high-cardinality facet fields.
    ///
    /// Gets a set of candidates by random sampling, then calculates their counts.
    fn sampled_approximate_facet(
        &self,
        facet_index: &impl FacetIndex,
        request: &FacetParams,
        sample_target: usize,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let Some(filter) = &request.filter else {
            // No filter: extract each values' count from the index, but sample candidates first
            let candidates = self.collect_candidate_values(
                sample_target,
                facet_index,
                None,
                is_stopped,
                hw_counter,
            )?;

            return self.get_facet_counts(facet_index, Some(candidates), hw_counter);
        };

        let probe = FilterProbe::Lazy {
            context: self.payload_index.filter_context(filter, hw_counter)?,
        };

        // Phase 1: sample candidate values that match the filter.
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
    /// facet value(s) of each into a count map.
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
            iter.for_each(|value| {
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

        // Count a value's posting points that are visible and pass the probe.
        let count_matching = |iter: &mut dyn Iterator<Item = PointOffsetType>| -> usize {
            #[cfg(debug_assertions)]
            let iter = {
                let mut prev_id = None;
                iter.inspect(move |&id| {
                    let previous = prev_id.get_or_insert(id);
                    debug_assert!(*previous <= id, "Sorted iter assertion broken");
                    *previous = id;
                })
            };

            iter.dedup()
                .take_while(|&point_id| point_id < max_id)
                .filter(|&point_id| probe.check(point_id))
                .count()
        };

        match candidates {
            // Sampling: visit only the candidate values' postings.
            Some(candidates) => {
                facet_index.for_values_map(candidates.into_iter(), hw_counter, |value, iter| {
                    check_process_stopped(is_stopped)?;
                    let count = count_matching(iter);
                    if count > 0 {
                        hits.insert(value, count);
                    }
                    Ok(())
                })?;
            }
            // Full scan: visit every value's posting.
            None => {
                facet_index.for_each_value_map(hw_counter, |value, iter| {
                    check_process_stopped(is_stopped)?;
                    let count = count_matching(iter);
                    if count > 0 {
                        hits.insert(value.to_owned(), count);
                    }
                    Ok(())
                })?;
            }
        }

        Ok(hits)
    }

    fn get_facet_counts(
        &self,
        facet_index: &impl FacetIndex,
        candidates: Option<HashSet<FacetValue>>,
        hw_counter: &HardwareCounterCell,
    ) -> Result<HashMap<FacetValue, usize>, OperationError> {
        let mut hits = HashMap::new();
        let deferred_internal_id = self.deferred_internal_id();
        match candidates {
            // Sampling: count only the candidate values, looking each up directly.
            Some(candidates) => {
                facet_index.for_counts_per_value(
                    candidates.into_iter(),
                    deferred_internal_id,
                    hw_counter,
                    |hit| {
                        if hit.count > 0 {
                            hits.insert(hit.value, hit.count);
                        }
                        Ok(())
                    },
                )?;
            }
            // No sampling: count every value in the index.
            None => {
                facet_index.for_each_count_per_value(deferred_internal_id, |hit| {
                    if hit.count > 0 {
                        hits.insert(hit.value.to_owned(), hit.count);
                    }
                    Ok(())
                })?;
            }
        }
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
        let id_stream = self
            .id_tracker
            .point_mappings()
            .iter_random_visible()
            .map(|(_external_id, internal_id)| internal_id)
            .filter(move |&internal_id| probe.is_none_or(|probe| probe.check(internal_id)));

        let mut seen_streak: usize = 0;
        let is_finished = AtomicBool::new(false);
        facet_index.for_points_values(
            id_stream.stop_if(is_stopped).stop_if(&is_finished),
            hw_counter,
            |_point_id, vals_iter: &mut dyn Iterator<Item = FacetValueRef<'_>>| {
                for v in vals_iter {
                    if seen_streak >= MAX_NO_NEW_POINTS || seen.len() >= sample_target {
                        is_finished.store(true, Ordering::Relaxed);
                        break;
                    }
                    if seen.insert(v.to_owned()) {
                        seen_streak = 0;
                    } else {
                        seen_streak += 1;
                    }
                }
            },
        )?;

        Ok(seen)
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
        let facet_index = self.facet_index_for(key)?;
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
