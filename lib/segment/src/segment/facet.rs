use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};

use common::iterator_ext::IteratorExt;
use common::math::SAMPLE_SIZE_CL99_ME01;
use itertools::{Either, Itertools};

use super::Segment;
use crate::common::operation_error::OperationResult;
use crate::data_types::facets::{FacetHit, FacetParams, FacetValue};
use crate::entry::entry_point::SegmentEntry;
use crate::index::PayloadIndex;
use crate::json_path::JsonPath;
use crate::types::Filter;

impl Segment {
    pub(super) fn approximate_facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        self.exact_or_estimate_facet(request, is_stopped, SAMPLE_SIZE_CL99_ME01)
    }

    /// For testing purposes only. To be able to adjust the threshold for easier testing
    #[cfg(feature = "testing")]
    pub fn exact_or_estimate_facet_for_test(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        cardinality_threshold: usize,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        self.exact_or_estimate_facet(request, is_stopped, cardinality_threshold)
    }

    /// Decides if the counts should be accurate or approximate based on the cardinality of the filter.
    ///
    /// If the cardinality of the filter exceeds the threshold, the counts will be
    /// as approximate as a cardinality estimation for each hit.
    fn exact_or_estimate_facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        cardinality_threshold: usize,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        const STOP_CHECK_INTERVAL: usize = 100;

        let payload_index = self.payload_index.borrow();

        let facet_index = payload_index.get_facet_index(&request.key)?;

        let hits_iter = if let Some(filter) = &request.filter {
            let id_tracker = self.id_tracker.borrow();
            let filter_cardinality = payload_index.estimate_cardinality(filter);

            // If the cardinality is low enough, it is fast enough to just go over all the points,
            // and we are also interested in higher accuracy at low counts
            let do_accurate_counts = filter_cardinality.exp < cardinality_threshold;

            let iter = if do_accurate_counts {
                // go over the filtered points and aggregate the values
                // aka. read from other indexes
                let iter = payload_index
                    .iter_filtered_points(filter, &*id_tracker, &filter_cardinality)
                    .check_stop_every(STOP_CHECK_INTERVAL, || is_stopped.load(Ordering::Relaxed))
                    .filter(|point_id| !id_tracker.is_deleted_point(*point_id))
                    .fold(HashMap::new(), |mut map, point_id| {
                        facet_index.get_values(point_id).unique().for_each(|value| {
                            *map.entry(value).or_insert(0) += 1;
                        });
                        map
                    })
                    .into_iter()
                    .map(|(value, count)| FacetHit { value, count });

                Either::Left(iter)
            } else {
                // Use cardinality estimations for each value to estimate the counts
                let cardinality_ratio =
                    filter_cardinality.exp as f32 / self.available_point_count() as f32;

                // Go over each value in the map index
                let iter = facet_index
                    .iter_counts_per_value()
                    .check_stop(|| is_stopped.load(Ordering::Relaxed))
                    .map(move |hit| FacetHit {
                        value: hit.value,
                        count: (hit.count as f32 * cardinality_ratio).round() as usize,
                    })
                    .filter(|hit| hit.count > 0);

                Either::Right(iter)
            };
            Either::Left(iter)
        } else {
            // just count how many points each value has
            let iter = facet_index
                .iter_counts_per_value()
                .check_stop(|| is_stopped.load(Ordering::Relaxed))
                .filter(|hit| hit.count > 0);

            Either::Right(iter)
        };

        // We can't just select top values, because we need to aggregate across segments,
        // which we can't assume to select the same best top.
        //
        // We need all values to be able to aggregate correctly across segments
        let hits: HashMap<_, _> = hits_iter
            .map(|hit| (hit.value.to_owned(), hit.count))
            .collect();

        Ok(hits)
    }

    pub(super) fn facet_values(
        &self,
        key: &JsonPath,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
    ) -> OperationResult<BTreeSet<FacetValue>> {
        let payload_index = self.payload_index.borrow();

        let facet_index = payload_index.get_facet_index(key)?;

        let values = if let Some(filter) = filter {
            let id_tracker = self.id_tracker.borrow();
            let filter_cardinality = payload_index.estimate_cardinality(filter);

            payload_index
                .iter_filtered_points(filter, &*id_tracker, &filter_cardinality)
                .check_stop(|| is_stopped.load(Ordering::Relaxed))
                .filter(|point_id| !id_tracker.is_deleted_point(*point_id))
                .fold(BTreeSet::new(), |mut set, point_id| {
                    set.extend(facet_index.get_values(point_id));
                    set
                })
                .into_iter()
                .map(|value| value.to_owned())
                .collect()
        } else {
            facet_index
                .iter_values()
                .check_stop(|| is_stopped.load(Ordering::Relaxed))
                .map(|value_ref| value_ref.to_owned())
                .collect()
        };

        Ok(values)
    }
}
