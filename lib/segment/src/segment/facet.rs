use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};

use common::iterator_ext::IteratorExt;
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
        const STOP_CHECK_INTERVAL: usize = 100;

        let payload_index = self.payload_index.borrow();

        // Shortcut if this segment has no points, prevent division by zero later
        let available_points = self.available_point_count();
        if available_points == 0 {
            return Ok(HashMap::new());
        }

        let facet_index = payload_index.get_facet_index(&request.key)?;
        let context;

        let hits_iter = if let Some(filter) = &request.filter {
            let id_tracker = self.id_tracker.borrow();
            let filter_cardinality = payload_index.estimate_cardinality(filter);

            let percentage_filtered = filter_cardinality.exp as f64 / available_points as f64;

            // TODO(facets): define a better estimate for this decision, the question is:
            // What is more expensive, to hash the same value excessively or to check with filter too many times?
            //
            // For now this is defined from some rudimentary benchmarking two scenarios:
            // - a collection with few keys
            // - a collection with almost a unique key per point
            let use_iterative_approach = percentage_filtered < 0.3;

            let iter = if use_iterative_approach {
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
                // go over the values and filter the points
                // aka. read from facet index
                //
                // This is more similar to a full-scan, but we won't be hashing so many times.
                context = payload_index.struct_filtered_context(filter);

                let iter = facet_index
                    .iter_filtered_counts_per_value(&context)
                    .check_stop(|| is_stopped.load(Ordering::Relaxed))
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
