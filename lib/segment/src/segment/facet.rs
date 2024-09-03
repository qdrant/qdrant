use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};

use common::iterator_ext::IteratorExt;
use common::math::ideal_sample_size;
use itertools::{Either, Itertools};

use super::Segment;
use crate::common::operation_error::OperationResult;
use crate::data_types::facets::{FacetHit, FacetParams, FacetValue};
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

        let facet_index = payload_index.get_facet_index(&request.key)?;

        let hits_iter = if let Some(filter) = &request.filter {
            let id_tracker = self.id_tracker.borrow();
            let filter_cardinality = payload_index.estimate_cardinality(filter);

            let ideal_sample_size = ideal_sample_size(None);

            let use_iterative_approach = filter_cardinality.exp < ideal_sample_size;

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
                // go over the filtered points, aggregate the values, but stop after ideal sample size.

                let (map, sample_size) = payload_index
                    .iter_filtered_points(filter, &*id_tracker, &filter_cardinality)
                    .check_stop_every(STOP_CHECK_INTERVAL, || is_stopped.load(Ordering::Relaxed))
                    .filter(|point_id| !id_tracker.is_deleted_point(*point_id))
                    .take(ideal_sample_size)
                    .fold((HashMap::new(), 0), |(mut map, sample_size), point_id| {
                        facet_index.get_values(point_id).unique().for_each(|value| {
                            *map.entry(value).or_insert(0) += 1;
                        });
                        (map, sample_size + 1)
                    });

                // Adjust counts based on the cardinality estimation.
                let iter = map.into_iter().map(move |(value, count)| FacetHit {
                    value,
                    count: (count as f32 * filter_cardinality.exp as f32 / sample_size as f32)
                        .ceil() as usize,
                });

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
