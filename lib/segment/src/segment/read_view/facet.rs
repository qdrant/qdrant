use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use itertools::Itertools;

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::facets::{FacetParams, FacetValue};
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::FacetIndexRead;
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::Filter;

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    pub fn approximate_facet(
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

        let facet_index = self
            .payload_index
            .facet_index_for(&request.key)
            .ok_or_else(|| OperationError::MissingMapIndexForFacet {
                key: request.key.to_string(),
            })?;
        let context;

        // We can't just select top values, because we need to aggregate across segments,
        // which we can't assume to select the same best top.
        //
        // We need all values to be able to aggregate correctly across segments.
        let mut hits = HashMap::new();

        if let Some(filter) = &request.filter {
            let filter_cardinality = self
                .payload_index
                .estimate_cardinality(filter, hw_counter)?;

            let percentage_filtered = filter_cardinality.exp as f64 / available_points as f64;

            // TODO(facets): define a better estimate for this decision, the question is:
            // What is more expensive, to hash the same value excessively or to check with filter
            // too many times?
            //
            // For now this is defined from some rudimentary benchmarking two scenarios:
            // - a collection with few keys
            // - a collection with almost a unique key per point
            let use_iterative_approach = percentage_filtered < 0.3;

            if use_iterative_approach {
                // Go over the filtered points and aggregate the values (read from other indexes).
                let point_mappings = self.id_tracker.point_mappings();
                let points = self
                    .payload_index
                    .iter_filtered_points(
                        filter,
                        self.id_tracker,
                        &point_mappings,
                        &filter_cardinality,
                        hw_counter,
                        is_stopped,
                        self.deferred_internal_id(),
                    )?
                    .filter(|&point_id| !self.id_tracker.is_deleted_point(point_id));
                facet_index.for_points_values(points, hw_counter, |_point_id, iter| {
                    iter.unique().for_each(|value| {
                        *hits.entry(value.to_owned()).or_insert(0) += 1;
                    });
                })?;
            } else {
                // Go over the values and filter the points (read from facet index).
                //
                // This is more similar to a full-scan, but we won't be hashing so many times.
                context = self.payload_index.filter_context(filter, hw_counter)?;

                let max_id = self.deferred_internal_id().unwrap_or(PointOffsetType::MAX);

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
                        .filter(|&point_id| context.check(point_id))
                        .count();

                    if count > 0 {
                        hits.insert(value.to_owned(), count);
                    }
                    Ok(())
                })?;
            }
        } else {
            // Just count how many points each value has.
            facet_index.for_each_count_per_value(self.deferred_internal_id(), |hit| {
                check_process_stopped(is_stopped)?;
                if hit.count > 0 {
                    hits.insert(hit.value.to_owned(), hit.count);
                }
                Ok(())
            })?;
        }

        Ok(hits)
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
            let point_mappings = self.id_tracker.point_mappings();

            let points = self
                .payload_index
                .iter_filtered_points(
                    filter,
                    self.id_tracker,
                    &point_mappings,
                    &filter_cardinality,
                    hw_counter,
                    is_stopped,
                    self.deferred_internal_id(),
                )?
                .filter(|&point_id| !self.id_tracker.is_deleted_point(point_id));
            facet_index.for_points_values(points, hw_counter, |_point_id, iter| {
                values.extend(iter.map(|v| v.to_owned()));
            })?;
        } else {
            facet_index.for_each_value(hw_counter, self.deferred_internal_id(), |value_ref| {
                check_process_stopped(is_stopped)?;
                values.insert(value_ref.to_owned());
                Ok(())
            })?;
        };

        Ok(values)
    }
}
