use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use futures::future;
use futures::future::try_join_all;
use itertools::{Itertools, process_results};
use segment::data_types::facets::{FacetParams, FacetValue, FacetValueHit};
use segment::types::{Condition, FieldCondition, Filter, Match};
use shard::common::stopping_guard::StoppingGuard;
use tokio::runtime::Handle;
use tokio::time::error::Elapsed;

use super::LocalShard;
use crate::collection_manager::holders::segment_holder::LockedSegment;
use crate::operations::types::{CollectionError, CollectionResult};

impl LocalShard {
    /// Returns values with approximate counts for the given facet request.
    pub async fn approx_facet(
        &self,
        request: Arc<FacetParams>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<FacetValueHit>> {
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);

        let stopping_guard = StoppingGuard::new();

        let spawn_read = |segment: LockedSegment, hw_counter: &HardwareCounterCell| {
            let request = Arc::clone(&request);
            let is_stopped = stopping_guard.get_is_stopped();

            let hw_counter = hw_counter.fork();
            search_runtime_handle.spawn_blocking(move || {
                let get_segment = segment.get();
                let read_segment = get_segment.read();

                read_segment.facet(&request, &is_stopped, &hw_counter)
            })
        };

        let all_reads = {
            let segments_lock = self.segments().read();

            let hw_counter = hw_measurement_acc.get_counter_cell();

            tokio::time::timeout(
                timeout,
                try_join_all(
                    segments_lock
                        .non_appendable_then_appendable_segments()
                        .map(|segment| spawn_read(segment, &hw_counter)),
                ),
            )
        }
        .await
        .map_err(|_: Elapsed| CollectionError::timeout(timeout.as_secs() as usize, "facet"))??;

        let merged_hits = process_results(all_reads, |reads| {
            reads.reduce(|mut acc, map| {
                map.into_iter()
                    .for_each(|(value, count)| *acc.entry(value).or_insert(0) += count);
                acc
            })
        })?;

        // We can't just select top values, because we need to aggregate across segments,
        // which we can't assume to select the same best top.
        //
        // We need all values to be able to aggregate correctly across segments
        let top_hits = merged_hits
            .map(|map| {
                map.iter()
                    .map(|(value, count)| FacetValueHit {
                        value: value.to_owned(),
                        count: *count,
                    })
                    .collect_vec()
            })
            .unwrap_or_default();

        Ok(top_hits)
    }

    /// Returns values with exact counts for a given facet request.
    pub async fn exact_facet(
        &self,
        request: Arc<FacetParams>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<FacetValueHit>> {
        // To return exact counts we need to consider that the same point can be in different segments if it has different versions.
        // So, we need to consider all point ids for a given filter in all segments to do an accurate count.
        //
        // To do this we will perform exact counts for each of the values in the field.

        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);

        let instant = std::time::Instant::now();

        // Get unique values for the field
        let unique_values = self
            .unique_values(
                Arc::clone(&request),
                search_runtime_handle,
                timeout,
                hw_measurement_acc.clone(),
            )
            .await?;

        // Make an exact count for each value
        let hits_futures = unique_values.into_iter().map(|value| {
            let match_value = Filter::new_must(Condition::Field(FieldCondition::new_match(
                request.key.clone(),
                Match::new_value(From::from(value.clone())),
            )));

            let filter = Filter::merge_opts(request.filter.clone(), Some(match_value));

            let hw_acc = hw_measurement_acc.clone();
            async move {
                let count = self
                    .read_filtered(filter.as_ref(), search_runtime_handle, hw_acc)
                    .await?
                    .len();
                CollectionResult::Ok(FacetValueHit { value, count })
            }
        });

        let hits = tokio::time::timeout(
            timeout.saturating_sub(instant.elapsed()),
            future::try_join_all(hits_futures),
        )
        .await
        .map_err(|_: Elapsed| CollectionError::timeout(timeout.as_secs() as usize, "facet"))??;

        Ok(hits)
    }

    async fn unique_values(
        &self,
        request: Arc<FacetParams>,
        handle: &Handle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<BTreeSet<FacetValue>> {
        let stopping_guard = StoppingGuard::new();

        let spawn_read = |segment: LockedSegment, hw_counter: &HardwareCounterCell| {
            let request = Arc::clone(&request);

            let is_stopped = stopping_guard.get_is_stopped();

            let hw_counter = hw_counter.fork();
            handle.spawn_blocking(move || {
                let get_segment = segment.get();
                let read_segment = get_segment.read();

                read_segment.unique_values(
                    &request.key,
                    request.filter.as_ref(),
                    &is_stopped,
                    &hw_counter,
                )
            })
        };

        let hw_counter = hw_measurement_acc.get_counter_cell();

        let all_reads = {
            let segments_lock = self.segments().read();

            tokio::time::timeout(
                timeout,
                try_join_all(
                    segments_lock
                        .non_appendable_then_appendable_segments()
                        .map(|segment| spawn_read(segment, &hw_counter)),
                ),
            )
        }
        .await
        .map_err(|_: Elapsed| CollectionError::timeout(timeout.as_secs() as usize, "facet"))??;

        let all_values =
            process_results(all_reads, |reads| reads.flatten().collect::<BTreeSet<_>>())?;

        Ok(all_values)
    }
}
