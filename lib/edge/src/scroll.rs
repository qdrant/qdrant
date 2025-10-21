use std::collections::HashSet;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools as _;
use rand::distr::weighted::WeightedIndex;
use rand::rngs::StdRng;
use rand::{Rng as _, SeedableRng as _};
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::order_by::{Direction, OrderBy};
use segment::types::*;
use shard::query::scroll::{QueryScrollRequestInternal, ScrollOrder};
use shard::retrieve::record_internal::RecordInternal;
use shard::retrieve::retrieve_blocking::retrieve_blocking;

use super::Shard;

impl Shard {
    pub fn query_scroll(
        &self,
        request: &QueryScrollRequestInternal,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let QueryScrollRequestInternal {
            limit,
            with_vector,
            filter,
            scroll_order,
            with_payload,
        } = request;

        let records = match scroll_order {
            ScrollOrder::ById => self.scroll_by_id(
                None,
                *limit,
                with_payload,
                with_vector,
                filter.as_ref(),
                HwMeasurementAcc::disposable(),
            )?,
            ScrollOrder::ByField(order_by) => self.scroll_by_field(
                *limit,
                with_payload,
                with_vector,
                filter.as_ref(),
                order_by,
                HwMeasurementAcc::disposable(),
            )?,
            ScrollOrder::Random => self.scroll_randomly(
                *limit,
                with_payload,
                with_vector,
                filter.as_ref(),
                HwMeasurementAcc::disposable(),
            )?,
        };

        let point_results = records
            .into_iter()
            .map(|record| ScoredPoint {
                id: record.id,
                version: 0,
                score: 1.0,
                payload: record.payload,
                vector: record.vector,
                shard_key: record.shard_key,
                order_value: record.order_value,
            })
            .collect();

        Ok(point_results)
    }

    fn scroll_by_id(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<RecordInternal>> {
        let (non_appendable, appendable) = self.segments.read().split_segments();
        let hw_counter = hw_measurement_acc.get_counter_cell();

        let point_ids: Vec<_> = non_appendable
            .into_iter()
            .chain(appendable)
            .map(|segment| {
                segment.get().read().read_filtered(
                    offset,
                    Some(limit),
                    filter,
                    &AtomicBool::new(false),
                    &hw_counter,
                )
            })
            .collect();

        let point_ids = point_ids
            .into_iter()
            .flatten()
            .sorted()
            .dedup()
            .take(limit)
            .collect_vec();

        let mut points = retrieve_blocking(
            self.segments.clone(),
            &point_ids,
            &WithPayload::from(with_payload_interface),
            with_vector,
            &AtomicBool::new(false),
            hw_measurement_acc,
        )?;

        let ordered_points = point_ids
            .iter()
            .filter_map(|point_id| points.remove(point_id))
            .collect();

        Ok(ordered_points)
    }

    fn scroll_by_field(
        &self,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        order_by: &OrderBy,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<RecordInternal>> {
        let (non_appendable, appendable) = self.segments.read().split_segments();
        let hw_counter = hw_measurement_acc.get_counter_cell();

        let read_results: Vec<_> = non_appendable
            .into_iter()
            .chain(appendable)
            .map(|segment| {
                segment.get().read().read_ordered_filtered(
                    Some(limit),
                    filter,
                    order_by,
                    &AtomicBool::new(false),
                    &hw_counter,
                )
            })
            .collect::<Result<_, _>>()?;

        let (order_values, point_ids): (Vec<_>, Vec<_>) = read_results
            .into_iter()
            .kmerge_by(|a, b| match order_by.direction() {
                Direction::Asc => a <= b,
                Direction::Desc => a >= b,
            })
            .dedup()
            .take(limit)
            .unzip();

        let points = retrieve_blocking(
            self.segments.clone(),
            &point_ids,
            &WithPayload::from(with_payload_interface),
            with_vector,
            &AtomicBool::new(false),
            hw_measurement_acc,
        )?;

        let ordered_points = point_ids
            .iter()
            .zip(order_values)
            .filter_map(|(point_id, value)| {
                let mut record = points.get(point_id).cloned()?;
                record.order_value = Some(value);
                Some(record)
            })
            .collect();

        Ok(ordered_points)
    }

    fn scroll_randomly(
        &self,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<RecordInternal>> {
        let (non_appendable, appendable) = self.segments.read().split_segments();
        let hw_counter = hw_measurement_acc.get_counter_cell();

        let (point_count, mut point_ids): (Vec<_>, Vec<_>) = non_appendable
            .into_iter()
            .chain(appendable)
            .map(|segment| {
                let segment = segment.get();
                let segment = segment.read();

                let point_count = segment.available_point_count();
                let point_ids = segment.read_random_filtered(
                    limit,
                    filter,
                    &AtomicBool::new(false),
                    &hw_counter,
                );

                (point_count, point_ids)
            })
            .unzip();

        // Shortcut if all segments are empty
        if point_count.iter().all(|&count| count == 0) {
            return Ok(Vec::new());
        }

        // Select points in a weighted fashion from each segment, depending on how many points each segment has.
        let distribution = WeightedIndex::new(point_count).map_err(|err| {
            OperationError::service_error(format!(
                "failed to create weighted index for random scroll: {err:?}"
            ))
        })?;

        let mut rng = StdRng::from_os_rng();
        let mut random_point_ids = HashSet::with_capacity(limit);

        // Randomly sample points in two stages
        //
        // 1. This loop iterates <= LIMIT times, and either breaks early if we
        // have enough points, or if some of the segments are exhausted.
        //
        // 2. If the segments are exhausted, we will fill up the rest of the
        // points from other segments. In total, the complexity is guaranteed to
        // be O(limit).
        while random_point_ids.len() < limit {
            let segment_idx = rng.sample(&distribution);
            let segment_point_ids = &mut point_ids[segment_idx];

            if let Some(point) = segment_point_ids.pop() {
                random_point_ids.insert(point);
            } else {
                // It seems that some segments are empty early,
                // so distribution does not make sense anymore.
                // This is only possible if segments size < limit.
                break;
            }
        }

        // If we still need more points, we will get them from the rest of the segments.
        // This is a rare case, as it seems we don't have enough points in individual segments.
        // Therefore, we can ignore "proper" distribution, as it won't be accurate anyway.
        if random_point_ids.len() < limit {
            for point_id in point_ids.into_iter().flatten() {
                random_point_ids.insert(point_id);
                if random_point_ids.len() >= limit {
                    break;
                }
            }
        }

        let random_point_ids: Vec<_> = random_point_ids.into_iter().collect();

        let random_points = retrieve_blocking(
            self.segments.clone(),
            &random_point_ids,
            &WithPayload::from(with_payload_interface),
            with_vector,
            &AtomicBool::new(false),
            hw_measurement_acc,
        )?
        .into_values()
        .collect();

        Ok(random_points)
    }
}
