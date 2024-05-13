use futures::future::try_join_all;
use itertools::Itertools as _;
use segment::data_types::order_by::{Direction, OrderBy};
use segment::types::{ExtendedPointId, Filter, WithPayload, WithPayloadInterface, WithVector};
use tokio::runtime::Handle;

use super::LocalShard;
use crate::collection_manager::holders::segment_holder::LockedSegment;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::{CollectionResult, Record};

impl LocalShard {
    pub async fn scroll_by_id(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Record>> {
        let segments = self.segments();

        let (non_appendable, appendable) = segments.read().split_segments();

        let read_filtered = |segment: LockedSegment| {
            let filter = filter.cloned();

            search_runtime_handle.spawn_blocking(move || {
                segment
                    .get()
                    .read()
                    .read_filtered(offset, Some(limit), filter.as_ref())
            })
        };

        let non_appendable = try_join_all(non_appendable.into_iter().map(read_filtered)).await?;
        let appendable = try_join_all(appendable.into_iter().map(read_filtered)).await?;

        let point_ids = non_appendable
            .into_iter()
            .chain(appendable)
            .flatten()
            .sorted()
            .dedup()
            .take(limit)
            .collect_vec();

        let with_payload = WithPayload::from(with_payload_interface);
        let mut points =
            SegmentsSearcher::retrieve(segments, &point_ids, &with_payload, with_vector)?;

        points.sort_by_key(|point| point.id);

        Ok(points)
    }

    pub async fn scroll_by_field(
        &self,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
        order_by: &OrderBy,
    ) -> CollectionResult<Vec<Record>> {
        let segments = self.segments();

        let (non_appendable, appendable) = segments.read().split_segments();

        let read_ordered_filtered = |segment: LockedSegment| {
            let filter = filter.cloned();
            let order_by = order_by.clone();

            search_runtime_handle.spawn_blocking(move || {
                segment
                    .get()
                    .read()
                    .read_ordered_filtered(Some(limit), filter.as_ref(), &order_by)
            })
        };

        let non_appendable =
            try_join_all(non_appendable.into_iter().map(read_ordered_filtered)).await?;
        let appendable = try_join_all(appendable.into_iter().map(read_ordered_filtered)).await?;

        let all_reads = non_appendable
            .into_iter()
            .chain(appendable)
            .collect::<Result<Vec<_>, _>>()?;

        let top_records = all_reads
            .into_iter()
            .kmerge_by(|a, b| match order_by.direction() {
                Direction::Asc => a <= b,
                Direction::Desc => a >= b,
            })
            .dedup()
            .take(limit)
            .collect_vec();

        let with_payload = WithPayload::from(with_payload_interface);

        let point_ids = top_records.iter().map(|(_, id)| *id).collect_vec();

        // Fetch with the requested vector and payload
        let mut records =
            SegmentsSearcher::retrieve(segments, &point_ids, &with_payload, with_vector)?;

        // Add order_by value to the payload. It will be removed in the next step, after crossing the shard boundary.
        records
            .iter_mut()
            .zip(top_records)
            .for_each(|(record, (value, _))| {
                let new_payload =
                    OrderBy::insert_order_value_in_payload(record.payload.take(), value);

                record.payload = Some(new_payload);
            });

        Ok(records)
    }
}
