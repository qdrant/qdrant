use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools as _;
use segment::data_types::order_by::{Direction, OrderBy, OrderValue};
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;

use super::LocalShard;
use crate::collection_manager::holders::segment_holder::LockedSegment;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::{
    CollectionError, CollectionResult, QueryScrollRequestInternal, Record,
};

impl LocalShard {
    /// Basic parallel batching, it is conveniently used for the universal query API.
    pub(super) async fn query_scroll_batch(
        &self,
        batch: Arc<Vec<QueryScrollRequestInternal>>,
        search_runtime_handle: &Handle,
        timeout: Duration,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let scrolls = batch
            .iter()
            .map(|request| self.query_scroll(request, search_runtime_handle));

        // execute all the scrolls concurrently
        let all_scroll_results = try_join_all(scrolls);
        tokio::time::timeout(timeout, all_scroll_results)
            .await
            .map_err(|_| {
                log::debug!(
                    "Query scroll timeout reached: {} seconds",
                    timeout.as_secs()
                );
                CollectionError::timeout(timeout.as_secs() as usize, "Query scroll")
            })?
    }

    /// Scroll a single page, to be used for the universal query API only.
    async fn query_scroll(
        &self,
        request: &QueryScrollRequestInternal,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let QueryScrollRequestInternal {
            limit,
            with_vector,
            filter,
            order_by,
            with_payload,
        } = request;

        let limit = *limit;

        let offset_id = None;

        let order_by = order_by.clone().map(OrderBy::from);

        let point_results = match order_by {
            None => self
                .scroll_by_id(
                    offset_id,
                    limit,
                    with_payload,
                    with_vector,
                    filter.as_ref(),
                    search_runtime_handle,
                )
                .await?
                .into_iter()
                .map(|record| ScoredPoint {
                    id: record.id,
                    version: 0,
                    score: 0.0,
                    payload: record.payload,
                    vector: record.vector,
                    shard_key: record.shard_key,
                    order_value: None,
                })
                .collect(),
            Some(order_by) => {
                let (records, values) = self
                    .scroll_by_field(
                        limit,
                        with_payload,
                        with_vector,
                        filter.as_ref(),
                        search_runtime_handle,
                        &order_by,
                    )
                    .await?;

                records
                    .into_iter()
                    .zip(values)
                    .map(|(record, value)| ScoredPoint {
                        id: record.id,
                        version: 0,
                        score: 0.0,
                        payload: record.payload,
                        vector: record.vector,
                        shard_key: record.shard_key,
                        order_value: Some(value),
                    })
                    .collect()
            }
        };

        Ok(point_results)
    }

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
        let records_map =
            SegmentsSearcher::retrieve(segments, &point_ids, &with_payload, with_vector)?;

        let ordered_records = point_ids
            .iter()
            .filter_map(|point| records_map.get(point).cloned())
            .collect();

        Ok(ordered_records)
    }

    pub async fn scroll_by_field(
        &self,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
        order_by: &OrderBy,
    ) -> CollectionResult<(Vec<Record>, Vec<OrderValue>)> {
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

        let (values, point_ids): (Vec<_>, Vec<_>) = all_reads
            .into_iter()
            .kmerge_by(|a, b| match order_by.direction() {
                Direction::Asc => a <= b,
                Direction::Desc => a >= b,
            })
            .dedup()
            .take(limit)
            .unzip();

        let with_payload = WithPayload::from(with_payload_interface);

        // Fetch with the requested vector and payload
        let records_map =
            SegmentsSearcher::retrieve(segments, &point_ids, &with_payload, with_vector)?;

        let ordered_records = point_ids
            .iter()
            .filter_map(|point| records_map.get(point).cloned())
            .collect();

        Ok((ordered_records, values))
    }
}
