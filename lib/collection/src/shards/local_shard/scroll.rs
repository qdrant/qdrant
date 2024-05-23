use std::sync::Arc;

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
use crate::operations::types::{CollectionResult, Record, ScrollRequestInternal};

impl LocalShard {
    /// Basic parallel batching, it is conveniently used for the universal query API.
    pub(super) async fn query_scroll_batch(
        &self,
        batch: Arc<Vec<ScrollRequestInternal>>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let default_request = ScrollRequestInternal::default();

        let scrolls = batch
            .iter()
            .map(|request| self.query_scroll(request, &default_request, search_runtime_handle));

        try_join_all(scrolls).await
    }

    /// Scroll a single page, to be used for the universal query API only.
    async fn query_scroll(
        &self,
        request: &ScrollRequestInternal,
        default_request: &ScrollRequestInternal,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let ScrollRequestInternal {
            offset,
            limit,
            with_vector,
            filter,
            order_by,
            with_payload,
        } = request;

        let with_payload = with_payload
            .as_ref()
            .or(default_request.with_payload.as_ref())
            .unwrap();
        let order_by = order_by.clone().map(OrderBy::from);

        match order_by {
            None => self
                .scroll_by_id(
                    *offset,
                    limit.or(default_request.limit).unwrap(),
                    with_payload,
                    with_vector,
                    filter.as_ref(),
                    search_runtime_handle,
                )
                .await
                .map(|records| {
                    records
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
                        .collect()
                }),
            Some(order_by) => {
                let (records, values) = self
                    .scroll_by_field(
                        limit.or(default_request.limit).unwrap(),
                        with_payload,
                        with_vector,
                        filter.as_ref(),
                        search_runtime_handle,
                        &order_by,
                    )
                    .await?;

                let scored_points = records
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
                    .collect();

                Ok(scored_points)
            }
        }
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
        let records = SegmentsSearcher::retrieve(segments, &point_ids, &with_payload, with_vector)?;

        Ok((records, values))
    }
}
