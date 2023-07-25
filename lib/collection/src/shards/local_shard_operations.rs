use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use segment::types::{
    Condition, Direction, ExtendedPointId, FieldCondition, Filter, OrderBy, ScoredPoint,
    WithPayload, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;
use tokio::sync::oneshot;

use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CountRequest, CountResult, PointRequest,
    Record, SearchRequestBatch, UpdateResult, UpdateStatus,
};
use crate::operations::CollectionUpdateOperations;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::update_handler::{OperationData, UpdateSignal};

#[async_trait]
impl ShardOperation for LocalShard {
    /// Imply interior mutability.
    /// Performs update operation on this collection asynchronously.
    /// Explicitly waits for result to be updated.
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let (callback_sender, callback_receiver) = if wait {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let operation_id = {
            let update_sender = self.update_sender.load();
            let channel_permit = update_sender.reserve().await?;
            let mut wal_lock = self.wal.lock();
            let operation_id = wal_lock.write(&operation)?;
            channel_permit.send(UpdateSignal::Operation(OperationData {
                op_num: operation_id,
                operation,
                sender: callback_sender,
                wait,
            }));
            operation_id
        };

        if let Some(receiver) = callback_receiver {
            let _res = receiver.await??;
            Ok(UpdateResult {
                operation_id,
                status: UpdateStatus::Completed,
            })
        } else {
            Ok(UpdateResult {
                operation_id,
                status: UpdateStatus::Acknowledged,
            })
        }
    }

    async fn scroll_by(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        order_by: Option<&OrderBy>,
    ) -> CollectionResult<Vec<Record>> {
        if let Some(OrderBy { key, direction, .. }) = order_by {
            let order = match direction.to_owned().unwrap_or_default() {
                Direction::ASC => 1,
                Direction::DESC => -1,
            };

            dbg!(key);
            let key_filter = Filter::new_must(Condition::Field(FieldCondition {
                key: key.to_owned(),
                ..Default::default()
            }));
            let combined_filter = if let Some(filter) = filter {
                filter.merge(&key_filter)
            } else {
                key_filter
            };
            let point_ids = self
                .segments()
                .read()
                .iter()
                .flat_map(|(_, segment)| {
                    segment
                        .get()
                        .read()
                        .read_filtered(offset, Some(limit), Some(&combined_filter))
                })
                .sorted()
                .dedup()
                // .take(limit)
                .collect_vec();
            dbg!(&point_ids);

            let with_payload = WithPayload::from(with_payload_interface);
            let mut points =
                SegmentsSearcher::retrieve(self.segments(), &point_ids, &with_payload, with_vector)
                    .await?;
            dbg!(&points);
            points.sort_by_key(|point| {
                point.payload.as_ref().map_or(0i64, |p| {
                    p.0.get(key)
                        .unwrap_or(&Default::default())
                        .as_i64()
                        .unwrap_or(0)
                        * order
                })
                // .map_or<>(0, |p| order * p.0.get(key).ok_or::<CollectionError>(todo!())?.as_i64().unwrap())
            });
            return Ok(points);
        }

        // ToDo: Make faster points selection with a set
        let segments = self.segments();
        let point_ids = segments
            .read()
            .iter()
            .flat_map(|(_, segment)| {
                segment
                    .get()
                    .read()
                    .read_filtered(offset, Some(limit), filter)
            })
            .sorted()
            .dedup()
            .take(limit)
            .collect_vec();

        let with_payload = WithPayload::from(with_payload_interface);
        let mut points =
            SegmentsSearcher::retrieve(segments, &point_ids, &with_payload, with_vector).await?;
        points.sort_by_key(|point| point.id);

        dbg!(&points);

        Ok(points)
    }

    /// Collect overview information about the shard
    async fn info(&self) -> CollectionResult<CollectionInfo> {
        Ok(self.local_shard_info().await)
    }

    async fn search(
        &self,
        request: Arc<SearchRequestBatch>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let collection_params = self.collection_config.read().await.params.clone();
        // check vector names existing
        for req in &request.searches {
            collection_params.get_vector_params(req.vector.get_name())?;
        }
        let res = SegmentsSearcher::search(
            self.segments(),
            request.clone(),
            search_runtime_handle,
            true,
        )
        .await?;
        let top_results = res
            .into_iter()
            .zip(request.searches.iter())
            .map(|(vector_res, req)| {
                let vector_name = req.vector.get_name();
                let distance = collection_params
                    .get_vector_params(vector_name)
                    .unwrap()
                    .distance;
                let processed_res = vector_res.into_iter().map(|mut scored_point| {
                    scored_point.score = distance.postprocess_score(scored_point.score);
                    scored_point
                });

                if let Some(threshold) = req.score_threshold {
                    processed_res
                        .take_while(|scored_point| {
                            distance.check_threshold(scored_point.score, threshold)
                        })
                        .collect()
                } else {
                    processed_res.collect()
                }
            })
            .collect();
        Ok(top_results)
    }

    async fn count(&self, request: Arc<CountRequest>) -> CollectionResult<CountResult> {
        let total_count = if request.exact {
            let all_points = self.read_filtered(request.filter.as_ref())?;
            all_points.len()
        } else {
            self.estimate_cardinality(request.filter.as_ref())?.exp
        };
        Ok(CountResult { count: total_count })
    }

    async fn retrieve(
        &self,
        request: Arc<PointRequest>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
    ) -> CollectionResult<Vec<Record>> {
        SegmentsSearcher::retrieve(self.segments(), &request.ids, with_payload, with_vector).await
    }
}
