use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::PointOperations;
use crate::operations::FieldIndexOperations;
use crate::shard::conversions::{
    internal_clear_payload, internal_clear_payload_by_filter, internal_create_index,
    internal_delete_index, internal_delete_payload, internal_delete_points,
    internal_delete_points_by_filter, internal_set_payload, internal_upsert_points,
};
use crate::shard::{PeerId, ShardId, ShardOperation};
use crate::{
    CollectionError, CollectionId, CollectionInfo, CollectionResult, CollectionSearcher,
    CollectionUpdateOperations, PointRequest, Record, SearchRequest, UpdateResult,
};
use api::grpc::qdrant::{
    collections_internal_client::CollectionsInternalClient,
    points_internal_client::PointsInternalClient, GetCollectionInfoRequest,
    GetCollectionInfoRequestInternal, GetPoints, GetPointsInternal, ScrollPoints,
    ScrollPointsInternal, SearchPoints, SearchPointsInternal,
};
use api::grpc::transport_channel_pool::TransportChannelPool;
use async_trait::async_trait;
use segment::types::{ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;
use tonic::transport::Channel;
use tonic::transport::Uri;
use tonic::Status;

/// RemoteShard
///
/// Remote Shard is a representation of a shard that is located on a remote peer.
/// Currently a placeholder implementation for later work.
#[allow(dead_code)]
pub struct RemoteShard {
    pub(crate) id: ShardId,
    pub(crate) collection_id: CollectionId,
    pub peer_id: PeerId,
    ip_to_address: Arc<std::sync::RwLock<HashMap<u64, Uri>>>,
    channel_pool: Arc<TransportChannelPool>,
}

impl RemoteShard {
    pub fn build(
        id: ShardId,
        collection_id: CollectionId,
        peer_id: PeerId,
        ip_to_address: Arc<std::sync::RwLock<HashMap<u64, Uri>>>,
        channel_pool: Arc<TransportChannelPool>,
    ) -> Self {
        Self {
            id,
            collection_id,
            peer_id,
            ip_to_address,
            channel_pool,
        }
    }

    fn current_address(&self) -> CollectionResult<Uri> {
        let guard_peer_address = self.ip_to_address.read()?;
        let peer_address = guard_peer_address.get(&self.peer_id).cloned();
        match peer_address {
            None => Err(CollectionError::service_error(format!(
                "no address found for peer {}",
                self.peer_id
            ))),
            Some(peer_address) => Ok(peer_address),
        }
    }

    async fn points_client(&self) -> CollectionResult<PointsInternalClient<Channel>> {
        let current_address = self.current_address()?;
        let pooled_channel = self
            .channel_pool
            .get_or_create_pooled_channel(&current_address)
            .await?;
        Ok(PointsInternalClient::new(pooled_channel))
    }

    async fn collections_client(&self) -> CollectionResult<CollectionsInternalClient<Channel>> {
        let current_address = self.current_address()?;
        let pooled_channel = self
            .channel_pool
            .get_or_create_pooled_channel(&current_address)
            .await?;
        Ok(CollectionsInternalClient::new(pooled_channel))
    }
}

#[async_trait]
#[allow(unused_variables)]
impl ShardOperation for &RemoteShard {
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let mut client = self.points_client().await?;

        let response = match operation {
            CollectionUpdateOperations::PointOperation(point_ops) => match point_ops {
                PointOperations::UpsertPoints(point_insert_operations) => {
                    let request =
                        tonic::Request::new(internal_upsert_points(point_insert_operations, self)?);
                    client.upsert(request).await?
                }
                PointOperations::DeletePoints { ids } => {
                    let request = tonic::Request::new(internal_delete_points(ids, self));
                    client.delete(request).await?
                }
                PointOperations::DeletePointsByFilter(filter) => {
                    let request =
                        tonic::Request::new(internal_delete_points_by_filter(filter, self));
                    client.delete(request).await?
                }
            },
            CollectionUpdateOperations::PayloadOperation(payload_ops) => match payload_ops {
                PayloadOps::SetPayload(set_payload) => {
                    let request = tonic::Request::new(internal_set_payload(set_payload, self));
                    client.set_payload(request).await?
                }
                PayloadOps::DeletePayload(delete_payload) => {
                    let request =
                        tonic::Request::new(internal_delete_payload(delete_payload, self));
                    client.delete_payload(request).await?
                }
                PayloadOps::ClearPayload { points } => {
                    let request = tonic::Request::new(internal_clear_payload(points, self));
                    client.clear_payload(request).await?
                }
                PayloadOps::ClearPayloadByFilter(filter) => {
                    let request =
                        tonic::Request::new(internal_clear_payload_by_filter(filter, self));
                    client.clear_payload(request).await?
                }
            },
            CollectionUpdateOperations::FieldIndexOperation(field_index_op) => match field_index_op
            {
                FieldIndexOperations::CreateIndex(create_index) => {
                    let request = tonic::Request::new(internal_create_index(create_index, self));
                    client.create_field_index(request).await?
                }
                FieldIndexOperations::DeleteIndex(delete_index) => {
                    let request = tonic::Request::new(internal_delete_index(delete_index, self));
                    client.delete_field_index(request).await?
                }
            },
        };

        let point_operation_response = response.into_inner();
        match point_operation_response.result {
            None => Err(CollectionError::service_error(
                "Malformed UpdateResult type".to_string(),
            )),
            Some(update_result) => update_result.try_into().map_err(|e: Status| e.into()),
        }
    }

    async fn scroll_by(
        &self,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: bool,
        filter: Option<&Filter>,
    ) -> CollectionResult<Vec<Record>> {
        let mut client = self.points_client().await?;

        let scroll_points = ScrollPoints {
            collection_name: self.collection_id.clone(),
            filter: filter.map(|f| f.clone().into()),
            offset: offset.map(|o| o.into()),
            limit: Some(limit as u32),
            with_vector: Some(with_vector),
            with_payload: Some(with_payload_interface.clone().into()),
        };
        let request = tonic::Request::new(ScrollPointsInternal {
            scroll_points: Some(scroll_points),
            shard_id: self.id,
        });
        let response = client.scroll(request).await?;
        let scroll_response = response.into_inner();
        let result: Result<Vec<Record>, Status> = scroll_response
            .result
            .into_iter()
            .map(|retrieved| retrieved.try_into())
            .collect();
        result.map_err(|e| e.into())
    }

    async fn info(&self) -> CollectionResult<CollectionInfo> {
        let mut client = self.collections_client().await?;

        let get_collection_info_request = GetCollectionInfoRequest {
            collection_name: self.collection_id.clone(),
        };
        let request = tonic::Request::new(GetCollectionInfoRequestInternal {
            get_collection_info_request: Some(get_collection_info_request),
            shard_id: self.id,
        });
        let response = client.get(request).await?;
        let get_collection_response = response.into_inner();
        let result: Result<CollectionInfo, Status> = get_collection_response.try_into();
        result.map_err(|e| e.into())
    }

    async fn search(
        &self,
        request: Arc<SearchRequest>,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let mut client = self.points_client().await?;

        let search_points = SearchPoints {
            collection_name: self.collection_id.clone(),
            vector: request.vector.clone(),
            filter: request.filter.clone().map(|f| f.into()),
            top: request.top as u64,
            with_vector: Some(request.with_vector),
            with_payload: request.with_payload.clone().map(|wp| wp.into()),
            params: request.params.map(|sp| sp.into()),
            score_threshold: request.score_threshold,
        };
        let request = tonic::Request::new(SearchPointsInternal {
            search_points: Some(search_points),
            shard_id: self.id,
        });
        let response = client.search(request).await?;
        let search_response = response.into_inner();
        let result: Result<Vec<ScoredPoint>, Status> = search_response
            .result
            .into_iter()
            .map(|scored| scored.try_into())
            .collect();
        result.map_err(|e| e.into())
    }

    async fn retrieve(
        &self,
        request: Arc<PointRequest>,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        with_payload: &WithPayload,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>> {
        let mut client = self.points_client().await?;

        let get_points = GetPoints {
            collection_name: self.collection_id.clone(),
            ids: request.ids.iter().copied().map(|v| v.into()).collect(),
            with_vector: Some(request.with_vector),
            with_payload: request.with_payload.clone().map(|wp| wp.into()),
        };
        let request = tonic::Request::new(GetPointsInternal {
            get_points: Some(get_points),
            shard_id: self.id,
        });
        let response = client.get(request).await?;
        let get_response = response.into_inner();
        let result: Result<Vec<Record>, Status> = get_response
            .result
            .into_iter()
            .map(|scored| scored.try_into())
            .collect();
        result.map_err(|e| e.into())
    }
}
