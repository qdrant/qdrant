use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use api::grpc::qdrant::collections_internal_client::CollectionsInternalClient;
use api::grpc::qdrant::points_internal_client::PointsInternalClient;
use api::grpc::qdrant::{
    CollectionOperationResponse, CountPoints, CountPointsInternal, GetCollectionInfoRequest,
    GetCollectionInfoRequestInternal, GetPoints, GetPointsInternal, InitiateShardTransferRequest,
    ScrollPoints, ScrollPointsInternal, SearchBatchPointsInternal,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use segment::telemetry::{TelemetryOperationAggregator, TelemetryOperationTimer};
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;
use tonic::transport::{Channel, Uri};
use tonic::Status;

use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::PointOperations;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CountRequest, CountResult, PointRequest,
    Record, SearchRequest, SearchRequestBatch, UpdateResult,
};
use crate::operations::{CollectionUpdateOperations, FieldIndexOperations};
use crate::shard::conversions::{
    internal_clear_payload, internal_clear_payload_by_filter, internal_create_index,
    internal_delete_index, internal_delete_payload, internal_delete_points,
    internal_delete_points_by_filter, internal_set_payload, internal_sync_points,
    internal_upsert_points,
};
use crate::shard::shard_config::ShardConfig;
use crate::shard::{ChannelService, CollectionId, PeerId, ShardId, ShardOperation};
use crate::telemetry::{RemoteShardTelemetry, ShardTelemetry};

/// RemoteShard
///
/// Remote Shard is a representation of a shard that is located on a remote peer.
pub struct RemoteShard {
    pub(crate) id: ShardId,
    pub(crate) collection_id: CollectionId,
    pub peer_id: PeerId,
    channel_service: ChannelService,
    searches_telemetry: Arc<Mutex<TelemetryOperationAggregator>>,
    updates_telemetry: Arc<Mutex<TelemetryOperationAggregator>>,
}

impl RemoteShard {
    /// Instantiate a new remote shard in memory
    pub fn new(
        id: ShardId,
        collection_id: CollectionId,
        peer_id: PeerId,
        channel_service: ChannelService,
    ) -> Self {
        Self {
            id,
            collection_id,
            peer_id,
            channel_service,
            searches_telemetry: TelemetryOperationAggregator::new(),
            updates_telemetry: TelemetryOperationAggregator::new(),
        }
    }

    /// Initialize remote shard by persisting its info on the file system.
    pub fn init(
        id: ShardId,
        collection_id: CollectionId,
        peer_id: PeerId,
        shard_path: PathBuf,
        channel_service: ChannelService,
    ) -> CollectionResult<Self> {
        // initialize remote shard config file
        let shard_config = ShardConfig::new_remote(peer_id);
        shard_config.save(&shard_path)?;
        Ok(RemoteShard::new(
            id,
            collection_id,
            peer_id,
            channel_service,
        ))
    }

    pub fn restore_snapshot(_snapshot_path: &Path) {
        // NO extra actions needed for remote shards
    }

    pub async fn create_snapshot(&self, target_path: &Path) -> CollectionResult<()> {
        let shard_config = ShardConfig::new_remote(self.peer_id);
        shard_config.save(target_path)?;
        Ok(())
    }

    fn current_address(&self) -> CollectionResult<Uri> {
        let guard_peer_address = self.channel_service.id_to_address.read();
        let peer_address = guard_peer_address.get(&self.peer_id).cloned();
        match peer_address {
            None => Err(CollectionError::service_error(format!(
                "no address found for peer {}",
                self.peer_id
            ))),
            Some(peer_address) => Ok(peer_address),
        }
    }

    async fn with_points_client<T, O: Future<Output = Result<T, Status>>>(
        &self,
        f: impl Fn(PointsInternalClient<Channel>) -> O,
    ) -> CollectionResult<T> {
        let current_address = self.current_address()?;
        self.channel_service
            .channel_pool
            .with_channel(&current_address, |channel| {
                f(PointsInternalClient::new(channel))
            })
            .await
            .map_err(|err| err.into())
    }

    async fn with_collections_client<T, O: Future<Output = Result<T, Status>>>(
        &self,
        f: impl Fn(CollectionsInternalClient<Channel>) -> O,
    ) -> CollectionResult<T> {
        let current_address = self.current_address()?;
        self.channel_service
            .channel_pool
            .with_channel(&current_address, |channel| {
                f(CollectionsInternalClient::new(channel))
            })
            .await
            .map_err(|err| err.into())
    }

    pub fn get_telemetry_data(&self) -> ShardTelemetry {
        ShardTelemetry::Remote(RemoteShardTelemetry {
            shard_id: self.id,
            searches: self.searches_telemetry.lock().get_statistics(),
            updates: self.updates_telemetry.lock().get_statistics(),
        })
    }

    pub async fn initiate_transfer(&self) -> CollectionResult<CollectionOperationResponse> {
        let res = self
            .with_collections_client(|mut client| async move {
                client
                    .initiate(InitiateShardTransferRequest {
                        collection_name: self.collection_id.clone(),
                        shard_id: self.id,
                    })
                    .await
            })
            .await?
            .into_inner();
        Ok(res)
    }
}

// New-type to own the type in the crate for conversions via From
pub struct CollectionSearchRequest<'a>(pub(crate) (CollectionId, &'a SearchRequest));

#[async_trait]
#[allow(unused_variables)]
impl ShardOperation for RemoteShard {
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let mut timer = TelemetryOperationTimer::new(&self.updates_telemetry);
        timer.set_success(false);

        let point_operation_response = match operation {
            CollectionUpdateOperations::PointOperation(point_ops) => match point_ops {
                PointOperations::UpsertPoints(point_insert_operations) => {
                    let request = &internal_upsert_points(point_insert_operations, self, wait)?;
                    self.with_points_client(|mut client| async move {
                        client.upsert(tonic::Request::new(request.clone())).await
                    })
                    .await?
                    .into_inner()
                }
                PointOperations::DeletePoints { ids } => {
                    let request = &internal_delete_points(ids, self, wait);
                    self.with_points_client(|mut client| async move {
                        client.delete(tonic::Request::new(request.clone())).await
                    })
                    .await?
                    .into_inner()
                }
                PointOperations::DeletePointsByFilter(filter) => {
                    let request = &internal_delete_points_by_filter(filter, self, wait);
                    self.with_points_client(|mut client| async move {
                        client.delete(tonic::Request::new(request.clone())).await
                    })
                    .await?
                    .into_inner()
                }
                PointOperations::SyncPoints(operation) => {
                    let request = &internal_sync_points(operation, self, wait)?;
                    self.with_points_client(|mut client| async move {
                        client.sync(tonic::Request::new(request.clone())).await
                    })
                    .await?
                    .into_inner()
                }
            },
            CollectionUpdateOperations::PayloadOperation(payload_ops) => match payload_ops {
                PayloadOps::SetPayload(set_payload) => {
                    let request = &internal_set_payload(set_payload, self, wait);
                    self.with_points_client(|mut client| async move {
                        client
                            .set_payload(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
                PayloadOps::DeletePayload(delete_payload) => {
                    let request = &internal_delete_payload(delete_payload, self, wait);
                    self.with_points_client(|mut client| async move {
                        client
                            .delete_payload(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
                PayloadOps::ClearPayload { points } => {
                    let request = &internal_clear_payload(points, self, wait);
                    self.with_points_client(|mut client| async move {
                        client
                            .clear_payload(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
                PayloadOps::ClearPayloadByFilter(filter) => {
                    let request = &internal_clear_payload_by_filter(filter, self, wait);
                    self.with_points_client(|mut client| async move {
                        client
                            .clear_payload(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
            },
            CollectionUpdateOperations::FieldIndexOperation(field_index_op) => match field_index_op
            {
                FieldIndexOperations::CreateIndex(create_index) => {
                    let request = &internal_create_index(create_index, self, wait);
                    self.with_points_client(|mut client| async move {
                        client
                            .create_field_index(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
                FieldIndexOperations::DeleteIndex(delete_index) => {
                    let request = &internal_delete_index(delete_index, self, wait);
                    self.with_points_client(|mut client| async move {
                        client
                            .delete_field_index(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
            },
        };

        match point_operation_response.result {
            None => Err(CollectionError::service_error(
                "Malformed UpdateResult type".to_string(),
            )),
            Some(update_result) => update_result.try_into().map_err(|e: Status| e.into()),
        }
    }

    async fn scroll_by(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
    ) -> CollectionResult<Vec<Record>> {
        let scroll_points = ScrollPoints {
            collection_name: self.collection_id.clone(),
            filter: filter.map(|f| f.clone().into()),
            offset: offset.map(|o| o.into()),
            limit: Some(limit as u32),
            with_vector: None,
            with_payload: Some(with_payload_interface.clone().into()),
            with_vectors: Some(with_vector.clone().into()),
        };
        let request = &ScrollPointsInternal {
            scroll_points: Some(scroll_points),
            shard_id: self.id,
        };

        let scroll_response = self
            .with_points_client(|mut client| async move {
                client.scroll(tonic::Request::new(request.clone())).await
            })
            .await?
            .into_inner();

        let result: Result<Vec<Record>, Status> = scroll_response
            .result
            .into_iter()
            .map(|retrieved| retrieved.try_into())
            .collect();
        result.map_err(|e| e.into())
    }

    async fn info(&self) -> CollectionResult<CollectionInfo> {
        let get_collection_info_request = GetCollectionInfoRequest {
            collection_name: self.collection_id.clone(),
        };
        let request = &GetCollectionInfoRequestInternal {
            get_collection_info_request: Some(get_collection_info_request),
            shard_id: self.id,
        };
        let get_collection_response = self
            .with_collections_client(|mut client| async move {
                client.get(tonic::Request::new(request.clone())).await
            })
            .await?
            .into_inner();

        let result: Result<CollectionInfo, Status> = get_collection_response.try_into();
        result.map_err(|e| e.into())
    }

    async fn search(
        &self,
        request: Arc<SearchRequestBatch>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let mut timer = TelemetryOperationTimer::new(&self.searches_telemetry);
        timer.set_success(false);

        let search_points = request
            .searches
            .iter()
            .map(|s| CollectionSearchRequest((self.collection_id.clone(), s)).into())
            .collect();

        let request = &SearchBatchPointsInternal {
            collection_name: self.collection_id.clone(),
            search_points,
            shard_id: self.id,
        };
        let search_batch_response = self
            .with_points_client(|mut client| async move {
                client
                    .search_batch(tonic::Request::new(request.clone()))
                    .await
            })
            .await?
            .into_inner();

        let result: Result<Vec<Vec<ScoredPoint>>, Status> = search_batch_response
            .result
            .into_iter()
            .map(|scored| scored.result.into_iter().map(|s| s.try_into()).collect())
            .collect();
        let result = result.map_err(|e| e.into());
        if result.is_ok() {
            timer.set_success(true);
        }
        result
    }

    async fn count(&self, request: Arc<CountRequest>) -> CollectionResult<CountResult> {
        let count_points = CountPoints {
            collection_name: self.collection_id.clone(),
            filter: request.filter.clone().map(|f| f.into()),
            exact: Some(request.exact),
        };

        let request = &CountPointsInternal {
            count_points: Some(count_points),
            shard_id: self.id,
        };
        let count_response = self
            .with_points_client(|mut client| async move {
                client.count(tonic::Request::new(request.clone())).await
            })
            .await?
            .into_inner();
        count_response.result.map_or_else(
            || {
                Err(CollectionError::service_error(
                    "Unexpected empty CountResult".to_string(),
                ))
            },
            |count_result| Ok(count_result.into()),
        )
    }

    async fn retrieve(
        &self,
        request: Arc<PointRequest>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
    ) -> CollectionResult<Vec<Record>> {
        let get_points = GetPoints {
            collection_name: self.collection_id.clone(),
            ids: request.ids.iter().copied().map(|v| v.into()).collect(),
            with_vector: None,
            with_payload: request.with_payload.clone().map(|wp| wp.into()),
            with_vectors: Some(with_vector.clone().into()),
        };
        let request = &GetPointsInternal {
            get_points: Some(get_points),
            shard_id: self.id,
        };

        let get_response = self
            .with_points_client(|mut client| async move {
                client.get(tonic::Request::new(request.clone())).await
            })
            .await?
            .into_inner();

        let result: Result<Vec<Record>, Status> = get_response
            .result
            .into_iter()
            .map(|scored| scored.try_into())
            .collect();
        result.map_err(|e| e.into())
    }
}
