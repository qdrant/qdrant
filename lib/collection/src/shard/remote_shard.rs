use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use api::grpc::qdrant::collections_internal_client::CollectionsInternalClient;
use api::grpc::qdrant::points_internal_client::PointsInternalClient;
use api::grpc::qdrant::{
    CountPoints, CountPointsInternal, GetCollectionInfoRequest, GetCollectionInfoRequestInternal,
    GetPoints, GetPointsInternal, ScrollPoints, ScrollPointsInternal, SearchPoints,
    SearchPointsInternal,
};
use async_trait::async_trait;
use segment::types::{ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface};
use tokio::runtime::Handle;
use tonic::transport::{Channel, Uri};
use tonic::Status;
use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::PointOperations;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CountRequest, CountResult, PointRequest,
    Record, SearchRequest, UpdateResult,
};
use crate::operations::{CollectionUpdateOperations, FieldIndexOperations};
use crate::shard::conversions::{
    internal_clear_payload, internal_clear_payload_by_filter, internal_create_index,
    internal_delete_index, internal_delete_payload, internal_delete_points,
    internal_delete_points_by_filter, internal_set_payload, internal_upsert_points,
};
use crate::shard::shard_config::ShardConfig;
use crate::shard::{ChannelService, CollectionId, PeerId, ShardId, ShardOperation};

/// RemoteShard
///
/// Remote Shard is a representation of a shard that is located on a remote peer.
pub struct RemoteShard {
    pub(crate) id: ShardId,
    pub(crate) collection_id: CollectionId,
    pub peer_id: PeerId,
    channel_service: ChannelService,
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
        f: impl FnOnce(PointsInternalClient<Channel>) -> O,
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
        f: impl FnOnce(CollectionsInternalClient<Channel>) -> O,
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
}

#[async_trait]
#[allow(unused_variables)]
impl ShardOperation for RemoteShard {
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {

        let response = match operation {
            CollectionUpdateOperations::PointOperation(point_ops) => match point_ops {
                PointOperations::UpsertPoints(point_insert_operations) => {
                    let request = tonic::Request::new(internal_upsert_points(
                        point_insert_operations,
                        self,
                        wait,
                    )?);
                    self.with_points_client(|mut client| client.upsert(request)).await?
                }
                PointOperations::DeletePoints { ids } => {
                    let request =
                        tonic::Request::new(internal_delete_points(ids, self, wait));
                    self.with_points_client(|mut client| client.delete(request)).await?
                }
                PointOperations::DeletePointsByFilter(filter) => {
                    let request = tonic::Request::new(internal_delete_points_by_filter(
                        filter, self, wait,
                    ));
                    self.with_points_client(|mut client| client.delete(request)).await?
                }
            },
            CollectionUpdateOperations::PayloadOperation(payload_ops) => {
                match payload_ops {
                    PayloadOps::SetPayload(set_payload) => {
                        let request = tonic::Request::new(internal_set_payload(
                            set_payload,
                            self,
                            wait,
                        ));
                        self.with_points_client(|mut client| client.set_payload(request)).await?
                    }
                    PayloadOps::DeletePayload(delete_payload) => {
                        let request = tonic::Request::new(internal_delete_payload(
                            delete_payload,
                            self,
                            wait,
                        ));
                        self.with_points_client(|mut client| client.delete_payload(request)).await?
                    }
                    PayloadOps::ClearPayload { points } => {
                        let request =
                            tonic::Request::new(internal_clear_payload(points, self, wait));
                        self.with_points_client(|mut client| client.clear_payload(request)).await?
                    }
                    PayloadOps::ClearPayloadByFilter(filter) => {
                        let request = tonic::Request::new(
                            internal_clear_payload_by_filter(filter, self, wait),
                        );
                        self.with_points_client(|mut client| client.clear_payload(request)).await?
                    }
                }
            }
            CollectionUpdateOperations::FieldIndexOperation(field_index_op) => {
                match field_index_op {
                    FieldIndexOperations::CreateIndex(create_index) => {
                        let request = tonic::Request::new(internal_create_index(
                            create_index,
                            self,
                            wait,
                        ));
                        self.with_points_client(|mut client| client.create_field_index(request)).await?
                    }
                    FieldIndexOperations::DeleteIndex(delete_index) => {
                        let request = tonic::Request::new(internal_delete_index(
                            delete_index,
                            self,
                            wait,
                        ));
                        self.with_points_client(|mut client| client.delete_field_index(request)).await?
                    }
                }
            }
        }

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
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: bool,
        filter: Option<&Filter>,
    ) -> CollectionResult<Vec<Record>> {

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

        let response = self.with_points_client(|mut client| client.scroll(request)).await?;

        let scroll_response = response.into_inner();
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
        let request = tonic::Request::new(GetCollectionInfoRequestInternal {
            get_collection_info_request: Some(get_collection_info_request),
            shard_id: self.id,
        });
        let response = self
            .with_collections_client(|mut client| client.get(request))
            .await?;

        let get_collection_response = response.into_inner();
        let result: Result<CollectionInfo, Status> = get_collection_response.try_into();
        result.map_err(|e| e.into())
    }

    async fn search(
        &self,
        request: Arc<SearchRequest>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {

        let search_points = SearchPoints {
            collection_name: self.collection_id.clone(),
            vector: request.vector.clone(),
            filter: request.filter.clone().map(|f| f.into()),
            limit: request.limit as u64,
            with_vector: Some(request.with_vector),
            with_payload: request.with_payload.clone().map(|wp| wp.into()),
            params: request.params.map(|sp| sp.into()),
            score_threshold: request.score_threshold,
            offset: Some(request.offset as u64),
        };
        let request = tonic::Request::new(SearchPointsInternal {
            search_points: Some(search_points),
            shard_id: self.id,
        });
        let response = self.with_points_client(|mut client| client.search(request)).await?;

        let search_response = response.into_inner();
        let result: Result<Vec<ScoredPoint>, Status> = search_response
            .result
            .into_iter()
            .map(|scored| scored.try_into())
            .collect();
        result.map_err(|e| e.into())
    }

    async fn count(&self, request: Arc<CountRequest>) -> CollectionResult<CountResult> {

        let count_points = CountPoints {
            collection_name: self.collection_id.clone(),
            filter: request.filter.clone().map(|f| f.into()),
            exact: Some(request.exact),
        };

        let request = tonic::Request::new(CountPointsInternal {
            count_points: Some(count_points),
            shard_id: self.id,
        });
        let response = self.with_points_client(|mut client| client.count(request)).await?;
        let count_response = response.into_inner();
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
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>> {

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
        let response = self.with_points_client(|mut client| client.get(request)).await?;

        let get_response = response.into_inner();
        let result: Result<Vec<Record>, Status> = get_response
            .result
            .into_iter()
            .map(|scored| scored.try_into())
            .collect();
        result.map_err(|e| e.into())
    }
}
