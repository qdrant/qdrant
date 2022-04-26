use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::{PointInsertOperations, PointOperations};
use crate::operations::FieldIndexOperations;
use crate::shard::{PeerId, ShardId, ShardOperation};
use crate::{
    CollectionError, CollectionId, CollectionInfo, CollectionResult, CollectionSearcher,
    CollectionUpdateOperations, PointRequest, Record, SearchRequest, UpdateResult,
};
use api::grpc::conversions::payload_to_proto;
use api::grpc::qdrant::collections_internal_client::CollectionsInternalClient;
use api::grpc::qdrant::points_internal_client::PointsInternalClient;
use api::grpc::qdrant::points_selector::PointsSelectorOneOf;
use api::grpc::qdrant::{
    ClearPayloadPoints, ClearPayloadPointsInternal, CreateFieldIndexCollection,
    CreateFieldIndexCollectionInternal, DeleteFieldIndexCollection,
    DeleteFieldIndexCollectionInternal, DeletePayloadPoints, DeletePayloadPointsInternal,
    DeletePoints, DeletePointsInternal, GetCollectionInfoRequest, GetCollectionInfoRequestInternal,
    GetPoints, GetPointsInternal, PointsIdsList, PointsSelector, ScrollPoints,
    ScrollPointsInternal, SearchPoints, SearchPointsInternal, SetPayloadPoints,
    SetPayloadPointsInternal, UpsertPoints, UpsertPointsInternal,
};
use async_trait::async_trait;
use segment::types::{ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tonic::transport::Channel;
use tonic::Status;
use tower::timeout::Timeout;

/// RemoteShard
///
/// Remote Shard is a representation of a shard that is located on a remote peer.
/// Currently a placeholder implementation for later work.
#[allow(dead_code)]
pub struct RemoteShard {
    id: ShardId,
    collection_id: CollectionId,
    pub peer_id: PeerId,
}

// TODO pool channels & inject timeout from consensus config
impl RemoteShard {
    async fn points_client(
        &self,
        ip_to_address: &HashMap<u64, SocketAddr>,
    ) -> CollectionResult<PointsInternalClient<Timeout<Channel>>> {
        let peer_address = ip_to_address.get(&self.peer_id).unwrap();
        let timeout = Duration::from_millis(1000);
        let channel = Channel::from_shared(peer_address.to_string())?
            .connect()
            .await?;
        let timeout_channel = Timeout::new(channel, timeout);
        Ok(PointsInternalClient::new(timeout_channel))
    }

    async fn collections_client(
        &self,
        ip_to_address: &HashMap<u64, SocketAddr>,
    ) -> CollectionResult<CollectionsInternalClient<Timeout<Channel>>> {
        let peer_address = ip_to_address.get(&self.peer_id).unwrap();
        let timeout = Duration::from_millis(1000);
        let channel = Channel::from_shared(peer_address.to_string())?
            .connect()
            .await?;
        let timeout_channel = Timeout::new(channel, timeout);
        Ok(CollectionsInternalClient::new(timeout_channel))
    }
}

#[async_trait]
#[allow(unused_variables)]
impl ShardOperation for &RemoteShard {
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        ip_to_address: &HashMap<u64, SocketAddr>,
    ) -> CollectionResult<UpdateResult> {
        let mut client = self.points_client(ip_to_address).await?;

        let response = match operation {
            CollectionUpdateOperations::PointOperation(point_ops) => match point_ops {
                PointOperations::UpsertPoints(upsert_points) => {
                    let request = tonic::Request::new(UpsertPointsInternal {
                        shard_id: self.id,
                        upsert_points: Some(UpsertPoints {
                            collection_name: self.collection_id.clone(),
                            wait: Some(true),
                            points: match upsert_points {
                                PointInsertOperations::PointsBatch(batch) => {
                                    return Err(CollectionError::service_error(
                                        "Operation not supported".to_string(),
                                    ))
                                }
                                PointInsertOperations::PointsList(list) => list
                                    .points
                                    .into_iter()
                                    .map(|id| id.try_into())
                                    .collect::<Result<Vec<_>, Status>>()?,
                            },
                        }),
                    });
                    client.upsert(request).await?
                }
                PointOperations::DeletePoints { ids } => {
                    let request = tonic::Request::new(DeletePointsInternal {
                        shard_id: self.id,
                        delete_points: Some(DeletePoints {
                            collection_name: self.collection_id.clone(),
                            wait: Some(true),
                            points: Some(PointsSelector {
                                points_selector_one_of: Some(PointsSelectorOneOf::Points(
                                    PointsIdsList {
                                        ids: ids.into_iter().map(|id| id.into()).collect(),
                                    },
                                )),
                            }),
                        }),
                    });
                    client.delete(request).await?
                }
                PointOperations::DeletePointsByFilter(filter) => {
                    let request = tonic::Request::new(DeletePointsInternal {
                        shard_id: self.id,
                        delete_points: Some(DeletePoints {
                            collection_name: self.collection_id.clone(),
                            wait: Some(true),
                            points: Some(PointsSelector {
                                points_selector_one_of: Some(PointsSelectorOneOf::Filter(
                                    filter.into(),
                                )),
                            }),
                        }),
                    });
                    client.delete(request).await?
                }
            },
            CollectionUpdateOperations::PayloadOperation(payload_ops) => match payload_ops {
                PayloadOps::SetPayload(set_payload) => {
                    let request = tonic::Request::new(SetPayloadPointsInternal {
                        shard_id: self.id,
                        set_payload_points: Some(SetPayloadPoints {
                            collection_name: self.collection_id.clone(),
                            wait: Some(true),
                            payload: payload_to_proto(set_payload.payload),
                            points: set_payload.points.into_iter().map(|id| id.into()).collect(),
                        }),
                    });
                    client.set_payload(request).await?
                }
                PayloadOps::DeletePayload(delete_payload) => {
                    let request = tonic::Request::new(DeletePayloadPointsInternal {
                        shard_id: self.id,
                        delete_payload_points: Some(DeletePayloadPoints {
                            collection_name: self.collection_id.clone(),
                            wait: Some(true),
                            keys: delete_payload.keys,
                            points: delete_payload
                                .points
                                .into_iter()
                                .map(|id| id.into())
                                .collect(),
                        }),
                    });
                    client.delete_payload(request).await?
                }
                PayloadOps::ClearPayload { points } => {
                    let request = tonic::Request::new(ClearPayloadPointsInternal {
                        shard_id: self.id,
                        clear_payload_points: Some(ClearPayloadPoints {
                            collection_name: self.collection_id.clone(),
                            wait: Some(true),
                            points: Some(PointsSelector {
                                points_selector_one_of: Some(PointsSelectorOneOf::Points(
                                    PointsIdsList {
                                        ids: points.into_iter().map(|id| id.into()).collect(),
                                    },
                                )),
                            }),
                        }),
                    });
                    client.clear_payload(request).await?
                }
                PayloadOps::ClearPayloadByFilter(filter) => {
                    let request = tonic::Request::new(ClearPayloadPointsInternal {
                        shard_id: self.id,
                        clear_payload_points: Some(ClearPayloadPoints {
                            collection_name: self.collection_id.clone(),
                            wait: Some(true),
                            points: Some(PointsSelector {
                                points_selector_one_of: Some(PointsSelectorOneOf::Filter(
                                    filter.into(),
                                )),
                            }),
                        }),
                    });
                    client.clear_payload(request).await?
                }
            },
            CollectionUpdateOperations::FieldIndexOperation(field_index_op) => match field_index_op
            {
                FieldIndexOperations::CreateIndex(create_index) => {
                    let request = tonic::Request::new(CreateFieldIndexCollectionInternal {
                        shard_id: self.id,
                        create_field_index_collection: Some(CreateFieldIndexCollection {
                            collection_name: self.collection_id.clone(),
                            wait: Some(true),
                            field_name: create_index.field_name,
                            field_type: create_index.field_type.map(|ft| ft.index()),
                        }),
                    });
                    client.create_field_index(request).await?
                }
                FieldIndexOperations::DeleteIndex(delete_index) => {
                    let request = tonic::Request::new(DeleteFieldIndexCollectionInternal {
                        shard_id: self.id,
                        delete_field_index_collection: Some(DeleteFieldIndexCollection {
                            collection_name: self.collection_id.clone(),
                            wait: Some(true),
                            field_name: delete_index,
                        }),
                    });
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
        ip_to_address: &HashMap<u64, SocketAddr>,
    ) -> CollectionResult<Vec<Record>> {
        let mut client = self.points_client(ip_to_address).await?;

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

    async fn info(
        &self,
        ip_to_address: &HashMap<u64, SocketAddr>,
    ) -> CollectionResult<CollectionInfo> {
        let mut client = self.collections_client(ip_to_address).await?;

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
        ip_to_address: &HashMap<u64, SocketAddr>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let mut client = self.points_client(ip_to_address).await?;

        let search_points = SearchPoints {
            collection_name: self.collection_id.clone(),
            vector: request.vector.clone(),
            filter: request.filter.clone().map(|f| f.into()),
            top: request.top as u64,
            with_vector: Some(request.with_vector),
            with_payload: request.with_payload.clone().map(|wp| wp.into()),
            params: request.params.map(|sp| sp.into()),
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
        ip_to_address: &HashMap<u64, SocketAddr>,
    ) -> CollectionResult<Vec<Record>> {
        let mut client = self.points_client(ip_to_address).await?;

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
