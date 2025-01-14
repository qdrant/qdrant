use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use api::grpc::qdrant::collections_internal_client::CollectionsInternalClient;
use api::grpc::qdrant::points_internal_client::PointsInternalClient;
use api::grpc::qdrant::qdrant_client::QdrantClient;
use api::grpc::qdrant::shard_snapshot_location::Location;
use api::grpc::qdrant::shard_snapshots_client::ShardSnapshotsClient;
use api::grpc::qdrant::{
    CollectionOperationResponse, CoreSearchBatchPointsInternal, CountPoints, CountPointsInternal,
    CountResponse, FacetCountsInternal, GetCollectionInfoRequest, GetCollectionInfoRequestInternal,
    GetPoints, GetPointsInternal, GetShardRecoveryPointRequest, HealthCheckRequest,
    InitiateShardTransferRequest, QueryBatchPointsInternal, QueryBatchResponseInternal,
    QueryShardPoints, RecoverShardSnapshotRequest, RecoverSnapshotResponse, ScrollPoints,
    ScrollPointsInternal, SearchBatchResponse, ShardSnapshotLocation,
    UpdateShardCutoffPointRequest, WaitForShardStateRequest,
};
use api::grpc::transport_channel_pool::{AddTimeout, MAX_GRPC_CHANNEL_TIMEOUT};
use async_trait::async_trait;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::TelemetryDetail;
use itertools::Itertools;
use parking_lot::Mutex;
use segment::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use segment::data_types::facets::{FacetParams, FacetResponse, FacetValueHit};
use segment::data_types::order_by::OrderBy;
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;
use tonic::codegen::InterceptedService;
use tonic::transport::{Channel, Uri};
use tonic::Status;
use url::Url;

use super::conversions::{
    internal_delete_vectors, internal_delete_vectors_by_filter, internal_update_vectors,
};
use super::local_shard::clock_map::RecoveryPoint;
use super::replica_set::ReplicaState;
use crate::operations::conversions::try_record_from_grpc;
use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::{PointOperations, WriteOrdering};
use crate::operations::snapshot_ops::SnapshotPriority;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch,
    CountRequestInternal, CountResult, PointRequestInternal, RecordInternal, UpdateResult,
};
use crate::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};
use crate::operations::vector_ops::VectorOperations;
use crate::operations::{CollectionUpdateOperations, FieldIndexOperations, OperationWithClockTag};
use crate::shards::channel_service::ChannelService;
use crate::shards::conversions::{
    internal_clear_payload, internal_clear_payload_by_filter, internal_create_index,
    internal_delete_index, internal_delete_payload, internal_delete_points,
    internal_delete_points_by_filter, internal_set_payload, internal_sync_points,
    internal_upsert_points, try_scored_point_from_grpc,
};
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_trait::ShardOperation;
use crate::shards::telemetry::RemoteShardTelemetry;
use crate::shards::CollectionId;

/// Timeout for transferring and recovering a shard snapshot on a remote peer.
const SHARD_SNAPSHOT_TRANSFER_RECOVER_TIMEOUT: Duration = MAX_GRPC_CHANNEL_TIMEOUT;

/// RemoteShard
///
/// Remote Shard is a representation of a shard that is located on a remote peer.
#[derive(Clone)]
pub struct RemoteShard {
    pub(crate) id: ShardId,
    pub(crate) collection_id: CollectionId,
    pub peer_id: PeerId,
    pub channel_service: ChannelService,
    telemetry_search_durations: Arc<Mutex<OperationDurationsAggregator>>,
    telemetry_update_durations: Arc<Mutex<OperationDurationsAggregator>>,
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
            telemetry_search_durations: OperationDurationsAggregator::new(),
            telemetry_update_durations: OperationDurationsAggregator::new(),
        }
    }

    pub fn restore_snapshot(_snapshot_path: &Path) {
        // NO extra actions needed for remote shards
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
        f: impl Fn(PointsInternalClient<InterceptedService<Channel, AddTimeout>>) -> O,
    ) -> CollectionResult<T> {
        let current_address = self.current_address()?;
        self.channel_service
            .channel_pool
            .with_channel(&current_address, |channel| {
                let client = PointsInternalClient::new(channel);
                let client = client.max_decoding_message_size(usize::MAX);
                f(client)
            })
            .await
            .map_err(|err| err.into())
    }

    async fn with_collections_client<T, O: Future<Output = Result<T, Status>>>(
        &self,
        f: impl Fn(CollectionsInternalClient<InterceptedService<Channel, AddTimeout>>) -> O,
    ) -> CollectionResult<T> {
        let current_address = self.current_address()?;
        self.channel_service
            .channel_pool
            .with_channel(&current_address, |channel| {
                let client = CollectionsInternalClient::new(channel);
                let client = client.max_decoding_message_size(usize::MAX);
                f(client)
            })
            .await
            .map_err(|err| err.into())
    }

    async fn with_shard_snapshots_client_timeout<T, O: Future<Output = Result<T, Status>>>(
        &self,
        f: impl Fn(ShardSnapshotsClient<InterceptedService<Channel, AddTimeout>>) -> O,
        timeout: Option<Duration>,
        retries: usize,
    ) -> CollectionResult<T> {
        let current_address = self.current_address()?;
        self.channel_service
            .channel_pool
            .with_channel_timeout(
                &current_address,
                |channel| {
                    let client = ShardSnapshotsClient::new(channel);
                    let client = client.max_decoding_message_size(usize::MAX);
                    f(client)
                },
                timeout,
                retries,
            )
            .await
            .map_err(|err| err.into())
    }

    async fn with_qdrant_client<T, Fut: Future<Output = Result<T, Status>>>(
        &self,
        f: impl Fn(QdrantClient<InterceptedService<Channel, AddTimeout>>) -> Fut,
    ) -> CollectionResult<T> {
        let current_address = self.current_address()?;
        self.channel_service
            .channel_pool
            .with_channel(&current_address, |channel| {
                let client = QdrantClient::new(channel);
                f(client)
            })
            .await
            .map_err(|err| err.into())
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> RemoteShardTelemetry {
        RemoteShardTelemetry {
            shard_id: self.id,
            peer_id: Some(self.peer_id),
            searches: self
                .telemetry_search_durations
                .lock()
                .get_statistics(detail),
            updates: self
                .telemetry_update_durations
                .lock()
                .get_statistics(detail),
        }
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

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn forward_update(
        &self,
        operation: OperationWithClockTag,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        // `RemoteShard::execute_update_operation` is cancel safe, so this method is cancel safe.

        self.execute_update_operation(
            Some(self.id),
            self.collection_id.clone(),
            operation,
            wait,
            Some(ordering),
        )
        .await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn execute_update_operation(
        &self,
        shard_id: Option<ShardId>,
        collection_name: String,
        operation: OperationWithClockTag,
        wait: bool,
        ordering: Option<WriteOrdering>,
    ) -> CollectionResult<UpdateResult> {
        // Cancelling remote request should always be safe on the client side and update API
        // *should be* cancel safe on the server side, so this method is cancel safe.

        let mut timer = ScopeDurationMeasurer::new(&self.telemetry_update_durations);
        timer.set_success(false);

        let point_operation_response = match operation.operation {
            CollectionUpdateOperations::PointOperation(point_ops) => match point_ops {
                PointOperations::UpsertPoints(point_insert_operations) => {
                    let request = &internal_upsert_points(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        point_insert_operations,
                        wait,
                        ordering,
                    )?;
                    self.with_points_client(|mut client| async move {
                        client.upsert(tonic::Request::new(request.clone())).await
                    })
                    .await?
                    .into_inner()
                }
                PointOperations::DeletePoints { ids } => {
                    let request = &internal_delete_points(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        ids,
                        wait,
                        ordering,
                    );
                    self.with_points_client(|mut client| async move {
                        client.delete(tonic::Request::new(request.clone())).await
                    })
                    .await?
                    .into_inner()
                }
                PointOperations::DeletePointsByFilter(filter) => {
                    let request = &internal_delete_points_by_filter(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        filter,
                        wait,
                        ordering,
                    );
                    self.with_points_client(|mut client| async move {
                        client.delete(tonic::Request::new(request.clone())).await
                    })
                    .await?
                    .into_inner()
                }
                PointOperations::SyncPoints(operation) => {
                    let request = &internal_sync_points(
                        shard_id,
                        None, // TODO!?
                        collection_name,
                        operation,
                        wait,
                        ordering,
                    )?;
                    self.with_points_client(|mut client| async move {
                        client.sync(tonic::Request::new(request.clone())).await
                    })
                    .await?
                    .into_inner()
                }
            },
            CollectionUpdateOperations::VectorOperation(vector_ops) => match vector_ops {
                VectorOperations::UpdateVectors(update_operation) => {
                    let request = &internal_update_vectors(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        update_operation,
                        wait,
                        ordering,
                    )?;
                    self.with_points_client(|mut client| async move {
                        client
                            .update_vectors(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
                VectorOperations::DeleteVectors(ids, vector_names) => {
                    let request = &internal_delete_vectors(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        ids.points,
                        vector_names.clone(),
                        wait,
                        ordering,
                    );
                    self.with_points_client(|mut client| async move {
                        client
                            .delete_vectors(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
                VectorOperations::DeleteVectorsByFilter(filter, vector_names) => {
                    let request = &internal_delete_vectors_by_filter(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        filter,
                        vector_names.clone(),
                        wait,
                        ordering,
                    );
                    self.with_points_client(|mut client| async move {
                        client
                            .delete_vectors(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
            },
            CollectionUpdateOperations::PayloadOperation(payload_ops) => match payload_ops {
                PayloadOps::SetPayload(set_payload) => {
                    let request = &internal_set_payload(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        set_payload,
                        wait,
                        ordering,
                    );
                    self.with_points_client(|mut client| async move {
                        client
                            .set_payload(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
                PayloadOps::DeletePayload(delete_payload) => {
                    let request = &internal_delete_payload(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        delete_payload,
                        wait,
                        ordering,
                    );
                    self.with_points_client(|mut client| async move {
                        client
                            .delete_payload(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
                PayloadOps::ClearPayload { points } => {
                    let request = &internal_clear_payload(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        points,
                        wait,
                        ordering,
                    );
                    self.with_points_client(|mut client| async move {
                        client
                            .clear_payload(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
                PayloadOps::ClearPayloadByFilter(filter) => {
                    let request = &internal_clear_payload_by_filter(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        filter,
                        wait,
                        ordering,
                    );
                    self.with_points_client(|mut client| async move {
                        client
                            .clear_payload(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
                PayloadOps::OverwritePayload(set_payload) => {
                    let request = &internal_set_payload(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        set_payload,
                        wait,
                        ordering,
                    );
                    self.with_points_client(|mut client| async move {
                        client
                            .overwrite_payload(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
            },
            CollectionUpdateOperations::FieldIndexOperation(field_index_op) => match field_index_op
            {
                FieldIndexOperations::CreateIndex(create_index) => {
                    let request = &internal_create_index(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        create_index,
                        wait,
                        ordering,
                    );
                    self.with_points_client(|mut client| async move {
                        client
                            .create_field_index(tonic::Request::new(request.clone()))
                            .await
                    })
                    .await?
                    .into_inner()
                }
                FieldIndexOperations::DeleteIndex(delete_index) => {
                    let request = &internal_delete_index(
                        shard_id,
                        operation.clock_tag,
                        collection_name,
                        delete_index,
                        wait,
                        ordering,
                    );
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

    /// Recover a shard at the remote from the given public `url`.
    ///
    /// # Warning
    ///
    /// This method specifies a timeout of 24 hours.
    ///
    /// Setting an API key may leak when requesting a snapshot file from a malicious server.
    /// This is potentially dangerous if a user has control over what URL is accessed.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn recover_shard_snapshot_from_url(
        &self,
        collection_name: &str,
        shard_id: ShardId,
        url: &Url,
        snapshot_priority: SnapshotPriority,
        api_key: Option<&str>,
    ) -> CollectionResult<RecoverSnapshotResponse> {
        let res = self
            .with_shard_snapshots_client_timeout(
                |mut client| async move {
                    client
                        .recover(RecoverShardSnapshotRequest {
                            collection_name: collection_name.into(),
                            shard_id,
                            snapshot_location: Some(ShardSnapshotLocation {
                                location: Some(Location::Url(url.to_string())),
                            }),
                            snapshot_priority: api::grpc::qdrant::ShardSnapshotPriority::from(
                                snapshot_priority,
                            ) as i32,
                            checksum: None,
                            api_key: api_key.map(Into::into),
                        })
                        .await
                },
                Some(SHARD_SNAPSHOT_TRANSFER_RECOVER_TIMEOUT),
                api::grpc::transport_channel_pool::DEFAULT_RETRIES,
            )
            .await?
            .into_inner();
        Ok(res)
    }

    /// Wait for a local shard on the remote to get into a certain state
    pub async fn wait_for_shard_state(
        &self,
        collection_name: &str,
        shard_id: ShardId,
        state: ReplicaState,
        timeout: Duration,
    ) -> CollectionResult<CollectionOperationResponse> {
        let res = self
            .with_collections_client(|mut client| async move {
                let mut wait_for_shard_request = tonic::Request::new(WaitForShardStateRequest {
                    collection_name: collection_name.into(),
                    shard_id,
                    state: api::grpc::qdrant::ReplicaState::from(state) as i32,
                    timeout: timeout.as_secs_f32().ceil() as u64,
                });
                wait_for_shard_request.set_timeout(timeout);
                client.wait_for_shard_state(wait_for_shard_request).await
            })
            .await?
            .into_inner();
        Ok(res)
    }

    /// Request the recovery point on the remote shard
    pub async fn shard_recovery_point(
        &self,
        collection_name: &str,
        shard_id: ShardId,
    ) -> CollectionResult<RecoveryPoint> {
        let res = self
            .with_collections_client(|mut client| async move {
                client
                    .get_shard_recovery_point(GetShardRecoveryPointRequest {
                        collection_name: collection_name.into(),
                        shard_id,
                    })
                    .await
            })
            .await?
            .into_inner();

        let Some(recovery_point) = res.recovery_point else {
            return Err(CollectionError::service_error(
                "Recovery point data is missing in recovery point response",
            ));
        };

        Ok(recovery_point.try_into()?)
    }

    /// Update the shard cutoff point on the remote shard
    pub async fn update_shard_cutoff_point(
        &self,
        collection_name: &str,
        shard_id: ShardId,
        cutoff: &RecoveryPoint,
    ) -> CollectionResult<()> {
        self.with_collections_client(|mut client| async move {
            client
                .update_shard_cutoff_point(UpdateShardCutoffPointRequest {
                    collection_name: collection_name.into(),
                    shard_id,
                    cutoff: Some(cutoff.into()),
                })
                .await
        })
        .await?;
        Ok(())
    }

    pub async fn health_check(&self) -> CollectionResult<()> {
        let _ = self
            .with_qdrant_client(|mut client| async move {
                client.health_check(HealthCheckRequest {}).await
            })
            .await?
            .into_inner();

        Ok(())
    }

    /// Validate timeout before making a read operation.
    /// - detect elapsed timeouts early to avoid unnecessary traffic
    /// - round up to the nearest second to well with our internal timeout handling
    ///
    /// Returns a new timeout value if the input is valid, otherwise an error.
    pub fn process_read_timeout(
        timeout: Option<Duration>,
        operation: &str,
    ) -> CollectionResult<Option<Duration>> {
        match timeout {
            None => Ok(timeout),
            Some(t) if t.is_zero() => Err(CollectionError::timeout(0, operation)),
            Some(t) => {
                // round up to avoid losing completely timeouts that are under 1 second
                let timeout_secs = t.as_secs_f32().ceil() as u64;
                Ok(Some(Duration::from_secs(timeout_secs)))
            }
        }
    }
}

// New-type to own the type in the crate for conversions via From
pub struct CollectionCoreSearchRequest<'a>(pub(crate) (CollectionId, &'a CoreSearchRequest));

#[async_trait]
impl ShardOperation for RemoteShard {
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn update(
        &self,
        operation: OperationWithClockTag,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        // `RemoteShard::execute_update_operation` is cancel safe, so this method is cancel safe.

        // targets the shard explicitly
        let shard_id = Some(self.id);
        self.execute_update_operation(shard_id, self.collection_id.clone(), operation, wait, None)
            .await
    }

    async fn scroll_by(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        _search_runtime_handle: &Handle,
        order_by: Option<&OrderBy>,
        timeout: Option<Duration>,
        _hw_measurement_acc: HwMeasurementAcc, // TODO(io_measurement) fill this with response data
    ) -> CollectionResult<Vec<RecordInternal>> {
        let processed_timeout = Self::process_read_timeout(timeout, "scroll")?;
        let scroll_points = ScrollPoints {
            collection_name: self.collection_id.clone(),
            filter: filter.map(|f| f.clone().into()),
            offset: offset.map(|o| o.into()),
            limit: Some(limit as u32),
            with_payload: Some(with_payload_interface.clone().into()),
            with_vectors: Some(with_vector.clone().into()),
            read_consistency: None,
            shard_key_selector: None,
            order_by: order_by.map(|o| o.clone().into()),
            timeout: processed_timeout.map(|t| t.as_secs()),
        };
        let scroll_request = &ScrollPointsInternal {
            scroll_points: Some(scroll_points),
            shard_id: Some(self.id),
        };

        let scroll_response = self
            .with_points_client(|mut client| async move {
                let mut request = tonic::Request::new(scroll_request.clone());
                if let Some(timeout) = processed_timeout {
                    request.set_timeout(timeout);
                }
                client.scroll(request).await
            })
            .await?
            .into_inner();

        // We need the `____ordered_with____` value even if the user didn't request payload
        let parse_payload = with_payload_interface.is_required() || order_by.is_some();

        let result: Result<Vec<RecordInternal>, Status> = scroll_response
            .result
            .into_iter()
            .map(|point| try_record_from_grpc(point, parse_payload))
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
    async fn core_search(
        &self,
        batch_request: Arc<CoreSearchRequestBatch>,
        _search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let processed_timeout = Self::process_read_timeout(timeout, "search")?;
        let mut timer = ScopeDurationMeasurer::new(&self.telemetry_search_durations);
        timer.set_success(false);

        let search_points = batch_request
            .searches
            .iter()
            .map(|s| CollectionCoreSearchRequest((self.collection_id.clone(), s)).into())
            .collect();

        let request = &CoreSearchBatchPointsInternal {
            collection_name: self.collection_id.clone(),
            search_points,
            shard_id: Some(self.id),
            timeout: processed_timeout.map(|t| t.as_secs()),
        };
        let search_batch_response = self
            .with_points_client(|mut client| async move {
                let mut request = tonic::Request::new(request.clone());

                if let Some(timeout) = processed_timeout {
                    request.set_timeout(timeout);
                }

                client.core_search_batch(request).await
            })
            .await?
            .into_inner();

        let SearchBatchResponse {
            result,
            time: _,
            usage,
        } = search_batch_response;

        if let Some(usage) = usage {
            hw_measurement_acc.accumulate_request(
                usage.cpu as usize,
                usage.io_read as usize,
                usage.io_write as usize,
            );
        }

        let result: Result<Vec<Vec<ScoredPoint>>, Status> = result
            .into_iter()
            .zip(batch_request.searches.iter())
            .map(|(batch_result, request)| {
                let is_payload_required = request
                    .with_payload
                    .as_ref()
                    .is_some_and(|with_payload| with_payload.is_required());

                batch_result
                    .result
                    .into_iter()
                    .map(|point| try_scored_point_from_grpc(point, is_payload_required))
                    .collect()
            })
            .collect();
        let result = result.map_err(|e| e.into());
        if result.is_ok() {
            timer.set_success(true);
        }
        result
    }

    async fn count(
        &self,
        request: Arc<CountRequestInternal>,
        _search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<CountResult> {
        let processed_timeout = Self::process_read_timeout(timeout, "count")?;
        let count_points = CountPoints {
            collection_name: self.collection_id.clone(),
            filter: request.filter.clone().map(|f| f.into()),
            exact: Some(request.exact),
            read_consistency: None,
            shard_key_selector: None,
            timeout: processed_timeout.map(|t| t.as_secs()),
        };

        let count_request = &CountPointsInternal {
            count_points: Some(count_points),
            shard_id: Some(self.id),
        };
        let count_response = self
            .with_points_client(|mut client| async move {
                let mut request = tonic::Request::new(count_request.clone());
                if let Some(timeout) = processed_timeout {
                    request.set_timeout(timeout);
                }
                client.count(request).await
            })
            .await?
            .into_inner();

        let CountResponse {
            result,
            time: _,
            usage,
        } = count_response;

        if let Some(usage) = usage {
            hw_measurement_acc.accumulate_request(
                usage.cpu as usize,
                usage.io_read as usize,
                usage.io_write as usize,
            );
        }

        result.map_or_else(
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
        request: Arc<PointRequestInternal>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        _search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        _hw_measurement_acc: HwMeasurementAcc, // TODO(io_measurement): uncomment below and use this parameter
    ) -> CollectionResult<Vec<RecordInternal>> {
        let processed_timeout = Self::process_read_timeout(timeout, "retrieve")?;
        let get_points = GetPoints {
            collection_name: self.collection_id.clone(),
            ids: request.ids.iter().copied().map(|v| v.into()).collect(),
            with_payload: request.with_payload.clone().map(|wp| wp.into()),
            with_vectors: Some(with_vector.clone().into()),
            read_consistency: None,
            shard_key_selector: None,
            timeout: processed_timeout.map(|t| t.as_secs()),
        };
        let get_request = &GetPointsInternal {
            get_points: Some(get_points),
            shard_id: Some(self.id),
        };

        let get_response = self
            .with_points_client(|mut client| async move {
                let mut request = tonic::Request::new(get_request.clone());
                if let Some(timeout) = processed_timeout {
                    request.set_timeout(timeout);
                }
                client.get(request).await
            })
            .await?
            .into_inner();

        // TODO(io_measurement) implement
        // if let Some(usage) = usage {
        //     hw_measurement_acc.accumulate_request(
        //         usage.cpu as usize,
        //         usage.io_read as usize,
        //         usage.io_write as usize,
        //     );
        // }

        let result: Result<Vec<RecordInternal>, Status> = get_response
            .result
            .into_iter()
            .map(|point| try_record_from_grpc(point, with_payload.enable))
            .collect();

        result.map_err(|e| e.into())
    }

    async fn query_batch(
        &self,
        requests: Arc<Vec<ShardQueryRequest>>,
        _search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        let processed_timeout = Self::process_read_timeout(timeout, "query_batch")?;
        let mut timer = ScopeDurationMeasurer::new(&self.telemetry_search_durations);
        timer.set_success(false);

        let requests = requests.as_ref();

        let batch_response = self
            .with_points_client(|mut client| async move {
                let query_points = requests
                    .iter()
                    .map(|request| QueryShardPoints::from(request.clone()))
                    .collect();

                let request = &QueryBatchPointsInternal {
                    collection_name: self.collection_id.clone(),
                    query_points,
                    shard_id: Some(self.id),
                    timeout: processed_timeout.map(|t| t.as_secs()),
                };

                let mut request = tonic::Request::new(request.clone());

                if let Some(timeout) = processed_timeout {
                    request.set_timeout(timeout);
                }

                client.query_batch(request).await
            })
            .await?
            .into_inner();

        let QueryBatchResponseInternal {
            results,
            time: _,
            usage,
        } = batch_response;

        if let Some(usage) = usage {
            hw_measurement_acc.accumulate_request(
                usage.cpu as usize,
                usage.io_read as usize,
                usage.io_write as usize,
            );
        }

        let result = results
            .into_iter()
            .zip(requests.iter())
            .map(|(query_result, request)| {
                let is_payload_required = request.with_payload.is_required();

                query_result
                    .intermediate_results
                    .into_iter()
                    .map(|intermediate| {
                        intermediate
                            .result
                            .into_iter()
                            .map(|point| try_scored_point_from_grpc(point, is_payload_required))
                            .collect()
                    })
                    .collect()
            })
            .try_collect()?;

        timer.set_success(true);

        Ok(result)
    }

    async fn facet(
        &self,
        request: Arc<FacetParams>,
        _search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<FacetResponse> {
        let processed_timeout = Self::process_read_timeout(timeout, "facet")?;
        let mut timer = ScopeDurationMeasurer::new(&self.telemetry_search_durations);
        timer.set_success(false);

        let FacetParams {
            key,
            limit,
            filter,
            exact,
        } = request.as_ref();

        let response = self
            .with_points_client(|mut client| async move {
                let request = &FacetCountsInternal {
                    collection_name: self.collection_id.clone(),
                    key: key.to_string(),
                    filter: filter.clone().map(api::grpc::qdrant::Filter::from),
                    limit: *limit as u64,
                    exact: *exact,
                    shard_id: self.id,
                    timeout: processed_timeout.map(|t| t.as_secs()),
                };

                let mut request = tonic::Request::new(request.clone());

                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }

                client.facet(request).await
            })
            .await?
            .into_inner();

        let hits = response
            .hits
            .into_iter()
            .map(FacetValueHit::try_from)
            .try_collect()?;

        let result = FacetResponse { hits };

        timer.set_success(true);

        Ok(result)
    }
}
