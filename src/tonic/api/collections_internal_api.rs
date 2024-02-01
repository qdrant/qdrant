use std::sync::Arc;
use std::time::{Duration, Instant};

use api::grpc::qdrant::collections_internal_server::CollectionsInternal;
use api::grpc::qdrant::{
    CollectionOperationResponse, GetCollectionInfoRequestInternal, GetCollectionInfoResponse,
    GetShardRecoveryPointRequest, GetShardRecoveryPointResponse, InitiateShardTransferRequest,
    WaitForShardStateRequest,
};
use storage::content_manager::conversions::error_to_status;
use storage::content_manager::toc::TableOfContent;
use tonic::{Request, Response, Status};

use super::validate_and_log;
use crate::tonic::api::collections_common::get;

pub struct CollectionsInternalService {
    toc: Arc<TableOfContent>,
}

impl CollectionsInternalService {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self { toc }
    }
}

#[tonic::async_trait]
impl CollectionsInternal for CollectionsInternalService {
    async fn get(
        &self,
        request: Request<GetCollectionInfoRequestInternal>,
    ) -> Result<Response<GetCollectionInfoResponse>, Status> {
        validate_and_log(request.get_ref());
        let GetCollectionInfoRequestInternal {
            get_collection_info_request,
            shard_id,
        } = request.into_inner();

        let get_collection_info_request = get_collection_info_request
            .ok_or_else(|| Status::invalid_argument("GetCollectionInfoRequest is missing"))?;

        get(
            self.toc.as_ref(),
            get_collection_info_request,
            Some(shard_id),
        )
        .await
    }

    async fn initiate(
        &self,
        request: Request<InitiateShardTransferRequest>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        // TODO: Ensure cancel safety!

        validate_and_log(request.get_ref());
        let timing = Instant::now();
        let InitiateShardTransferRequest {
            collection_name,
            shard_id,
        } = request.into_inner();

        // TODO: Ensure cancel safety!
        self.toc
            .initiate_receiving_shard(collection_name, shard_id)
            .await
            .map_err(error_to_status)?;

        let response = CollectionOperationResponse {
            result: true,
            time: timing.elapsed().as_secs_f64(),
        };
        Ok(Response::new(response))
    }

    async fn wait_for_shard_state(
        &self,
        request: Request<WaitForShardStateRequest>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        let request = request.into_inner();
        validate_and_log(&request);

        let timing = Instant::now();
        let WaitForShardStateRequest {
            collection_name,
            shard_id,
            state,
            timeout,
        } = request;
        let state = state.try_into()?;
        let timeout = Duration::from_secs(timeout);

        let collection_read = self
            .toc
            .get_collection(&collection_name)
            .await
            .map_err(|err| {
                Status::not_found(format!(
                    "Collection {collection_name} could not be found: {err}"
                ))
            })?;

        // Wait for replica state
        collection_read
            .wait_local_shard_replica_state(shard_id, state, timeout)
            .await
            .map_err(|err| {
                Status::aborted(format!(
                    "Failed to wait for shard {shard_id} to get into {state:?} state: {err}"
                ))
            })?;

        let response = CollectionOperationResponse {
            result: true,
            time: timing.elapsed().as_secs_f64(),
        };
        Ok(Response::new(response))
    }

    async fn get_shard_recovery_point(
        &self,
        request: Request<GetShardRecoveryPointRequest>,
    ) -> Result<Response<GetShardRecoveryPointResponse>, Status> {
        validate_and_log(request.get_ref());

        let timing = Instant::now();
        let GetShardRecoveryPointRequest {
            collection_name,
            shard_id,
        } = request.into_inner();

        let collection_read = self
            .toc
            .get_collection(&collection_name)
            .await
            .map_err(|err| {
                Status::not_found(format!(
                    "Collection {collection_name} could not be found: {err}"
                ))
            })?;

        // Get shard recovery point
        let recovery_point = collection_read
            .shard_recovery_point(shard_id)
            .await
            .map_err(|err| {
                Status::internal(format!(
                    "Failed to get recovery point for shard {shard_id}: {err}"
                ))
            })?;

        let response = GetShardRecoveryPointResponse {
            recovery_point,
            time: timing.elapsed().as_secs_f64(),
        };
        Ok(Response::new(response))
    }
}
