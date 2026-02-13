use std::sync::Arc;
use std::time::{Duration, Instant};

use api::grpc::qdrant::collections_internal_server::CollectionsInternal;
use api::grpc::qdrant::{
    CollectionOperationResponse, GetCollectionInfoRequestInternal, GetCollectionInfoResponse,
    GetShardOptimizationsRequest, GetShardOptimizationsResponse, GetShardRecoveryPointRequest,
    GetShardRecoveryPointResponse, InitiateShardTransferRequest, UpdateShardCutoffPointRequest,
    WaitForShardStateRequest,
};
use collection::operations::types::OptimizationsRequestOptions;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::{Access, AccessRequirements, Auth, AuthType, CollectionPass};
use tonic::{Request, Response, Status};

use super::validate_and_log;
use crate::tonic::api::collections_common::get;

fn full_internal_auth() -> Auth {
    Auth::new(Access::full("Internal API"), None, None, AuthType::Internal)
}

fn full_access_pass(collection_name: &str) -> Result<CollectionPass<'_>, Status> {
    full_internal_auth()
        .check_collection_access(
            collection_name,
            AccessRequirements::new(),
            "internal_collection_access",
        )
        .map_err(Status::from)
}

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

        let auth = full_internal_auth();
        get(
            self.toc.as_ref(),
            get_collection_info_request,
            &auth,
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
            .await?;

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
            .get_collection(&full_access_pass(&collection_name)?)
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
            .get_collection(&full_access_pass(&collection_name)?)
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
            recovery_point: Some(recovery_point.into()),
            time: timing.elapsed().as_secs_f64(),
        };
        Ok(Response::new(response))
    }

    async fn update_shard_cutoff_point(
        &self,
        request: Request<UpdateShardCutoffPointRequest>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        validate_and_log(request.get_ref());

        let timing = Instant::now();
        let UpdateShardCutoffPointRequest {
            collection_name,
            shard_id,
            cutoff,
        } = request.into_inner();

        let cutoff = cutoff.ok_or_else(|| Status::invalid_argument("Missing cutoff point"))?;

        let collection_read = self
            .toc
            .get_collection(&full_access_pass(&collection_name)?)
            .await
            .map_err(|err| {
                Status::not_found(format!(
                    "Collection {collection_name} could not be found: {err}"
                ))
            })?;

        // Set the shard cutoff point
        collection_read
            .update_shard_cutoff_point(shard_id, &cutoff.try_into()?)
            .await
            .map_err(|err| {
                Status::internal(format!(
                    "Failed to set shard cutoff point for shard {shard_id}: {err}"
                ))
            })?;

        let response = CollectionOperationResponse {
            result: true,
            time: timing.elapsed().as_secs_f64(),
        };
        Ok(Response::new(response))
    }

    async fn get_shard_optimizations(
        &self,
        request: Request<GetShardOptimizationsRequest>,
    ) -> Result<Response<GetShardOptimizationsResponse>, Status> {
        validate_and_log(request.get_ref());

        let timing = Instant::now();
        let GetShardOptimizationsRequest {
            collection_name,
            shard_id,
            with_queued,
            completed_limit,
            with_idle_segments,
        } = request.into_inner();

        let options = OptimizationsRequestOptions {
            queued: with_queued,
            completed_limit: completed_limit.map(|l| l as usize),
            idle_segments: with_idle_segments,
        };

        let collection_read = self
            .toc
            .get_collection(&full_access_pass(&collection_name)?)
            .await
            .map_err(|err| {
                Status::not_found(format!(
                    "Collection {collection_name} could not be found: {err}"
                ))
            })?;

        let optimizations_response = collection_read
            .local_shard_optimizations(shard_id, options)
            .await
            .map_err(|err| {
                Status::internal(format!(
                    "Failed to get optimizations for shard {shard_id}: {err}"
                ))
            })?;

        let optimizations_json = serde_json::to_vec(&optimizations_response).map_err(|err| {
            Status::internal(format!("Failed to serialize optimizations response: {err}"))
        })?;

        let response = GetShardOptimizationsResponse {
            optimizations_json,
            time: timing.elapsed().as_secs_f64(),
        };
        Ok(Response::new(response))
    }
}
