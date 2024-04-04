use std::sync::Arc;
use std::time::Instant;

use api::grpc::qdrant::shard_snapshots_server::ShardSnapshots;
use api::grpc::qdrant::snapshots_server::Snapshots;
use api::grpc::qdrant::{
    CreateFullSnapshotRequest, CreateShardSnapshotRequest, CreateSnapshotRequest,
    CreateSnapshotResponse, DeleteFullSnapshotRequest, DeleteShardSnapshotRequest,
    DeleteSnapshotRequest, DeleteSnapshotResponse, ListFullSnapshotsRequest,
    ListShardSnapshotsRequest, ListSnapshotsRequest, ListSnapshotsResponse,
    RecoverShardSnapshotRequest, RecoverSnapshotResponse,
};
use storage::content_manager::conversions::error_to_status;
use storage::content_manager::snapshots::{
    do_create_full_snapshot, do_delete_collection_snapshot, do_delete_full_snapshot,
    do_list_full_snapshots,
};
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use tonic::{async_trait, Request, Response, Status};

use super::{validate, validate_and_log};
use crate::common;
use crate::common::collections::{do_create_snapshot, do_list_snapshots};
use crate::common::http_client::HttpClient;
use crate::tonic::auth::extract_access;

pub struct SnapshotsService {
    dispatcher: Arc<Dispatcher>,
}

impl SnapshotsService {
    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        Self { dispatcher }
    }
}

#[async_trait]
impl Snapshots for SnapshotsService {
    async fn create(
        &self,
        mut request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let collection_name = request.into_inner().collection_name;
        let timing = Instant::now();
        let dispatcher = self.dispatcher.clone();
        let response = async move {
            do_create_snapshot(
                Arc::clone(dispatcher.toc(&access)),
                access,
                &collection_name,
            )?
            .await?
        }
        .await
        .map_err(error_to_status)?;
        Ok(Response::new(CreateSnapshotResponse {
            snapshot_description: Some(response.into()),
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn list(
        &self,
        mut request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let ListSnapshotsRequest { collection_name } = request.into_inner();

        let timing = Instant::now();
        let snapshots = do_list_snapshots(self.dispatcher.toc(&access), access, &collection_name)
            .await
            .map_err(error_to_status)?;
        Ok(Response::new(ListSnapshotsResponse {
            snapshot_descriptions: snapshots.into_iter().map(|s| s.into()).collect(),
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn delete(
        &self,
        mut request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let DeleteSnapshotRequest {
            collection_name,
            snapshot_name,
        } = request.into_inner();
        let timing = Instant::now();
        let _response = async move {
            do_delete_collection_snapshot(
                &self.dispatcher,
                access,
                &collection_name,
                &snapshot_name,
            )
            .await?
            .await?
        }
        .await
        .map_err(error_to_status)?;
        Ok(Response::new(DeleteSnapshotResponse {
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn create_full(
        &self,
        mut request: Request<CreateFullSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        validate(request.get_ref())?;
        let timing = Instant::now();
        let access = extract_access(&mut request);
        let response = async move { do_create_full_snapshot(&self.dispatcher, access)?.await? }
            .await
            .map_err(error_to_status)?;

        Ok(Response::new(CreateSnapshotResponse {
            snapshot_description: Some(response.into()),
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn list_full(
        &self,
        mut request: Request<ListFullSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        validate(request.get_ref())?;
        let timing = Instant::now();
        let access = extract_access(&mut request);
        let snapshots = do_list_full_snapshots(self.dispatcher.toc(&access), access)
            .await
            .map_err(error_to_status)?;
        Ok(Response::new(ListSnapshotsResponse {
            snapshot_descriptions: snapshots.into_iter().map(|s| s.into()).collect(),
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn delete_full(
        &self,
        mut request: Request<DeleteFullSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        validate(request.get_ref())?;
        let access = extract_access(&mut request);
        let snapshot_name = request.into_inner().snapshot_name;
        let timing = Instant::now();
        let _response = async move {
            Ok(
                do_delete_full_snapshot(&self.dispatcher, access, &snapshot_name)
                    .await?
                    .await?,
            )
        }
        .await
        .map_err(error_to_status)?;
        Ok(Response::new(DeleteSnapshotResponse {
            time: timing.elapsed().as_secs_f64(),
        }))
    }
}

pub struct ShardSnapshotsService {
    toc: Arc<TableOfContent>,
    http_client: HttpClient,
}

impl ShardSnapshotsService {
    pub fn new(toc: Arc<TableOfContent>, http_client: HttpClient) -> Self {
        Self { toc, http_client }
    }
}

#[async_trait]
impl ShardSnapshots for ShardSnapshotsService {
    async fn create(
        &self,
        mut request: Request<CreateShardSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let access = extract_access(&mut request);
        let request = request.into_inner();
        validate_and_log(&request);

        let timing = Instant::now();

        let snapshot_description = common::snapshots::create_shard_snapshot(
            self.toc.clone(),
            access,
            request.collection_name,
            request.shard_id,
        )
        .await
        .map_err(error_to_status)?;

        Ok(Response::new(CreateSnapshotResponse {
            snapshot_description: Some(snapshot_description.into()),
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn list(
        &self,
        mut request: Request<ListShardSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let access = extract_access(&mut request);
        let request = request.into_inner();
        validate_and_log(&request);

        let timing = Instant::now();

        let snapshot_descriptions = common::snapshots::list_shard_snapshots(
            self.toc.clone(),
            access,
            request.collection_name,
            request.shard_id,
        )
        .await
        .map_err(error_to_status)?;

        Ok(Response::new(ListSnapshotsResponse {
            snapshot_descriptions: snapshot_descriptions.into_iter().map(Into::into).collect(),
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn delete(
        &self,
        mut request: Request<DeleteShardSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let access = extract_access(&mut request);
        let request = request.into_inner();
        validate_and_log(&request);

        let timing = Instant::now();

        common::snapshots::delete_shard_snapshot(
            self.toc.clone(),
            access,
            request.collection_name,
            request.shard_id,
            request.snapshot_name,
        )
        .await
        .map_err(error_to_status)?;

        Ok(Response::new(DeleteSnapshotResponse {
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn recover(
        &self,
        mut request: Request<RecoverShardSnapshotRequest>,
    ) -> Result<Response<RecoverSnapshotResponse>, Status> {
        let access = extract_access(&mut request);
        let request = request.into_inner();
        validate_and_log(&request);

        let timing = Instant::now();

        common::snapshots::recover_shard_snapshot(
            self.toc.clone(),
            access,
            request.collection_name,
            request.shard_id,
            request.snapshot_location.try_into()?,
            request.snapshot_priority.try_into()?,
            request.checksum,
            self.http_client.clone(),
        )
        .await
        .map_err(error_to_status)?;

        Ok(Response::new(RecoverSnapshotResponse {
            time: timing.elapsed().as_secs_f64(),
        }))
    }
}
