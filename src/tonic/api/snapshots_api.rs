use std::sync::Arc;
use std::time::Instant;

use ::common::ambient::AmbientFutureExt;
use ::common::reason::reason;
use api::grpc::qdrant::shard_snapshots_server::ShardSnapshots;
use api::grpc::qdrant::snapshots_server::Snapshots;
use api::grpc::qdrant::{
    CreateFullSnapshotRequest, CreateShardSnapshotRequest, CreateSnapshotRequest,
    CreateSnapshotResponse, DeleteFullSnapshotRequest, DeleteShardSnapshotRequest,
    DeleteSnapshotRequest, DeleteSnapshotResponse, ListFullSnapshotsRequest,
    ListShardSnapshotsRequest, ListSnapshotsRequest, ListSnapshotsResponse,
    RecoverShardSnapshotRequest, RecoverSnapshotResponse,
};
use collection::operations::snapshot_ops::{
    ShardSnapshotLocation, SnapshotDescription, SnapshotPriority,
};
use collection::operations::verification::new_unchecked_verification_pass;
use storage::content_manager::errors::StorageError;
use storage::content_manager::snapshots::{
    do_create_full_snapshot, do_delete_collection_snapshot, do_delete_full_snapshot,
    do_list_full_snapshots,
};
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use tonic::{Request, Response, Status, async_trait};

use super::{validate, validate_and_log};
use crate::common;
use crate::common::collections::{do_create_snapshot, do_list_snapshots};
use crate::common::http_client::HttpClient;
use crate::settings::ServiceConfig;
use crate::tonic::auth::extract_auth;

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
        let auth = extract_auth(&mut request);
        let collection_name = request.into_inner().collection_name;
        let timing = Instant::now();
        let dispatcher = self.dispatcher.clone();

        // Nothing to verify here.
        let pass = new_unchecked_verification_pass();

        let response = do_create_snapshot(
            Arc::clone(dispatcher.toc(&auth, &pass)),
            &auth,
            &collection_name,
        )
        .await?;

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

        let timing = Instant::now();
        let auth = extract_auth(&mut request);
        let ListSnapshotsRequest { collection_name } = request.into_inner();

        // Nothing to verify here.
        let pass = new_unchecked_verification_pass();

        let snapshots =
            do_list_snapshots(self.dispatcher.toc(&auth, &pass), &auth, &collection_name).await?;

        Ok(Response::new(ListSnapshotsResponse {
            snapshot_descriptions: snapshots
                .into_iter()
                .map(SnapshotDescription::into)
                .collect(),
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn delete(
        &self,
        mut request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        validate(request.get_ref())?;

        let timing = Instant::now();
        let auth = extract_auth(&mut request);
        let DeleteSnapshotRequest {
            collection_name,
            snapshot_name,
        } = request.into_inner();

        let _response =
            do_delete_collection_snapshot(&self.dispatcher, auth, &collection_name, &snapshot_name)
                .await?;

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
        let auth = extract_auth(&mut request);

        let response = do_create_full_snapshot(&self.dispatcher, auth.clone()).await?;

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
        let auth = extract_auth(&mut request);

        // Nothing to verify here.
        let pass = new_unchecked_verification_pass();

        let snapshots = do_list_full_snapshots(self.dispatcher.toc(&auth, &pass), auth).await?;
        Ok(Response::new(ListSnapshotsResponse {
            snapshot_descriptions: snapshots
                .into_iter()
                .map(SnapshotDescription::into)
                .collect(),
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn delete_full(
        &self,
        mut request: Request<DeleteFullSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        validate(request.get_ref())?;

        let timing = Instant::now();
        let auth = extract_auth(&mut request);
        let snapshot_name = request.into_inner().snapshot_name;

        let _response = do_delete_full_snapshot(&self.dispatcher, auth, &snapshot_name).await?;

        Ok(Response::new(DeleteSnapshotResponse {
            time: timing.elapsed().as_secs_f64(),
        }))
    }
}

pub struct ShardSnapshotsService {
    toc: Arc<TableOfContent>,
    http_client: HttpClient,
    service_config: ServiceConfig,
}

impl ShardSnapshotsService {
    pub fn new(
        toc: Arc<TableOfContent>,
        http_client: HttpClient,
        service_config: ServiceConfig,
    ) -> Self {
        Self {
            toc,
            http_client,
            service_config,
        }
    }
}

#[async_trait]
impl ShardSnapshots for ShardSnapshotsService {
    async fn create(
        &self,
        mut request: Request<CreateShardSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let auth = extract_auth(&mut request);
        let request = request.into_inner();
        validate_and_log(&request);

        let timing = Instant::now();

        let snapshot_description = common::snapshots::create_shard_snapshot(
            self.toc.clone(),
            &auth,
            request.collection_name,
            request.shard_id,
        )
        .await?;

        Ok(Response::new(CreateSnapshotResponse {
            snapshot_description: Some(snapshot_description.into()),
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn list(
        &self,
        mut request: Request<ListShardSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let auth = extract_auth(&mut request);
        let request = request.into_inner();
        validate_and_log(&request);

        let timing = Instant::now();

        let snapshot_descriptions = common::snapshots::list_shard_snapshots(
            self.toc.clone(),
            &auth,
            request.collection_name,
            request.shard_id,
        )
        .await?;

        Ok(Response::new(ListSnapshotsResponse {
            snapshot_descriptions: snapshot_descriptions
                .into_iter()
                .map(SnapshotDescription::into)
                .collect(),
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn delete(
        &self,
        mut request: Request<DeleteShardSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let auth = extract_auth(&mut request);
        let request = request.into_inner();
        validate_and_log(&request);

        let timing = Instant::now();

        common::snapshots::delete_shard_snapshot(
            self.toc.clone(),
            &auth,
            request.collection_name,
            request.shard_id,
            request.snapshot_name,
        )
        .await?;

        Ok(Response::new(DeleteSnapshotResponse {
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn recover(
        &self,
        mut request: Request<RecoverShardSnapshotRequest>,
    ) -> Result<Response<RecoverSnapshotResponse>, Status> {
        let auth = extract_auth(&mut request);
        let request = request.into_inner();

        validate_and_log(&request);

        let RecoverShardSnapshotRequest {
            collection_name,
            shard_id,
            snapshot_location,
            snapshot_priority,
            checksum,
            api_key,
            from_peer_id,
        } = request;

        let timing = Instant::now();

        let snapshot_location = ShardSnapshotLocation::try_from(snapshot_location)?;
        let snapshot_priority = SnapshotPriority::try_from(snapshot_priority)?;

        // In a snapshot shard transfer, the receiver downloads the snapshot from the REST API of
        // the source peer. If URL recovery is disabled, allow only that.
        if !self.service_config.enable_snapshot_url_recovery
            && let ShardSnapshotLocation::Url(url) = &snapshot_location
        {
            let is_shard_transfer = match snapshot_priority {
                SnapshotPriority::ShardTransfer => true,
                SnapshotPriority::NoSync
                | SnapshotPriority::Snapshot
                | SnapshotPriority::Replica => false,
            };

            if !is_shard_transfer
                || !self
                    .toc
                    .get_channel_service()
                    .is_peer_host(url, from_peer_id)
            {
                return Err(Status::from(StorageError::forbidden(
                    "Snapshot recovery from remote URLs is disabled in the configuration, \
                     except for shard transfers from cluster peers",
                )));
            }
        }

        common::snapshots::recover_shard_snapshot(
            self.toc.clone(),
            &auth,
            collection_name,
            shard_id,
            snapshot_location,
            snapshot_priority,
            checksum,
            self.http_client.clone(),
            api_key,
            from_peer_id,
        )
        .unmeasured(reason("Snapshot recovery isn't measured"))
        .await?;

        Ok(Response::new(RecoverSnapshotResponse {
            time: timing.elapsed().as_secs_f64(),
        }))
    }
}
