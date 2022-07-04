use crate::common::collections::{do_create_snapshot, do_list_snapshots};
use api::grpc::qdrant::snapshots_server::Snapshots;
use api::grpc::qdrant::{
    CreateSnapshotRequest, CreateSnapshotResponse, ListSnapshotsRequest, ListSnapshotsResponse,
};
use std::sync::Arc;
use std::time::Instant;
use storage::content_manager::conversions::error_to_status;
use storage::content_manager::toc::TableOfContent;
use tonic::{async_trait, Request, Response, Status};

pub struct SnapshotsService {
    toc: Arc<TableOfContent>,
}

impl SnapshotsService {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self { toc }
    }
}

#[async_trait]
impl Snapshots for SnapshotsService {
    async fn create(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let collection_name = request.into_inner().collection_name;
        let timing = Instant::now();
        let response = do_create_snapshot(&self.toc, &collection_name)
            .await
            .map_err(error_to_status)?;
        Ok(Response::new(CreateSnapshotResponse {
            snapshot_description: Some(response.into()),
            time: timing.elapsed().as_secs_f64(),
        }))
    }

    async fn list(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let collection_name = request.into_inner().collection_name;

        let timing = Instant::now();
        let snapshots = do_list_snapshots(&self.toc, &collection_name)
            .await
            .map_err(error_to_status)?;
        Ok(Response::new(ListSnapshotsResponse {
            snapshot_descriptions: snapshots.into_iter().map(|s| s.into()).collect(),
            time: timing.elapsed().as_secs_f64(),
        }))
    }
}
