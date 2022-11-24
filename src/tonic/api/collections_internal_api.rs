use std::sync::Arc;
use std::time::Instant;

use api::grpc::qdrant::collections_internal_server::CollectionsInternal;
use api::grpc::qdrant::{
    CollectionOperationResponse, GetCollectionInfoRequestInternal, GetCollectionInfoResponse,
    InitiateShardTransferRequest,
};
use storage::content_manager::conversions::error_to_status;
use storage::content_manager::toc::TableOfContent;
use tonic::{Request, Response, Status};

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
    #[tracing::instrument(skip(self))]
    async fn get(
        &self,
        request: Request<GetCollectionInfoRequestInternal>,
    ) -> Result<Response<GetCollectionInfoResponse>, Status> {
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

    #[tracing::instrument(skip(self))]
    async fn initiate(
        &self,
        request: Request<InitiateShardTransferRequest>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        let timing = Instant::now();
        let InitiateShardTransferRequest {
            collection_name,
            shard_id,
        } = request.into_inner();

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
}
