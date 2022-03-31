use tonic::{Request, Response, Status};

use crate::common::collections::*;
use api::grpc::qdrant::collections_server::Collections;
use api::grpc::qdrant::{
    ChangeAliases, CollectionOperationResponse, CreateCollection, DeleteCollection,
    GetCollectionInfoRequest, GetCollectionInfoResponse, ListCollectionsRequest,
    ListCollectionsResponse, UpdateCollection,
};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Instant;

use storage::content_manager::conversions::error_to_status;
use storage::content_manager::toc::TableOfContent;

pub struct CollectionsService {
    toc: Arc<TableOfContent>,
}

impl CollectionsService {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self { toc }
    }
}

#[tonic::async_trait]
impl Collections for CollectionsService {
    async fn get(
        &self,
        request: Request<GetCollectionInfoRequest>,
    ) -> Result<Response<GetCollectionInfoResponse>, Status> {
        let timing = Instant::now();
        let collection_name = request.into_inner().collection_name;
        let result = do_get_collection(&self.toc, &collection_name)
            .await
            .map_err(error_to_status)?;
        let response = GetCollectionInfoResponse {
            result: Some(result.into()),
            time: timing.elapsed().as_secs_f64(),
        };

        Ok(Response::new(response))
    }

    async fn list(
        &self,
        _request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        let timing = Instant::now();
        let result = do_list_collections(&self.toc).await;

        let response = ListCollectionsResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn create(
        &self,
        request: Request<CreateCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        let operations =
            storage::content_manager::collection_meta_ops::CollectionMetaOperations::try_from(
                request.into_inner(),
            )?;
        let timing = Instant::now();
        let result = self
            .toc
            .submit_collection_operation(operations)
            .await
            .map_err(error_to_status)?;

        let response = CollectionOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn update(
        &self,
        request: Request<UpdateCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        let operations =
            storage::content_manager::collection_meta_ops::CollectionMetaOperations::from(
                request.into_inner(),
            );
        let timing = Instant::now();
        let result = self
            .toc
            .submit_collection_operation(operations)
            .await
            .map_err(error_to_status)?;

        let response = CollectionOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn delete(
        &self,
        request: Request<DeleteCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        let operations =
            storage::content_manager::collection_meta_ops::CollectionMetaOperations::from(
                request.into_inner(),
            );
        let timing = Instant::now();
        let result = self
            .toc
            .submit_collection_operation(operations)
            .await
            .map_err(error_to_status)?;

        let response = CollectionOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn update_aliases(
        &self,
        request: Request<ChangeAliases>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        let operations =
            storage::content_manager::collection_meta_ops::CollectionMetaOperations::try_from(
                request.into_inner(),
            )?;
        let timing = Instant::now();
        let result = self
            .toc
            .submit_collection_operation(operations)
            .await
            .map_err(error_to_status)?;

        let response = CollectionOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }
}
