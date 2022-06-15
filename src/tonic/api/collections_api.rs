use storage::Dispatcher;
use tonic::{Request, Response, Status};

use crate::common::collections::*;
use api::grpc::qdrant::collections_server::Collections;
use api::grpc::qdrant::{
    ChangeAliases, CollectionOperationResponse, CreateCollection, DeleteCollection,
    GetCollectionInfoRequest, GetCollectionInfoResponse, ListCollectionsRequest,
    ListCollectionsResponse, UpdateCollection,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::tonic::api::collections_common::get;
use storage::content_manager::conversions::error_to_status;

pub struct CollectionsService {
    dispatcher: Arc<Dispatcher>,
}

impl CollectionsService {
    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        Self { dispatcher }
    }

    async fn perform_operation<O>(
        &self,
        request: Request<O>,
    ) -> Result<Response<CollectionOperationResponse>, Status>
    where
        O: WithTimeout
            + TryInto<
                storage::content_manager::collection_meta_ops::CollectionMetaOperations,
                Error = Status,
            >,
    {
        let operation = request.into_inner();
        let wait_timeout = operation.wait_timeout();
        let timing = Instant::now();
        let result = self
            .dispatcher
            .submit_collection_meta_op(operation.try_into()?, wait_timeout)
            .await
            .map_err(error_to_status)?;

        let response = CollectionOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }
}

#[tonic::async_trait]
impl Collections for CollectionsService {
    async fn get(
        &self,
        request: Request<GetCollectionInfoRequest>,
    ) -> Result<Response<GetCollectionInfoResponse>, Status> {
        get(self.dispatcher.as_ref(), request.into_inner(), None).await
    }

    async fn list(
        &self,
        _request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        let timing = Instant::now();
        let result = do_list_collections(&self.dispatcher).await;

        let response = ListCollectionsResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn create(
        &self,
        request: Request<CreateCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        self.perform_operation(request).await
    }

    async fn update(
        &self,
        request: Request<UpdateCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        self.perform_operation(request).await
    }

    async fn delete(
        &self,
        request: Request<DeleteCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        self.perform_operation(request).await
    }

    async fn update_aliases(
        &self,
        request: Request<ChangeAliases>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        self.perform_operation(request).await
    }
}

trait WithTimeout {
    fn wait_timeout(&self) -> Option<Duration>;
}

macro_rules! impl_with_timeout {
    ($operation:ty) => {
        impl WithTimeout for $operation {
            fn wait_timeout(&self) -> Option<Duration> {
                self.timeout.map(Duration::from_secs)
            }
        }
    };
}

impl_with_timeout!(CreateCollection);
impl_with_timeout!(UpdateCollection);
impl_with_timeout!(DeleteCollection);
impl_with_timeout!(ChangeAliases);
