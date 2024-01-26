use std::sync::Arc;
use std::time::{Duration, Instant};

use api::grpc::qdrant::collections_server::Collections;
use api::grpc::qdrant::{
    AliasDescription, ChangeAliases, CollectionClusterInfoRequest, CollectionClusterInfoResponse,
    CollectionExistsRequest, CollectionExistsResponse, CollectionOperationResponse,
    CreateCollection, CreateShardKeyRequest, CreateShardKeyResponse, DeleteCollection,
    DeleteShardKeyRequest, DeleteShardKeyResponse, GetCollectionInfoRequest,
    GetCollectionInfoResponse, ListAliasesRequest, ListAliasesResponse,
    ListCollectionAliasesRequest, ListCollectionsRequest, ListCollectionsResponse,
    UpdateCollection, UpdateCollectionClusterSetupRequest, UpdateCollectionClusterSetupResponse,
};
use collection::operations::cluster_ops::{
    ClusterOperations, CreateShardingKeyOperation, DropShardingKeyOperation,
};
use storage::content_manager::conversions::error_to_status;
use storage::dispatcher::Dispatcher;
use tonic::{Request, Response, Status};

use super::validate;
use crate::common::collections::*;
use crate::tonic::api::collections_common::get;

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
        let timing = Instant::now();
        let operation = request.into_inner();
        let wait_timeout = operation.wait_timeout();
        let result = self
            .dispatcher
            .submit_collection_meta_op(operation.try_into()?, wait_timeout)
            .await
            .map_err(error_to_status)?;

        let response = CollectionOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn list_aliases(
        &self,
        _request: Request<ListAliasesRequest>,
    ) -> Result<Response<ListAliasesResponse>, Status> {
        let timing = Instant::now();
        let aliases = self
            .dispatcher
            .toc()
            .list_aliases()
            .await
            .map(|aliases| aliases.into_iter().map(|alias| alias.into()).collect())
            .map_err(error_to_status)?;
        let response = ListAliasesResponse {
            aliases,
            time: timing.elapsed().as_secs_f64(),
        };
        Ok(Response::new(response))
    }

    async fn list_collection_aliases(
        &self,
        request: Request<ListCollectionAliasesRequest>,
    ) -> Result<Response<ListAliasesResponse>, Status> {
        let timing = Instant::now();
        let ListCollectionAliasesRequest { collection_name } = request.into_inner();
        let aliases = self
            .dispatcher
            .toc()
            .collection_aliases(&collection_name)
            .await
            .map(|aliases| {
                aliases
                    .into_iter()
                    .map(|alias| AliasDescription {
                        alias_name: alias,
                        collection_name: collection_name.clone(),
                    })
                    .collect()
            })
            .map_err(error_to_status)?;
        let response = ListAliasesResponse {
            aliases,
            time: timing.elapsed().as_secs_f64(),
        };
        Ok(Response::new(response))
    }
}

#[tonic::async_trait]
impl Collections for CollectionsService {
    async fn get(
        &self,
        request: Request<GetCollectionInfoRequest>,
    ) -> Result<Response<GetCollectionInfoResponse>, Status> {
        validate(request.get_ref())?;
        get(self.dispatcher.as_ref(), request.into_inner(), None).await
    }

    async fn list(
        &self,
        request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        validate(request.get_ref())?;
        let timing = Instant::now();
        let result = do_list_collections(&self.dispatcher).await;

        let response = ListCollectionsResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn create(
        &self,
        request: Request<CreateCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        validate(request.get_ref())?;
        self.perform_operation(request).await
    }

    async fn update(
        &self,
        request: Request<UpdateCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        validate(request.get_ref())?;
        self.perform_operation(request).await
    }

    async fn delete(
        &self,
        request: Request<DeleteCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        validate(request.get_ref())?;
        self.perform_operation(request).await
    }

    async fn update_aliases(
        &self,
        request: Request<ChangeAliases>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        validate(request.get_ref())?;
        self.perform_operation(request).await
    }

    async fn list_collection_aliases(
        &self,
        request: Request<ListCollectionAliasesRequest>,
    ) -> Result<Response<ListAliasesResponse>, Status> {
        validate(request.get_ref())?;
        self.list_collection_aliases(request).await
    }

    async fn list_aliases(
        &self,
        request: Request<ListAliasesRequest>,
    ) -> Result<Response<ListAliasesResponse>, Status> {
        validate(request.get_ref())?;
        self.list_aliases(request).await
    }

    async fn collection_exists(
        &self,
        request: Request<CollectionExistsRequest>,
    ) -> Result<Response<CollectionExistsResponse>, Status> {
        validate(request.get_ref())?;
        let CollectionExistsRequest { collection_name } = request.into_inner();
        let response = do_collection_exists(self.dispatcher.toc(), &collection_name)
            .await
            .map_err(error_to_status)?;

        Ok(Response::new(response))
    }

    async fn collection_cluster_info(
        &self,
        request: Request<CollectionClusterInfoRequest>,
    ) -> Result<Response<CollectionClusterInfoResponse>, Status> {
        validate(request.get_ref())?;
        let response = do_get_collection_cluster(
            self.dispatcher.toc(),
            request.into_inner().collection_name.as_str(),
        )
        .await
        .map_err(error_to_status)?
        .into();

        Ok(Response::new(response))
    }

    async fn update_collection_cluster_setup(
        &self,
        request: Request<UpdateCollectionClusterSetupRequest>,
    ) -> Result<Response<UpdateCollectionClusterSetupResponse>, Status> {
        validate(request.get_ref())?;
        let UpdateCollectionClusterSetupRequest {
            collection_name,
            operation,
            timeout,
            ..
        } = request.into_inner();
        let result = do_update_collection_cluster(
            self.dispatcher.as_ref(),
            collection_name,
            operation
                .ok_or(Status::new(tonic::Code::InvalidArgument, "empty operation"))?
                .try_into()?,
            timeout.map(std::time::Duration::from_secs),
        )
        .await
        .map_err(error_to_status)?;
        Ok(Response::new(UpdateCollectionClusterSetupResponse {
            result,
        }))
    }

    async fn create_shard_key(
        &self,
        request: Request<CreateShardKeyRequest>,
    ) -> Result<Response<CreateShardKeyResponse>, Status> {
        let CreateShardKeyRequest {
            collection_name,
            request,
            timeout,
        } = request.into_inner();

        let Some(request) = request else {
            return Err(Status::new(tonic::Code::InvalidArgument, "empty request"));
        };

        let timeout = timeout.map(std::time::Duration::from_secs);

        let operation = ClusterOperations::CreateShardingKey(CreateShardingKeyOperation {
            create_sharding_key: request.try_into()?,
        });

        let result = do_update_collection_cluster(
            self.dispatcher.as_ref(),
            collection_name,
            operation,
            timeout,
        )
        .await
        .map_err(error_to_status)?;

        Ok(Response::new(CreateShardKeyResponse { result }))
    }

    async fn delete_shard_key(
        &self,
        request: Request<DeleteShardKeyRequest>,
    ) -> Result<Response<DeleteShardKeyResponse>, Status> {
        let DeleteShardKeyRequest {
            collection_name,
            request,
            timeout,
        } = request.into_inner();

        let Some(request) = request else {
            return Err(Status::new(tonic::Code::InvalidArgument, "empty request"));
        };

        let timeout = timeout.map(std::time::Duration::from_secs);

        let operation = ClusterOperations::DropShardingKey(DropShardingKeyOperation {
            drop_sharding_key: request.try_into()?,
        });

        let result = do_update_collection_cluster(
            self.dispatcher.as_ref(),
            collection_name,
            operation,
            timeout,
        )
        .await
        .map_err(error_to_status)?;

        Ok(Response::new(DeleteShardKeyResponse { result }))
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
impl_with_timeout!(UpdateCollectionClusterSetupRequest);
