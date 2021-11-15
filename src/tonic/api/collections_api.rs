use tonic::{Request, Response, Status};

use crate::common::collections::*;
use crate::common::models::CollectionsResponse;
use crate::tonic::proto::collections_server::Collections;
use crate::tonic::proto::{
    CollectionDescription, CollectionOperationResponse, CreateCollection, DeleteCollection,
    GetCollectionsRequest, GetCollectionsResponse, HnswConfigDiff, OptimizersConfigDiff,
    UpdateCollection, WalConfigDiff,
};
use num_traits::FromPrimitive;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Instant;
use storage::content_manager::errors::StorageError;
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
        _request: Request<GetCollectionsRequest>,
    ) -> Result<Response<GetCollectionsResponse>, Status> {
        let timing = Instant::now();
        let result = do_get_collections(&self.toc).await;

        let response = GetCollectionsResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn create(
        &self,
        request: Request<CreateCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        let operations = storage::content_manager::storage_ops::StorageOperations::try_from(
            request.into_inner(),
        )?;
        let timing = Instant::now();
        let result = self.toc.perform_collection_operation(operations).await;

        let response = CollectionOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn update(
        &self,
        request: Request<UpdateCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        let operations =
            storage::content_manager::storage_ops::StorageOperations::from(request.into_inner());
        let timing = Instant::now();
        let result = self.toc.perform_collection_operation(operations).await;

        let response = CollectionOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn delete(
        &self,
        request: Request<DeleteCollection>,
    ) -> Result<Response<CollectionOperationResponse>, Status> {
        let operations =
            storage::content_manager::storage_ops::StorageOperations::from(request.into_inner());
        let timing = Instant::now();
        let result = self.toc.perform_collection_operation(operations).await;

        let response = CollectionOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }
}

impl From<(Instant, CollectionsResponse)> for GetCollectionsResponse {
    fn from(value: (Instant, CollectionsResponse)) -> Self {
        let (timing, response) = value;
        let collections = response
            .collections
            .into_iter()
            .map(|desc| CollectionDescription { name: desc.name })
            .collect::<Vec<_>>();
        Self {
            collections,
            time: timing.elapsed().as_secs_f64(),
        }
    }
}

impl TryFrom<CreateCollection> for storage::content_manager::storage_ops::StorageOperations {
    type Error = Status;

    fn try_from(value: CreateCollection) -> Result<Self, Self::Error> {
        if let Some(distance) = FromPrimitive::from_i32(value.distance) {
            Ok(Self::CreateCollection {
                name: value.name,
                vector_size: value.vector_size as usize,
                distance,
                hnsw_config: value.hnsw_config.map(|v| v.into()),
                wal_config: value.wal_config.map(|v| v.into()),
                optimizers_config: value.optimizers_config.map(|v| v.into()),
            })
        } else {
            Err(Status::failed_precondition("Bad value of distance field!"))
        }
    }
}

impl From<HnswConfigDiff> for collection::operations::config_diff::HnswConfigDiff {
    fn from(value: HnswConfigDiff) -> Self {
        Self {
            m: value.m.map(|v| v as usize),
            ef_construct: value.ef_construct.map(|v| v as usize),
            full_scan_threshold: value.full_scan_threshold.map(|v| v as usize),
        }
    }
}

impl From<WalConfigDiff> for collection::operations::config_diff::WalConfigDiff {
    fn from(value: WalConfigDiff) -> Self {
        Self {
            wal_capacity_mb: value.wal_capacity_mb.map(|v| v as usize),
            wal_segments_ahead: value.wal_segments_ahead.map(|v| v as usize),
        }
    }
}

impl From<OptimizersConfigDiff> for collection::operations::config_diff::OptimizersConfigDiff {
    fn from(value: OptimizersConfigDiff) -> Self {
        Self {
            deleted_threshold: value.deleted_threshold,
            vacuum_min_vector_number: value.vacuum_min_vector_number.map(|v| v as usize),
            max_segment_number: value.max_segment_number.map(|v| v as usize),
            memmap_threshold: value.memmap_threshold.map(|v| v as usize),
            indexing_threshold: value.indexing_threshold.map(|v| v as usize),
            payload_indexing_threshold: value.payload_indexing_threshold.map(|v| v as usize),
            flush_interval_sec: value.flush_interval_sec,
        }
    }
}

impl From<(Instant, Result<bool, StorageError>)> for CollectionOperationResponse {
    fn from(value: (Instant, Result<bool, StorageError>)) -> Self {
        let (timing, response) = value;
        match response {
            Ok(res) => CollectionOperationResponse {
                result: Some(res),
                error: None,
                time: timing.elapsed().as_secs_f64(),
            },
            Err(err) => {
                let error_description = match err {
                    StorageError::BadInput { description } => description,
                    StorageError::NotFound { description } => description,
                    StorageError::ServiceError { description } => description,
                    StorageError::BadRequest { description } => description,
                };
                CollectionOperationResponse {
                    result: None,
                    error: Some(error_description),
                    time: timing.elapsed().as_secs_f64(),
                }
            }
        }
    }
}

impl From<UpdateCollection> for storage::content_manager::storage_ops::StorageOperations {
    fn from(value: UpdateCollection) -> Self {
        Self::UpdateCollection {
            name: value.name,
            optimizers_config: value.optimizers_config.map(|v| v.into()),
        }
    }
}

impl From<DeleteCollection> for storage::content_manager::storage_ops::StorageOperations {
    fn from(value: DeleteCollection) -> Self {
        Self::DeleteCollection(value.name)
    }
}
