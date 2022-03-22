use tonic::{Request, Response, Status};

use crate::common::collections::*;
use crate::common::models::CollectionsResponse;
use crate::tonic::api::common::error_to_status;
use crate::tonic::qdrant::collections_server::Collections;
use crate::tonic::qdrant::{
    CollectionConfig, CollectionDescription, CollectionInfo, CollectionOperationResponse,
    CollectionParams, CollectionStatus, CreateCollection, DeleteCollection, Distance,
    GetCollectionInfoRequest, GetCollectionInfoResponse, HnswConfigDiff, ListCollectionsRequest,
    ListCollectionsResponse, OptimizerStatus, OptimizersConfigDiff, PayloadSchemaInfo,
    PayloadSchemaType, UpdateCollection, WalConfigDiff,
};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Instant;
use storage::content_manager::collection_meta_ops::{
    CreateCollection as StorageCreateCollection, CreateCollectionOperation,
    DeleteCollectionOperation, UpdateCollection as StorageUpdateCollection,
    UpdateCollectionOperation,
};
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
        let response = GetCollectionInfoResponse::from((timing, result));

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
            .perform_collection_operation(operations)
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
            .perform_collection_operation(operations)
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
            .perform_collection_operation(operations)
            .await
            .map_err(error_to_status)?;

        let response = CollectionOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }
}

impl From<(Instant, CollectionsResponse)> for ListCollectionsResponse {
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

impl TryFrom<CreateCollection>
    for storage::content_manager::collection_meta_ops::CollectionMetaOperations
{
    type Error = Status;

    fn try_from(value: CreateCollection) -> Result<Self, Self::Error> {
        let internal_distance = match Distance::from_i32(value.distance) {
            Some(Distance::Cosine) => segment::types::Distance::Cosine,
            Some(Distance::Euclid) => segment::types::Distance::Euclid,
            Some(Distance::Dot) => segment::types::Distance::Dot,
            Some(_) => return Err(Status::failed_precondition("Unknown distance")),
            _ => return Err(Status::failed_precondition("Bad value of distance field!")),
        };

        Ok(Self::CreateCollection(CreateCollectionOperation {
            collection_name: value.collection_name,
            create_collection: StorageCreateCollection {
                vector_size: value.vector_size as usize,
                distance: internal_distance,
                hnsw_config: value.hnsw_config.map(|v| v.into()),
                wal_config: value.wal_config.map(|v| v.into()),
                optimizers_config: value.optimizers_config.map(|v| v.into()),
                shard_number: value.shard_number.unwrap_or_else(
                    storage::content_manager::collection_meta_ops::default_shard_number,
                ),
            },
        }))
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
            default_segment_number: value.default_segment_number.map(|v| v as usize),
            max_segment_size: value.max_segment_size.map(|v| v as usize),
            memmap_threshold: value.memmap_threshold.map(|v| v as usize),
            indexing_threshold: value.indexing_threshold.map(|v| v as usize),
            payload_indexing_threshold: value.payload_indexing_threshold.map(|v| v as usize),
            flush_interval_sec: value.flush_interval_sec,
            max_optimization_threads: value.max_optimization_threads.map(|v| v as usize),
        }
    }
}

impl From<(Instant, collection::operations::types::CollectionInfo)> for GetCollectionInfoResponse {
    fn from(value: (Instant, collection::operations::types::CollectionInfo)) -> Self {
        let (timing, response) = value;

        let collection::operations::types::CollectionInfo {
            status,
            optimizer_status,
            vectors_count,
            segments_count,
            disk_data_size,
            ram_data_size,
            config,
            payload_schema,
        } = response;

        GetCollectionInfoResponse {
            result: Some(CollectionInfo {
                status: match status {
                    collection::operations::types::CollectionStatus::Green => {
                        CollectionStatus::Green
                    }
                    collection::operations::types::CollectionStatus::Yellow => {
                        CollectionStatus::Yellow
                    }
                    collection::operations::types::CollectionStatus::Red => CollectionStatus::Red,
                }
                .into(),
                optimizer_status: Some(match optimizer_status {
                    collection::operations::types::OptimizersStatus::Ok => OptimizerStatus {
                        ok: true,
                        error: "".to_string(),
                    },
                    collection::operations::types::OptimizersStatus::Error(error) => {
                        OptimizerStatus { ok: false, error }
                    }
                }),
                vectors_count: vectors_count as u64,
                segments_count: segments_count as u64,
                disk_data_size: disk_data_size as u64,
                ram_data_size: ram_data_size as u64,
                config: Some(CollectionConfig {
                    params: Some(CollectionParams {
                        vector_size: config.params.vector_size as u64,
                        distance: match config.params.distance {
                            segment::types::Distance::Cosine => Distance::Cosine,
                            segment::types::Distance::Euclid => Distance::Euclid,
                            segment::types::Distance::Dot => Distance::Dot,
                        }
                        .into(),
                        shard_number: config.params.shard_number.get(),
                    }),
                    hnsw_config: Some(HnswConfigDiff {
                        m: Some(config.hnsw_config.m as u64),
                        ef_construct: Some(config.hnsw_config.ef_construct as u64),
                        full_scan_threshold: Some(config.hnsw_config.full_scan_threshold as u64),
                    }),
                    optimizer_config: Some(OptimizersConfigDiff {
                        deleted_threshold: Some(config.optimizer_config.deleted_threshold),
                        vacuum_min_vector_number: Some(
                            config.optimizer_config.vacuum_min_vector_number as u64,
                        ),
                        default_segment_number: Some(
                            config.optimizer_config.default_segment_number as u64,
                        ),
                        max_segment_size: Some(config.optimizer_config.max_segment_size as u64),
                        memmap_threshold: Some(config.optimizer_config.memmap_threshold as u64),
                        indexing_threshold: Some(config.optimizer_config.indexing_threshold as u64),
                        payload_indexing_threshold: Some(
                            config.optimizer_config.payload_indexing_threshold as u64,
                        ),
                        flush_interval_sec: Some(config.optimizer_config.flush_interval_sec as u64),
                        max_optimization_threads: Some(
                            config.optimizer_config.max_optimization_threads as u64,
                        ),
                    }),
                    wal_config: Some(WalConfigDiff {
                        wal_capacity_mb: Some(config.wal_config.wal_capacity_mb as u64),
                        wal_segments_ahead: Some(config.wal_config.wal_segments_ahead as u64),
                    }),
                }),
                payload_schema: payload_schema
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            }),
            time: timing.elapsed().as_secs_f64(),
        }
    }
}

impl From<segment::types::PayloadIndexInfo> for PayloadSchemaInfo {
    fn from(schema: segment::types::PayloadIndexInfo) -> Self {
        PayloadSchemaInfo {
            data_type: match schema.data_type {
                segment::types::PayloadSchemaType::Keyword => PayloadSchemaType::Keyword,
                segment::types::PayloadSchemaType::Integer => PayloadSchemaType::Integer,
                segment::types::PayloadSchemaType::Float => PayloadSchemaType::Float,
                segment::types::PayloadSchemaType::Geo => PayloadSchemaType::Geo,
            }
            .into(),
        }
    }
}

impl From<(Instant, bool)> for CollectionOperationResponse {
    fn from(value: (Instant, bool)) -> Self {
        let (timing, result) = value;
        CollectionOperationResponse {
            result,
            time: timing.elapsed().as_secs_f64(),
        }
    }
}

impl From<UpdateCollection>
    for storage::content_manager::collection_meta_ops::CollectionMetaOperations
{
    fn from(value: UpdateCollection) -> Self {
        Self::UpdateCollection(UpdateCollectionOperation {
            collection_name: value.collection_name,
            update_collection: StorageUpdateCollection {
                optimizers_config: value.optimizers_config.map(|v| v.into()),
            },
        })
    }
}

impl From<DeleteCollection>
    for storage::content_manager::collection_meta_ops::CollectionMetaOperations
{
    fn from(value: DeleteCollection) -> Self {
        Self::DeleteCollection(DeleteCollectionOperation(value.collection_name))
    }
}
