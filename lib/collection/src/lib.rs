//! Crate, which implements all functions required for operations with a single collection

use std::{path::Path, sync::Arc};

use collection_manager::collection_managers::CollectionSearcher;
use config::CollectionConfig;
use operations::{
    config_diff::OptimizersConfigDiff,
    types::{
        CollectionError, CollectionInfo, CollectionResult, RecommendRequest, Record, ScrollRequest,
        ScrollResult, SearchRequest, UpdateResult,
    },
    CollectionUpdateOperations, SplitByShard, Validate,
};
use segment::types::{PointIdType, ScoredPoint, VectorElementType, WithPayload};
use shard::{Shard, ShardId};
use tokio::runtime::Handle;

use crate::operations::OperationToShard;

pub mod collection_manager;
mod common;
pub mod config;
pub mod operations;
pub mod optimizers_builder;
pub mod shard;
mod update_handler;
mod wal;

#[cfg(test)]
mod tests;

type CollectionId = String;

/// Collection's data is split into several shards.
pub struct Collection {
    shard: Shard,
}

impl Collection {
    pub fn new(
        id: CollectionId,
        path: &Path,
        config: &CollectionConfig,
    ) -> Result<Self, CollectionError> {
        Ok(Self {
            shard: Shard::build(0, id, path, config)?,
        })
    }

    pub fn load(id: CollectionId, path: &Path) -> Self {
        Self {
            shard: Shard::load(0, id, path),
        }
    }

    fn shard_by_id(&self, _id: ShardId) -> &Shard {
        &self.shard
    }

    fn all_shards(&self) -> Vec<&Shard> {
        vec![&self.shard]
    }

    pub async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        operation.validate()?;
        let by_shard = operation.split_by_shard();
        let mut results = Vec::new();
        match by_shard {
            OperationToShard::ByShard(by_shard) => {
                for (shard_id, operation) in by_shard {
                    results.push(self.shard_by_id(shard_id).update(operation, wait).await)
                }
            }
            OperationToShard::ToAll(operation) => {
                for shard in self.all_shards() {
                    results.push(shard.update(operation.clone(), wait).await)
                }
            }
        }
        // At least one result is always present.
        // This is a stub to keep the current API as we have only 1 shard for now.
        results.pop().unwrap()
    }

    pub async fn recommend_by(
        &self,
        request: RecommendRequest,
        segment_searcher: &(dyn CollectionSearcher),
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        self.shard
            .recommend_by(request, segment_searcher, search_runtime_handle)
            .await
    }

    pub async fn search(
        &self,
        request: SearchRequest,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        segment_searcher
            .search(
                self.shard.segments(),
                Arc::new(request),
                search_runtime_handle,
            )
            .await
    }

    pub async fn scroll_by(
        &self,
        request: ScrollRequest,
        segment_searcher: &(dyn CollectionSearcher),
    ) -> CollectionResult<ScrollResult> {
        self.shard.scroll_by(request, segment_searcher).await
    }

    pub async fn retrieve(
        &self,
        points: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: bool,
        segment_searcher: &(dyn CollectionSearcher),
    ) -> CollectionResult<Vec<Record>> {
        segment_searcher
            .retrieve(self.shard.segments(), points, with_payload, with_vector)
            .await
    }

    /// Updates shard optimization params:
    /// - Saves new params on disk
    /// - Stops existing optimization loop
    /// - Runs new optimizers with new params
    pub async fn update_optimizer_params(
        &self,
        optimizer_config_diff: OptimizersConfigDiff,
    ) -> CollectionResult<()> {
        self.shard
            .update_optimizer_params(optimizer_config_diff)
            .await
    }

    pub async fn info(&self) -> CollectionResult<CollectionInfo> {
        self.shard.info().await
    }
}

pub fn avg_vectors<'a>(
    vectors: impl Iterator<Item = &'a Vec<VectorElementType>>,
) -> Vec<VectorElementType> {
    let mut count: usize = 0;
    let mut avg_vector: Vec<VectorElementType> = vec![];
    for vector in vectors {
        count += 1;
        for i in 0..vector.len() {
            if i >= avg_vector.len() {
                avg_vector.push(vector[i])
            } else {
                avg_vector[i] += vector[i];
            }
        }
    }

    for item in &mut avg_vector {
        *item /= count as VectorElementType;
    }

    avg_vector
}
