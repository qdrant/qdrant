pub mod local_shard;
pub mod remote_shard;

use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::shard::remote_shard::RemoteShard;
use crate::{
    CollectionInfo, CollectionResult, CollectionSearcher, CollectionUpdateOperations, LocalShard,
    OptimizersConfigDiff, Record, UpdateResult,
};
use parking_lot::RwLock;
use segment::types::{ExtendedPointId, Filter, WithPayloadInterface};

pub type ShardId = u32;

pub type PeerId = u32;

/// Shard
///
/// A shard can either be local or remote
///
#[allow(clippy::large_enum_variant)]
pub enum Shard {
    Local(LocalShard),
    Remote(RemoteShard),
}

impl Shard {
    pub async fn before_drop(&mut self) {
        match self {
            Shard::Local(local_shard) => local_shard.before_drop().await,
            Shard::Remote(_) => (),
        }
    }

    pub async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        match self {
            Shard::Local(local_shard) => local_shard.update(operation, wait).await,
            Shard::Remote(_) => todo!(),
        }
    }

    pub fn segments(&self) -> &RwLock<SegmentHolder> {
        match self {
            Shard::Local(local_shard) => local_shard.segments(),
            Shard::Remote(_) => todo!(),
        }
    }

    pub async fn scroll_by(
        &self,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: bool,
        filter: Option<&Filter>,
    ) -> CollectionResult<Vec<Record>> {
        match self {
            Shard::Local(local_shard) => {
                local_shard
                    .scroll_by(
                        segment_searcher,
                        offset,
                        limit,
                        with_payload_interface,
                        with_vector,
                        filter,
                    )
                    .await
            }
            Shard::Remote(_) => todo!(),
        }
    }

    pub async fn update_optimizer_params(
        &self,
        optimizer_config_diff: OptimizersConfigDiff,
    ) -> CollectionResult<()> {
        match self {
            Shard::Local(local_shard) => {
                local_shard
                    .update_optimizer_params(optimizer_config_diff)
                    .await
            }
            Shard::Remote(_) => todo!(),
        }
    }

    pub async fn info(&self) -> CollectionResult<CollectionInfo> {
        match self {
            Shard::Local(local_shard) => local_shard.info().await,
            Shard::Remote(_) => todo!(),
        }
    }
}
