mod conversions;
pub mod local_shard;
pub mod remote_shard;
pub mod shard_config;

use crate::operations::types::BatchSearchRequest;
use crate::shard::remote_shard::RemoteShard;
use crate::{
    CollectionInfo, CollectionResult, CollectionSearcher, CollectionUpdateOperations, LocalShard,
    PeerId, PointRequest, Record, SearchRequest, UpdateResult,
};
use async_trait::async_trait;
use segment::types::{ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface};
use std::sync::Arc;
use tokio::runtime::Handle;

pub type ShardId = u32;

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
    pub fn get(&self) -> Arc<dyn ShardOperation + Sync + Send + '_> {
        match self {
            Shard::Local(local_shard) => Arc::new(local_shard),
            Shard::Remote(remote_shard) => Arc::new(remote_shard),
        }
    }

    pub async fn before_drop(&mut self) {
        match self {
            Shard::Local(local_shard) => local_shard.before_drop().await,
            Shard::Remote(_) => (),
        }
    }

    pub fn peer_id(&self, this_peer_id: PeerId) -> PeerId {
        match self {
            Shard::Local(_) => this_peer_id,
            Shard::Remote(remote) => remote.peer_id,
        }
    }
}

#[async_trait]
pub trait ShardOperation {
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult>;

    #[allow(clippy::too_many_arguments)]
    async fn scroll_by(
        &self,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: bool,
        filter: Option<&Filter>,
    ) -> CollectionResult<Vec<Record>>;

    async fn info(&self) -> CollectionResult<CollectionInfo>;

    async fn search(
        &self,
        request: Arc<SearchRequest>,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>>;

    async fn batch_search(
        &self,
        request: Arc<BatchSearchRequest>,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>>;

    async fn retrieve(
        &self,
        request: Arc<PointRequest>,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        with_payload: &WithPayload,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>>;
}
