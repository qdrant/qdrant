pub mod collection_shard_distribution;
mod conversions;
pub mod forward_proxy_shard;
pub mod local_shard;
pub mod local_shard_operations;
pub mod proxy_shard;
pub mod remote_shard;
pub mod shard_config;
pub mod shard_holder;
pub mod shard_versioning;
pub mod transfer;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use api::grpc::transport_channel_pool::TransportChannelPool;
use async_trait::async_trait;
use schemars::JsonSchema;
use segment::types::{ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tonic::transport::Uri;

use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CountRequest, CountResult, PointRequest,
    Record, SearchRequestBatch, UpdateResult,
};
use crate::operations::CollectionUpdateOperations;
use crate::shard::forward_proxy_shard::ForwardProxyShard;
use crate::shard::local_shard::LocalShard;
use crate::shard::proxy_shard::ProxyShard;
use crate::shard::remote_shard::RemoteShard;
use crate::shard::shard_versioning::suggest_next_version_path;
use crate::telemetry::ShardTelemetry;

pub type ShardId = u32;

/// Shard
///
/// A shard can either be local or remote
///
#[allow(clippy::large_enum_variant)]
pub enum Shard {
    Local(LocalShard),
    Remote(RemoteShard),
    Proxy(ProxyShard),
    ForwardProxy(ForwardProxyShard),
}

impl Shard {
    pub fn get(&self) -> &(dyn ShardOperation + Sync + Send + '_) {
        match self {
            Shard::Local(local_shard) => local_shard,
            Shard::Remote(remote_shard) => remote_shard,
            Shard::Proxy(proxy_shard) => proxy_shard,
            Shard::ForwardProxy(proxy_shard) => proxy_shard,
        }
    }

    pub async fn before_drop(&mut self) {
        match self {
            Shard::Local(local_shard) => local_shard.before_drop().await,
            Shard::Remote(_) => (),
            Shard::Proxy(proxy_shard) => proxy_shard.before_drop().await,
            Shard::ForwardProxy(proxy_shard) => proxy_shard.before_drop().await,
        }
    }

    pub fn peer_id(&self, this_peer_id: PeerId) -> PeerId {
        match self {
            Shard::Local(_) => this_peer_id,
            Shard::Remote(remote) => remote.peer_id,
            Shard::Proxy(_) => this_peer_id,
            Shard::ForwardProxy(_) => this_peer_id,
        }
    }

    pub fn get_telemetry_data(&self) -> ShardTelemetry {
        match self {
            Shard::Local(local_shard) => local_shard.get_telemetry_data(),
            Shard::Remote(remote_shard) => remote_shard.get_telemetry_data(),
            Shard::Proxy(proxy_shard) => proxy_shard.get_telemetry_data(),
            Shard::ForwardProxy(proxy_shard) => proxy_shard.get_telemetry_data(),
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
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: bool,
        filter: Option<&Filter>,
    ) -> CollectionResult<Vec<Record>>;

    async fn info(&self) -> CollectionResult<CollectionInfo>;

    async fn search(
        &self,
        request: Arc<SearchRequestBatch>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>>;

    async fn count(&self, request: Arc<CountRequest>) -> CollectionResult<CountResult>;

    async fn retrieve(
        &self,
        request: Arc<PointRequest>,
        with_payload: &WithPayload,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>>;
}

pub const HASH_RING_SHARD_SCALE: u32 = 100;

pub type CollectionId = String;

pub type ShardVersion = usize;

pub type PeerId = u64;

#[derive(Clone)]
pub struct ChannelService {
    pub id_to_address: Arc<parking_lot::RwLock<HashMap<PeerId, Uri>>>,
    pub channel_pool: Arc<TransportChannelPool>,
}

impl ChannelService {
    pub fn new(
        id_to_address: Arc<parking_lot::RwLock<HashMap<PeerId, Uri>>>,
        channel_pool: Arc<TransportChannelPool>,
    ) -> Self {
        Self {
            id_to_address,
            channel_pool,
        }
    }

    pub async fn remove_peer(&self, peer_id: PeerId) {
        let removed = self.id_to_address.write().remove(&peer_id);
        if let Some(uri) = removed {
            self.channel_pool.drop_pool(&uri).await;
        }
    }
}

impl Default for ChannelService {
    fn default() -> Self {
        Self {
            id_to_address: Arc::new(Default::default()),
            channel_pool: Arc::new(Default::default()),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ShardTransfer {
    pub shard_id: ShardId,
    pub from: PeerId,
    pub to: PeerId,
}

pub async fn create_shard_dir(
    collection_path: &Path,
    shard_id: ShardId,
) -> CollectionResult<PathBuf> {
    loop {
        let shard_path = suggest_next_version_path(collection_path, shard_id).await?;

        match tokio::fs::create_dir(&shard_path).await {
            Ok(_) => return Ok(shard_path),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    continue;
                } else {
                    return Err(CollectionError::from(e));
                }
            }
        }
    }
}
