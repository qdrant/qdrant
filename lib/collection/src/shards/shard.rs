use core::marker::{Send, Sync};
use core::result::Result::Ok;
use std::path::Path;

use crate::operations::types::CollectionResult;
use crate::shards::forward_proxy_shard::ForwardProxyShard;
use crate::shards::local_shard::LocalShard;
use crate::shards::proxy_shard::ProxyShard;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ReplicaSet;
use crate::shards::shard_trait::ShardOperation;
use crate::telemetry::ShardTelemetry;

pub type ShardId = u32;

pub type PeerId = u64;

/// Shard
///
/// Contains a part of the collection's points
///
#[allow(clippy::large_enum_variant)]
pub enum Shard {
    Local(LocalShard),
    Remote(RemoteShard),
    Proxy(ProxyShard),
    ForwardProxy(ForwardProxyShard),
    ReplicaSet(ReplicaSet),
}

impl Shard {
    pub fn variant_name(&self) -> &str {
        match self {
            Shard::Local(_) => "local shard",
            Shard::Remote(_) => "remote shard",
            Shard::Proxy(_) => "proxy shard",
            Shard::ForwardProxy(_) => "forward proxy shard",
            Shard::ReplicaSet(_) => "replica set",
        }
    }

    pub fn get(&self) -> &(dyn ShardOperation + Sync + Send + '_) {
        match self {
            Shard::Local(local_shard) => local_shard,
            Shard::Remote(remote_shard) => remote_shard,
            Shard::Proxy(proxy_shard) => proxy_shard,
            Shard::ForwardProxy(proxy_shard) => proxy_shard,
            Shard::ReplicaSet(replica_set) => replica_set,
        }
    }

    pub async fn before_drop(&mut self) {
        match self {
            Shard::Local(local_shard) => local_shard.before_drop().await,
            Shard::Remote(_) => (),
            Shard::Proxy(proxy_shard) => proxy_shard.before_drop().await,
            Shard::ForwardProxy(proxy_shard) => proxy_shard.before_drop().await,
            Shard::ReplicaSet(shard) => shard.before_drop().await,
        }
    }

    pub fn peer_ids(&self, this_peer_id: PeerId) -> Vec<PeerId> {
        match self {
            Shard::Local(_) => vec![this_peer_id],
            Shard::Remote(remote) => vec![remote.peer_id],
            Shard::Proxy(_) => vec![this_peer_id],
            Shard::ForwardProxy(_) => vec![this_peer_id],
            Shard::ReplicaSet(replicas) => replicas.peer_ids(),
        }
    }

    pub fn get_telemetry_data(&self) -> ShardTelemetry {
        match self {
            Shard::Local(local_shard) => local_shard.get_telemetry_data(),
            Shard::Remote(remote_shard) => remote_shard.get_telemetry_data(),
            Shard::Proxy(proxy_shard) => proxy_shard.get_telemetry_data(),
            Shard::ForwardProxy(proxy_shard) => proxy_shard.get_telemetry_data(),
            Shard::ReplicaSet(replica_set) => replica_set.get_telemetry_data(),
        }
    }

    pub async fn create_snapshot(&self, target_path: &Path) -> CollectionResult<()> {
        match self {
            Shard::Local(local_shard) => local_shard.create_snapshot(target_path).await,
            Shard::Remote(remote_shard) => remote_shard.create_snapshot(target_path).await,
            Shard::Proxy(proxy_shard) => proxy_shard.create_snapshot(target_path).await,
            Shard::ForwardProxy(proxy_shard) => proxy_shard.create_snapshot(target_path).await,
            Shard::ReplicaSet(replica_set) => replica_set.create_snapshot(target_path).await,
        }
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        match self {
            Shard::Local(local_shard) => local_shard.on_optimizer_config_update().await,
            Shard::Remote(_) => Ok(()),
            Shard::Proxy(proxy_shard) => proxy_shard.on_optimizer_config_update().await,
            Shard::ForwardProxy(proxy_shard) => proxy_shard.on_optimizer_config_update().await,
            Shard::ReplicaSet(shard) => shard.on_optimizer_config_update().await,
        }
    }
}
