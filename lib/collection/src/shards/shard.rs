use core::marker::{Send, Sync};
use std::path::Path;

use crate::operations::types::CollectionResult;
use crate::shards::dummy_shard::DummyShard;
use crate::shards::forward_proxy_shard::ForwardProxyShard;
use crate::shards::local_shard::LocalShard;
use crate::shards::proxy_shard::ProxyShard;
use crate::shards::shard_trait::ShardOperation;
use crate::shards::telemetry::LocalShardTelemetry;

pub type ShardId = u32;

pub type PeerId = u64;

/// Shard
///
/// Contains a part of the collection's points
///
#[allow(clippy::large_enum_variant)]
pub enum Shard {
    Local(LocalShard),
    Proxy(ProxyShard),
    ForwardProxy(ForwardProxyShard),
    Dummy(DummyShard),
}

impl Shard {
    pub fn variant_name(&self) -> &str {
        match self {
            Shard::Local(_) => "local shard",
            Shard::Proxy(_) => "proxy shard",
            Shard::ForwardProxy(_) => "forward proxy shard",
            Shard::Dummy(_) => "dummy shard",
        }
    }

    pub fn get(&self) -> &(dyn ShardOperation + Sync + Send + '_) {
        match self {
            Shard::Local(local_shard) => local_shard,
            Shard::Proxy(proxy_shard) => proxy_shard,
            Shard::ForwardProxy(proxy_shard) => proxy_shard,
            Shard::Dummy(dummy_shard) => dummy_shard,
        }
    }

    pub async fn before_drop(&mut self) {
        match self {
            Shard::Local(local_shard) => local_shard.before_drop().await,
            Shard::Proxy(proxy_shard) => proxy_shard.before_drop().await,
            Shard::ForwardProxy(proxy_shard) => proxy_shard.before_drop().await,
            Shard::Dummy(_) => (), // `DummyShard` don't need (and don't have) `before_drop`
        }
    }

    pub fn get_telemetry_data(&self) -> LocalShardTelemetry {
        let mut telemetry = match self {
            Shard::Local(local_shard) => local_shard.get_telemetry_data(),
            Shard::Proxy(proxy_shard) => proxy_shard.get_telemetry_data(),
            Shard::ForwardProxy(proxy_shard) => proxy_shard.get_telemetry_data(),
            Shard::Dummy(dummy_shard) => dummy_shard.get_telemetry_data(),
        };
        telemetry.variant_name = Some(self.variant_name().to_string());
        telemetry
    }

    pub async fn create_snapshot(
        &self,
        temp_path: &Path,
        target_path: &Path,
        save_wal: bool,
    ) -> CollectionResult<()> {
        match self {
            Shard::Local(local_shard) => {
                local_shard
                    .create_snapshot(temp_path, target_path, save_wal)
                    .await
            }
            Shard::Proxy(proxy_shard) => {
                proxy_shard
                    .create_snapshot(temp_path, target_path, save_wal)
                    .await
            }
            Shard::ForwardProxy(proxy_shard) => {
                proxy_shard
                    .create_snapshot(temp_path, target_path, save_wal)
                    .await
            }
            Shard::Dummy(dummy_shard) => {
                dummy_shard
                    .create_snapshot(temp_path, target_path, save_wal)
                    .await
            }
        }
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        match self {
            Shard::Local(local_shard) => local_shard.on_optimizer_config_update().await,
            Shard::Proxy(proxy_shard) => proxy_shard.on_optimizer_config_update().await,
            Shard::ForwardProxy(proxy_shard) => proxy_shard.on_optimizer_config_update().await,
            Shard::Dummy(dummy_shard) => dummy_shard.on_optimizer_config_update().await,
        }
    }
}
