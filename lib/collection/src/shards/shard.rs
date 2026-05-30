use core::marker::{Send, Sync};
use std::future::{self, Future};
use std::path::Path;

use common::types::TelemetryDetail;

use super::local_shard::clock_map::RecoveryPoint;
use super::update_tracker::UpdateTracker;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::dummy_shard::DummyShard;
use crate::shards::forward_proxy_shard::ForwardProxyShard;
use crate::shards::local_shard::LocalShard;
use crate::shards::proxy_shard::ProxyShard;
use crate::shards::queue_proxy_shard::QueueProxyShard;
use crate::shards::shard_trait::ShardOperation;
use crate::shards::telemetry::LocalShardTelemetry;

pub type ShardId = u32;

pub type PeerId = u64;

/// List of peers that should be used to place replicas of a shard
pub type ShardReplicasPlacement = Vec<PeerId>;

/// List of shards placements. Each element defines placements of replicas for a single shard.
/// Number of elements corresponds to the number of shards.
/// Example: [
///     [1, 2],
///     [2, 3],
///     [3, 4]
/// ] - 3 shards, each has 2 replicas
pub type ShardsPlacement = Vec<ShardReplicasPlacement>;

/// Shard
///
/// Contains a part of the collection's points
#[allow(clippy::large_enum_variant)]
pub enum Shard {
    Local(LocalShard),
    Proxy(ProxyShard),
    ForwardProxy(ForwardProxyShard),
    QueueProxy(QueueProxyShard),
    Dummy(DummyShard),
}

impl Shard {
    pub fn variant_name(&self) -> &str {
        match self {
            Shard::Local(_) => "local shard",
            Shard::Proxy(_) => "proxy shard",
            Shard::ForwardProxy(_) => "forward proxy shard",
            Shard::QueueProxy(_) => "queue proxy shard",
            Shard::Dummy(_) => "dummy shard",
        }
    }

    pub fn get(&self) -> &(dyn ShardOperation + Sync + Send + '_) {
        match self {
            Shard::Local(local_shard) => local_shard,
            Shard::Proxy(proxy_shard) => proxy_shard,
            Shard::ForwardProxy(proxy_shard) => proxy_shard,
            Shard::QueueProxy(proxy_shard) => proxy_shard,
            Shard::Dummy(dummy_shard) => dummy_shard,
        }
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> LocalShardTelemetry {
        let mut telemetry = match self {
            Shard::Local(local_shard) => local_shard.get_telemetry_data(detail),
            Shard::Proxy(proxy_shard) => proxy_shard.get_telemetry_data(detail),
            Shard::ForwardProxy(proxy_shard) => proxy_shard.get_telemetry_data(detail),
            Shard::QueueProxy(proxy_shard) => proxy_shard.get_telemetry_data(detail),
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
            Shard::QueueProxy(proxy_shard) => {
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
            Shard::QueueProxy(proxy_shard) => proxy_shard.on_optimizer_config_update().await,
            Shard::Dummy(dummy_shard) => dummy_shard.on_optimizer_config_update().await,
        }
    }

    pub fn is_update_in_progress(&self) -> bool {
        self.update_tracker()
            .map_or(false, UpdateTracker::is_update_in_progress)
    }

    pub fn watch_for_update(&self) -> impl Future<Output = ()> {
        let update_watcher = self.update_tracker().map(UpdateTracker::watch_for_update);

        async move {
            match update_watcher {
                Some(update_watcher) => update_watcher.await,
                None => future::pending().await,
            }
        }
    }

    fn update_tracker(&self) -> Option<&UpdateTracker> {
        let update_tracker = match self {
            Self::Local(local_shard) => local_shard.update_tracker(),
            Self::Proxy(proxy_shard) => proxy_shard.update_tracker(),
            Self::ForwardProxy(proxy_shard) => proxy_shard.update_tracker(),
            Self::QueueProxy(proxy_shard) => proxy_shard.update_tracker(),
            Self::Dummy(_) => return None,
        };

        Some(update_tracker)
    }

    pub async fn shard_recovery_point(&self) -> CollectionResult<RecoveryPoint> {
        match self {
            Self::Local(local_shard) => Ok(local_shard.recovery_point().await),
            Self::ForwardProxy(proxy_shard) => Ok(proxy_shard.wrapped_shard.recovery_point().await),

            Self::Proxy(_) | Self::QueueProxy(_) | Self::Dummy(_) => {
                Err(CollectionError::service_error(format!(
                    "Recovery point not supported on {}",
                    self.variant_name(),
                )))
            }
        }
    }

    pub async fn update_cutoff(&self, cutoff: &RecoveryPoint) -> CollectionResult<()> {
        match self {
            Self::Local(local_shard) => local_shard.update_cutoff(cutoff).await,

            Self::Proxy(_) | Self::ForwardProxy(_) | Self::QueueProxy(_) | Self::Dummy(_) => {
                return Err(CollectionError::service_error(format!(
                    "Setting cutoff point not supported on {}",
                    self.variant_name(),
                )));
            }
        }
        Ok(())
    }

    pub async fn resolve_wal_delta(
        &self,
        recovery_point: RecoveryPoint,
    ) -> CollectionResult<Option<u64>> {
        let wal = match self {
            Self::Local(local_shard) => &local_shard.wal,

            Self::Proxy(_) | Self::ForwardProxy(_) | Self::QueueProxy(_) | Self::Dummy(_) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot resolve WAL delta on {}",
                    self.variant_name(),
                )));
            }
        };

        // Resolve WAL delta and report
        match wal.resolve_wal_delta(recovery_point).await {
            Ok(Some(version)) => {
                let size = wal.wal.lock().last_index().saturating_sub(version);
                log::debug!("Resolved WAL delta from {version}, which counts {size} records");
                Ok(Some(version))
            }

            Ok(None) => {
                log::debug!("Resolved WAL delta that is empty");
                Ok(None)
            }

            Err(err) => Err(CollectionError::service_error(format!(
                "Failed to resolve WAL delta on local shard: {err}"
            ))),
        }
    }
}
