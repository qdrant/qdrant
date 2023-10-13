use segment::types::PointIdType;

use super::ShardReplicaSet;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::forward_proxy_shard::ForwardProxyShard;
use crate::shards::queue_proxy_shard::QueueProxyShard;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::shard::Shard;

impl ShardReplicaSet {
    pub async fn proxify_local(&self, remote_shard: RemoteShard) -> CollectionResult<()> {
        let mut local_write = self.local.write().await;

        match &*local_write {
            // Expected state, continue
            Some(Shard::Local(_)) => {}
            // Unexpected states, error
            Some(Shard::ForwardProxy(proxy)) => {
                return if proxy.remote_shard.peer_id == remote_shard.peer_id {
                    Ok(())
                } else {
                    Err(CollectionError::service_error(format!(
                        "Cannot proxify local shard {} to peer {} because it is already proxified to peer {}",
                        self.shard_id, remote_shard.peer_id, proxy.remote_shard.peer_id
                    )))
                };
            }
            Some(Shard::QueueProxy(_)) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot proxify local shard {} to peer {} because it is already queue proxified",
                    self.shard_id, remote_shard.peer_id,
                )));
            }
            Some(Shard::Proxy(_)) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot queue proxify local shard {} to peer {} because it already is a proxy",
                    self.shard_id, remote_shard.peer_id,
                )));
            }
            Some(Shard::Dummy(_)) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot proxify local dummy shard {} to peer {}",
                    self.shard_id, remote_shard.peer_id,
                )));
            }
            None => {
                return Err(CollectionError::service_error(format!(
                    "Cannot proxify local shard {} on peer {} because it is not active",
                    self.shard_id,
                    self.this_peer_id()
                )));
            }
        };

        if let Some(Shard::Local(local)) = local_write.take() {
            let proxy_shard = ForwardProxyShard::new(local, remote_shard);
            let _ = local_write.insert(Shard::ForwardProxy(proxy_shard));
        }

        Ok(())
    }

    pub async fn queue_proxify_local(&self, remote_shard: RemoteShard) -> CollectionResult<()> {
        let mut local_write = self.local.write().await;

        match &*local_write {
            // Expected state, continue
            Some(Shard::Local(_)) => {}
            Some(Shard::ForwardProxy(proxy))
                if proxy.remote_shard.peer_id == remote_shard.peer_id => {}
            // Unexpected states, error
            Some(Shard::QueueProxy(_)) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot queue proxify local shard {} to peer {} because it is already queue proxified",
                    self.shard_id, remote_shard.peer_id,
                )));
            }
            Some(Shard::ForwardProxy(proxy)) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot queue proxify local shard {} to peer {} because it is already proxified to peer {}",
                    self.shard_id, remote_shard.peer_id, proxy.remote_shard.peer_id
                )));
            }
            Some(Shard::Proxy(_)) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot queue proxify local shard {} to peer {} because it already is a proxy",
                    self.shard_id, remote_shard.peer_id,
                )));
            }
            Some(Shard::Dummy(_)) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot proxify local dummy shard {} to peer {}",
                    self.shard_id, remote_shard.peer_id,
                )));
            }
            None => {
                return Err(CollectionError::service_error(format!(
                    "Cannot queue proxify local shard {} on peer {} because it is not active",
                    self.shard_id,
                    self.this_peer_id()
                )));
            }
        };

        if let Some(Shard::Local(local)) = local_write.take() {
            let proxy_shard = QueueProxyShard::new(local, remote_shard).await;
            let _ = local_write.insert(Shard::QueueProxy(proxy_shard));
        }

        Ok(())
    }

    /// Un-proxify local shard wrapped as `ForwardProxy` or `QueueProxy`.
    ///
    /// Returns true if the replica was un-proxified, false if it was already handled.
    pub async fn un_proxify_local(&self) -> CollectionResult<()> {
        let mut local_write = self.local.write().await;

        match &*local_write {
            // Expected states, continue
            Some(Shard::ForwardProxy(_) | Shard::QueueProxy(_)) => {}
            Some(Shard::Local(_)) => return Ok(()),
            // Unexpected states, error
            Some(shard) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot un-proxify local shard {} because it has unexpected type - {}",
                    self.shard_id,
                    shard.variant_name(),
                )));
            }
            None => {
                return Err(CollectionError::service_error(format!(
                    "Cannot un-proxify local shard {} on peer {} because it is not active",
                    self.shard_id,
                    self.this_peer_id(),
                )));
            }
        };

        // Unproxify local shard of above types
        match local_write.take() {
            Some(Shard::ForwardProxy(proxy)) => {
                let local_shard = proxy.wrapped_shard;
                let _ = local_write.insert(Shard::Local(local_shard));
                Ok(())
            }
            Some(Shard::QueueProxy(proxy)) => match proxy.finalize().await {
                // Transfer remaining queue items and convert into local shard
                Ok(local_shard) => {
                    let _ = local_write.insert(Shard::Local(local_shard));
                    Ok(())
                }
                // Transferring remaining queue items failed
                // Keep shard as proxy so we don't loose queue state and return error
                // TODO: what happens here if remote is dead? Can never unwrap?
                Err((err, proxy)) => {
                    log::error!("Failed to un-proxify local shard because transferring remaining queue items to remote failed: {err}");
                    let _ = local_write.insert(Shard::QueueProxy(proxy));
                    Err(err)
                }
            },
            _ => unreachable!(),
        }
    }

    /// Custom operation for transferring data from one shard to another during transfer
    pub async fn transfer_batch(
        &self,
        offset: Option<PointIdType>,
        batch_size: usize,
    ) -> CollectionResult<Option<PointIdType>> {
        let read_local = self.local.read().await;
        if let Some(Shard::ForwardProxy(proxy)) = &*read_local {
            proxy
                .transfer_batch(offset, batch_size, &self.search_runtime)
                .await
        } else {
            Err(CollectionError::service_error(format!(
                "Cannot transfer batch from shard {} because it is not proxified",
                self.shard_id
            )))
        }
    }

    /// Custom operation for transferring indexes from one shard to another during transfer
    pub async fn transfer_indexes(&self) -> CollectionResult<()> {
        let read_local = self.local.read().await;
        if let Some(Shard::ForwardProxy(proxy)) = &*read_local {
            proxy.transfer_indexes().await
        } else {
            Err(CollectionError::service_error(format!(
                "Cannot transfer indexes from shard {} because it is not proxified",
                self.shard_id
            )))
        }
    }

    /// If we have a local queue proxy shard, transfer all missed updates to remote.
    pub async fn queue_proxy_transfer_updates(&self) -> CollectionResult<()> {
        let read_local = self.local.read().await;
        if let Some(Shard::QueueProxy(proxy)) = &*read_local {
            proxy.transfer_all_missed_updates().await?;
        }

        Ok(())
    }
}
