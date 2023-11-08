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
            // If a forward proxy to same remote, return early
            Some(Shard::ForwardProxy(proxy))
                if proxy.remote_shard.peer_id == remote_shard.peer_id =>
            {
                return Ok(())
            }
            // Unexpected states, error
            Some(Shard::ForwardProxy(proxy)) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot proxify local shard {} to peer {} because it is already proxified to peer {}",
                    self.shard_id, remote_shard.peer_id, proxy.remote_shard.peer_id
                )));
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
            // If a forward proxy to same remote, continue and change into queue proxy
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

        match local_write.take() {
            Some(Shard::Local(local)) => {
                let proxy_shard = QueueProxyShard::new(local, remote_shard).await;
                let _ = local_write.insert(Shard::QueueProxy(proxy_shard));
            }
            Some(Shard::ForwardProxy(proxy)) => {
                let proxy_shard = QueueProxyShard::new(proxy.wrapped_shard, remote_shard).await;
                let _ = local_write.insert(Shard::QueueProxy(proxy_shard));
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    /// Un-proxify local shard wrapped as `ForwardProxy` or `QueueProxy`.
    pub async fn un_proxify_local(&self) -> CollectionResult<()> {
        let mut local_write = self.local.write().await;

        match &*local_write {
            // Expected states, continue
            Some(Shard::ForwardProxy(_) | Shard::QueueProxy(_)) => {}
            Some(Shard::Local(_)) => return Ok(()),
            // Unexpected states, error
            Some(shard @ (Shard::Proxy(_) | Shard::Dummy(_))) => {
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
            Some(Shard::QueueProxy(proxy)) => {
                // We should not unproxify a queue proxy shard directly because it can fail if it
                // fails to send all updates to the remote shard.
                // Instead we should transform it into a forward proxy shard before unproxify is
                // called to handle errors at an earlier time.
                // See `Self::queue_proxy_into_forward_proxy()` for more details.
                log::warn!(
                    "Directly unproxifying queue proxy shard, this should not happen normally"
                );

                // Finalize, insert local shard back and return finalize result
                let result = proxy.finalize().await;
                let (result, local_shard) = match result {
                    Ok((local_shard, _)) => (Ok(()), local_shard),
                    Err((err, queue_proxy)) => {
                        log::error!("Failed to un-proxify local shard because transferring remaining queue items to remote failed: {err}");
                        let (wrapped_shard, _remote_shard) =
                            queue_proxy.forget_updates_and_finalize();
                        (Err(err), wrapped_shard)
                    }
                };
                let _ = local_write.insert(Shard::Local(local_shard));
                result
            }
            _ => unreachable!(),
        }
    }

    /// Revert usage of a `QueueProxy` shard and forget all updates, then un-proxify to local
    ///
    /// This can be used to intentionally forget all updates that are collected by the queue proxy
    /// shard and revert back to a local shard. This is useful if a shard transfer operation using
    /// a queue proxy must be aborted.
    ///
    /// Does nothing if the local shard is not a queue proxy shard.
    /// This method cannot fail.
    ///
    /// # Warning
    ///
    /// This intentionally forgets and drops updates pending to be transferred to the remote shard.
    /// The remote shard may therefore therefore be left in an inconsistent state, which should be
    /// resolved separately.
    pub async fn revert_queue_proxy_local(&self) {
        let mut local_write = self.local.write().await;

        // Take out queue proxy shard or return
        if !matches!(*local_write, Some(Shard::QueueProxy(_))) {
            return;
        };
        let Some(Shard::QueueProxy(queue_proxy)) = local_write.take() else {
            unreachable!();
        };

        log::debug!("Forgetting queue proxy updates and reverting to local shard");
        let (local_shard, _) = queue_proxy.forget_updates_and_finalize();
        let _ = local_write.insert(Shard::Local(local_shard));
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
            log::trace!(
                "Transferring indexes to shard {}",
                proxy.remote_shard.peer_id,
            );
            proxy.transfer_indexes().await
        } else {
            Err(CollectionError::service_error(format!(
                "Cannot transfer indexes from shard {} because it is not proxified",
                self.shard_id,
            )))
        }
    }

    /// Send all queue proxy updates to remote and transform into forward proxy
    ///
    /// When a queue or forward proxy shard needs to be unproxified into a local shard again we
    /// typically don't have room to handle errors. A queue proxy shard may error if it fails to
    /// send updates to the remote shard, while a forward proxy does not fail at all when
    /// transforming.
    ///
    /// This method allows to transfer queued updates before the shard is unproxified. This allows
    /// for proper error handling at the time this method is called. Because the shard is
    /// transformed into a forward proxy after this operation it will not error again when the
    /// shard is eventually unproxified again.
    ///
    /// If the local shard is a queue proxy:
    /// - Transfers all missed updates to remote
    /// - Transforms queue proxy into forward proxy
    ///
    /// Does nothing if the local shard is not a queue proxy.
    ///
    /// # Errors
    ///
    /// Returns an error if transferring all updates to the remote failed.
    pub async fn queue_proxy_into_forward_proxy(&self) -> CollectionResult<()> {
        // First pass: transfer all missed updates with shared read lock
        {
            let local_read = self.local.read().await;
            let Some(Shard::QueueProxy(proxy)) = &*local_read else {
                return Ok(());
            };
            proxy.transfer_all_missed_updates().await?;
        }

        // Second pass: transfer new updates, safely finalize and transform
        let mut local_write = self.local.write().await;
        if !matches!(*local_write, Some(Shard::QueueProxy(_))) {
            return Ok(());
        }
        let Some(Shard::QueueProxy(queue_proxy)) = local_write.take() else {
            unreachable!();
        };
        match queue_proxy.finalize().await {
            // When finalization is successful, transform into forward proxy
            Ok((local_shard, remote_shard)) => {
                log::trace!(
                    "Transferred all queue proxy operations, transforming into forward proxy now"
                );
                let forward_proxy = ForwardProxyShard::new(local_shard, remote_shard);
                let _ = local_write.insert(Shard::ForwardProxy(forward_proxy));
                Ok(())
            }
            // When finalization fails, put the queue proxy back
            Err((err, queue_proxy)) => {
                let _ = local_write.insert(Shard::QueueProxy(queue_proxy));
                Err(CollectionError::service_error(format!(
                    "Failed to finalize queue proxy and transform into forward proxy: {err}"
                )))
            }
        }
    }
}
