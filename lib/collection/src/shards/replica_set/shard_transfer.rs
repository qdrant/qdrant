use std::ops::Deref as _;

use segment::types::PointIdType;

use super::ShardReplicaSet;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::forward_proxy_shard::ForwardProxyShard;
use crate::shards::local_shard::clock_map::RecoveryPoint;
use crate::shards::queue_proxy_shard::QueueProxyShard;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::shard::Shard;

impl ShardReplicaSet {
    /// Convert `Local` shard into `ForwardProxy`.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn proxify_local(&self, remote_shard: RemoteShard) -> CollectionResult<()> {
        let mut local = self.local.write().await;

        match local.deref() {
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

        // Explicit `match` instead of `if-let` to catch `unreachable` condition if top `match` is
        // changed
        let local_shard = match local.take() {
            Some(Shard::Local(local_shard)) => local_shard,
            _ => unreachable!(),
        };

        let proxy_shard = ForwardProxyShard::new(local_shard, remote_shard);
        let _ = local.insert(Shard::ForwardProxy(proxy_shard));

        Ok(())
    }

    /// Queue proxy our local shard, pointing to the remote shard.
    ///
    /// A `from_version` may be provided to start queueing the WAL from a specific version. The
    /// point may be in the past, but can never be outside the range of what we currently have in
    /// WAL. If `None` is provided, it'll queue from the latest available WAL version at this time.
    ///
    /// For snapshot transfer we queue from the latest version, so we can send all new updates once
    /// the remote shard has been recovered. For WAL delta transfer we queue from a specific
    /// version based on our recovery point.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn queue_proxify_local(
        &self,
        remote_shard: RemoteShard,
        from_version: Option<u64>,
    ) -> CollectionResult<()> {
        let mut local = self.local.write().await;

        match local.deref() {
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

        // Get `max_ack_version` without "taking" local shard (to maintain cancel safety)
        let local_shard = match local.deref() {
            Some(Shard::Local(local)) => local,
            Some(Shard::ForwardProxy(proxy)) => &proxy.wrapped_shard,
            _ => unreachable!(),
        };

        let wal_keep_from = local_shard
            .update_handler
            .lock()
            .await
            .wal_keep_from
            .clone();

        // Proxify local shard
        //
        // Making `await` calls between `local.take()` and `local.insert(...)` is *not* cancel safe!
        let local_shard = match local.take() {
            Some(Shard::Local(local)) => local,
            Some(Shard::ForwardProxy(proxy)) => proxy.wrapped_shard,
            _ => unreachable!(),
        };

        // Try to queue proxify with or without version
        let proxy_shard = match from_version {
            None => Ok(QueueProxyShard::new(
                local_shard,
                remote_shard,
                wal_keep_from,
            )),
            Some(from_version) => QueueProxyShard::new_from_version(
                local_shard,
                remote_shard,
                wal_keep_from,
                from_version,
            ),
        };

        // Insert queue proxy shard on success or revert to local shard on failure
        match proxy_shard {
            // All good, insert queue proxy shard
            Ok(proxy_shard) => {
                let _ = local.insert(Shard::QueueProxy(proxy_shard));
                Ok(())
            }
            Err((local_shard, err)) => {
                log::warn!("Failed to queue proxify shard, reverting to local shard: {err}");
                let _ = local.insert(Shard::Local(local_shard));
                Err(err)
            }
        }
    }

    /// Un-proxify local shard wrapped as `ForwardProxy` or `QueueProxy`.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn un_proxify_local(&self) -> CollectionResult<()> {
        let mut local = self.local.write().await;

        match local.deref() {
            // Expected states, continue
            Some(Shard::Local(_)) => return Ok(()),
            Some(Shard::ForwardProxy(_) | Shard::QueueProxy(_)) => {}

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

        // Perform async finalization without "taking" local shard (to maintain cancel safety)
        //
        // Explicit `match` instead of `if-let` on `Shard::QueueProxy` to catch `unreachable`
        // condition if top `match` is changed
        let result = match local.deref() {
            Some(Shard::ForwardProxy(_)) => Ok(()),

            Some(Shard::QueueProxy(proxy)) => {
                // We should not unproxify a queue proxy shard directly because it can fail if it
                // fails to send all updates to the remote shard.
                // Instead we should transform it into a forward proxy shard before unproxify is
                // called to handle errors at an earlier time.
                // Also, we're holding a write lock here which could block other accessors for a
                // long time if transferring updates takes a long time.
                // See `Self::queue_proxy_into_forward_proxy()` for more details.

                log::warn!(
                    "Directly unproxifying queue proxy shard, this should not happen normally"
                );

                let result = proxy.transfer_all_missed_updates().await;

                if let Err(err) = &result {
                    log::error!(
                        "Failed to un-proxify local shard because transferring remaining queue \
                         items to remote failed: {err}"
                    );
                }

                result
            }

            _ => unreachable!(),
        };

        // Un-proxify local shard
        //
        // Making `await` calls between `local.take()` and `local.insert(...)` is *not* cancel safe!
        let local_shard = match local.take() {
            Some(Shard::ForwardProxy(proxy)) => proxy.wrapped_shard,

            Some(Shard::QueueProxy(proxy)) => {
                let (local_shard, _) = proxy.forget_updates_and_finalize();
                local_shard
            }

            _ => unreachable!(),
        };

        let _ = local.insert(Shard::Local(local_shard));

        result
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
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    ///
    /// If cancelled - the queue proxy may not be reverted to a local proxy.
    pub async fn revert_queue_proxy_local(&self) {
        let mut local = self.local.write().await;

        // Take out queue proxy shard or return
        if !matches!(local.deref(), Some(Shard::QueueProxy(_))) {
            return;
        };

        log::debug!("Forgetting queue proxy updates and reverting to local shard");

        // Making `await` calls between `local.take()` and `local.insert(...)` is *not* cancel safe!
        let Some(Shard::QueueProxy(queue_proxy)) = local.take() else {
            unreachable!();
        };

        let (local_shard, _) = queue_proxy.forget_updates_and_finalize();
        let _ = local.insert(Shard::Local(local_shard));
    }

    /// Custom operation for transferring data from one shard to another during transfer
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn transfer_batch(
        &self,
        offset: Option<PointIdType>,
        batch_size: usize,
    ) -> CollectionResult<Option<PointIdType>> {
        let local = self.local.read().await;

        let Some(Shard::ForwardProxy(proxy)) = local.deref() else {
            return Err(CollectionError::service_error(format!(
                "Cannot transfer batch from shard {} because it is not proxified",
                self.shard_id
            )));
        };

        proxy
            .transfer_batch(offset, batch_size, &self.search_runtime)
            .await
    }

    /// Custom operation for transferring indexes from one shard to another during transfer
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn transfer_indexes(&self) -> CollectionResult<()> {
        let local = self.local.read().await;

        let Some(Shard::ForwardProxy(proxy)) = local.deref() else {
            return Err(CollectionError::service_error(format!(
                "Cannot transfer indexes from shard {} because it is not proxified",
                self.shard_id,
            )));
        };

        log::trace!(
            "Transferring indexes to shard {}",
            proxy.remote_shard.peer_id,
        );

        proxy.transfer_indexes().await
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
    ///
    /// # Cancel safety
    ///
    /// This function is cancel safe.
    ///
    /// If cancelled - transforming the queue proxy into a forward proxy may not actually complete.
    /// None, some or all queued operations may be transmitted to the remote.
    pub async fn queue_proxy_into_forward_proxy(&self) -> CollectionResult<()> {
        // First pass: transfer all missed updates with shared read lock
        {
            let local = self.local.read().await;

            let Some(Shard::QueueProxy(proxy)) = local.deref() else {
                return Ok(());
            };

            proxy.transfer_all_missed_updates().await?;
        }

        // Second pass: transfer new updates
        let mut local = self.local.write().await;

        let Some(Shard::QueueProxy(proxy)) = local.deref() else {
            return Ok(());
        };

        proxy.transfer_all_missed_updates().await?;

        // Transform `QueueProxyShard` into `ForwardProxyShard`
        log::trace!("Transferred all queue proxy operations, transforming into forward proxy now");

        // Making `await` calls between `local.take()` and `local.insert(...)` is *not* cancel safe!
        let Some(Shard::QueueProxy(queue_proxy)) = local.take() else {
            unreachable!();
        };

        let (local_shard, remote_shard) = queue_proxy.forget_updates_and_finalize();
        let forward_proxy = ForwardProxyShard::new(local_shard, remote_shard);
        let _ = local.insert(Shard::ForwardProxy(forward_proxy));

        Ok(())
    }

    pub async fn resolve_wal_delta(
        &self,
        recovery_point: RecoveryPoint,
    ) -> CollectionResult<Option<u64>> {
        let local_shard_read = self.local.read().await;
        let Some(local_shard) = local_shard_read.deref() else {
            return Err(CollectionError::service_error(
                "Cannot resolve WAL delta, shard replica set does not have local shard",
            ));
        };

        local_shard.resolve_wal_delta(recovery_point).await
    }
}
