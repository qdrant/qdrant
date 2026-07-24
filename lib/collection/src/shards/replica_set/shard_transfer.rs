use std::ops::Deref as _;
use std::sync::Arc;

use parking_lot::Mutex;
use segment::types::{Filter, PointIdType};

use super::ShardReplicaSet;
use crate::hash_ring::HashRingRouter;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::forward_proxy_shard::{ForwardProxyShard, PreparedTransferBatch};
use crate::shards::local_shard::LocalShard;
use crate::shards::local_shard::clock_map::RecoveryPoint;
use crate::shards::queue_proxy_shard::QueueProxyShard;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::shard::Shard;
use crate::shards::transfer::transfer_tasks_pool::TransferTaskProgress;

// Whitebox hook for stopping in the cancellation window after `local.take()`.
#[cfg(test)]
static QUEUE_PROXIFY_LOCAL_AFTER_TAKE_HOOK: std::sync::Mutex<
    Option<tokio::sync::oneshot::Sender<()>>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
fn notify_queue_proxify_local_after_take() {
    if let Some(sender) = QUEUE_PROXIFY_LOCAL_AFTER_TAKE_HOOK.lock().unwrap().take() {
        let _ = sender.send(());
    }
}

struct TakenLocalShardGuard<'a> {
    slot: &'a mut Option<Shard>,
    original_shard: Option<Shard>,
}

impl Drop for TakenLocalShardGuard<'_> {
    fn drop(&mut self) {
        if let Some(original_shard) = self.original_shard.take() {
            log::warn!("Reverting taken shard due to cancel or panic");
            self.slot.get_or_insert(original_shard);
        }
    }
}

impl ShardReplicaSet {
    /// Convert `Local` shard into `ForwardProxy`.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn proxify_local(
        &self,
        remote_shard: RemoteShard,
        resharding_hash_ring: Option<HashRingRouter>,
        filter: Option<Filter>,
    ) -> CollectionResult<()> {
        let mut local = self.local.write().await;

        match local.deref() {
            // Expected state, continue
            Some(Shard::Local(_)) => {}

            // If a forward proxy to same remote, return early
            Some(Shard::ForwardProxy(proxy))
                if proxy.remote_shard.peer_id == remote_shard.peer_id =>
            {
                return Ok(());
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
        let Some(Shard::Local(local_shard)) = local.take() else {
            unreachable!()
        };

        let proxy_shard_res = ForwardProxyShard::new(
            self.shard_id,
            local_shard,
            remote_shard,
            resharding_hash_ring,
            filter,
        );

        match proxy_shard_res {
            Ok(proxy_shard) => {
                let _ = local.insert(Shard::ForwardProxy(proxy_shard));
                Ok(())
            }
            Err((err, local_shard)) => {
                log::warn!("Failed to proxify shard, reverting to local shard: {err}");
                let _ = local.insert(Shard::Local(local_shard));
                Err(err)
            }
        }
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
        progress: Arc<Mutex<TransferTaskProgress>>,
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

        // Get `wal_keep_from` and start `version` without "taking" local shard (to maintain cancel safety)
        let local_shard = match local.deref() {
            Some(Shard::Local(local)) => local,
            Some(Shard::ForwardProxy(proxy)) => &proxy.wrapped_shard,
            Some(Shard::Proxy(_)) => unreachable!(),
            Some(Shard::QueueProxy(_)) => unreachable!(),
            Some(Shard::Dummy(_)) => unreachable!(),
            None => unreachable!(),
        };

        let wal_keep_from = local_shard
            .update_handler
            .lock()
            .await
            .wal_keep_from
            .clone();

        let wal = local_shard.wal.wal.clone();
        let _wal_lock = wal.lock().await;

        let version = match from_version {
            None => _wal_lock.last_index() + 1,
            Some(version) => {
                // If start version is not in current WAL bounds [first_idx, last_idx + 1], we cannot reliably transfer WAL
                // Allow it to be one higher than the last index to only send new updates
                let (first_idx, last_idx) =
                    (_wal_lock.first_closed_index(), _wal_lock.last_index());
                if !(first_idx..=last_idx + 1).contains(&version) {
                    return Err(CollectionError::service_error(format!(
                        "Cannot create queue proxy shard from version {version} because it is out of WAL bounds ({first_idx}..={last_idx})",
                    )));
                }
                version
            }
        };

        // Proxify local shard
        let original_shard = match local.take() {
            Some(shard @ Shard::Local(_)) => shard,
            Some(shard @ Shard::ForwardProxy(_)) => shard,
            Some(Shard::Proxy(_)) => unreachable!(),
            Some(Shard::QueueProxy(_)) => unreachable!(),
            Some(Shard::Dummy(_)) => unreachable!(),
            None => unreachable!(),
        };

        // Guard to restore local shard in case of cancellation or panic while we construct the proxy
        let mut guard = TakenLocalShardGuard {
            slot: &mut local,
            original_shard: Some(original_shard),
        };

        #[cfg(test)]
        notify_queue_proxify_local_after_take();

        #[cfg(test)]
        // Yield to allow the test to cancel us while the shard is taken and the guard is active
        tokio::task::yield_now().await;

        let original_shard = guard.original_shard.take().unwrap();
        let local_shard = match original_shard {
            Shard::Local(local) => local,
            Shard::ForwardProxy(proxy) => proxy.wrapped_shard,
            Shard::Proxy(_) => unreachable!(),
            Shard::QueueProxy(_) => unreachable!(),
            Shard::Dummy(_) => unreachable!(),
        };

        let proxy_shard = QueueProxyShard::new_prevalidated(
            local_shard,
            remote_shard,
            wal_keep_from,
            version,
            progress,
        );

        let _ = guard.slot.insert(Shard::QueueProxy(proxy_shard));

        Ok(())
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

    /// Revert any proxy wrapper on the local shard back into a plain local shard, discarding
    /// all proxy state. Nothing is ever sent to a remote shard: queued updates are forgotten
    /// and update forwarding stops.
    ///
    /// Does nothing if there is no local shard, or if the local shard is not a proxy.
    ///
    /// This method cannot fail and performs no remote calls.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    ///
    /// If cancelled - the proxy may not be reverted to a local shard.
    pub async fn discard_proxy_local(&self) {
        let mut local = self.local.write().await;

        let Some(shard) = local.take() else {
            return;
        };

        // Making `await` calls between `local.take()` and `local.insert(...)` is *not* cancel safe!
        let shard = match shard {
            shard @ (Shard::Local(_) | Shard::Dummy(_)) => shard,
            Shard::ForwardProxy(proxy) => {
                log::debug!("Discarding forward proxy and reverting to local shard");
                Shard::Local(proxy.wrapped_shard)
            }
            Shard::QueueProxy(proxy) => {
                log::debug!("Forgetting queue proxy updates and reverting to local shard");
                let (local_shard, _) = proxy.forget_updates_and_finalize();
                Shard::Local(local_shard)
            }
            Shard::Proxy(proxy) => {
                log::debug!("Discarding proxy and reverting to local shard");
                Shard::Local(proxy.wrapped_shard)
            }
        };

        let _ = local.insert(shard);
    }

    /// Read a transfer batch without sending it yet.
    ///
    /// Returns a [`PreparedTransferBatch`] that holds the update lock. The caller can then drop
    /// other locks (e.g. the shard holder lock) before calling [`PreparedTransferBatch::send`].
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn read_transfer_batch(
        &self,
        offset: Option<PointIdType>,
        batch_size: usize,
        hashring_filter: Option<&HashRingRouter>,
        merge_points: bool,
    ) -> CollectionResult<PreparedTransferBatch> {
        let local = self.local.read().await;

        let Some(Shard::ForwardProxy(proxy)) = local.deref() else {
            return Err(CollectionError::service_error(format!(
                "Cannot transfer batch from shard {} because it is not proxified",
                self.shard_id
            )));
        };

        proxy
            .read_transfer_batch(
                offset,
                batch_size,
                hashring_filter,
                merge_points,
                &self.search_runtime,
            )
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

    /// Send all queue proxy updates to remote
    ///
    /// This method allows to transfer queued updates at any point, before the shard is
    /// unproxified for example. This allows for proper error handling at the time this method is
    /// called. Because the shard is transformed into a forward proxy after this operation it will
    /// not error again when the shard is eventually unproxified again.
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
    pub async fn queue_proxy_flush(&self) -> CollectionResult<()> {
        let local = self.local.read().await;

        let Some(Shard::QueueProxy(proxy)) = local.deref() else {
            return Ok(());
        };

        proxy.transfer_all_missed_updates().await?;

        Ok(())
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
        self.queue_proxy_flush().await?;

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
        let forward_proxy_res =
            ForwardProxyShard::new(self.shard_id, local_shard, remote_shard, None, None);

        match forward_proxy_res {
            Ok(forward_proxy) => {
                let _ = local.insert(Shard::ForwardProxy(forward_proxy));
                Ok(())
            }
            Err((err, local_shard)) => {
                log::warn!(
                    "Failed to transform queue proxy shard into forward proxy, reverting to local shard: {err}"
                );
                let _ = local.insert(Shard::Local(local_shard));
                Err(err)
            }
        }
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

    pub async fn wal_version(&self) -> CollectionResult<Option<u64>> {
        let local_shard_read = self.local.read().await;
        let Some(local_shard) = local_shard_read.deref() else {
            return Err(CollectionError::service_error(
                "Cannot get WAL version, shard replica set does not have local shard",
            ));
        };

        local_shard.wal_version().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use common::budget::ResourceBudget;
    use common::save_on_disk::SaveOnDisk;
    use segment::types::Distance;
    use tempfile::{Builder, TempDir};
    use tokio::runtime::Handle;
    use tokio::sync::{RwLock, oneshot};

    use super::*;
    use crate::collection::payload_index_schema::PayloadIndexSchema;
    use crate::common::adaptive_handle::AdaptiveSearchHandle;
    use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
    use crate::operations::shared_storage_config::SharedStorageConfig;
    use crate::operations::types::VectorsConfig;
    use crate::operations::vector_params_builder::VectorParamsBuilder;
    use crate::optimizers_builder::OptimizersConfig;
    use crate::shards::channel_service::ChannelService;
    use crate::shards::replica_set::replica_set_state::ReplicaState;
    use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_cancel_queue_proxify_after_local_take_drops_local_shard() {
        let collection_dir = Builder::new()
            .prefix("queue-proxy-cancel")
            .tempdir()
            .unwrap();
        let replica_set = new_shard_replica_set(&collection_dir).await;

        let (after_take_tx, after_take_rx) = oneshot::channel();
        install_queue_proxify_local_after_take_hook(after_take_tx);

        let remote_shard = RemoteShard::new(
            replica_set.shard_id,
            replica_set.collection_id.clone(),
            2,
            ChannelService::default(),
        );
        let progress = Arc::new(Mutex::new(TransferTaskProgress::new()));
        let cancel = cancel::CancellationToken::new();

        let queue_proxify = replica_set.queue_proxify_local(remote_shard, None, progress);
        let cancelable_queue_proxify =
            cancel::future::cancel_on_token(cancel.clone(), queue_proxify);
        tokio::pin!(cancelable_queue_proxify);

        tokio::select! {
            _ = after_take_rx => {}
            result = &mut cancelable_queue_proxify => {
                panic!("queue proxification completed before the cancellation window: {result:?}");
            }
        }

        cancel.cancel();
        let result = cancelable_queue_proxify.await;
        assert!(matches!(result, Err(cancel::Error::Cancelled)));

        assert!(
            replica_set.has_local_shard().await,
            "queue_proxification cancellation must not drop the local shard slot"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_cancel_queue_proxify_from_forward_proxy_restores_forward_proxy() {
        let collection_dir = Builder::new()
            .prefix("queue-proxy-cancel-fp")
            .tempdir()
            .unwrap();
        let replica_set = new_shard_replica_set(&collection_dir).await;

        let remote_shard = RemoteShard::new(
            replica_set.shard_id,
            replica_set.collection_id.clone(),
            2,
            ChannelService::default(),
        );

        // Turn local shard into ForwardProxy first
        replica_set
            .proxify_local(remote_shard.clone(), None, None)
            .await
            .unwrap();

        // Verify it is a ForwardProxy now
        {
            let local = replica_set.local.read().await;
            assert!(matches!(local.deref(), Some(Shard::ForwardProxy(_))));
        }

        let (after_take_tx, after_take_rx) = oneshot::channel();
        install_queue_proxify_local_after_take_hook(after_take_tx);

        let progress = Arc::new(Mutex::new(TransferTaskProgress::new()));
        let cancel = cancel::CancellationToken::new();

        let queue_proxify = replica_set.queue_proxify_local(remote_shard, None, progress);
        let cancelable_queue_proxify =
            cancel::future::cancel_on_token(cancel.clone(), queue_proxify);
        tokio::pin!(cancelable_queue_proxify);

        tokio::select! {
            _ = after_take_rx => {}
            result = &mut cancelable_queue_proxify => {
                panic!("queue proxification completed before the cancellation window: {result:?}");
            }
        }

        cancel.cancel();
        let result = cancelable_queue_proxify.await;
        assert!(matches!(result, Err(cancel::Error::Cancelled)));

        // Verify that the shard was restored specifically as a ForwardProxy (and not Local or None)
        let local = replica_set.local.read().await;
        match local.deref() {
            Some(Shard::ForwardProxy(_)) => {}
            Some(Shard::Local(_)) => panic!("Restored as Local instead of ForwardProxy"),
            Some(Shard::Proxy(_)) => panic!("Restored as Proxy"),
            Some(Shard::QueueProxy(_)) => panic!("Restored as QueueProxy"),
            Some(Shard::Dummy(_)) => panic!("Restored as Dummy"),
            None => panic!("Restored as None"),
        }
    }

    fn install_queue_proxify_local_after_take_hook(sender: oneshot::Sender<()>) {
        let mut hook = QUEUE_PROXIFY_LOCAL_AFTER_TAKE_HOOK.lock().unwrap();
        assert!(
            hook.is_none(),
            "queue-proxify test hook is already installed"
        );
        *hook = Some(sender);
    }

    async fn new_shard_replica_set(collection_dir: &TempDir) -> ShardReplicaSet {
        let update_runtime = Handle::current();
        let search_runtime = AdaptiveSearchHandle::current_for_tests();

        let wal_config = WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
            wal_retain_closed: 1,
        };

        let collection_params = CollectionParams {
            vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
            shard_number: NonZeroU32::new(1).unwrap(),
            replication_factor: NonZeroU32::new(1).unwrap(),
            write_consistency_factor: NonZeroU32::new(1).unwrap(),
            ..CollectionParams::empty()
        };

        let optimizers_config = OptimizersConfig::fixture();
        let config = CollectionConfigInternal {
            params: collection_params,
            optimizer_config: optimizers_config.clone(),
            wal_config,
            hnsw_config: Default::default(),
            quantization_config: None,
            strict_mode_config: None,
            uuid: None,
            metadata: None,
        };

        let payload_index_schema_file = collection_dir.path().join("payload-schema.json");
        let payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
            Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());
        let shared_config = Arc::new(RwLock::new(config));

        ShardReplicaSet::build(
            1,
            None,
            "test_collection".to_string(),
            1,
            true,
            HashSet::from([2]),
            dummy_on_replica_failure(),
            dummy_abort_shard_transfer(),
            collection_dir.path(),
            shared_config,
            optimizers_config,
            Arc::new(SharedStorageConfig::default()),
            payload_index_schema,
            ChannelService::default(),
            update_runtime,
            search_runtime,
            ResourceBudget::default(),
            Some(ReplicaState::Active),
        )
        .await
        .unwrap()
    }

    fn dummy_on_replica_failure() -> ChangePeerFromState {
        Arc::new(move |_peer_id, _shard_id, _from_state| {})
    }

    fn dummy_abort_shard_transfer() -> AbortShardTransfer {
        Arc::new(|_shard_transfer, _reason| {})
    }
}
