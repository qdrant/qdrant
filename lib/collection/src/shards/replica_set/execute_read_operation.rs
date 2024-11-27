use std::fmt::Write as _;
use std::ops::Deref as _;

use futures::future::{self, BoxFuture};
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use rand::seq::SliceRandom as _;

use super::ShardReplicaSet;
use crate::operations::consistency_params::{ReadConsistency, ReadConsistencyType};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::remote_shard::RemoteShard;
use crate::shards::resolve::{Resolve, ResolveCondition};
use crate::shards::shard::Shard;
use crate::shards::shard_trait::ShardOperation;

impl ShardReplicaSet {
    /// Execute read op. on replica set:
    /// 1 - Prefer local replica
    /// 2 - Otherwise uses `read_fan_out_ratio` to compute list of active remote shards.
    /// 3 - Fallbacks to all remaining shards if the optimisations fails.
    /// It does not report failing peer_ids to the consensus.
    pub async fn execute_read_operation<Res, F>(
        &self,
        read_operation: F,
        local_only: bool,
    ) -> CollectionResult<Res>
    where
        F: Fn(&(dyn ShardOperation + Send + Sync)) -> BoxFuture<'_, CollectionResult<Res>>,
    {
        if local_only {
            return self.execute_local_read_operation(read_operation).await;
        }

        let mut responses = self
            .execute_cluster_read_operation(read_operation, 1, None)
            .await?;

        Ok(responses.pop().unwrap())
    }

    pub async fn execute_and_resolve_read_operation<Res, F>(
        &self,
        read_operation: F,
        read_consistency: Option<ReadConsistency>,
        local_only: bool,
    ) -> CollectionResult<Res>
    where
        F: Fn(&(dyn ShardOperation + Send + Sync)) -> BoxFuture<'_, CollectionResult<Res>>,
        Res: Resolve,
    {
        if local_only {
            return self.execute_local_read_operation(read_operation).await;
        }

        let read_consistency = read_consistency.unwrap_or_default();

        let local_count = usize::from(self.peer_state(self.this_peer_id()).is_some());
        let active_local_count = usize::from(self.peer_is_active(self.this_peer_id()));

        let remotes = self.remotes.read().await;

        let remotes_count = remotes.len();

        // TODO(resharding): Handle resharded shard?
        let active_remotes_count = remotes
            .iter()
            .filter(|remote| self.peer_is_active(remote.peer_id))
            .count();

        let total_count = local_count + remotes_count;
        let active_count = active_local_count + active_remotes_count;

        let (required_successful_results, condition) = match read_consistency {
            ReadConsistency::Type(ReadConsistencyType::All) => (total_count, ResolveCondition::All),

            ReadConsistency::Type(ReadConsistencyType::Majority) => {
                (total_count, ResolveCondition::Majority)
            }

            ReadConsistency::Type(ReadConsistencyType::Quorum) => {
                (total_count / 2 + 1, ResolveCondition::All)
            }

            ReadConsistency::Factor(factor) => {
                (factor.clamp(1, total_count), ResolveCondition::All)
            }
        };

        if active_count < required_successful_results {
            return Err(CollectionError::service_error(format!(
                "The replica set for shard {} on peer {} does not have enough active replicas",
                self.shard_id,
                self.this_peer_id(),
            )));
        }

        let mut responses = self
            .execute_cluster_read_operation(
                read_operation,
                required_successful_results,
                Some(remotes),
            )
            .await?;

        if responses.len() == 1 {
            Ok(responses.pop().unwrap())
        } else {
            Ok(Res::resolve(responses, condition))
        }
    }

    async fn execute_local_read_operation<Res, F>(&self, read_operation: F) -> CollectionResult<Res>
    where
        F: Fn(&(dyn ShardOperation + Send + Sync)) -> BoxFuture<'_, CollectionResult<Res>>,
    {
        let local = self.local.read().await;

        let Some(local) = local.deref() else {
            return Err(CollectionError::service_error(format!(
                "Local shard {} not found",
                self.shard_id
            )));
        };

        read_operation(local.get()).await
    }

    async fn execute_cluster_read_operation<Res, F>(
        &self,
        read_operation: F,
        required_successful_results: usize,
        remotes: Option<tokio::sync::RwLockReadGuard<'_, Vec<RemoteShard>>>,
    ) -> CollectionResult<Vec<Res>>
    where
        F: Fn(&(dyn ShardOperation + Send + Sync)) -> BoxFuture<'_, CollectionResult<Res>>,
    {
        let remotes = match remotes {
            Some(remotes) => remotes,
            None => self.remotes.read().await,
        };

        let (local, is_local_ready, update_watcher) = match self.local.try_read() {
            Ok(local) => {
                let update_watcher = local.deref().as_ref().map(Shard::watch_for_update);

                let is_local_ready = local
                    .deref()
                    .as_ref()
                    .is_some_and(|local| !local.is_update_in_progress());

                (
                    future::ready(local).left_future(),
                    is_local_ready,
                    update_watcher,
                )
            }

            Err(_) => (self.local.read().right_future(), false, None),
        };

        let local_is_active = self.peer_is_active(self.this_peer_id());

        let local_operation = if local_is_active {
            let local_operation = async {
                let local = local.await;

                let Some(local) = local.deref() else {
                    return Err(CollectionError::service_error(format!(
                        "Local shard {} not found",
                        self.shard_id
                    )));
                };

                read_operation(local.get()).await
            };

            Some(local_operation.map(|result| (result, true)).left_future())
        } else {
            None
        };

        // TODO(resharding): Handle resharded shard?
        let mut active_remotes: Vec<_> = remotes
            .iter()
            .filter(|remote| self.peer_is_active(remote.peer_id))
            .collect();

        active_remotes.shuffle(&mut rand::thread_rng());

        let remote_operations = active_remotes.into_iter().map(|remote| {
            read_operation(remote)
                .map(|result| (result, false))
                .right_future()
        });

        let mut operations = local_operation.into_iter().chain(remote_operations);

        // Possible scenarios:
        //
        // - Local is available: default fan-out is 0 (no fan-out, unless explicitly requested)
        // - Local is not available: default fan-out is 1
        // - There is no local: default fan-out is 1

        let default_fan_out = if is_local_ready && local_is_active {
            0
        } else {
            1
        };

        let read_fan_out_factor: usize = self
            .collection_config
            .read()
            .await
            .params
            .read_fan_out_factor
            .unwrap_or(default_fan_out)
            .try_into()
            .expect("u32 can be converted into usize");

        let initial_concurrent_operations = required_successful_results + read_fan_out_factor;

        let mut pending_operations: FuturesUnordered<_> = operations
            .by_ref()
            .take(initial_concurrent_operations)
            .collect();

        let mut responses = Vec::new();
        let mut errors = Vec::new();

        let mut is_local_operation_resolved = false;

        let update_watcher = async move {
            match update_watcher {
                Some(update_watcher) => update_watcher.await,
                None => future::pending().await,
            }
        };

        let update_watcher = update_watcher.fuse();

        tokio::pin!(update_watcher);

        loop {
            let result;

            tokio::select! {
                operation_result = pending_operations.next() => {
                    let Some(operation_result) = operation_result else {
                        break;
                    };

                    let (operation_result, is_local_operation) = operation_result;

                    result = operation_result;

                    if is_local_operation {
                        is_local_operation_resolved = true;
                    }
                }

                _ = &mut update_watcher, if local_is_active && !is_local_operation_resolved => {
                    pending_operations.extend(operations.next());
                    continue;
                }
            }

            match result {
                Ok(response) => {
                    responses.push(response);

                    if responses.len() >= required_successful_results {
                        break;
                    }
                }

                Err(error) => {
                    if error.is_transient() {
                        log::debug!("Read operation failed: {error}");
                        errors.push(error);
                    } else {
                        return Err(error);
                    }

                    pending_operations.extend(operations.next());

                    if responses.len() + pending_operations.len() < required_successful_results {
                        break;
                    }
                }
            }
        }

        if responses.len() >= required_successful_results {
            Ok(responses)
        } else {
            let errors_count = errors.len();
            let operations_count = responses.len() + errors.len();
            let errors_separator = if !errors.is_empty() { ":" } else { "" };

            let mut message = format!(
                "{errors_count} of {operations_count} read operations failed{errors_separator}"
            );

            for error in errors {
                write!(&mut message, "\n  {error}").expect("writing into String always succeeds");
            }

            Err(CollectionError::service_error(message))
        }
    }
}
