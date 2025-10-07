use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use ahash::AHashMap;
use cancel::{CancellationToken, DropGuard};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use parking_lot::RwLock;
use segment::types::ExtendedPointId;
use tokio::sync::watch::{Receiver, Sender};
use tokio::task::JoinHandle;

use super::Collection;
use crate::operations::types::{CollectionError, CollectionResult, UpdateResult, UpdateStatus};
use crate::operations::{CollectionUpdateOperations, OperationWithClockTag};
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::LockedShardHolder;
use crate::telemetry::{
    ShardCleanStatusFailedTelemetry, ShardCleanStatusProgressTelemetry, ShardCleanStatusTelemetry,
};

/// Number of points the delete background task will delete in each iteration.
///
/// This number is arbitrary and seemed 'good enough' in local testing. It is not too low to
/// prevent having a huge amount of iterations, nor is it too large causing latency spikes in user
/// operations.
const CLEAN_BATCH_SIZE: usize = 5_000;

/// A collection of local shard clean tasks
///
/// Manages tasks for shards and allows easily awaiting a completion state.
///
/// These tasks are not persisted in any way and are lost on restart. In case of resharding,
/// cluster manager will take care of calling the task again and again until it eventually
/// completes. Once it completes it does not have to be run again on restart.
#[derive(Default)]
pub(super) struct ShardCleanTasks {
    tasks: Arc<RwLock<AHashMap<ShardId, ShardCleanTask>>>,
}

impl ShardCleanTasks {
    /// Clean a shard and await for the operation to finish
    ///
    /// Creates a new task if none is going, the task failed or if the task was invalidated. Joins
    /// the existing task if one is currently ongoing or if it successfully finished.
    ///
    /// If `wait` is `false`, the function will return immediately after creating a new task with
    /// the `Acknowledged` status. If `wait` is `true` this function will await at most `timeout`
    /// for the task to finish. If the task is not completed within the timeout the `Acknowledged`
    /// status is returned.
    ///
    /// To probe for completeness this function must be called again repeatedly until it returns
    /// the `Completed` status.
    ///
    /// # Cancel safety
    ///
    /// This function is cancel safe. It either will or will not spawn a task if cancelled, and
    /// will not abort any ongoing task.
    async fn clean_and_await(
        &self,
        shards_holder: &Arc<LockedShardHolder>,
        shard_id: ShardId,
        wait: bool,
        timeout: Option<Duration>,
    ) -> CollectionResult<UpdateStatus> {
        let mut tasks = self.tasks.upgradable_read();

        // Await existing task if not cancelled or failed
        if let Some(task) = tasks
            .get(&shard_id)
            .filter(|task| !task.is_cancelled_or_failed())
        {
            let receiver = task.status.clone();
            drop(tasks);
            return Self::await_task(receiver, wait, timeout).await;
        }

        // Create and await new task
        let receiver = tasks.with_upgraded(|tasks| {
            let task = ShardCleanTask::new(shards_holder, shard_id);
            let receiver = task.status.clone();
            tasks.insert(shard_id, task);
            receiver
        });
        drop(tasks);

        Self::await_task(receiver, wait, timeout).await
    }

    /// Await for an ongoing task to finish by its status receiver
    ///
    /// # Cancel safety
    ///
    /// This function is cancel safe.
    async fn await_task(
        mut receiver: Receiver<ShardCleanStatus>,
        wait: bool,
        timeout: Option<Duration>,
    ) -> CollectionResult<UpdateStatus> {
        let start = Instant::now();

        loop {
            match receiver.borrow_and_update().deref() {
                ShardCleanStatus::Started => {}
                ShardCleanStatus::Progress { .. } => {}
                ShardCleanStatus::Done => return Ok(UpdateStatus::Completed),
                ShardCleanStatus::Failed { reason } => {
                    return Err(CollectionError::service_error(format!(
                        "Failed to clean shard points: {reason}",
                    )));
                }
                ShardCleanStatus::Cancelled => {
                    return Err(CollectionError::service_error(
                        "Failed to clean shard points due to cancellation, please try again",
                    ));
                }
            }

            if !wait {
                return Ok(UpdateStatus::Acknowledged);
            }

            let result = if let Some(timeout) = timeout {
                tokio::time::timeout(timeout.saturating_sub(start.elapsed()), receiver.changed())
                    .await
            } else {
                Ok(receiver.changed().await)
            };
            match result {
                // Status updated, loop again to check it another time
                Ok(Ok(_)) => (),
                // Channel dropped, return error
                Ok(Err(_)) => {
                    return Err(CollectionError::service_error(
                        "Failed to clean shard points, notification channel dropped",
                    ));
                }
                // Timeout elapsed, acknowledge so the client can probe again later
                Err(_) => return Ok(UpdateStatus::Acknowledged),
            }
        }
    }

    /// Invalidate shard cleaning operations for the given shards, marking them as dirty
    ///
    /// Aborts any ongoing cleaning tasks and waits until all tasks are stopped.
    ///
    /// # Cancel safety
    ///
    /// This function is cancel safe. If cancelled, we may not actually await on all tasks to
    /// finish but they will always still abort in the background.
    pub(super) async fn invalidate(&self, shard_ids: impl IntoIterator<Item = ShardId>) {
        // Take relevant tasks out of task list, abort and take handles
        let handles = {
            let mut tasks = self.tasks.write();
            shard_ids
                .into_iter()
                .filter_map(|shard_id| tasks.remove(&shard_id))
                .map(ShardCleanTask::abort)
                .collect::<Vec<_>>()
        };

        // Await all tasks to finish
        for handle in handles {
            if let Err(err) = handle.await {
                log::error!("Failed to join shard clean task: {err}");
            }
        }
    }

    /// List all shards we have any status for
    ///
    /// Only includes shards we've triggered cleaning for. On restart, or when invalidating shards,
    /// items are removed from the list.
    pub fn statuses(&self) -> AHashMap<ShardId, ShardCleanStatus> {
        self.tasks
            .read()
            .iter()
            .map(|(shard_id, task)| (*shard_id, task.status.borrow().clone()))
            .collect()
    }
}

/// A background task for cleaning a shard
///
/// The task will delete points that don't belong in the shard according to the hash ring. This is
/// used in context of resharding, where points are transferred to different shards.
pub(super) struct ShardCleanTask {
    /// Handle of the clean task
    handle: JoinHandle<()>,
    /// Watch channel with current status of the task
    status: Receiver<ShardCleanStatus>,
    /// Cancellation token drop guard, cancels the task if this is dropped
    cancel: DropGuard,
}

impl ShardCleanTask {
    /// Create a new shard clean task and immediately execute it
    pub fn new(shards_holder: &Arc<LockedShardHolder>, shard_id: ShardId) -> Self {
        let (sender, receiver) = tokio::sync::watch::channel(ShardCleanStatus::Started);
        let shard_holder = Arc::downgrade(shards_holder);
        let cancel = CancellationToken::default();

        let task = tokio::task::spawn(Self::task(shard_holder, shard_id, sender, cancel.clone()));

        ShardCleanTask {
            handle: task,
            status: receiver.clone(),
            cancel: cancel.drop_guard(),
        }
    }

    pub fn is_cancelled_or_failed(&self) -> bool {
        matches!(
            self.status.borrow().deref(),
            ShardCleanStatus::Cancelled | ShardCleanStatus::Failed { .. }
        )
    }

    fn abort(self) -> JoinHandle<()> {
        // Explicitly cancel clean task
        self.cancel.disarm().cancel();
        self.handle
    }

    async fn task(
        shard_holder: Weak<LockedShardHolder>,
        shard_id: ShardId,
        sender: Sender<ShardCleanStatus>,
        cancel: CancellationToken,
    ) {
        let task = clean_task(shard_holder, shard_id, sender.clone());
        let status = match cancel.run_until_cancelled(task).await {
            Some(Ok(())) => {
                log::trace!("Background task to clean shard {shard_id} is completed");
                ShardCleanStatus::Done
            }
            Some(Err(err)) => {
                log::error!("Background task to clean shard {shard_id} failed: {err}");
                ShardCleanStatus::Failed {
                    reason: err.to_string(),
                }
            }
            None => {
                log::trace!("Background task to clean shard {shard_id} is cancelled");
                ShardCleanStatus::Cancelled
            }
        };

        // Ignore channel dropped error, then there's no one listening anyway
        let _ = sender.send(status);
    }
}

async fn clean_task(
    shard_holder: Weak<LockedShardHolder>,
    shard_id: ShardId,
    sender: Sender<ShardCleanStatus>,
) -> CollectionResult<()> {
    // Do not measure the hardware usage of these deletes as clean the shard is always considered an internal operation
    // users should not be billed for.

    let mut offset = None;
    let mut deleted_points = 0;

    loop {
        // Get shard
        let Some(shard_holder) = shard_holder.upgrade() else {
            return Err(CollectionError::not_found("Shard holder dropped"));
        };
        let shard_holder = shard_holder.read().await;
        let Some(shard) = shard_holder.get_shard(shard_id) else {
            return Err(CollectionError::not_found(format!(
                "Shard {shard_id} not found",
            )));
        };
        if !shard.is_local().await {
            return Err(CollectionError::not_found(format!(
                "Shard {shard_id} is not a local shard",
            )));
        }

        // Scroll next batch of points
        let mut ids = match shard
            .scroll_by(
                offset,
                CLEAN_BATCH_SIZE + 1,
                &false.into(),
                &false.into(),
                None,
                None,
                true,
                None,
                None,
                HwMeasurementAcc::disposable(), // Internal operation, no measurement needed!
            )
            .await
        {
            Ok(batch) => batch.into_iter().map(|entry| entry.id).collect::<Vec<_>>(),
            Err(err) => {
                return Err(CollectionError::service_error(format!(
                    "Failed to read points to delete from shard: {err}",
                )));
            }
        };

        // Update offset for next batch
        offset = (ids.len() > CLEAN_BATCH_SIZE).then(|| ids.pop().unwrap());
        deleted_points += ids.len();
        let last_batch = offset.is_none();

        // Filter list of point IDs after scrolling, delete points that don't belong in this shard
        // Checking the hash ring to determine if a point belongs in the shard is very expensive.
        // We scroll all point IDs and only filter points by the hash ring after scrolling on
        // purpose, this way we check each point ID against the hash ring only once. Naively we
        // might pass a hash ring filter into the scroll operation itself, but that will make it
        // significantly slower, because then we'll do the expensive hash ring check on every
        // point, in every segment.
        // See: <https://github.com/qdrant/qdrant/pull/6085>
        let hashring = shard_holder.hash_ring_router(shard_id).ok_or_else(|| {
            CollectionError::service_error(format!(
                "Shard {shard_id} cannot be cleaned, failed to get shard hash ring"
            ))
        })?;
        let ids: Vec<ExtendedPointId> = ids
            .into_iter()
            // TODO: run test with this inverted?
            .filter(|id| !hashring.is_in_shard(id, shard_id))
            .collect();

        // Delete points from local shard
        let delete_operation =
            OperationWithClockTag::from(CollectionUpdateOperations::PointOperation(
                crate::operations::point_ops::PointOperations::DeletePoints { ids },
            ));
        if let Err(err) = shard
            .update_local(
                delete_operation,
                last_batch,
                HwMeasurementAcc::disposable(),
                false,
            )
            .await
        {
            return Err(CollectionError::service_error(format!(
                "Failed to delete points from shard: {err}",
            )));
        }

        let _ = sender.send(ShardCleanStatus::Progress { deleted_points });

        // Finish if this was the last batch
        if last_batch {
            return Ok(());
        }
    }
}

impl Collection {
    pub async fn cleanup_local_shard(
        &self,
        shard_id: ShardId,
        wait: bool,
        timeout: Option<Duration>,
    ) -> CollectionResult<UpdateResult> {
        // Ensure we have this local shard
        {
            let shard_holder = self.shards_holder.read().await;
            let Some(shard) = shard_holder.get_shard(shard_id) else {
                return Err(CollectionError::not_found(format!(
                    "Shard {shard_id} not found",
                )));
            };
            if !shard.is_local().await {
                return Err(CollectionError::not_found(format!(
                    "Shard {shard_id} is not a local shard",
                )));
            }
        }

        let status = self
            .shard_clean_tasks
            .clean_and_await(&self.shards_holder, shard_id, wait, timeout)
            .await?;

        Ok(UpdateResult {
            operation_id: None,
            status,
            clock_tag: None,
        })
    }

    /// Invalidate shard cleaning operations for the given shards
    ///
    /// Aborts any ongoing cleaning tasks and waits until all tasks are stopped.
    ///
    /// This does nothing if the given shards have no known status or do not exist.
    ///
    /// # Cancel safety
    ///
    /// This function is cancel safe. If cancelled, we may not actually await on all tasks to
    /// finish but they will always still abort in the background.
    pub(super) async fn invalidate_clean_local_shards(
        &self,
        shard_ids: impl IntoIterator<Item = ShardId>,
    ) {
        self.shard_clean_tasks.invalidate(shard_ids).await;
    }

    pub fn clean_local_shards_statuses(&self) -> HashMap<ShardId, ShardCleanStatusTelemetry> {
        self.shard_clean_tasks
            .statuses()
            .into_iter()
            .map(|(shard_id, status)| (shard_id, status.into()))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(super) enum ShardCleanStatus {
    Started,
    Progress { deleted_points: usize },
    Done,
    Failed { reason: String },
    Cancelled,
}

impl From<ShardCleanStatus> for ShardCleanStatusTelemetry {
    fn from(status: ShardCleanStatus) -> Self {
        match status {
            ShardCleanStatus::Started => ShardCleanStatusTelemetry::Started,
            ShardCleanStatus::Progress { deleted_points } => {
                ShardCleanStatusTelemetry::Progress(ShardCleanStatusProgressTelemetry {
                    deleted_points,
                })
            }
            ShardCleanStatus::Done => ShardCleanStatusTelemetry::Done,
            ShardCleanStatus::Failed { reason } => {
                ShardCleanStatusTelemetry::Failed(ShardCleanStatusFailedTelemetry { reason })
            }
            ShardCleanStatus::Cancelled => ShardCleanStatusTelemetry::Cancelled,
        }
    }
}
