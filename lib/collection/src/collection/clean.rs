use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use cancel::{CancellationToken, DropGuard};
use parking_lot::RwLock;
use segment::types::{Condition, Filter};
use tokio::sync::watch::{Receiver, Sender};
use tokio::task::JoinHandle;

use super::Collection;
use crate::operations::types::{CollectionError, CollectionResult, UpdateResult, UpdateStatus};
use crate::operations::{CollectionUpdateOperations, OperationWithClockTag};
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::LockedShardHolder;

const CLEAN_BATCH_SIZE: usize = 5_000;

#[derive(Debug, Clone)]
pub(super) enum ShardCleanStatus {
    Started,
    Done,
    Failed(String),
    Cancelled,
}

/// A collection of local shard clean tasks
///
/// Manages tasks for shards and allows easily awaiting a completion state.
///
/// These tasks are not persisted in any way and are lost on restart. In case of resharding,
/// cluster manager will take care of calling the task again and again until it eventually
/// completes. Once it completes it does not have to be run again on restart.
#[derive(Default)]
pub(super) struct ShardCleanTasks {
    tasks: Arc<RwLock<HashMap<ShardId, ShardCleanTask>>>,
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
    async fn await_task(
        mut receiver: Receiver<ShardCleanStatus>,
        wait: bool,
        timeout: Option<Duration>,
    ) -> CollectionResult<UpdateStatus> {
        let start = Instant::now();

        loop {
            match receiver.borrow_and_update().deref() {
                ShardCleanStatus::Started => {}
                ShardCleanStatus::Done => return Ok(UpdateStatus::Completed),
                ShardCleanStatus::Failed(err) => {
                    return Err(CollectionError::service_error(format!(
                        "failed to clean shard points: {err}",
                    )))
                }
                ShardCleanStatus::Cancelled => {
                    return Err(CollectionError::service_error(
                        "failed to clean shard points due to cancellation, please try again",
                    ))
                }
            }

            if !wait {
                return Ok(UpdateStatus::Acknowledged);
            }

            let result = if let Some(timeout) = timeout {
                tokio::time::timeout(timeout - start.elapsed(), receiver.changed()).await
            } else {
                Ok(receiver.changed().await)
            };
            match result {
                // Status updated, loop again to check it another time
                Ok(Ok(_)) => continue,
                // Channel dropped, return error
                Ok(Err(_)) => {
                    return Err(CollectionError::service_error(
                        "Failed to clean shard points, notification channel dropped",
                    ))
                }
                // Timeout elapsed, acknowledge so the client can probe again later
                Err(_) => return Ok(UpdateStatus::Acknowledged),
            }
        }
    }

    /// Cancel cleaning of a shard and mark it as dirty
    pub(super) fn invalidate(&self, shard_id: ShardId) {
        let removed = self.tasks.write().remove(&shard_id);
        if let Some(task) = removed {
            task.abort();
        }
    }

    /// List shards that are currently undergoing cleaning
    pub fn list_ongoing(&self) -> Vec<ShardId> {
        self.tasks
            .read()
            .iter()
            .filter(|(_, task)| task.is_ongoing())
            .map(|(shard_id, _)| *shard_id)
            .collect()
    }
}

/// A background task for cleaning a shard
///
/// The task will delete points that don't belong in the shard according to the hash ring. This is
/// used in context of resharding, where points are transferred to different shards.
pub(super) struct ShardCleanTask {
    /// Handle of the clean task
    #[allow(dead_code)]
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

        let task = tokio::task::spawn(Self::task(sender, shard_holder, shard_id, cancel.clone()));

        ShardCleanTask {
            handle: task,
            status: receiver.clone(),
            cancel: cancel.drop_guard(),
        }
    }

    pub fn is_ongoing(&self) -> bool {
        matches!(self.status.borrow().deref(), ShardCleanStatus::Started)
    }

    pub fn is_cancelled_or_failed(&self) -> bool {
        matches!(
            self.status.borrow().deref(),
            ShardCleanStatus::Cancelled | ShardCleanStatus::Failed(_)
        )
    }

    pub fn abort(self) {
        // Explicitly cancel clean task
        // We don't have to because the drop guard does it for us, but this makes it more explicit
        self.cancel.disarm().cancel();
    }

    async fn task(
        sender: Sender<ShardCleanStatus>,
        shard_holder: Weak<LockedShardHolder>,
        shard_id: ShardId,
        cancel: CancellationToken,
    ) {
        let mut offset = None;

        let status = loop {
            // Check if cancelled
            if cancel.is_cancelled() {
                break ShardCleanStatus::Cancelled;
            }

            // Get shard
            let Some(shard_holder) = shard_holder.upgrade() else {
                break ShardCleanStatus::Failed("Shard holder dropped".into());
            };
            let shard_holder = shard_holder.read().await;
            let Some(shard) = shard_holder.get_shard(shard_id) else {
                break ShardCleanStatus::Failed(format!("Shard {shard_id} not found"));
            };
            if !shard.is_local().await {
                break ShardCleanStatus::Failed(format!("Shard {shard_id} is not a local shard"));
            }

            // Scroll batch of points with hash ring filter
            let filter = shard_holder
                .hash_ring_filter(shard_id)
                .expect("hash ring filter");
            let filter = Filter::new_must_not(Condition::CustomIdChecker(Arc::new(filter)));
            let mut ids = match shard
                .scroll_by(
                    offset,
                    CLEAN_BATCH_SIZE + 1,
                    &false.into(),
                    &false.into(),
                    Some(&filter),
                    None,
                    true,
                    None,
                    None,
                )
                .await
            {
                Ok(batch) => batch.into_iter().map(|entry| entry.id).collect::<Vec<_>>(),
                Err(err) => {
                    break ShardCleanStatus::Failed(format!(
                        "Failed to read points to delete from shard: {err}"
                    ));
                }
            };

            // Check if cancelled
            if cancel.is_cancelled() {
                break ShardCleanStatus::Cancelled;
            }

            // Update offset for next batch
            offset = (ids.len() > CLEAN_BATCH_SIZE).then(|| ids.pop().unwrap());
            let last_batch = offset.is_none();

            // Delete points from local shard
            let delete_operation =
                OperationWithClockTag::from(CollectionUpdateOperations::PointOperation(
                    crate::operations::point_ops::PointOperations::DeletePoints { ids },
                ));
            if let Err(err) = shard.update_local(delete_operation, true).await {
                break ShardCleanStatus::Failed(format!(
                    "Failed to delete points from shard: {err}"
                ));
            }

            // Finish if this was the last batch
            if last_batch {
                break ShardCleanStatus::Done;
            }
        };

        let _ = sender.send(status);
    }
}

impl Collection {
    pub async fn cleanup_local_shard(
        &self,
        shard_id: ShardId,
        wait: bool,
        timeout: Option<Duration>,
    ) -> CollectionResult<UpdateResult> {
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

    pub(super) fn invalidate_clean_local_shard(&self, shard_id: ShardId) {
        self.shard_clean_tasks.invalidate(shard_id);
    }

    pub fn list_clean_local_shards(&self) -> Vec<ShardId> {
        self.shard_clean_tasks.list_ongoing()
    }
}
