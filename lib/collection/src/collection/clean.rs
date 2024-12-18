use std::ops::Deref;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use segment::types::{Condition, Filter};
use tokio::sync::watch::{Receiver, Sender};

use super::Collection;
use crate::operations::types::{CollectionError, CollectionResult, UpdateStatus};
use crate::operations::{CollectionUpdateOperations, OperationWithClockTag};
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::LockedShardHolder;

const CLEAN_BATCH_SIZE: usize = 10_000;

#[derive(Debug, Clone)]
pub(super) enum ShardCleanStatus {
    Started,
    Done,
    Failed(String),
}

impl Collection {
    pub async fn clean_local_shard(
        &self,
        shard_id: ShardId,
        wait: bool,
        timeout: Option<Duration>,
    ) -> CollectionResult<UpdateStatus> {
        let clean_tasks = self.shard_clean_tasks.upgradable_read();

        // Bind to existing task
        if let Some((_, receiver)) = clean_tasks.get(&shard_id) {
            let receiver = receiver.clone();
            drop(clean_tasks);
            return self.await_clean_local_shard(receiver, wait, timeout).await;
        }

        // Create new task
        let receiver = {
            let mut clean_tasks = parking_lot::RwLockUpgradableReadGuard::upgrade(clean_tasks);

            let (sender, receiver) = tokio::sync::watch::channel(ShardCleanStatus::Started);
            let shard_holder = Arc::downgrade(&self.shards_holder);
            let task =
                tokio::task::spawn(Self::clean_local_shard_task(sender, shard_holder, shard_id));
            clean_tasks.insert(shard_id, (task, receiver.clone()));

            receiver
        };

        self.await_clean_local_shard(receiver, wait, timeout).await
    }

    async fn clean_local_shard_task(
        sender: Sender<ShardCleanStatus>,
        shard_holder: Weak<LockedShardHolder>,
        shard_id: ShardId,
    ) {
        let mut offset = None;

        let status = loop {
            // Get shard
            let Some(shard_holder) = shard_holder.upgrade() else {
                break ShardCleanStatus::Failed("Shard holder dropped".into());
            };
            let shard_holder = shard_holder.read().await;
            let Some(shard) = shard_holder.get_shard(shard_id) else {
                break ShardCleanStatus::Failed(format!("Shard {shard_id} not found"));
            };

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

    async fn await_clean_local_shard(
        &self,
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
                        "failed to clean shard points: {err}"
                    )));
                }
            }

            if !wait {
                return Ok(UpdateStatus::Acknowledged);
            }

            // Await receiver with or without timeout
            let result = if let Some(timeout) = timeout {
                match tokio::time::timeout(timeout - start.elapsed(), receiver.changed()).await {
                    Ok(notified) => notified,
                    Err(_elapsed) => return Ok(UpdateStatus::Acknowledged),
                }
            } else {
                receiver.changed().await
            };
            if let Err(err) = result {
                return Err(CollectionError::service_error(format!(
                    "failed to clean shard points, notification channel dropped: {err}"
                )));
            }
        }
    }
}
