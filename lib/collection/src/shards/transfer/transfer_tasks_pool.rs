use std::collections::HashMap;

use crate::common::stoppable_task_async::CancellableAsyncTaskHandle;
use crate::shards::transfer::{ShardTransfer, ShardTransferKey};
use crate::shards::CollectionId;

pub struct TransferTasksPool {
    collection_id: CollectionId,
    tasks: HashMap<ShardTransferKey, CancellableAsyncTaskHandle<bool>>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum TaskResult {
    Finished,
    NotFound,
    Stopped,
    Failed,
}

impl TaskResult {
    pub fn is_finished(&self) -> bool {
        matches!(self, TaskResult::Finished)
    }
}

impl TransferTasksPool {
    pub fn new(collection_id: CollectionId) -> Self {
        Self {
            collection_id,
            tasks: HashMap::new(),
        }
    }

    /// Returns true if transfer task is still running
    pub fn check_if_still_running(&self, transfer_key: &ShardTransferKey) -> bool {
        if let Some(task) = self.tasks.get(transfer_key) {
            !task.is_finished()
        } else {
            false
        }
    }

    /// Return true if task finished
    /// Return false if task failed or stopped
    /// Return None if task not found or not finished
    pub fn get_task_result(&self, transfer_key: &ShardTransferKey) -> Option<bool> {
        if let Some(task) = self.tasks.get(transfer_key) {
            task.get_result()
        } else {
            None
        }
    }

    /// Returns true if the task was actually stopped
    /// Returns false if the task was not found
    pub async fn stop_if_exists(&mut self, transfer_key: &ShardTransferKey) -> TaskResult {
        if let Some(task) = self.tasks.remove(transfer_key) {
            match task.cancel().await {
                Ok(res) => {
                    if res {
                        log::info!(
                            "Transfer of shard {}:{} -> {} finished",
                            self.collection_id,
                            transfer_key.shard_id,
                            transfer_key.to
                        );
                        TaskResult::Finished
                    } else {
                        log::info!(
                            "Transfer of shard {}:{} -> {} stopped",
                            self.collection_id,
                            transfer_key.shard_id,
                            transfer_key.to
                        );
                        TaskResult::Stopped
                    }
                }
                Err(err) => {
                    log::warn!(
                        "Transfer task for shard {}:{} -> {} failed: {}",
                        self.collection_id,
                        transfer_key.shard_id,
                        transfer_key.to,
                        err
                    );
                    TaskResult::Failed
                }
            }
        } else {
            TaskResult::NotFound
        }
    }

    pub fn add_task(
        &mut self,
        shard_transfer: &ShardTransfer,
        task: CancellableAsyncTaskHandle<bool>,
    ) {
        self.tasks.insert(shard_transfer.key(), task);
    }
}
