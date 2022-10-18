use std::collections::HashMap;

use crate::common::stoppable_task_async::StoppableAsyncTaskHandle;
use crate::shards::transfer::shard_transfer::{ShardTransfer, ShardTransferKey};

#[derive(Default)]
pub struct TransferTasksPool {
    tasks: HashMap<ShardTransferKey, StoppableAsyncTaskHandle<bool>>,
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
    /// Returns true if the task was actually stopped
    /// Returns false if the task was not found
    pub async fn stop_if_exists(&mut self, transfer_key: &ShardTransferKey) -> TaskResult {
        if let Some(task) = self.tasks.remove(transfer_key) {
            match task.stop().await {
                Ok(res) => {
                    if res {
                        log::info!(
                            "Transfer of shard {} -> {} finished",
                            transfer_key.shard_id,
                            transfer_key.to
                        );
                        TaskResult::Finished
                    } else {
                        log::info!(
                            "Transfer of shard {} -> {} stopped",
                            transfer_key.shard_id,
                            transfer_key.to
                        );
                        TaskResult::Stopped
                    }
                }
                Err(err) => {
                    log::warn!(
                        "Transfer task for shard {} -> {} failed: {}",
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
        task: StoppableAsyncTaskHandle<bool>,
    ) {
        self.tasks.insert(shard_transfer.key(), task);
    }
}
