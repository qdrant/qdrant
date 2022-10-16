use std::collections::HashMap;

use crate::common::stoppable_task_async::StoppableAsyncTaskHandle;
use crate::shards::transfer::shard_transfer::ShardTransfer;

#[derive(Default)]
pub struct TransferTasksPool {
    tasks: HashMap<ShardTransfer, StoppableAsyncTaskHandle<bool>>,
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
    pub async fn stop_if_exists(&mut self, transfer: &ShardTransfer) -> TaskResult {
        if let Some(task) = self.tasks.remove(transfer) {
            match task.stop().await {
                Ok(res) => {
                    if res {
                        log::info!(
                            "Transfer of shard {} -> {} finished",
                            transfer.shard_id,
                            transfer.to
                        );
                        TaskResult::Finished
                    } else {
                        log::info!(
                            "Transfer of shard {} -> {} stopped",
                            transfer.shard_id,
                            transfer.to
                        );
                        TaskResult::Stopped
                    }
                }
                Err(err) => {
                    log::warn!(
                        "Transfer task for shard {} -> {} failed: {}",
                        transfer.shard_id,
                        transfer.to,
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
        self.tasks.insert(shard_transfer.clone(), task);
    }

    pub fn get_transfers(&self) -> Vec<ShardTransfer> {
        self.tasks.keys().cloned().collect()
    }
}
