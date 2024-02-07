use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::common::eta_calculator::EtaCalculator;
use crate::common::stoppable_task_async::CancellableAsyncTaskHandle;
use crate::shards::transfer::{ShardTransfer, ShardTransferKey};
use crate::shards::CollectionId;

pub struct TransferTasksPool {
    collection_id: CollectionId,
    tasks: HashMap<ShardTransferKey, TransferTaskItem>,
}

pub struct TransferTaskItem {
    pub task: CancellableAsyncTaskHandle<bool>,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub progress: Arc<Mutex<TransferTaskProgress>>,
}

pub struct TransferTaskProgress {
    pub points_transferred: usize,
    pub points_total: usize,
    pub eta: EtaCalculator,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum TaskResult {
    Running,
    Finished,
    Failed,
}

pub struct TransferTaskStatus {
    pub result: TaskResult,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub points_transferred: usize,
    pub points_total: usize,
    pub eta: Option<Duration>,
}

impl TransferTaskProgress {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            points_transferred: 0,
            points_total: 0,
            eta: EtaCalculator::new(),
        }
    }
}

impl TransferTasksPool {
    pub fn new(collection_id: CollectionId) -> Self {
        Self {
            collection_id,
            tasks: HashMap::new(),
        }
    }

    /// Get the status of the task. If the task is not found, return None.
    pub fn get_task_status(&self, transfer_key: &ShardTransferKey) -> Option<TransferTaskStatus> {
        let task = self.tasks.get(transfer_key)?;
        let result = match task.task.get_result() {
            Some(true) => TaskResult::Finished,
            Some(false) => TaskResult::Failed,
            None if task.task.is_finished() => TaskResult::Failed,
            None => TaskResult::Running,
        };
        let progress = task.progress.lock();
        Some(TransferTaskStatus {
            result,
            started_at: task.started_at,
            points_transferred: progress.points_transferred,
            points_total: progress.points_total,
            eta: progress.eta.estimate(progress.points_total),
        })
    }

    /// Stop the task and return the result. If the task is not found, return None.
    pub async fn stop_task(&mut self, transfer_key: &ShardTransferKey) -> Option<TaskResult> {
        let task = self.tasks.remove(transfer_key)?;
        Some(match task.task.cancel().await {
            Ok(true) => {
                log::info!(
                    "Transfer of shard {}:{} -> {} finished",
                    self.collection_id,
                    transfer_key.shard_id,
                    transfer_key.to
                );
                TaskResult::Finished
            }
            Ok(false) => {
                log::info!(
                    "Transfer of shard {}:{} -> {} stopped",
                    self.collection_id,
                    transfer_key.shard_id,
                    transfer_key.to
                );
                TaskResult::Failed
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
        })
    }

    pub fn add_task(&mut self, shard_transfer: &ShardTransfer, item: TransferTaskItem) {
        self.tasks.insert(shard_transfer.key(), item);
    }
}
