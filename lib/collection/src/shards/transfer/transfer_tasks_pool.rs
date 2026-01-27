use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;

use crate::common::eta_calculator::EtaCalculator;
use crate::common::stoppable_task_async::CancellableAsyncTaskHandle;
use crate::shards::CollectionId;
use crate::shards::transfer::{ShardTransfer, ShardTransferKey};

pub struct TransferTasksPool {
    collection_id: CollectionId,
    tasks: HashMap<ShardTransferKey, TransferTaskItem>,
}

pub struct TransferTaskItem {
    pub task: CancellableAsyncTaskHandle<bool>,
    pub started_at: chrono::DateTime<Utc>,
    pub progress: Arc<Mutex<TransferTaskProgress>>,
}

pub struct TransferTaskProgress {
    points_transferred: usize,
    points_total: usize,
    pub eta: EtaCalculator,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum TaskResult {
    Running,
    Finished,
    Failed,
}

#[derive(Clone, Copy, Debug)]
pub struct TransferTaskStatus {
    pub result: TaskResult,
    pub points_transferred: usize,
    pub points_total: usize,
    pub eta: Option<Duration>,
    pub started_at: DateTime<Utc>,
    pub elapsed: usize,
}

impl TransferTaskStatus {
    pub fn comment(&self) -> String {
        let mut comment = format!(
            "Transferring records ({}/{}), started {}s ago, ETA: ",
            self.points_transferred, self.points_total, self.elapsed,
        );
        if let Some(eta) = self.eta {
            write!(comment, "{:.2}s", eta.as_secs_f64()).unwrap();
        } else {
            comment.push('-');
        }

        comment
    }

    pub fn failed(&self) -> bool {
        matches!(self.result, TaskResult::Failed)
    }

    pub fn running(&self) -> bool {
        matches!(self.result, TaskResult::Running)
    }
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

    pub fn add(&mut self, delta: usize) {
        self.points_transferred += delta;
        self.points_total = max(self.points_total, self.points_transferred);
        self.eta.set_progress(self.points_transferred);
    }

    pub fn set(&mut self, transferred: usize, total: usize) {
        self.points_transferred = transferred;
        self.points_total = max(total, transferred);
        self.eta.set_progress(transferred);
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

        let elapsed = (Utc::now() - task.started_at).num_seconds().max(0) as usize;

        Some(TransferTaskStatus {
            result,
            points_transferred: progress.points_transferred,
            points_total: progress.points_total,
            eta: progress.eta.estimate(progress.points_total),
            started_at: task.started_at,
            elapsed,
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
                    transfer_key.to,
                );
                TaskResult::Finished
            }
            Ok(false) => {
                log::info!(
                    "Transfer of shard {}:{} -> {} stopped",
                    self.collection_id,
                    transfer_key.shard_id,
                    transfer_key.to,
                );
                TaskResult::Failed
            }
            Err(err) => {
                log::warn!(
                    "Transfer task for shard {}:{} -> {} failed: {err}",
                    self.collection_id,
                    transfer_key.shard_id,
                    transfer_key.to,
                );
                TaskResult::Failed
            }
        })
    }

    pub fn add_task(&mut self, shard_transfer: &ShardTransfer, item: TransferTaskItem) {
        self.tasks.insert(shard_transfer.key(), item);
    }
}
