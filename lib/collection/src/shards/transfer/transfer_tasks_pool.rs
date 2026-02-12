use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::common::eta_calculator::EtaCalculator;
use crate::common::stoppable_task_async::CancellableAsyncTaskHandle;
use crate::shards::CollectionId;
use crate::shards::transfer::{RecoveryStage, ShardTransfer, ShardTransferKey, TransferStage};

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
    points_transferred: usize,
    points_total: usize,
    pub eta: EtaCalculator,
    // Stage tracking for profiling
    current_stage: Option<TransferStage>,
    stage_started: Option<Instant>,
    // Cumulative batch timing breakdown (local read vs remote send)
    batch_read_duration: Duration,
    batch_send_duration: Duration,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum TaskResult {
    Running,
    Finished,
    Failed,
}

pub struct TransferTaskStatus {
    pub result: TaskResult,
    pub comment: String,
}

impl TransferTaskProgress {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            points_transferred: 0,
            points_total: 0,
            eta: EtaCalculator::new(),
            current_stage: None,
            stage_started: None,
            batch_read_duration: Duration::ZERO,
            batch_send_duration: Duration::ZERO,
        }
    }

    pub fn add(&mut self, delta: usize) {
        self.points_transferred += delta;
        self.points_total = max(self.points_total, self.points_transferred);
        self.eta.set_progress(self.points_transferred);
    }

    pub fn set(&mut self, transferred: usize, total: usize) {
        self.points_transferred = transferred;
        self.points_total = total;
        self.eta.set_progress(transferred);
    }

    /// Set the current transfer stage (resets stage elapsed time)
    pub fn set_stage(&mut self, stage: TransferStage) {
        self.current_stage = Some(stage);
        self.stage_started = Some(Instant::now());
    }

    /// Get the current transfer stage
    pub fn current_stage(&self) -> Option<TransferStage> {
        self.current_stage
    }

    /// Get elapsed seconds in current stage (with decimal precision)
    pub fn stage_elapsed_secs(&self) -> Option<f64> {
        self.stage_started.map(|t| t.elapsed().as_secs_f64())
    }

    /// Update cumulative batch timing breakdown (local read vs remote send)
    pub fn set_batch_durations(&mut self, read: Duration, send: Duration) {
        self.batch_read_duration = read;
        self.batch_send_duration = send;
    }
}

/// Progress tracking for snapshot recovery on the receiver (destination) node.
///
/// Tracks the sub-stages of recovery: downloading, unpacking, restoring.
pub struct RecoveryProgress {
    current_stage: Option<RecoveryStage>,
    stage_started: Option<Instant>,
}

impl RecoveryProgress {
    pub fn new() -> Self {
        Self {
            current_stage: None,
            stage_started: None,
        }
    }

    /// Set the current recovery stage (resets stage elapsed time)
    pub fn set_stage(&mut self, stage: RecoveryStage) {
        self.current_stage = Some(stage);
        self.stage_started = Some(Instant::now());
    }

    /// Get the current recovery stage
    pub fn current_stage(&self) -> Option<RecoveryStage> {
        self.current_stage
    }

    /// Get elapsed seconds in current stage (with decimal precision)
    pub fn stage_elapsed_secs(&self) -> Option<f64> {
        self.stage_started.map(|t| t.elapsed().as_secs_f64())
    }

    /// Format a comment string showing current stage and elapsed time
    pub fn format_comment(&self) -> Option<String> {
        let stage = self.current_stage?;
        let elapsed = self.stage_elapsed_secs().unwrap_or(0.0);
        Some(format!("{} ({:.2}s)", stage.as_str(), elapsed))
    }
}

impl Default for RecoveryProgress {
    fn default() -> Self {
        Self::new()
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
        let total = max(progress.points_transferred, progress.points_total);

        let mut comment = String::new();
        if let Some(stage) = progress.current_stage() {
            let elapsed = progress.stage_elapsed_secs().unwrap_or(0.0);
            write!(comment, "{} ({:.2}s)", stage.as_str(), elapsed).unwrap();
        }

        // Append records progress only when points are actually tracked
        if total > 0 {
            if !comment.is_empty() {
                comment.push_str(" | ");
            }
            write!(
                comment,
                "Transferring records ({}/{})",
                progress.points_transferred, total
            )
            .unwrap();
            if let Some(eta) = progress.eta.estimate(total) {
                write!(comment, ", ETA: {:.2}s", eta.as_secs_f64()).unwrap();
            }
        }

        // Append batch timing breakdown when available
        if !progress.batch_read_duration.is_zero() || !progress.batch_send_duration.is_zero() {
            if !comment.is_empty() {
                comment.push_str(" | ");
            }
            write!(
                comment,
                "read: {:.2}s, send: {:.2}s",
                progress.batch_read_duration.as_secs_f64(),
                progress.batch_send_duration.as_secs_f64(),
            )
            .unwrap();
        }

        Some(TransferTaskStatus { result, comment })
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

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_transfer_stage_as_str() {
        assert_eq!(TransferStage::Proxifying.as_str(), "proxifying");
        assert_eq!(TransferStage::Plunging.as_str(), "applying queued updates");
        assert_eq!(
            TransferStage::CreatingSnapshot.as_str(),
            "creating snapshot"
        );
        assert_eq!(TransferStage::Transferring.as_str(), "transferring");
        assert_eq!(TransferStage::Recovering.as_str(), "recovering");
        assert_eq!(
            TransferStage::FlushingQueue.as_str(),
            "syncing queued updates"
        );
        assert_eq!(
            TransferStage::WaitingConsensus.as_str(),
            "waiting consensus"
        );
        assert_eq!(TransferStage::Finalizing.as_str(), "finalizing");
    }

    #[test]
    fn test_stage_tracking_initial_state() {
        let progress = TransferTaskProgress::new();

        assert!(progress.current_stage().is_none());
        assert!(progress.stage_elapsed_secs().is_none());
    }

    #[test]
    fn test_stage_tracking_set_and_get() {
        let mut progress = TransferTaskProgress::new();

        progress.set_stage(TransferStage::Proxifying);
        assert_eq!(progress.current_stage(), Some(TransferStage::Proxifying));
        assert!(progress.stage_elapsed_secs().is_some());
        assert!(progress.stage_elapsed_secs().unwrap() < 2.00);
    }

    #[test]
    fn test_stage_tracking_elapsed_time() {
        let mut progress = TransferTaskProgress::new();

        progress.set_stage(TransferStage::Transferring);
        let elapsed_before = progress.stage_elapsed_secs().unwrap();

        thread::sleep(Duration::from_millis(50));

        let elapsed_after = progress.stage_elapsed_secs().unwrap();
        // Elapsed time should not decrease
        assert!(elapsed_after >= elapsed_before);
    }

    #[test]
    fn test_stage_tracking_change_resets_elapsed() {
        let mut progress = TransferTaskProgress::new();

        progress.set_stage(TransferStage::Proxifying);
        thread::sleep(Duration::from_millis(50));

        // Change stage - should reset elapsed time
        progress.set_stage(TransferStage::Transferring);
        assert_eq!(progress.current_stage(), Some(TransferStage::Transferring));
        // New stage should have very small elapsed time
        assert!(progress.stage_elapsed_secs().unwrap() < 1.00);
    }

    #[test]
    fn test_stage_tracking_all_stages() {
        let mut progress = TransferTaskProgress::new();

        let stages = [
            TransferStage::Proxifying,
            TransferStage::Plunging,
            TransferStage::CreatingSnapshot,
            TransferStage::Transferring,
            TransferStage::Recovering,
            TransferStage::FlushingQueue,
            TransferStage::WaitingConsensus,
            TransferStage::Finalizing,
        ];

        for stage in stages {
            progress.set_stage(stage);
            assert_eq!(progress.current_stage(), Some(stage));
            assert!(progress.stage_elapsed_secs().is_some());
        }
    }
}
