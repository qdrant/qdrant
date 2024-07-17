use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::stoppable_task_async::CancellableAsyncTaskHandle;
use crate::shards::resharding::ReshardKey;
use crate::shards::CollectionId;

pub struct ReshardTasksPool {
    collection_id: CollectionId,
    tasks: HashMap<ReshardKey, ReshardTaskItem>,
}

pub struct ReshardTaskItem {
    pub task: CancellableAsyncTaskHandle<bool>,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub progress: Arc<Mutex<ReshardTaskProgress>>,
}

pub struct ReshardTaskProgress {
    pub description: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum TaskResult {
    Running,
    Finished,
    Failed,
}

pub struct ReshardTaskStatus {
    pub result: TaskResult,
    pub comment: String,
}

impl ReshardTaskProgress {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { description: None }
    }
}

impl ReshardTasksPool {
    pub fn new(collection_id: CollectionId) -> Self {
        Self {
            collection_id,
            tasks: HashMap::new(),
        }
    }

    /// Get the status of the task. If the task is not found, return None.
    pub fn get_task_status(&self, reshard_key: &ReshardKey) -> Option<ReshardTaskStatus> {
        let task = self.tasks.get(reshard_key)?;
        let result = match task.task.get_result() {
            Some(true) => TaskResult::Finished,
            Some(false) => TaskResult::Failed,
            None if task.task.is_finished() => TaskResult::Failed,
            None => TaskResult::Running,
        };

        let progress = task.progress.lock();
        let mut parts = vec![];
        if let Some(description) = &progress.description {
            parts.push(description.clone());
        }
        parts.push(format!(
            "since {}s ago",
            chrono::Utc::now()
                .signed_duration_since(task.started_at)
                .num_seconds(),
        ));
        let comment = parts.join(", ");

        Some(ReshardTaskStatus { result, comment })
    }

    /// Stop the task and return the result. If the task is not found, return None.
    pub async fn stop_task(&mut self, reshard_key: &ReshardKey) -> Option<TaskResult> {
        let task = self.tasks.remove(reshard_key)?;
        Some(match task.task.cancel().await {
            Ok(true) => {
                log::info!(
                    "Resharding to shard {}:{} finished",
                    self.collection_id,
                    reshard_key.shard_id,
                );
                TaskResult::Finished
            }
            Ok(false) => {
                log::info!(
                    "Resharding to shard {}:{} stopped",
                    self.collection_id,
                    reshard_key.shard_id,
                );
                TaskResult::Failed
            }
            Err(err) => {
                log::warn!(
                    "Resharding to shard {}:{} failed: {err}",
                    self.collection_id,
                    reshard_key.shard_id,
                );
                TaskResult::Failed
            }
        })
    }

    pub fn add_task(&mut self, reshard_key: ReshardKey, item: ReshardTaskItem) {
        self.tasks.insert(reshard_key, item);
    }
}
