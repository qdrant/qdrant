use std::sync::Arc;

use crate::operations::types::{CollectionError, CollectionResult};

#[derive(Debug)]
pub struct PartialSnapshotMeta {
    recovery_lock: Arc<tokio::sync::Mutex<()>>,
    read_lock: Arc<tokio::sync::Semaphore>,
}

impl PartialSnapshotMeta {
    pub fn new() -> Self {
        Self {
            recovery_lock: Arc::new(tokio::sync::Mutex::new(())),
            read_lock: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    pub fn take_recovery_lock(&self) -> CollectionResult<tokio::sync::OwnedMutexGuard<()>> {
        self.recovery_lock.clone().try_lock_owned().map_err(|_| {
            CollectionError::bad_request("partial snapshot recovery is already in progress")
        })
    }

    pub fn take_read_lock(&self) -> CollectionResult<tokio::sync::OwnedSemaphorePermit> {
        self.read_lock.clone().try_acquire_owned().map_err(|_| {
            CollectionError::bad_request("partial snapshot recovery is already in progress")
        })
    }

    pub fn check_read_lock(&self) -> CollectionResult<()> {
        let read_operation_permits = self.read_lock.available_permits();

        if read_operation_permits > 0 {
            Ok(())
        } else {
            Err(CollectionError::ServiceError {
                error: "shard unavailable, partial snapshot recovery is in progress".into(),
                backtrace: None,
            })
        }
    }
}
