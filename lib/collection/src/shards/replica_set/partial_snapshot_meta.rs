use std::sync::Arc;

use crate::operations::types::{CollectionError, CollectionResult};

/// API Flow:
///
///  ┌─────────────────┐
///  │ recover_from API│
///  └───────┬─────────┘
///          │◄──────────────
///          │             recovery_lock.take
///  ┌───────▼─────────┐   (accept reads, but decline
///  │ Download snap   │            new recover requests)
///  └───────┬─────────┘
///          │◄───────────────
///          │              search_lock.write
///  ┌───────▼─────────┐    (After this, reject reads)
///  │ Recover snapshot│
///  └───────┬─────────┘
///          │
///          │
///  ┌───────▼─────────┐
///  │ Swap shard      │
///  └─────────────────┘
///
#[derive(Debug, Default)]
pub struct PartialSnapshotMeta {
    recovery_lock: Arc<tokio::sync::Mutex<()>>,
    search_lock: Arc<tokio::sync::RwLock<()>>,
}

impl PartialSnapshotMeta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn try_take_recovery_lock(&self) -> CollectionResult<tokio::sync::OwnedMutexGuard<()>> {
        self.recovery_lock.clone().try_lock_owned().map_err(|_| {
            CollectionError::bad_request("partial snapshot recovery is already in progress")
        })
    }

    pub async fn take_search_write_lock(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.search_lock.clone().write_owned().await
    }

    pub fn try_take_search_read_lock(
        &self,
    ) -> CollectionResult<tokio::sync::OwnedRwLockReadGuard<()>> {
        self.search_lock
            .clone()
            .try_read_owned()
            .map_err(|_| CollectionError::ServiceError {
                error: "shard unavailable, partial snapshot recovery is in progress".into(),
                backtrace: None,
            })
    }
}
