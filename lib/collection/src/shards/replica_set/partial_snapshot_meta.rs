use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use common::scope_tracker::{ScopeTracker, ScopeTrackerGuard};

use crate::operations::types::{CollectionError, CollectionResult};

/// API Flow:
///
///  ┌─────────────────┐
///  │ recover_from API│
///  └───────┬─────────┘
///          │◄──────────────
///          │             recovery_lock.lock()
///  ┌───────▼─────────┐   (accept reads,
///  │ Download snap   │    but decline new recover requests)
///  └───────┬─────────┘
///          │◄───────────────
///          │              local_shard.write()
///  ┌───────▼─────────┐    (reject reads,
///  │ Recover snapshot│     if both local_shard and recovery_lock are locked)
///  └───────┬─────────┘
///          │
///          │
///  ┌───────▼─────────┐
///  │ Swap shard      │
///  └─────────────────┘
///
#[derive(Debug, Default)]
pub struct PartialSnapshotMeta {
    /// Tracks ongoing *create* partial snapshot requests. There might be multiple parallel
    /// create partial snapshot requests, so we track them with a counter.
    ongoing_create_snapshot_requests_tracker: ScopeTracker,

    /// Limits parallel *recover* partial snapshot requests. We are using `RwLock`, so that multiple
    /// read requests can check if recovery is in progress (by doing `try_read`) without blocking
    /// each other.
    recovery_lock: Arc<tokio::sync::RwLock<()>>,

    /// Rejects read requests when partial snapshot recovery is in proggress.
    search_lock: tokio::sync::RwLock<()>,

    /// Timestamp of the last successful snapshot recovery.
    recovery_timestamp: AtomicU64,
}

impl PartialSnapshotMeta {
    pub fn ongoing_create_snapshot_requests(&self) -> usize {
        self.ongoing_create_snapshot_requests_tracker
            .get(Ordering::Relaxed)
    }

    #[must_use]
    pub fn track_create_snapshot_request(&self) -> ScopeTrackerGuard {
        self.ongoing_create_snapshot_requests_tracker
            .measure_scope()
    }

    pub fn try_take_recovery_lock(
        &self,
    ) -> CollectionResult<tokio::sync::OwnedRwLockWriteGuard<()>> {
        self.recovery_lock
            .clone()
            .try_write_owned()
            .map_err(|_| recovery_in_progress())
    }

    pub fn is_recovery_lock_taken(&self) -> bool {
        self.recovery_lock.try_read().is_err()
    }

    pub async fn take_search_write_lock(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        self.search_lock.write().await
    }

    pub fn try_take_search_read_lock(
        &self,
    ) -> CollectionResult<tokio::sync::RwLockReadGuard<'_, ()>> {
        self.search_lock
            .try_read()
            .map_err(|_| recovery_in_progress())
    }

    pub fn recovery_timestamp(&self) -> u64 {
        self.recovery_timestamp.load(Ordering::Relaxed)
    }

    pub fn snapshot_recovered(&self) {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        self.recovery_timestamp.store(timestamp, Ordering::Relaxed);
    }
}

fn recovery_in_progress() -> CollectionError {
    CollectionError::shard_unavailable("partial snapshot recovery is in progress")
}
