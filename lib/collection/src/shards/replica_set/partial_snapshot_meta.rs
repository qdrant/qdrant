use std::sync::Arc;
use std::sync::atomic::{self, AtomicU64, AtomicUsize};
use std::time::{Duration, SystemTime};

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
    ongoing_create_snapshot_requests_tracker: RequestTracker,

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
        self.ongoing_create_snapshot_requests_tracker.requests()
    }

    pub fn track_create_snapshot_request(&self) -> RequestGuard {
        self.ongoing_create_snapshot_requests_tracker
            .track_request()
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
        self.recovery_timestamp.load(atomic::Ordering::Relaxed)
    }

    pub fn snapshot_recovered(&self) {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        self.recovery_timestamp
            .store(timestamp, atomic::Ordering::Relaxed);
    }
}

fn recovery_in_progress() -> CollectionError {
    CollectionError::shard_unavailable("partial snapshot recovery is in progress")
}

#[derive(Debug, Default)]
pub struct RequestTracker {
    requests: Arc<AtomicUsize>,
}

impl RequestTracker {
    pub fn requests(&self) -> usize {
        self.requests.load(atomic::Ordering::Relaxed)
    }

    pub fn track_request(&self) -> RequestGuard {
        RequestGuard::new(self.requests.clone())
    }
}

#[derive(Clone, Debug)]
pub struct RequestGuard {
    requests: Arc<AtomicUsize>,
}

impl RequestGuard {
    fn new(requests: Arc<AtomicUsize>) -> Self {
        requests.fetch_add(1, atomic::Ordering::Relaxed);
        Self { requests }
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.requests.fetch_sub(1, atomic::Ordering::Relaxed);
    }
}
