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
    /// Tracks ongoing operations of creation of partial snapshots.
    /// There might be multiple parallel requests to create partial snapshots,
    /// so we track them with a counter.
    ongoing_create_snapshot_requests_tracker: RequestTracker,
    recovery_lock: Arc<tokio::sync::Mutex<()>>,
    search_lock: Arc<tokio::sync::RwLock<()>>,
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
