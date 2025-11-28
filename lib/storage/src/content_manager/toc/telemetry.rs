use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use collection::operations::types::CollectionResult;
use collection::telemetry::{
    CollectionSnapshotTelemetry, CollectionTelemetry, CollectionsAggregatedTelemetry,
};
use common::scope_tracker::{ScopeTracker, ScopeTrackerGuard};
use common::types::TelemetryDetail;
use dashmap::DashMap;

use crate::content_manager::toc::TableOfContent;
use crate::rbac::Access;

/// Collects various telemetry handled by ToC.
#[derive(Default)]
pub(super) struct TocTelemetryCollector {
    snapshots: DashMap<String, SnapshotTelemetryCollector>,
}

/// Collected telemetry data provided by ToC.
pub struct TocTelemetryData {
    pub collection_telemetry: Vec<CollectionTelemetry>,
    pub snapshot_telemetry: Vec<CollectionSnapshotTelemetry>,
}

/// Collects telemetry data for snapshots.
#[derive(Default, Clone)]
pub struct SnapshotTelemetryCollector {
    // Counter for currently running snapshot tasks.
    pub running_snapshots: ScopeTracker,

    // Counter for currently running snapshot tasks.
    pub running_snapshot_recovery: ScopeTracker,

    // Counter for snapshot creations since startup, until now.
    pub snapshots_total: Arc<AtomicUsize>,
}

impl TableOfContent {
    pub async fn get_telemetry_data(
        &self,
        detail: TelemetryDetail,
        access: &Access,
        timeout: Duration,
        is_stopped: &AtomicBool,
    ) -> CollectionResult<TocTelemetryData> {
        let all_collections = self.all_collections_access(access).await;
        let mut collection_telemetry = Vec::new();
        for collection_pass in &all_collections {
            if is_stopped.load(Ordering::Relaxed) {
                break;
            }
            if let Ok(collection) = self.get_collection(collection_pass).await {
                collection_telemetry.push(collection.get_telemetry_data(detail, timeout).await?);
            }
        }

        let snapshot_telemetry: Vec<_> = self
            .telemetry
            .snapshots
            .iter()
            .map(|item| {
                let snapshot = item.value();
                CollectionSnapshotTelemetry {
                    id: item.key().clone(),
                    running_snapshots: Some(snapshot.running_snapshots.get(Ordering::Relaxed)),
                    running_snapshot_recovery: Some(
                        snapshot.running_snapshot_recovery.get(Ordering::Relaxed),
                    ),
                    total_snapshot_creations: Some(
                        snapshot.snapshots_total.load(Ordering::Relaxed),
                    ),
                }
            })
            .collect();

        Ok(TocTelemetryData {
            collection_telemetry,
            snapshot_telemetry,
        })
    }

    pub async fn get_aggregated_telemetry_data(
        &self,
        access: &Access,
        timeout: Duration,
        is_stopped: &AtomicBool,
    ) -> CollectionResult<Vec<CollectionsAggregatedTelemetry>> {
        let mut result = Vec::new();
        let all_collections = self.all_collections_access(access).await;
        for collection_pass in &all_collections {
            if is_stopped.load(Ordering::Relaxed) {
                break;
            }
            if let Ok(collection) = self.get_collection(collection_pass).await {
                result.push(collection.get_aggregated_telemetry_data(timeout).await?);
            }
        }
        Ok(result)
    }

    pub fn max_collections(&self) -> Option<usize> {
        self.storage_config.max_collections
    }

    /// Returns atomic counters for snapshot telemetry and metadata for the given collection.
    /// If no entry with the given collection name was found, empty counters will be created and returned.
    pub fn snapshot_telemetry_collector(&self, collection: &str) -> SnapshotTelemetryCollector {
        self.telemetry
            .snapshots
            .entry(collection.to_string())
            .or_default()
            // We don't need a reference since all counters are atomic.
            // To prevent potential deadlocks and locking the dashmap longer than necessary, we clone here.
            .value()
            .clone()
    }

    /// Increase snapshot creation counter and 'currently-running` counter.
    ///
    /// Returns `ScopeCounterGuard` to measure the scope of snapshot creation.
    /// Therefore this must always be bound to a variable in order to correctly account for the whole scope.
    /// For more information see [`ScopeTracker`] and [`ScopeTrackerGuard`].
    #[must_use]
    pub fn count_snapshot_creation(&self, collection_name: &str) -> ScopeTrackerGuard {
        // Increment current running counter.
        let running_snapshots_guard = self
            .snapshot_telemetry_collector(collection_name)
            .running_snapshots
            .measure_scope();

        // Increment total counter.
        self.snapshot_telemetry_collector(collection_name)
            .snapshots_total
            .fetch_add(1, Ordering::Relaxed);

        running_snapshots_guard
    }
}
