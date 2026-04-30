use std::path::PathBuf;

use async_trait::async_trait;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::AsyncFlusher;
use crate::common::operation_error::OperationResult;
use crate::json_path::JsonPath;
use crate::types::{Filter, Payload};

/// Trait for payload data storage. Should allow filter checks.
///
/// All I/O-touching methods are `async` so the storage can be polled cooperatively
/// by the Tokio runtime instead of blocking a thread on disk access.
///
/// The futures returned by these methods are `!Send` because [`HardwareCounterCell`]
/// uses interior mutability via [`Cell`](std::cell::Cell). Run them on a single-
/// threaded executor (`tokio::runtime::Builder::new_current_thread()`,
/// `tokio::task::LocalSet`, or a per-segment task that does not migrate threads).
#[async_trait(?Send)]
pub trait PayloadStorage: Send + Sync {
    /// Overwrite payload for point_id. If payload already exists, replace it
    async fn overwrite(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Set payload for point_id. If payload already exists, merge it with existing
    async fn set(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Set payload to a point_id by key. If payload already exists, merge it with existing
    async fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    async fn get(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload>;

    async fn get_sequential(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload>;

    /// Delete payload by point_id and key
    async fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>>;

    /// Clear all payload of the point
    async fn clear(
        &mut self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>>;

    /// Completely delete payload storage, without keeping allocated memory. Pufff!
    #[cfg(test)]
    async fn clear_all(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()>;

    /// Return function that forces persistence of current storage state.
    ///
    /// Invoke the returned closure and `.await` the resulting future on the
    /// Tokio runtime to persist.
    fn flusher(&self) -> AsyncFlusher;

    /// Iterate over all stored payload and apply the provided callback.
    /// Stop iteration if callback returns false or error.
    ///
    /// Required for building payload index. The callback is synchronous to keep
    /// the hot loop tight; the async-ness here is at the storage I/O level.
    async fn iter<F>(&self, callback: F, hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>;

    /// Return all files that are used by storage to include in snapshots.
    /// RocksDB storages are captured outside of this trait.
    fn files(&self) -> Vec<PathBuf>;

    /// Returns a list of files, that are immutable, to exclude from partial snapshots.
    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new()
    }

    /// Return storage size in bytes
    fn get_storage_size_bytes(&self) -> OperationResult<usize>;

    /// Whether this storage is on-disk or in-memory.
    fn is_on_disk(&self) -> bool;
}

pub trait ConditionChecker {
    /// Check if point satisfies filter condition. Return true if satisfies
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool;
}

pub trait FilterContext {
    /// Check if point satisfies filter condition. Return true if satisfies
    fn check(&self, point_id: PointOffsetType) -> bool;
}

pub type ConditionCheckerSS = dyn ConditionChecker + Sync + Send;
