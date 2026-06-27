use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::json_path::JsonPath;
use crate::types::{OwnedPayloadRef, Payload};

/// Read-only trait for payload data storage.
///
/// Defines all read operations on payload storage. Search and retrieval logic
/// only requires this trait, which makes it possible to implement read-only
/// segments without duplicating storage code.
pub trait PayloadStorageRead {
    fn get(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload>;

    fn get_sequential(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload>;

    /// Return a borrowed or owned reference to the payload for `point_offset`.
    ///
    /// In-memory implementations should return `OwnedPayloadRef::Ref(...)` to
    /// avoid a clone on the hot path. On-disk implementations may materialise
    /// a copy and return `OwnedPayloadRef::Owned(...)`.
    ///
    /// For points without payload, return an empty payload via
    /// `OwnedPayloadRef::Owned(...)` so the caller never has to handle `None`.
    fn payload_ref(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<OwnedPayloadRef<'_>>;

    fn read_payloads<P: AccessPattern, U: common::universal_io::UserData>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, Payload) -> OperationResult<()>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Iterate over all stored payload and apply the provided callback.
    /// Stop iteration if callback returns false or error.
    ///
    /// Required for building payload index.
    fn iter<F>(&self, callback: F, hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>;

    /// Return storage size in bytes
    fn get_storage_size_bytes(&self) -> OperationResult<usize>;

    /// Whether this storage is on-disk or in-memory.
    fn is_on_disk(&self) -> bool;
}

/// Trait for payload data storage with mutating operations. Should allow filter checks
pub trait PayloadStorage: PayloadStorageRead {
    /// Overwrite payload for point_id. If payload already exists, replace it
    fn overwrite(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Set payload for point_id. If payload already exists, merge it with existing
    fn set(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Set payload to a point_id by key. If payload already exists, merge it with existing
    fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Delete payload by point_id and key
    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>>;

    /// Clear all payload of the point
    fn clear(
        &mut self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>>;

    /// Completely delete payload storage, without keeping allocated memory. Pufff!
    #[cfg(test)]
    fn clear_all(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()>;

    /// Return function that forces persistence of current storage state.
    fn flusher(&self) -> Flusher;

    /// Return all files that are used by storage to include in snapshots.
    /// RocksDB storages are captured outside of this trait.
    fn files(&self) -> Vec<PathBuf>;

    /// Returns a list of files, that are immutable, to exclude from partial snapshots.
    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new()
    }
}
