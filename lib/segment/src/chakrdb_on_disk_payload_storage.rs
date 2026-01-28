#[cfg(feature = "chakrdb")]
use std::path::PathBuf;
#[cfg(feature = "chakrdb")]
use std::sync::Arc;

#[cfg(feature = "chakrdb")]
use common::counter::hardware_counter::HardwareCounterCell;
#[cfg(feature = "chakrdb")]
use common::types::PointOffsetType;
#[cfg(feature = "chakrdb")]
use parking_lot::RwLock;
#[cfg(feature = "chakrdb")]
use serde_json::Value;

#[cfg(feature = "chakrdb")]
use crate::common::Flusher;
#[cfg(feature = "chakrdb")]
use crate::common::operation_error::{OperationError, OperationResult};
#[cfg(feature = "chakrdb")]
use crate::common::chakrdb_wrapper::{DB_PAYLOAD_CF, DatabaseColumnWrapper};
#[cfg(feature = "chakrdb")]
use crate::json_path::JsonPath;
#[cfg(feature = "chakrdb")]
use crate::payload_storage::PayloadStorage;
#[cfg(feature = "chakrdb")]
use crate::types::Payload;
#[cfg(feature = "chakrdb")]
use chakrdb_wrapper::ChakrDbClient;

/// ChakrDB-based on-disk implementation of `PayloadStorage`.
/// Persists all changes to disk using ChakrDB, does not keep payload in memory
#[cfg(feature = "chakrdb")]
#[derive(Debug)]
pub struct ChakrDbOnDiskPayloadStorage {
    db_wrapper: DatabaseColumnWrapper,
}

#[cfg(feature = "chakrdb")]
impl ChakrDbOnDiskPayloadStorage {
    pub fn open(database: Arc<RwLock<ChakrDbClient>>) -> OperationResult<Self> {
        let db_wrapper = DatabaseColumnWrapper::new(database, DB_PAYLOAD_CF);
        Ok(ChakrDbOnDiskPayloadStorage { db_wrapper })
    }

    pub fn remove_from_storage(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let serialized = serde_cbor::to_vec(&point_id).unwrap();
        hw_counter
            .payload_io_write_counter()
            .incr_delta(serialized.len());
        self.db_wrapper.remove(serialized)
    }

    pub fn update_storage(
        &self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let point_id_serialized = serde_cbor::to_vec(&point_id).unwrap();
        let payload_serialized = serde_cbor::to_vec(payload).unwrap();
        hw_counter
            .payload_io_write_counter()
            .incr_delta(point_id_serialized.len() + payload_serialized.len());
        self.db_wrapper.put(point_id_serialized, payload_serialized)
    }

    pub fn read_payload(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        let key = serde_cbor::to_vec(&point_id).unwrap();
        self.db_wrapper
            .get_pinned(&key, |raw| {
                hw_counter.payload_io_read_counter().incr_delta(raw.len());
                serde_cbor::from_slice(raw)
            })?
            .transpose()
            .map_err(OperationError::from)
    }

    /// Destroy this payload storage, remove persisted data from ChakrDB
    pub fn destroy(&self) -> OperationResult<()> {
        self.db_wrapper.remove_column_family()?;
        Ok(())
    }
}

#[cfg(feature = "chakrdb")]
impl PayloadStorage for ChakrDbOnDiskPayloadStorage {
    fn overwrite(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.update_storage(point_id, payload, hw_counter)
    }

    fn set(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let stored_payload = self.read_payload(point_id, hw_counter)?;
        match stored_payload {
            Some(mut point_payload) => {
                point_payload.merge(payload);
                self.update_storage(point_id, &point_payload, hw_counter)?
            }
            None => self.update_storage(point_id, payload, hw_counter)?,
        }
        Ok(())
    }

    fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let stored_payload = self.read_payload(point_id, hw_counter)?;
        match stored_payload {
            Some(mut point_payload) => {
                point_payload.merge_by_key(payload, key);
                self.update_storage(point_id, &point_payload, hw_counter)
            }
            None => {
                let mut dest_payload = Payload::default();
                dest_payload.merge_by_key(payload, key);
                self.update_storage(point_id, &dest_payload, hw_counter)
            }
        }
    }

    fn get(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        let payload = self.read_payload(point_id, hw_counter)?;
        match payload {
            Some(payload) => Ok(payload),
            None => Ok(Default::default()),
        }
    }

    fn get_sequential(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        // No sequential access optimizations for ChakrDB.
        self.get(point_id, hw_counter)
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>> {
        let mut stored_payload = self.get(point_id, hw_counter)?;
        let deleted_values = stored_payload.remove_by_key(key);
        if !deleted_values.is_empty() {
            self.update_storage(point_id, &stored_payload, hw_counter)?;
        }
        Ok(deleted_values)
    }

    fn clear(
        &mut self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        let stored_payload = self.read_payload(point_id, hw_counter)?;
        if stored_payload.is_some() {
            self.remove_from_storage(point_id, hw_counter)?;
        }
        Ok(stored_payload)
    }

    #[cfg(test)]
    fn clear_all(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        // TODO: Implement clear_all using iterator when available
        // For now, this is a placeholder
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }

    fn iter<F>(&self, mut callback: F, hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        // TODO: Implement iterator using ChakrDB Scan API when available
        // For now, this is a placeholder
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        // ChakrDB files would be in the database path
        // TODO: Return actual file paths
        Vec::new()
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        self.db_wrapper.get_storage_size_bytes()
    }

    fn is_on_disk(&self) -> bool {
        true
    }
}

