#[cfg(feature = "chakrdb")]
use std::sync::Arc;

#[cfg(feature = "chakrdb")]
use ahash::AHashMap;
#[cfg(feature = "chakrdb")]
use common::counter::hardware_counter::HardwareCounterCell;
#[cfg(feature = "chakrdb")]
use common::types::PointOffsetType;
#[cfg(feature = "chakrdb")]
use parking_lot::RwLock;

#[cfg(feature = "chakrdb")]
use std::path::PathBuf;
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
use crate::payload_storage::PayloadStorage;
#[cfg(feature = "chakrdb")]
use crate::types::Payload;
#[cfg(feature = "chakrdb")]
use chakrdb_wrapper::ChakrDbClient;

/// ChakrDB-based in-memory implementation of `PayloadStorage`.
/// Similar to SimplePayloadStorage but uses ChakrDB instead of RocksDB
#[cfg(feature = "chakrdb")]
#[derive(Debug)]
pub struct ChakrDbSimplePayloadStorage {
    pub(crate) payload: AHashMap<PointOffsetType, Payload>,
    pub(crate) db_wrapper: DatabaseColumnWrapper,
}

#[cfg(feature = "chakrdb")]
impl ChakrDbSimplePayloadStorage {
    pub fn open(database: Arc<RwLock<ChakrDbClient>>) -> OperationResult<Self> {
        let mut payload_map: AHashMap<PointOffsetType, Payload> = Default::default();

        let db_wrapper = DatabaseColumnWrapper::new(database, DB_PAYLOAD_CF);

        // Load all payloads from ChakrDB into memory
        // Note: Iterator implementation is TODO, so for now we'll start with empty map
        // In a full implementation, we'd iterate through all keys and load them
        // For now, this is a placeholder that will work once iterator is implemented
        
        Ok(ChakrDbSimplePayloadStorage {
            payload: payload_map,
            db_wrapper,
        })
    }

    pub(crate) fn update_storage(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let point_id_serialized = serde_cbor::to_vec(&point_id).unwrap();
        hw_counter
            .payload_io_write_counter()
            .incr_delta(point_id_serialized.len());

        match self.payload.get(&point_id) {
            None => self.db_wrapper.remove(point_id_serialized),
            Some(payload) => {
                let payload_serialized = serde_cbor::to_vec(payload).unwrap();
                hw_counter
                    .payload_io_write_counter()
                    .incr_delta(payload_serialized.len());
                self.db_wrapper.put(point_id_serialized, payload_serialized)
            }
        }
    }

    pub fn payload_ptr(&self, point_id: PointOffsetType) -> Option<&Payload> {
        self.payload.get(&point_id)
    }

    /// Destroy this payload storage, remove persisted data from ChakrDB
    pub fn destroy(&self) -> OperationResult<()> {
        self.db_wrapper.remove_column_family()?;
        Ok(())
    }
}

#[cfg(feature = "chakrdb")]
impl PayloadStorage for ChakrDbSimplePayloadStorage {
    fn overwrite(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.payload.insert(point_id, payload.clone());
        self.update_storage(point_id, hw_counter)
    }

    fn set(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self.payload.get_mut(&point_id) {
            Some(existing_payload) => {
                existing_payload.merge(payload);
            }
            None => {
                self.payload.insert(point_id, payload.clone());
            }
        }
        self.update_storage(point_id, hw_counter)
    }

    fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self.payload.get_mut(&point_id) {
            Some(existing_payload) => {
                existing_payload.merge_by_key(payload, key);
            }
            None => {
                let mut new_payload = Payload::default();
                new_payload.merge_by_key(payload, key);
                self.payload.insert(point_id, new_payload);
            }
        }
        self.update_storage(point_id, hw_counter)
    }

    fn get(
        &self,
        point_id: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        Ok(self.payload.get(&point_id).cloned().unwrap_or_default())
    }

    fn get_sequential(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        self.get(point_id, hw_counter)
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>> {
        let deleted_values = self
            .payload
            .get_mut(&point_id)
            .map(|p| p.remove_by_key(key))
            .unwrap_or_default();
        if !deleted_values.is_empty() {
            self.update_storage(point_id, hw_counter)?;
        }
        Ok(deleted_values)
    }

    fn clear(
        &mut self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        let removed = self.payload.remove(&point_id);
        if removed.is_some() {
            self.update_storage(point_id, hw_counter)?;
        }
        Ok(removed)
    }

    #[cfg(test)]
    fn clear_all(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        // TODO: Implement clear_all using iterator when available
        for point_id in self.payload.keys().copied().collect::<Vec<_>>() {
            self.clear(point_id, hw_counter)?;
        }
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }

    fn iter<F>(&self, mut callback: F, _hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        for (point_id, payload) in &self.payload {
            if !callback(*point_id, payload)? {
                break;
            }
        }
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
        false // In-memory with disk persistence
    }
}

