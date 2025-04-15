use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::config::StorageOptions;
use gridstore::{Blob, Gridstore};
use parking_lot::RwLock;
use serde_json::Value;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorage;
use crate::types::{Payload, PayloadKeyTypeRef};

const STORAGE_PATH: &str = "payload_storage";

impl Blob for Payload {
    fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    fn from_bytes(data: &[u8]) -> Self {
        serde_json::from_slice(data).unwrap()
    }
}

#[derive(Debug)]
pub struct MmapPayloadStorage {
    storage: Arc<RwLock<Gridstore<Payload>>>,
}

impl MmapPayloadStorage {
    pub fn open_or_create(path: &Path) -> OperationResult<Self> {
        let path = path.join(STORAGE_PATH);
        if path.exists() {
            Self::open(path)
        } else {
            // create folder if it does not exist
            std::fs::create_dir_all(&path).map_err(|_| {
                OperationError::service_error("Failed to create mmap payload storage directory")
            })?;
            Ok(Self::new(path)?)
        }
    }

    fn open(path: PathBuf) -> OperationResult<Self> {
        let storage = Gridstore::open(path).map_err(|err| {
            OperationError::service_error(format!("Failed to open mmap payload storage: {err}"))
        })?;
        let storage = Arc::new(RwLock::new(storage));
        Ok(Self { storage })
    }

    fn new(path: PathBuf) -> OperationResult<Self> {
        let storage = Gridstore::new(path, StorageOptions::default())
            .map_err(OperationError::service_error)?;
        let storage = Arc::new(RwLock::new(storage));
        Ok(Self { storage })
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.storage.read().populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.read().clear_cache()?;
        Ok(())
    }
}

impl PayloadStorage for MmapPayloadStorage {
    fn overwrite(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage
            .write()
            .put_value(point_id, payload, hw_counter.ref_payload_io_write_counter())
            .map_err(OperationError::service_error)?;
        Ok(())
    }

    fn set(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut guard = self.storage.write();
        match guard.get_value(point_id, hw_counter) {
            Some(mut point_payload) => {
                point_payload.merge(payload);
                guard
                    .put_value(
                        point_id,
                        &point_payload,
                        hw_counter.ref_payload_io_write_counter(),
                    )
                    .map_err(OperationError::service_error)?;
            }
            None => {
                guard
                    .put_value(point_id, payload, hw_counter.ref_payload_io_write_counter())
                    .map_err(OperationError::service_error)?;
            }
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
        let mut guard = self.storage.write();
        match guard.get_value(point_id, hw_counter) {
            Some(mut point_payload) => {
                point_payload.merge_by_key(payload, key);
                guard
                    .put_value(
                        point_id,
                        &point_payload,
                        hw_counter.ref_payload_io_write_counter(),
                    )
                    .map_err(OperationError::service_error)?;
            }
            None => {
                let mut dest_payload = Payload::default();
                dest_payload.merge_by_key(payload, key);
                guard
                    .put_value(
                        point_id,
                        &dest_payload,
                        hw_counter.ref_payload_io_write_counter(),
                    )
                    .map_err(OperationError::service_error)?;
            }
        }
        Ok(())
    }

    fn get(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self.storage.read().get_value(point_id, hw_counter) {
            Some(payload) => Ok(payload),
            None => Ok(Default::default()),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>> {
        let mut guard = self.storage.write();
        match guard.get_value(point_id, hw_counter) {
            Some(mut payload) => {
                let res = payload.remove(key);
                if !res.is_empty() {
                    guard
                        .put_value(
                            point_id,
                            &payload,
                            hw_counter.ref_payload_io_write_counter(),
                        )
                        .map_err(OperationError::service_error)?;
                }
                Ok(res)
            }
            None => Ok(vec![]),
        }
    }

    fn clear(
        &mut self,
        point_id: PointOffsetType,
        _: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        let res = self.storage.write().delete_value(point_id);
        Ok(res)
    }

    #[cfg(test)]
    fn wipe(&mut self, _: &HardwareCounterCell) -> OperationResult<()> {
        self.storage.write().wipe();
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let storage = self.storage.clone();
        Box::new(move || {
            storage.read().flush().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to flush mmap payload storage: {err}"
                ))
            })?;
            Ok(())
        })
    }

    fn iter<F>(&self, mut callback: F, hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        self.storage.read().iter(
            |point_id, payload| {
                callback(point_id, payload).map_err(|e|
                    // TODO return proper error
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
            },
            hw_counter.ref_payload_io_read_counter(),
        )?;
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        self.storage.read().files()
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        Ok(self.storage.read().get_storage_size_bytes())
    }
}
