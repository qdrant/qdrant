use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::types::PointOffsetType;
use mmap_value_storage::value::Value as StorageValue;
use mmap_value_storage::ValueStorage;
use parking_lot::RwLock;
use serde_json::Value;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::Flusher;
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorage;
use crate::types::{Payload, PayloadKeyTypeRef};

const STORAGE_PATH: &str = "payload_storage";

impl StorageValue for Payload {
    fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    fn from_bytes(data: &[u8]) -> Self {
        serde_json::from_slice(data).unwrap()
    }
}

#[derive(Debug)]
pub struct MmapPayloadStorage {
    storage: Arc<RwLock<ValueStorage<Payload>>>,
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
            Ok(Self::new(path))
        }
    }

    fn open(path: PathBuf) -> OperationResult<Self> {
        if let Some(storage) = ValueStorage::open(path, None) {
            let storage = Arc::new(RwLock::new(storage));
            Ok(Self { storage })
        } else {
            Err(OperationError::service_error(
                "Failed to open mmap payload storage",
            ))
        }
    }

    fn new(path: PathBuf) -> Self {
        let storage = ValueStorage::new(path, None);
        let storage = Arc::new(RwLock::new(storage));
        Self { storage }
    }
}

impl PayloadStorage for MmapPayloadStorage {
    fn overwrite(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        self.storage.write().put_value(point_id, payload);
        Ok(())
    }

    fn set(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        let mut guard = self.storage.write();
        match guard.get_value(point_id) {
            Some(mut point_payload) => {
                point_payload.merge(payload);
                guard.put_value(point_id, &point_payload);
            }
            None => {
                guard.put_value(point_id, payload);
            }
        }
        Ok(())
    }

    fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
    ) -> OperationResult<()> {
        let mut guard = self.storage.write();
        match guard.get_value(point_id) {
            Some(mut point_payload) => {
                point_payload.merge_by_key(payload, key);
                guard.put_value(point_id, &point_payload);
            }
            None => {
                let mut dest_payload = Payload::default();
                dest_payload.merge_by_key(payload, key);
                guard.put_value(point_id, &dest_payload);
            }
        }
        Ok(())
    }

    fn get(&self, point_id: PointOffsetType) -> OperationResult<Payload> {
        match self.storage.read().get_value(point_id) {
            Some(payload) => Ok(payload),
            None => Ok(Default::default()),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Vec<Value>> {
        let mut guard = self.storage.write();
        match guard.get_value(point_id) {
            Some(mut payload) => {
                let res = payload.remove(key);
                if !res.is_empty() {
                    guard.put_value(point_id, &payload);
                }
                Ok(res)
            }
            None => Ok(vec![]),
        }
    }

    fn clear(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        let res = self.storage.write().delete_value(point_id);
        Ok(res)
    }

    fn wipe(&mut self) -> OperationResult<()> {
        self.storage.write().wipe();
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let storage = self.storage.clone();
        Box::new(move || {
            storage.write().flush().unwrap(); // TODO: error conversion
            Ok(())
        })
    }

    fn iter<F>(&self, mut callback: F) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        self.storage.read().iter(|point_id, payload| {
            match callback(point_id, &payload) {
                Ok(true) => Ok(true),
                Ok(false) => Ok(false),
                Err(e) => {
                    // TODO return proper error
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                }
            }
        })?;
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        self.storage.read().files()
    }
}
