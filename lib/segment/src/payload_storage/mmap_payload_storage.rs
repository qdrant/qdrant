use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::types::PointOffsetType;
use mmap_payload_storage::PayloadStorage as CratePayloadStorage;
use parking_lot::RwLock;
use serde_json::Value;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::Flusher;
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorage;
use crate::types::{Payload, PayloadKeyTypeRef};

const STORAGE_PATH: &str = "payload_storage";

#[derive(Debug)]
pub struct MmapPayloadStorage {
    storage: Arc<RwLock<CratePayloadStorage>>,
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
        if let Some(storage) = CratePayloadStorage::open(path, None) {
            let storage = Arc::new(RwLock::new(storage));
            Ok(Self { storage })
        } else {
            Err(OperationError::service_error(
                "Failed to open mmap payload storage",
            ))
        }
    }

    fn new(path: PathBuf) -> Self {
        let storage = CratePayloadStorage::new(path, None);
        let storage = Arc::new(RwLock::new(storage));
        Self { storage }
    }
}

// TODO delete this after integration
impl From<mmap_payload_storage::payload::Payload> for Payload {
    fn from(payload: mmap_payload_storage::payload::Payload) -> Self {
        Payload(payload.0)
    }
}

// TODO delete this after integration
impl From<Payload> for mmap_payload_storage::payload::Payload {
    fn from(payload: Payload) -> Self {
        mmap_payload_storage::payload::Payload(payload.0)
    }
}

// TODO delete this after integration
impl From<&Payload> for mmap_payload_storage::payload::Payload {
    fn from(payload: &Payload) -> Self {
        mmap_payload_storage::payload::Payload(payload.clone().0)
    }
}

impl PayloadStorage for MmapPayloadStorage {
    fn overwrite(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        self.storage.write().put_payload(point_id, &payload.into());
        Ok(())
    }

    fn set(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        let mut guard = self.storage.write();
        match guard.get_payload(point_id) {
            Some(point_payload) => {
                let mut converted = Payload::from(point_payload);
                converted.merge(payload);
                guard.put_payload(point_id, &converted.into());
            }
            None => {
                guard.put_payload(point_id, &payload.into());
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
        match guard.get_payload(point_id) {
            Some(point_payload) => {
                let mut converted = Payload::from(point_payload);
                converted.merge_by_key(payload, key);
                guard.put_payload(point_id, &converted.into());
            }
            None => {
                let mut dest_payload = Payload::default();
                dest_payload.merge_by_key(payload, key);
                guard.put_payload(point_id, &dest_payload.into());
            }
        }
        Ok(())
    }

    fn get(&self, point_id: PointOffsetType) -> OperationResult<Payload> {
        match self.storage.read().get_payload(point_id) {
            Some(payload) => Ok(payload.into()),
            None => Ok(Default::default()),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Vec<Value>> {
        let mut guard = self.storage.write();
        match guard.get_payload(point_id) {
            Some(payload) => {
                let mut converted = Payload::from(payload);
                let res = converted.remove(key);
                if !res.is_empty() {
                    guard.put_payload(point_id, &converted.into());
                }
                Ok(res)
            }
            None => Ok(vec![]),
        }
    }

    fn clear(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        let res = self.storage.write().delete_payload(point_id);
        Ok(res.map(|payload| payload.into()))
    }

    fn wipe(&mut self) -> OperationResult<()> {
        self.storage.write().wipe();
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let storage = self.storage.clone();
        Box::new(move || {
            storage.write().flush()?;
            Ok(())
        })
    }

    fn iter<F>(&self, mut callback: F) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        self.storage.read().iter(|point_id, payload| {
            match callback(point_id, &payload.clone().into()) {
                Ok(true) => Ok(true),
                Ok(false) => Ok(false),
                Err(e) => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )),
            }
        })?;
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        self.storage.read().files()
    }
}
