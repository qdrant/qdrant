use std::path::{Path, PathBuf};

use async_trait::async_trait;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{Random, Sequential};
use common::types::PointOffsetType;
use fs_err as fs;
use gridstore::config::StorageOptions;
use gridstore::{Blob, Gridstore};
use serde_json::Value;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::{AsyncFlusher, async_flusher_from_sync};
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorage;
use crate::types::Payload;

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
    storage: Gridstore<Payload>,
    populate: bool,
}

impl MmapPayloadStorage {
    pub fn open_or_create(path: PathBuf, populate: bool) -> OperationResult<Self> {
        let path = storage_dir(path);
        if path.exists() {
            Self::open(path, populate)
        } else {
            // create folder if it does not exist
            fs::create_dir_all(&path).map_err(|_| {
                OperationError::service_error("Failed to create mmap payload storage directory")
            })?;
            Ok(Self::new(path, populate)?)
        }
    }

    fn open(path: PathBuf, populate: bool) -> OperationResult<Self> {
        let storage = Gridstore::open(path).map_err(|err| {
            OperationError::service_error(format!("Failed to open mmap payload storage: {err}"))
        })?;

        if populate {
            storage.populate()?;
        }

        Ok(Self { storage, populate })
    }

    fn new(path: PathBuf, populate: bool) -> OperationResult<Self> {
        let storage = Gridstore::new(path, StorageOptions::default())?;

        if populate {
            storage.populate()?;
        }

        Ok(Self { storage, populate })
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.storage.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache()?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl PayloadStorage for MmapPayloadStorage {
    async fn overwrite(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // mmap is sync; we keep the body sync because page-fault driven I/O
        // can't actually be awaited. The async signature lets callers compose
        // this with truly-async storages on the Tokio runtime.
        self.storage
            .put_value(point_id, payload, hw_counter.ref_payload_io_write_counter())?;
        Ok(())
    }

    async fn set(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self.storage.get_value::<Random>(point_id, hw_counter)? {
            Some(mut point_payload) => {
                point_payload.merge(payload);
                self.storage.put_value(
                    point_id,
                    &point_payload,
                    hw_counter.ref_payload_io_write_counter(),
                )?;
            }
            None => {
                self.storage.put_value(
                    point_id,
                    payload,
                    hw_counter.ref_payload_io_write_counter(),
                )?;
            }
        }
        Ok(())
    }

    async fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self.storage.get_value::<Random>(point_id, hw_counter)? {
            Some(mut point_payload) => {
                point_payload.merge_by_key(payload, key);
                self.storage.put_value(
                    point_id,
                    &point_payload,
                    hw_counter.ref_payload_io_write_counter(),
                )?;
            }
            None => {
                let mut dest_payload = Payload::default();
                dest_payload.merge_by_key(payload, key);
                self.storage.put_value(
                    point_id,
                    &dest_payload,
                    hw_counter.ref_payload_io_write_counter(),
                )?;
            }
        }
        Ok(())
    }

    async fn get(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self.storage.get_value::<Random>(point_id, hw_counter)? {
            Some(payload) => Ok(payload),
            None => Ok(Default::default()),
        }
    }

    async fn get_sequential(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self.storage.get_value::<Sequential>(point_id, hw_counter)? {
            Some(payload) => Ok(payload),
            None => Ok(Default::default()),
        }
    }

    async fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>> {
        match self.storage.get_value::<Random>(point_id, hw_counter)? {
            Some(mut payload) => {
                let res = payload.remove(key);
                if !res.is_empty() {
                    self.storage.put_value(
                        point_id,
                        &payload,
                        hw_counter.ref_payload_io_write_counter(),
                    )?;
                }
                Ok(res)
            }
            None => Ok(vec![]),
        }
    }

    async fn clear(
        &mut self,
        point_id: PointOffsetType,
        _: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        let res = self.storage.delete_value(point_id)?;
        Ok(res)
    }

    #[cfg(test)]
    async fn clear_all(&mut self, _: &HardwareCounterCell) -> OperationResult<()> {
        self.storage.clear().map_err(|err| {
            OperationError::service_error(format!("Failed to clear mmap payload storage: {err}"))
        })
    }

    fn flusher(&self) -> AsyncFlusher {
        let storage_flusher = self.storage.flusher();
        async_flusher_from_sync(move || {
            storage_flusher().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to flush mmap payload gridstore: {err}"
                ))
            })
        })
    }

    async fn iter<F>(&self, mut callback: F, hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        self.storage.iter(
            |point_id, payload| callback(point_id, &payload),
            hw_counter.ref_payload_io_read_counter(),
        )
    }

    fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.storage.immutable_files()
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        Ok(self.storage.get_storage_size_bytes()?)
    }

    fn is_on_disk(&self) -> bool {
        !self.populate
    }
}

/// Get storage directory for this payload storage
pub fn storage_dir<P: AsRef<Path>>(segment_path: P) -> PathBuf {
    segment_path.as_ref().join(STORAGE_PATH)
}
