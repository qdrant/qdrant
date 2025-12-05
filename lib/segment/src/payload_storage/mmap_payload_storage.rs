use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use fs_err as fs;
use gridstore::config::StorageOptions;
use gridstore::{Blob, Gridstore};
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
        let storage = Gridstore::new(path, StorageOptions::default())
            .map_err(OperationError::service_error)?;

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

impl PayloadStorage for MmapPayloadStorage {
    fn overwrite(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage
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
        match self.storage.get_value::<false>(point_id, hw_counter) {
            Some(mut point_payload) => {
                point_payload.merge(payload);
                self.storage
                    .put_value(
                        point_id,
                        &point_payload,
                        hw_counter.ref_payload_io_write_counter(),
                    )
                    .map_err(OperationError::service_error)?;
            }
            None => {
                self.storage
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
        match self.storage.get_value::<false>(point_id, hw_counter) {
            Some(mut point_payload) => {
                point_payload.merge_by_key(payload, key);
                self.storage
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
                self.storage
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
        match self.storage.get_value::<false>(point_id, hw_counter) {
            Some(payload) => Ok(payload),
            None => Ok(Default::default()),
        }
    }

    fn get_sequential(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self.storage.get_value::<true>(point_id, hw_counter) {
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
        match self.storage.get_value::<false>(point_id, hw_counter) {
            Some(mut payload) => {
                let res = payload.remove(key);
                if !res.is_empty() {
                    self.storage
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
        let res = self.storage.delete_value(point_id);
        Ok(res)
    }

    #[cfg(test)]
    fn clear_all(&mut self, _: &HardwareCounterCell) -> OperationResult<()> {
        self.storage.clear().map_err(|err| {
            OperationError::service_error(format!("Failed to clear mmap payload storage: {err}"))
        })
    }

    fn flusher(&self) -> (Flusher, Flusher) {
        let (stage_1_flusher, stage_2_flusher) = self.storage.flusher();

        let stage_1_flusher = Box::new(move || {
            stage_1_flusher().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to flush mmap payload gridstore: {err}"
                ))
            })
        });
        let stage_2_flusher = Box::new(move || {
            stage_2_flusher().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to flush mmap payload gridstore deletes: {err}"
                ))
            })
        });

        (stage_1_flusher, stage_2_flusher)
    }

    fn iter<F>(&self, mut callback: F, hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        self.storage.iter(
            |point_id, payload| {
                callback(point_id, &payload).map_err(|e|
                    // TODO return proper error
                    std::io::Error::other(
                        e.to_string(),
                    ))
            },
            hw_counter.ref_payload_io_read_counter(),
        )?;
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.storage.immutable_files()
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        Ok(self.storage.get_storage_size_bytes())
    }

    fn is_on_disk(&self) -> bool {
        !self.populate
    }
}

/// Get storage directory for this payload storage
pub fn storage_dir<P: AsRef<Path>>(segment_path: P) -> PathBuf {
    segment_path.as_ref().join(STORAGE_PATH)
}
