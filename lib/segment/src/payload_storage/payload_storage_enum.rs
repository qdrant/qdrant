use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorage;
#[cfg(feature = "testing")]
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::payload_storage::mmap_payload_storage::MmapPayloadStorage;
use crate::types::Payload;

#[derive(Debug)]
pub enum PayloadStorageEnum {
    #[cfg(feature = "testing")]
    InMemoryPayloadStorage(InMemoryPayloadStorage),
    MmapPayloadStorage(MmapPayloadStorage),
}

#[cfg(feature = "testing")]
impl From<InMemoryPayloadStorage> for PayloadStorageEnum {
    fn from(a: InMemoryPayloadStorage) -> Self {
        PayloadStorageEnum::InMemoryPayloadStorage(a)
    }
}

impl From<MmapPayloadStorage> for PayloadStorageEnum {
    fn from(a: MmapPayloadStorage) -> Self {
        PayloadStorageEnum::MmapPayloadStorage(a)
    }
}

impl PayloadStorage for PayloadStorageEnum {
    fn overwrite(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => {
                s.overwrite(point_id, payload, hw_counter)
            }
            PayloadStorageEnum::MmapPayloadStorage(s) => s.overwrite(point_id, payload, hw_counter),
        }
    }

    fn set(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.set(point_id, payload, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.set(point_id, payload, hw_counter),
        }
    }

    fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => {
                s.set_by_key(point_id, payload, key, hw_counter)
            }
            PayloadStorageEnum::MmapPayloadStorage(s) => {
                s.set_by_key(point_id, payload, key, hw_counter)
            }
        }
    }

    fn get(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.get(point_id, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.get(point_id, hw_counter),
        }
    }

    fn get_sequential(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.get_sequential(point_id, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.get_sequential(point_id, hw_counter),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.delete(point_id, key, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.delete(point_id, key, hw_counter),
        }
    }

    fn clear(
        &mut self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.clear(point_id, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.clear(point_id, hw_counter),
        }
    }

    #[cfg(test)]
    fn clear_all(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.clear_all(hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.clear_all(hw_counter),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.flusher(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.flusher(),
        }
    }

    fn iter<F>(&self, callback: F, hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.iter(callback, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.iter(callback, hw_counter),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.files(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.files(),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.immutable_files(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.immutable_files(),
        }
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.get_storage_size_bytes(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.get_storage_size_bytes(),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.is_on_disk(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.is_on_disk(),
        }
    }
}

impl PayloadStorageEnum {
    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(_) => {}
            PayloadStorageEnum::MmapPayloadStorage(s) => s.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(_) => {}
            PayloadStorageEnum::MmapPayloadStorage(s) => s.clear_cache()?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::types::Payload;

    #[rstest]
    fn test_mmap_storage(#[values(false, true)] populate: bool) {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let hw_counter = HardwareCounterCell::new();

        let mut storage: PayloadStorageEnum =
            MmapPayloadStorage::open_or_create(dir.path().to_path_buf(), populate)
                .unwrap()
                .into();
        let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        storage.clear_all(&hw_counter).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        storage.clear_all(&hw_counter).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        assert!(!storage.get(100, &hw_counter).unwrap().is_empty());
        storage.clear_all(&hw_counter).unwrap();
        assert_eq!(storage.get(100, &hw_counter).unwrap(), Default::default());
    }
}
