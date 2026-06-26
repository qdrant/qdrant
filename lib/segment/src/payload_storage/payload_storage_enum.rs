use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
#[cfg(target_os = "linux")]
use common::universal_io::IoUringFile;
use serde_json::Value;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::json_path::JsonPath;
#[cfg(feature = "testing")]
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::payload_storage::payload_storage_impl::PayloadStorageImpl;
use crate::payload_storage::{PayloadStorage, PayloadStorageRead};
use crate::types::{OwnedPayloadRef, Payload};

#[derive(Debug)]
pub enum PayloadStorageEnum {
    #[cfg(feature = "testing")]
    InMemory(InMemoryPayloadStorage),
    Mmap(PayloadStorageImpl),
    #[cfg(target_os = "linux")]
    IoUring(PayloadStorageImpl<IoUringFile>),
}

#[cfg(feature = "testing")]
impl From<InMemoryPayloadStorage> for PayloadStorageEnum {
    fn from(a: InMemoryPayloadStorage) -> Self {
        PayloadStorageEnum::InMemory(a)
    }
}

impl From<PayloadStorageImpl> for PayloadStorageEnum {
    fn from(a: PayloadStorageImpl) -> Self {
        PayloadStorageEnum::Mmap(a)
    }
}

#[cfg(target_os = "linux")]
impl From<PayloadStorageImpl<IoUringFile>> for PayloadStorageEnum {
    fn from(a: PayloadStorageImpl<IoUringFile>) -> Self {
        PayloadStorageEnum::IoUring(a)
    }
}

impl PayloadStorageRead for PayloadStorageEnum {
    fn get(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => s.get(point_offset, hw_counter),
            PayloadStorageEnum::Mmap(s) => s.get(point_offset, hw_counter),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.get(point_offset, hw_counter),
        }
    }

    fn get_sequential(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => s.get_sequential(point_offset, hw_counter),
            PayloadStorageEnum::Mmap(s) => s.get_sequential(point_offset, hw_counter),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.get_sequential(point_offset, hw_counter),
        }
    }

    fn payload_ref(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<OwnedPayloadRef<'_>> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => s.payload_ref(point_offset, hw_counter),
            PayloadStorageEnum::Mmap(s) => s.payload_ref(point_offset, hw_counter),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.payload_ref(point_offset, hw_counter),
        }
    }

    fn read_payloads<P: AccessPattern, U: common::universal_io::UserData>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, Payload) -> OperationResult<()>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => {
                s.read_payloads::<P, _>(point_offsets, callback, hw_counter)
            }

            PayloadStorageEnum::Mmap(s) => {
                s.read_payloads::<P, _>(point_offsets, callback, hw_counter)
            }
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => {
                s.read_payloads::<P, _>(point_offsets, callback, hw_counter)
            }
        }
    }

    fn iter<F>(&self, callback: F, hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => s.iter(callback, hw_counter),
            PayloadStorageEnum::Mmap(s) => s.iter(callback, hw_counter),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.iter(callback, hw_counter),
        }
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => s.get_storage_size_bytes(),
            PayloadStorageEnum::Mmap(s) => s.get_storage_size_bytes(),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.get_storage_size_bytes(),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => s.is_on_disk(),
            PayloadStorageEnum::Mmap(s) => s.is_on_disk(),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.is_on_disk(),
        }
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
            PayloadStorageEnum::InMemory(s) => s.overwrite(point_id, payload, hw_counter),
            PayloadStorageEnum::Mmap(s) => s.overwrite(point_id, payload, hw_counter),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.overwrite(point_id, payload, hw_counter),
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
            PayloadStorageEnum::InMemory(s) => s.set(point_id, payload, hw_counter),
            PayloadStorageEnum::Mmap(s) => s.set(point_id, payload, hw_counter),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.set(point_id, payload, hw_counter),
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
            PayloadStorageEnum::InMemory(s) => s.set_by_key(point_id, payload, key, hw_counter),
            PayloadStorageEnum::Mmap(s) => s.set_by_key(point_id, payload, key, hw_counter),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.set_by_key(point_id, payload, key, hw_counter),
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
            PayloadStorageEnum::InMemory(s) => s.delete(point_id, key, hw_counter),
            PayloadStorageEnum::Mmap(s) => s.delete(point_id, key, hw_counter),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.delete(point_id, key, hw_counter),
        }
    }

    fn clear(
        &mut self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => s.clear(point_id, hw_counter),
            PayloadStorageEnum::Mmap(s) => s.clear(point_id, hw_counter),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.clear(point_id, hw_counter),
        }
    }

    #[cfg(test)]
    fn clear_all(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => s.clear_all(hw_counter),
            PayloadStorageEnum::Mmap(s) => s.clear_all(hw_counter),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.clear_all(hw_counter),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => s.flusher(),
            PayloadStorageEnum::Mmap(s) => s.flusher(),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => s.files(),
            PayloadStorageEnum::Mmap(s) => s.files(),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.files(),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => s.immutable_files(),
            PayloadStorageEnum::Mmap(s) => s.immutable_files(),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.immutable_files(),
        }
    }
}

impl PayloadStorageEnum {
    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(_) => {}
            PayloadStorageEnum::Mmap(s) => s.populate()?,
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(_) => {}
            PayloadStorageEnum::Mmap(s) => s.clear_cache()?,
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.clear_cache()?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::universal_io::MmapFile;
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::types::Payload;

    #[rstest]
    fn test_mmap_storage(#[values(false, true)] populate: bool) {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let hw_counter = HardwareCounterCell::new();

        let mut storage: PayloadStorageEnum =
            PayloadStorageImpl::<MmapFile>::open_or_create(dir.path().to_path_buf(), populate)
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
