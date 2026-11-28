use std::path::PathBuf;

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
use crate::types::{IoBackend, OwnedPayloadRef, Payload};

#[derive(Debug)]
pub enum PayloadStorageEnum {
    #[cfg(feature = "testing")]
    InMemory(InMemoryPayloadStorage),
    Mmap(PayloadStorageImpl),
    #[cfg(target_os = "linux")]
    IoUring(PayloadStorageImpl<IoUringFile>),
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory($pattern) => $result,
            PayloadStorageEnum::Mmap($pattern) => $result,
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring($pattern) => $result,
        }
    };
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
    fn get(&self, point_offset: PointOffsetType) -> OperationResult<Payload> {
        for_all!(self, s => s.get(point_offset))
    }

    fn get_sequential(&self, point_offset: PointOffsetType) -> OperationResult<Payload> {
        for_all!(self, s => s.get_sequential(point_offset))
    }

    fn payload_ref(&self, point_offset: PointOffsetType) -> OperationResult<OwnedPayloadRef<'_>> {
        for_all!(self, s => s.payload_ref(point_offset))
    }

    fn payload_ref_field(
        &self,
        point_offset: PointOffsetType,
        field: &str,
    ) -> OperationResult<OwnedPayloadRef<'_>> {
        for_all!(self, s => s.payload_ref_field(point_offset, field))
    }

    fn read_payloads<P: AccessPattern, U: common::universal_io::UserData>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, Payload) -> OperationResult<()>,
    ) -> OperationResult<()> {
        for_all!(self, s => s.read_payloads::<P, _>(point_offsets, callback))
    }

    fn read_payloads_raw<P: AccessPattern, U: common::universal_io::UserData>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, Option<&[u8]>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        for_all!(self, s => s.read_payloads_raw::<P, _>(point_offsets, callback))
    }

    fn read_payload_lens<P: AccessPattern, U: common::universal_io::UserData>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, Option<usize>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        for_all!(self, s => s.read_payload_lens::<P, _>(point_offsets, callback))
    }

    fn iter<F>(&self, callback: F) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        for_all!(self, s => s.iter(callback))
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        for_all!(self, s => s.get_storage_size_bytes())
    }

    fn is_cold(&self) -> bool {
        for_all!(self, s => s.is_cold())
    }

    fn io_backend(&self) -> Option<IoBackend> {
        match self {
            // Heap-only, never reads through a universal-IO backend
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(_) => None,
            PayloadStorageEnum::Mmap(_) => Some(IoBackend::Mmap),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(_) => Some(IoBackend::IoUring),
        }
    }
}

impl PayloadStorage for PayloadStorageEnum {
    fn overwrite(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        for_all!(self, s => s.overwrite(point_id, payload))
    }

    fn set(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        for_all!(self, s => s.set(point_id, payload))
    }

    fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
    ) -> OperationResult<()> {
        for_all!(self, s => s.set_by_key(point_id, payload, key))
    }

    fn delete(&mut self, point_id: PointOffsetType, key: &JsonPath) -> OperationResult<Vec<Value>> {
        for_all!(self, s => s.delete(point_id, key))
    }

    fn clear(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        for_all!(self, s => s.clear(point_id))
    }

    #[cfg(test)]
    fn clear_all(&mut self) -> OperationResult<()> {
        for_all!(self, s => s.clear_all())
    }

    fn flusher(&self) -> Flusher {
        for_all!(self, s => s.flusher())
    }

    fn files(&self) -> Vec<PathBuf> {
        for_all!(self, s => s.files())
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        for_all!(self, s => s.immutable_files())
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

    /// Don't journal value mappings on flush, see [`Blobstore::disable_journal`].
    ///
    /// [`Blobstore::disable_journal`]: blobstore::Blobstore::disable_journal
    pub fn disable_journal(&mut self) {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(_) => {}
            PayloadStorageEnum::Mmap(s) => s.disable_journal(),
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.disable_journal(),
        }
    }

    /// Switch to a layout for a storage that is only read from now on, for storages that have
    /// one. Persisted by the next flush, the storage stays writable.
    pub fn make_immutable(&self) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(_) => {}
            PayloadStorageEnum::Mmap(s) => s.make_immutable()?,
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => s.make_immutable()?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::ambient;
    use common::universal_io::MmapFile;
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::types::Payload;

    #[rstest]
    fn test_mmap_storage(#[values(false, true)] populate: bool) {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let _scope = ambient::test_guard();

        let mut storage: PayloadStorageEnum =
            PayloadStorageImpl::<MmapFile>::open_or_create(dir.path().to_path_buf(), populate)
                .unwrap()
                .into();
        let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
        storage.set(100, &payload).unwrap();
        storage.clear_all().unwrap();
        storage.set(100, &payload).unwrap();
        storage.clear_all().unwrap();
        storage.set(100, &payload).unwrap();
        assert!(!storage.get(100).unwrap().is_empty());
        storage.clear_all().unwrap();
        assert_eq!(storage.get(100).unwrap(), Default::default());
    }
}
