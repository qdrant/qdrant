use std::borrow::Cow;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::mmap::{Advice, AdviceSetting, MmapFlusher};
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs, UniversalKind, UniversalRead};

use crate::common::operation_error::OperationResult;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::{ChunkedVectors, ChunkedVectorsRead};

pub struct QuantizedChunkedMmapStorage {
    data: ChunkedVectors<u8, MmapFile>,
}

impl QuantizedChunkedMmapStorage {
    pub fn new(path: &Path, quantized_vector_size: usize, in_ram: bool) -> OperationResult<Self> {
        let advice = if in_ram {
            AdviceSetting::from(Advice::Normal)
        } else {
            AdviceSetting::Global
        };
        let data = ChunkedVectors::open(
            MmapFs,
            path,
            quantized_vector_size,
            advice,
            Some(in_ram), // populate
        )?;
        Ok(Self { data })
    }

    pub fn populate(&self) -> OperationResult<()> {
        self.data.populate()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self { data } = self;
        data.clear_cache()
    }
}

impl quantization::EncodedStorage for QuantizedChunkedMmapStorage {
    fn get_vector_data(&self, index: PointOffsetType) -> Cow<'_, [u8]> {
        self.data
            .get::<Random>(index as VectorOffsetType)
            .unwrap_or_default()
    }

    fn iter_batch(
        &self,
        offsets: &[PointOffsetType],
    ) -> impl Iterator<Item = (usize, Cow<'_, [u8]>)> {
        let offsets = offsets
            .iter()
            .enumerate()
            .map(|(index, &offset)| (index, offset, 1));

        self.data.iter_vectors::<Random, _>(offsets)
    }

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        hw_counter: &common::counter::hardware_counter::HardwareCounterCell,
    ) -> std::io::Result<()> {
        self.data
            .insert(id as VectorOffsetType, vector, hw_counter)
            .map_err(std::io::Error::other)
    }

    fn is_in_ram_or_mmap() -> bool {
        type StorageType = ChunkedVectors<u8, MmapFile>;

        // This should produce compilation error, if `QuantizedChunkedMmapStorage::data` type is changed,
        // to ensure that we always use correct type for this check
        fn _static_assert_storage_type(storage: QuantizedChunkedMmapStorage) {
            let _: StorageType = storage.data;
        }

        match StorageType::storage_kind() {
            UniversalKind::IoUring |
            UniversalKind::DiskCache | UniversalKind::S3 |
            UniversalKind::Gcs |
            UniversalKind::Azure => false,
            UniversalKind::SimpleDiskCache | // FIXME: only `true` if it was entirely prefilled
            UniversalKind::Mmap => true,
        }
    }

    fn is_on_disk(&self) -> bool {
        self.data.is_on_disk()
    }

    fn vectors_count(&self) -> usize {
        self.data.len()
    }

    fn flusher(&self) -> MmapFlusher {
        let flusher = self.data.flusher();
        Box::new(move || {
            Ok(flusher().map_err(|e| {
                std::io::Error::other(format!("Failed to flush quantization storage: {e}"))
            })?)
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        self.data.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.data.immutable_files()
    }

    fn heap_size_bytes(&self) -> usize {
        let Self { data } = self;
        data.heap_size_bytes()
    }
}

/// Read-only counterpart of [`QuantizedChunkedMmapStorage`], generic over the
/// [`UniversalRead`] backend `S`.
///
/// Reads the appendable (chunked) on-disk layout without opening it for writing,
/// so the mutable storage format can be loaded by the read-only quantized storage.
#[derive(Debug)]
pub struct QuantizedChunkedStorageRead<S: UniversalRead> {
    data: ChunkedVectorsRead<u8, S>,
}

impl<S: UniversalRead> QuantizedChunkedStorageRead<S> {
    pub fn open(fs: &S::Fs, path: &Path, quantized_vector_size: usize) -> OperationResult<Self> {
        let data = ChunkedVectorsRead::open(
            fs,
            path,
            quantized_vector_size,
            AdviceSetting::Global,
            None, // populate
        )?;
        Ok(Self { data })
    }

    pub fn populate(&self) -> OperationResult<()> {
        self.data.populate()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.data.clear_cache()
    }
}

impl<S: UniversalRead> quantization::EncodedStorage for QuantizedChunkedStorageRead<S> {
    fn get_vector_data(&self, index: PointOffsetType) -> Cow<'_, [u8]> {
        self.data
            .get::<Random>(index as VectorOffsetType)
            .unwrap_or_default()
    }

    fn iter_batch(
        &self,
        offsets: &[PointOffsetType],
    ) -> impl Iterator<Item = (usize, Cow<'_, [u8]>)> {
        let offsets = offsets
            .iter()
            .enumerate()
            .map(|(index, &offset)| (index, offset, 1));

        self.data.iter_vectors::<Random, _>(offsets)
    }

    fn upsert_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Cannot upsert vector in read-only chunked storage",
        ))
    }

    fn is_in_ram_or_mmap() -> bool {
        match S::kind() {
            UniversalKind::Mmap | UniversalKind::SimpleDiskCache => true,
            UniversalKind::IoUring
            | UniversalKind::DiskCache
            | UniversalKind::S3
            | UniversalKind::Gcs
            | UniversalKind::Azure => false,
        }
    }

    fn is_on_disk(&self) -> bool {
        self.data.is_on_disk()
    }

    fn vectors_count(&self) -> usize {
        self.data.len()
    }

    fn flusher(&self) -> MmapFlusher {
        // Read-only storage is never dirty.
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<PathBuf> {
        self.data.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.data.immutable_files()
    }

    fn heap_size_bytes(&self) -> usize {
        self.data.heap_size_bytes()
    }
}

pub struct QuantizedChunkedMmapStorageBuilder {
    data: ChunkedVectors<u8, MmapFile>,
    hw_counter: HardwareCounterCell,
}

impl QuantizedChunkedMmapStorageBuilder {
    pub fn new(path: &Path, quantized_vector_size: usize, in_ram: bool) -> OperationResult<Self> {
        let advice = if in_ram {
            AdviceSetting::from(Advice::Normal)
        } else {
            AdviceSetting::Global
        };
        let data = ChunkedVectors::open(
            MmapFs,
            path,
            quantized_vector_size,
            advice,
            Some(in_ram), // populate
        )?;
        Ok(Self {
            data,
            hw_counter: HardwareCounterCell::disposable(),
        })
    }
}

impl quantization::EncodedStorageBuilder for QuantizedChunkedMmapStorageBuilder {
    type Storage = QuantizedChunkedMmapStorage;
    type Error = std::io::Error;

    fn build(self) -> std::io::Result<QuantizedChunkedMmapStorage> {
        let Self {
            data,
            hw_counter: _,
        } = self;

        data.flusher()().map_err(|e| {
            std::io::Error::other(format!("Failed to flush quantization storage: {e}"))
        })?;

        Ok(QuantizedChunkedMmapStorage { data })
    }

    fn push_vector_data(&mut self, other: &[u8]) -> std::io::Result<()> {
        self.data
            .push(other, &self.hw_counter)
            .map(|_| ())
            .map_err(|e| std::io::Error::other(format!("Failed to push vector data: {e}")))
    }
}
