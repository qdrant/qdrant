use std::borrow::Cow;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::mmap::{AdviceSetting, MmapFlusher};
use common::types::PointOffsetType;
use common::universal_io::{Populate, UniversalRead};

use crate::common::operation_error::OperationResult;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;

/// Read-only counterpart of [`super::QuantizedChunkedStorage`], generic over the
/// [`UniversalRead`] backend `S`.
///
/// Reads the appendable (chunked) on-disk layout without opening it for writing,
/// so the mutable storage format can be loaded by the read-only quantized storage.
#[derive(Debug)]
pub struct QuantizedChunkedStorageRead<S: UniversalRead> {
    pub(super) data: ChunkedVectorsRead<u8, S>,
}

impl<S: UniversalRead> QuantizedChunkedStorageRead<S> {
    pub fn open(fs: &S::Fs, path: &Path, quantized_vector_size: usize) -> OperationResult<Self> {
        let data = ChunkedVectorsRead::open(
            fs,
            path,
            quantized_vector_size,
            AdviceSetting::Global,
            Populate::No, // TODO(uio): consider `always_in_ram`?
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
        self.get_vector_data_opt(index).unwrap_or_default()
    }

    fn get_vector_data_opt(&self, index: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        self.data.get::<Random>(index as VectorOffsetType)
    }

    fn for_each_batch(
        &self,
        offsets: &[PointOffsetType],
        mut callback: impl FnMut(usize, Cow<'_, [u8]>),
    ) {
        let offsets = offsets
            .iter()
            .enumerate()
            .map(|(index, &offset)| (index, offset, 1));

        self.data
            .for_each_vector::<Random, _>(offsets, |index, vector| {
                callback(index, vector);
                Ok(())
            })
            .expect("vectors read");
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
        S::kind().is_in_ram_or_mmap()
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
