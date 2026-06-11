use std::borrow::Cow;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{AccessPattern, Random};
use common::mmap::{Advice, AdviceSetting, MmapFlusher};
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, UniversalWrite};
use quantization::EncodedStorage;

use crate::common::operation_error::OperationResult;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::ChunkedVectors;

/// Appendable (chunked) quantized storage, generic over the [`UniversalWrite`] backend `S`.
///
/// Read-write counterpart of [`super::QuantizedChunkedStorageRead`]. The backend `fs` is
/// supplied by the caller (defaulting to [`MmapFile`]) rather than hardcoded.
pub struct QuantizedChunkedStorage<S: UniversalWrite + Send + 'static = MmapFile> {
    data: ChunkedVectors<u8, S>,
}

impl<S: UniversalWrite + Send + 'static> QuantizedChunkedStorage<S> {
    pub fn new(
        fs: S::Fs,
        path: &Path,
        quantized_vector_size: usize,
        in_ram: bool,
    ) -> OperationResult<Self> {
        let advice = if in_ram {
            AdviceSetting::from(Advice::Normal)
        } else {
            AdviceSetting::Global
        };
        let data = ChunkedVectors::open(
            fs,
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

    /// Record slots left in the chunk containing `start`.
    pub fn get_remaining_chunk_keys(&self, start: PointOffsetType) -> usize {
        self.data
            .get_remaining_chunk_keys(start as VectorOffsetType)
    }

    /// Returns multiple continuous vectors given a start `index` and a `count` of vectors to return.
    ///
    /// This function returns `None` iff the vector is out of bounds.
    /// If a chunk boundary is crossed, this function returns a Cow::Owned copy of the vectors.
    pub fn get_many<P>(&self, index: PointOffsetType, count: usize) -> Option<Cow<'_, [u8]>>
    where
        P: AccessPattern,
    {
        self.data.get_many::<P>(index as usize, count).or_else(|| {
            let mut blob = Vec::with_capacity(count * self.data.dim());
            for inner_id in index..index + count as u32 {
                blob.extend_from_slice(&self.get_vector_data_opt(inner_id)?);
            }
            Some(Cow::Owned(blob))
        })
    }
}

impl<S: UniversalWrite + Send + 'static> quantization::EncodedStorage
    for QuantizedChunkedStorage<S>
{
    fn get_vector_data(&self, index: PointOffsetType) -> Cow<'_, [u8]> {
        self.get_vector_data_opt(index).unwrap_or_default()
    }

    fn get_vector_data_opt(&self, index: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        self.data.get::<Random>(index as VectorOffsetType)
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
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        self.data
            .insert(id as VectorOffsetType, vector, hw_counter)
            .map_err(std::io::Error::other)
    }

    fn is_in_ram_or_mmap() -> bool {
        ChunkedVectors::<u8, S>::storage_kind().is_in_ram_or_mmap()
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

pub struct QuantizedChunkedStorageBuilder<S: UniversalWrite + Send + 'static = MmapFile> {
    data: ChunkedVectors<u8, S>,
    hw_counter: HardwareCounterCell,
}

impl<S: UniversalWrite + Send + 'static> QuantizedChunkedStorageBuilder<S> {
    pub fn new(
        fs: S::Fs,
        path: &Path,
        quantized_vector_size: usize,
        in_ram: bool,
    ) -> OperationResult<Self> {
        let advice = if in_ram {
            AdviceSetting::from(Advice::Normal)
        } else {
            AdviceSetting::Global
        };
        let data = ChunkedVectors::open(
            fs,
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

impl<S: UniversalWrite + Send + 'static> quantization::EncodedStorageBuilder
    for QuantizedChunkedStorageBuilder<S>
{
    type Storage = QuantizedChunkedStorage<S>;
    type Error = std::io::Error;

    fn build(self) -> std::io::Result<QuantizedChunkedStorage<S>> {
        let Self {
            data,
            hw_counter: _,
        } = self;

        data.flusher()().map_err(|e| {
            std::io::Error::other(format!("Failed to flush quantization storage: {e}"))
        })?;

        Ok(QuantizedChunkedStorage { data })
    }

    fn push_vector_data(&mut self, other: &[u8]) -> std::io::Result<()> {
        self.data
            .push(other, &self.hw_counter)
            .map(|_| ())
            .map_err(|e| std::io::Error::other(format!("Failed to push vector data: {e}")))
    }
}
