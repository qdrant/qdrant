use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::{Advice, AdviceSetting, MmapFlusher};
use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;
use crate::vector_storage::chunked_mmap_vectors::ChunkedMmapVectors;
use crate::vector_storage::{Random, VectorOffsetType};

pub struct QuantizedChunkedMmapStorage {
    data: ChunkedMmapVectors<u8>,
}

impl QuantizedChunkedMmapStorage {
    pub fn new(path: &Path, quantized_vector_size: usize, in_ram: bool) -> OperationResult<Self> {
        let advice = if in_ram {
            AdviceSetting::from(Advice::Normal)
        } else {
            AdviceSetting::Global
        };
        let data = ChunkedMmapVectors::<u8>::open(
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
}

impl quantization::EncodedStorage for QuantizedChunkedMmapStorage {
    fn get_vector_data(&self, index: PointOffsetType) -> &[u8] {
        self.data
            .get::<Random>(index as VectorOffsetType)
            .unwrap_or_default()
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

    fn is_on_disk(&self) -> bool {
        true
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
        ChunkedMmapVectors::files(&self.data)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        ChunkedMmapVectors::immutable_files(&self.data)
    }
}

#[allow(dead_code)]
pub struct QuantizedChunkedMmapStorageBuilder {
    data: ChunkedMmapVectors<u8>,
    hw_counter: HardwareCounterCell,
}

impl QuantizedChunkedMmapStorageBuilder {
    #[allow(dead_code)]
    pub fn new(path: &Path, quantized_vector_size: usize, in_ram: bool) -> OperationResult<Self> {
        let advice = if in_ram {
            AdviceSetting::from(Advice::Normal)
        } else {
            AdviceSetting::Global
        };
        let data = ChunkedMmapVectors::<u8>::open(
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
