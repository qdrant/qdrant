mod lifecycle;
mod live_reload;
mod storage;
#[cfg(test)]
mod tests;

use std::alloc::Layout;
use std::borrow::Cow;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, UniversalRead};

pub use self::storage::ReadOnlyQuantizedVectorStorage;
use super::{QUANTIZED_CONFIG_PATH, QuantizedVectorsConfig};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::QueryVector;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::RawScorer;
use crate::vector_storage::quantized::quantized_query_scorer::InternalScorerUnsupported;
use crate::vector_storage::quantized::quantized_scorer_builder::{
    QuantizedScorerDispatch, build_quantized_raw_scorer,
};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsRead;

/// Read-only counterpart of [`super::super::QuantizedVectors`].
///
/// Generic over the [`UniversalRead`] backend `S`, it is opened from existing
/// on-disk data (see [`Self::open`]) and exposes only read operations: it has no
/// `create`/`upsert`/builder path and never writes to disk.
#[derive(Debug)]
pub struct ReadOnlyQuantizedVectors<S: UniversalRead = MmapFile> {
    storage_impl: ReadOnlyQuantizedVectorStorage<S>,
    config: QuantizedVectorsConfig,
    path: PathBuf,
    distance: Distance,
    datatype: VectorStorageDatatype,
}

impl<S: UniversalRead> ReadOnlyQuantizedVectors<S> {
    pub(super) fn new(
        storage_impl: ReadOnlyQuantizedVectorStorage<S>,
        config: QuantizedVectorsConfig,
        path: PathBuf,
        distance: Distance,
        datatype: VectorStorageDatatype,
    ) -> Self {
        Self {
            storage_impl,
            config,
            path,
            distance,
            datatype,
        }
    }

    pub fn config(&self) -> &QuantizedVectorsConfig {
        &self.config
    }

    pub fn get_storage(&self) -> &ReadOnlyQuantizedVectorStorage<S> {
        &self.storage_impl
    }

    pub fn distance(&self) -> Distance {
        self.distance
    }

    pub fn datatype(&self) -> VectorStorageDatatype {
        self.datatype
    }

    pub fn is_on_disk(&self) -> bool {
        self.storage_impl.is_on_disk()
    }

    pub fn raw_scorer<'a>(
        &'a self,
        query: QueryVector,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        build_quantized_raw_scorer(
            &self.storage_impl,
            &self.config.quantization_config,
            &self.distance,
            self.datatype,
            self.storage_impl.is_on_disk(),
            query,
            hardware_counter,
        )
    }

    /// Build a raw scorer for the specified `point_id`.
    /// If not supported, return [`InternalScorerUnsupported`] with the original `hardware_counter`.
    pub fn raw_internal_scorer<'a>(
        &'a self,
        point_id: PointOffsetType,
        hardware_counter: HardwareCounterCell,
    ) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported> {
        self.storage_impl
            .raw_internal_scorer(point_id, hardware_counter)
    }

    pub fn default_rescoring(&self) -> bool {
        self.storage_impl.default_rescoring()
    }

    /// Get layout for a single quantized vector (size in bytes and required alignment).
    pub fn get_quantized_vector_layout(&self) -> OperationResult<Layout> {
        self.storage_impl.get_quantized_vector_layout()
    }

    pub fn get_quantized_vector(&self, id: PointOffsetType) -> Cow<'_, [u8]> {
        self.storage_impl.get_quantized_vector(id)
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = self.storage_impl.files();
        files.push(self.path.join(QUANTIZED_CONFIG_PATH));
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = self.storage_impl.immutable_files();
        files.push(self.path.join(QUANTIZED_CONFIG_PATH));
        files
    }

    pub fn populate(&self) -> OperationResult<()> {
        self.storage_impl.populate()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage_impl.clear_cache()
    }

    pub fn flusher(&self) -> Flusher {
        let flusher = self.storage_impl.flusher();
        Box::new(move || flusher().map_err(OperationError::from))
    }
}

impl<S: UniversalRead> QuantizedVectorsRead for ReadOnlyQuantizedVectors<S> {
    fn config(&self) -> &QuantizedVectorsConfig {
        self.config()
    }

    fn default_rescoring(&self) -> bool {
        self.default_rescoring()
    }

    fn raw_scorer<'a>(
        &'a self,
        query: QueryVector,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        self.raw_scorer(query, hardware_counter)
    }

    fn raw_internal_scorer<'a>(
        &'a self,
        point_id: PointOffsetType,
        hardware_counter: HardwareCounterCell,
    ) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported> {
        self.raw_internal_scorer(point_id, hardware_counter)
    }
}

impl<S: UniversalRead> crate::common::memory_usage::MemoryReporter for ReadOnlyQuantizedVectors<S> {
    fn memory_usage(&self) -> crate::common::memory_usage::ComponentMemoryUsage {
        use crate::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent};

        let files = self.files();
        let heap_bytes = self.storage_impl.heap_size_bytes() as u64;

        let intent = FileStorageIntent::OnDisk;

        if heap_bytes > 0 {
            ComponentMemoryUsage::from_files_and_ram(files, intent, heap_bytes)
        } else {
            ComponentMemoryUsage::from_files(files, intent)
        }
    }
}
