use std::borrow::Cow;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::{Advice, AdviceSetting, MmapFlusher};
use common::types::PointOffsetType;
use common::universal_io::{OneshotFile, OpenOptions, Populate, UniversalRead, UniversalReadFs};
use fs_err as fs;
use fs_err::File;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::vector_utils::TrySetCapacityExact;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::volatile_chunked_vectors::VolatileChunkedVectors;

#[derive(Debug)]
pub struct QuantizedRamStorage {
    vectors: VolatileChunkedVectors<u8>,
    path: PathBuf,
}

impl QuantizedRamStorage {
    /// Load all quantized vectors into RAM through the provided [`UniversalRead`]
    /// filesystem, performing no writes.
    ///
    /// The data is read once through the pluggable `S` backend (mmap, io_uring,
    /// object storage, …) and evicted from the RAM/page cache afterwards via
    /// [`OneshotFile`], since we keep our own heap copy.
    pub fn from_file<S: UniversalRead>(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        quantized_vector_size: usize,
    ) -> OperationResult<Self> {
        if quantized_vector_size == 0 {
            return Err(OperationError::service_error(
                "`quantized_vector_size` must be non-zero",
            ));
        }

        let storage = OneshotFile::new(fs.open(
            path,
            OpenOptions {
                writeable: false,
                need_sequential: true,
                populate: Populate::No,
                advice: AdviceSetting::Advice(Advice::Sequential),
            },
            Default::default(),
        )?);

        // Read the whole file in a single access and validate against the returned
        // buffer's length, rather than querying `len()` separately. Avoids an extra
        // metadata round-trip on backends where size lookups are expensive (e.g. S3).
        let data = storage.read_whole::<u8>()?;
        let len = data.len();
        if !len.is_multiple_of(quantized_vector_size) {
            return Err(OperationError::inconsistent_storage(format!(
                "Encoded file size ({len}) is not a multiple of quantized_vector_size ({quantized_vector_size})",
            )));
        }

        let mut vectors = VolatileChunkedVectors::<u8>::new(quantized_vector_size);
        vectors.extend(&data).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to load quantized vectors into RAM: {err}"
            ))
        })?;
        vectors.shrink_last_chunk();

        Ok(QuantizedRamStorage {
            vectors,
            path: path.to_path_buf(),
        })
    }
}

impl quantization::EncodedStorage for QuantizedRamStorage {
    fn get_vector_data(&self, index: PointOffsetType) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.vectors.get(index as VectorOffsetType))
    }

    fn get_vector_data_opt(&self, index: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        Some(Cow::Borrowed(
            self.vectors.get_opt(index as VectorOffsetType)?,
        ))
    }

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        // Skip hardware counter increment because it's a RAM storage.
        self.vectors
            .insert(id as usize, vector)
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        Ok(())
    }

    fn is_in_ram_or_mmap() -> bool {
        true
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn vectors_count(&self) -> usize {
        self.vectors.len()
    }

    fn flusher(&self) -> MmapFlusher {
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn heap_size_bytes(&self) -> usize {
        let Self { vectors, path: _ } = self;
        vectors.heap_size_bytes()
    }
}

pub struct QuantizedRamStorageBuilder {
    pub vectors: VolatileChunkedVectors<u8>,
    pub path: PathBuf,
}

impl QuantizedRamStorageBuilder {
    pub fn new(path: &Path, count: usize, dim: usize) -> OperationResult<Self> {
        let mut vectors = VolatileChunkedVectors::new(dim);
        vectors.try_set_capacity_exact(count)?;
        Ok(Self {
            vectors,
            path: path.to_path_buf(),
        })
    }
}

impl quantization::EncodedStorageBuilder for QuantizedRamStorageBuilder {
    type Storage = QuantizedRamStorage;
    type Error = std::io::Error;

    fn build(self) -> std::io::Result<QuantizedRamStorage> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }
        let mut buffer = BufWriter::new(File::create(&self.path)?);
        for i in 0..self.vectors.len() {
            buffer.write_all(self.vectors.get(i))?;
        }

        // Explicitly flush write buffer so we can catch IO errors
        buffer.flush()?;
        buffer.into_inner()?.sync_all()?;

        Ok(QuantizedRamStorage {
            vectors: self.vectors,
            path: self.path,
        })
    }

    fn push_vector_data(&mut self, other: &[u8]) -> std::io::Result<()> {
        self.vectors
            .push(other)
            .map(|_| ())
            .map_err(|e| std::io::Error::other(format!("Failed to push vector data: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use common::universal_io::{MmapFile, MmapFs};

    use super::*;
    use crate::common::operation_error::OperationError;

    #[test]
    fn rejects_zero_quantized_vector_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("quantized.bin");
        fs::write(&path, [1, 2, 3, 4]).unwrap();

        let err = QuantizedRamStorage::from_file::<MmapFile>(&MmapFs, &path, 0).unwrap_err();

        assert!(matches!(
            err,
            OperationError::ServiceError { description, .. }
                if description == "`quantized_vector_size` must be non-zero"
        ));
    }

    #[test]
    fn rejects_trailing_partial_vector() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("quantized.bin");
        fs::write(&path, [1, 2, 3, 4, 5]).unwrap();

        let err = QuantizedRamStorage::from_file::<MmapFile>(&MmapFs, &path, 2).unwrap_err();

        assert!(matches!(
            err,
            OperationError::InconsistentStorage { description }
                if description == "Encoded file size (5) is not a multiple of quantized_vector_size (2)"
        ));
    }

    #[test]
    fn loads_complete_vectors() {
        use quantization::EncodedStorage;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("quantized.bin");
        fs::write(&path, [1, 2, 3, 4]).unwrap();

        let storage = QuantizedRamStorage::from_file::<MmapFile>(&MmapFs, &path, 2).unwrap();

        assert_eq!(storage.vectors_count(), 2);
        assert_eq!(storage.get_vector_data(0).as_ref(), [1, 2]);
        assert_eq!(storage.get_vector_data(1).as_ref(), [3, 4]);
    }
}
