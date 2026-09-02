use std::borrow::Cow;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::MmapFlusher;
use common::prefetch::{
    MAX_UNPREFETCHED_BATCH, MIN_PREFETCH_STORAGE_BYTES, prefetch_slice, prefetch_slice_l2,
    prefetch_windows,
};
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, OneshotFile, UniversalRead, UniversalReadFs};
use fs_err as fs;
use fs_err::File;
use quantization::encoded_storage::default_for_each_batch;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::vector_utils::TrySetCapacityExact;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::query_scorer::is_read_with_prefetch_efficient;
use crate::vector_storage::volatile_chunked_vectors::VolatileChunkedVectors;

#[derive(Debug)]
pub struct QuantizedRamStorage {
    vectors: VolatileChunkedVectors<u8>,
    path: PathBuf,
}

impl QuantizedRamStorage {
    /// Schedule background prefetch of the data file [`Self::from_file`] reads.
    ///
    /// The load reads the whole file, so the prefetch populates it.
    pub fn preopen<S: UniversalRead>(fs: &impl CachedReadFs<File = S>, path: &Path) {
        OneshotFile::<S>::preopen(fs, path)
    }

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

        let storage = OneshotFile::open(fs, path)?;

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

impl quantization::EncodedStorageWrite for QuantizedRamStorage {
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

    fn heap_size_bytes(&self) -> usize {
        let Self { vectors, path: _ } = self;
        vectors.heap_size_bytes()
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

    fn for_each_batch(
        &self,
        offsets: &[PointOffsetType],
        mut callback: impl FnMut(usize, Cow<'_, [u8]>),
    ) {
        // Tiny batches gain nothing from hints, cache-resident storages have
        // nothing to fetch, and dense-ascending batches stream — in all three
        // the hardware prefetcher already covers the access and software
        // prefetch is pure overhead.
        let storage_bytes = self.vectors.len() * self.vectors.vector_size_bytes();
        if offsets.len() <= MAX_UNPREFETCHED_BATCH
            || storage_bytes < MIN_PREFETCH_STORAGE_BYTES
            || is_read_with_prefetch_efficient(offsets)
        {
            default_for_each_batch(self, offsets, callback);
            return;
        }

        // Heap-resident vectors have the same random-access DRAM latency as
        // the mmap-backed storage; prefetch up to `far` vectors ahead of the
        // scorer to hide it. Warm-up fills the initial windows: the first
        // `near` vectors go straight to L1, the rest of the far window to L2.
        let (near, far) = prefetch_windows(self.vectors.vector_size_bytes());
        for &offset in offsets.iter().take(far).skip(near) {
            prefetch_slice_l2(self.vectors.get(offset as VectorOffsetType));
        }
        for &offset in offsets.iter().take(near) {
            prefetch_slice(self.vectors.get(offset as VectorOffsetType));
        }

        for (index, &offset) in offsets.iter().enumerate() {
            if far > 0
                && let Some(&upcoming) = offsets.get(index + far)
            {
                prefetch_slice_l2(self.vectors.get(upcoming as VectorOffsetType));
            }
            if let Some(&upcoming) = offsets.get(index + near) {
                prefetch_slice(self.vectors.get(upcoming as VectorOffsetType));
            }
            callback(
                index,
                Cow::Borrowed(self.vectors.get(offset as VectorOffsetType)),
            );
        }
    }

    fn for_each_run(
        &self,
        offsets: &[PointOffsetType],
        mut callback: impl FnMut(usize, usize, Cow<'_, [u8]>),
    ) {
        quantization::encoded_storage::for_each_consecutive_run(offsets, |first, start, len| {
            let bytes = self
                .vectors
                .get_many(start as VectorOffsetType, len)
                .expect("vectors read");
            callback(first, len, bytes);
        });
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
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
        use quantization::{EncodedStorage, EncodedStorageWrite};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("quantized.bin");
        fs::write(&path, [1, 2, 3, 4]).unwrap();

        let storage = QuantizedRamStorage::from_file::<MmapFile>(&MmapFs, &path, 2).unwrap();

        assert_eq!(storage.vectors_count(), 2);
        assert_eq!(storage.get_vector_data(0).as_ref(), [1, 2]);
        assert_eq!(storage.get_vector_data(1).as_ref(), [3, 4]);
    }

    /// `for_each_run` must serve every offset exactly once, in order, with
    /// bytes identical to per-point `get_vector_data` — including a run that
    /// straddles the internal chunk boundary.
    #[test]
    fn runs_match_per_point_reads_across_chunk_boundary() {
        use quantization::EncodedStorage;

        use crate::vector_storage::common::CHUNK_SIZE;

        const VECTOR_SIZE: usize = 68;
        // Enough vectors to cross the first chunk boundary.
        let count = CHUNK_SIZE / VECTOR_SIZE + 100;

        let mut vectors = VolatileChunkedVectors::<u8>::new(VECTOR_SIZE);
        for i in 0..count {
            let vector: Vec<u8> = (0..VECTOR_SIZE).map(|b| (i * 31 + b) as u8).collect();
            vectors.push(&vector).unwrap();
        }
        let storage = QuantizedRamStorage {
            vectors,
            path: PathBuf::new(),
        };

        let ascending: Vec<PointOffsetType> = (0..count as PointOffsetType).collect();
        let scattered: Vec<PointOffsetType> = (0..count as PointOffsetType).step_by(7).collect();

        for ids in [&ascending, &scattered] {
            let mut visited = 0;
            storage.for_each_run(ids, |first, run_len, bytes| {
                assert_eq!(first, visited, "runs must cover `ids` in order");
                assert_eq!(bytes.len(), run_len * VECTOR_SIZE);
                for (i, vector) in bytes.as_chunks::<VECTOR_SIZE>().0.iter().enumerate() {
                    assert_eq!(
                        vector.as_slice(),
                        storage.get_vector_data(ids[first + i]).as_ref(),
                        "run bytes diverge at offset {}",
                        ids[first + i],
                    );
                }
                visited += run_len;
            });
            assert_eq!(visited, ids.len());
        }
    }
}
