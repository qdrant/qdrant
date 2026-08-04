#[cfg(test)]
mod tests;

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::universal_io::{
    OpenOptions, Populate, StoredStruct, UniversalAppend, UniversalReadFileOps, UniversalReadFs,
    UniversalWrite, UniversalWriteFileOps,
};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::chunked_vectors::chunks::{
    check_mmap_file_name_pattern, chunk_name, chunks_prefix,
};
use crate::vector_storage::chunked_vectors::config::{ChunkedVectorsConfig, Status, status_file};

/// Short-lived append-only writer for chunked vectors storage.
///
/// Holds no chunk handles: each [`append_many`](Self::append_many) opens the
/// touched chunks, appends, and persists the vector count, so a batch is
/// durable when it returns and there is nothing to flush.
#[derive(Debug)]
#[cfg_attr(not(test), expect(dead_code))]
pub struct UpdateOnlyChunkedVectors<T, S: UniversalAppend + UniversalWrite> {
    directory: PathBuf,
    config: ChunkedVectorsConfig,
    status: StoredStruct<S, Status>,
    fs: S::Fs,
    _t: PhantomData<T>,
}

#[cfg_attr(not(test), expect(dead_code))]
impl<T, S> UpdateOnlyChunkedVectors<T, S>
where
    T: bytemuck::Pod + Send,
    S: UniversalAppend + UniversalWrite + 'static,
{
    /// Open a chunked-vectors directory for appending, creating it if missing.
    pub fn open(fs: S::Fs, directory: &Path, dim: usize) -> OperationResult<Self> {
        let status_path = status_file(directory);
        if !fs.exists(&status_path)? {
            fs.create_dir(directory)?;
        }
        ChunkedVectors::<T, S>::ensure_status_file(&fs, &status_path)?;
        let status: StoredStruct<S, Status> = StoredStruct::open(
            &fs,
            &status_path,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;
        let config = ChunkedVectors::<T, S>::ensure_config(&fs, directory, dim, false)?;

        let appender = Self {
            directory: directory.to_owned(),
            config,
            status,
            fs,
            _t: PhantomData,
        };
        appender.check_chunk_lengths()?;
        Ok(appender)
    }

    /// Compare every chunk file's length against the stored vector count,
    /// without opening any chunk.
    ///
    /// Rejects mismatches in both directions: a shorter chunk lost
    /// acknowledged data, a longer one holds unacknowledged appends from a
    /// crashed writer, which we don't attempt to adopt for now.
    fn check_chunk_lengths(&self) -> OperationResult<()> {
        let total_bytes = self.status.len * self.config.dim * size_of::<T>();
        let num_chunks = self.status.len.div_ceil(self.config.chunk_size_vectors);

        let mut listed_sizes: AHashMap<usize, u64> = AHashMap::new();
        for listed in self.fs.list_files(&chunks_prefix(&self.directory))? {
            let chunk_id = listed
                .path
                .file_name()
                .and_then(|file_name| file_name.to_str())
                .and_then(check_mmap_file_name_pattern);
            if let Some(chunk_id) = chunk_id {
                listed_sizes.insert(chunk_id, listed.size);
            }
        }

        for chunk_id in 0..num_chunks {
            let expected = self
                .config
                .chunk_size_bytes
                .min(total_bytes.saturating_sub(chunk_id * self.config.chunk_size_bytes))
                as u64;
            match listed_sizes.remove(&chunk_id) {
                Some(size) if size == expected => {}
                Some(size) => {
                    return Err(OperationError::inconsistent_storage(format!(
                        "Chunk {chunk_id} length {size} doesn't match the expected {expected}",
                    )));
                }
                None => {
                    return Err(OperationError::inconsistent_storage(format!(
                        "Missing chunk {chunk_id} in {}",
                        self.directory.display(),
                    )));
                }
            }
        }

        // Files past the watermark hold no acknowledged data; a non-empty one
        // is an unacknowledged append from a crashed writer.
        for (chunk_id, size) in listed_sizes {
            if size > 0 {
                return Err(OperationError::inconsistent_storage(format!(
                    "Chunk {chunk_id} past the stored vector count is not empty ({size} bytes)",
                )));
            }
        }

        Ok(())
    }

    /// Append a batch of vectors at the end of the storage, one file append
    /// per touched chunk, then persist the new vector count.
    pub fn append_many<'a>(
        &mut self,
        vectors: impl IntoIterator<Item = &'a [T]>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut vectors = vectors.into_iter().peekable();
        let mut len = self.status.len;

        while vectors.peek().is_some() {
            let chunk_idx = self.config.get_chunk_index(len);
            let chunk_offset = self.config.get_chunk_offset(len);
            let capacity = self.config.chunk_size_vectors - len % self.config.chunk_size_vectors;

            let batch: Vec<&[T]> = vectors.by_ref().take(capacity).collect();
            for vector in &batch {
                assert_eq!(vector.len(), self.config.dim, "Vector size mismatch");
            }
            let batch_bytes = batch.iter().map(|vector| size_of_val(*vector)).sum();

            let mut chunk = self.open_chunk_for_append(chunk_idx, chunk_offset == 0)?;
            chunk.append_batch(
                (chunk_offset * size_of::<T>()) as u64,
                batch.iter().copied(),
            )?;
            // Flush in case of local backends.
            chunk.flusher()()?;

            hw_counter.vector_io_write_counter().incr_delta(batch_bytes);
            len += batch.len();
        }

        // Persist the watermark only after the data landed
        self.status.len = len;
        self.status.flusher()()?;

        Ok(())
    }

    /// Open the chunk for appending; a `new` chunk is past the watermark, so
    /// it is created, truncating any leftover from a crashed writer.
    fn open_chunk_for_append(&self, chunk_idx: usize, new: bool) -> OperationResult<S> {
        let path = chunk_name(&self.directory, chunk_idx);
        if new {
            self.fs.create(&path, 0)?;
        }
        Ok(self.fs.open(
            &path,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?)
    }
}
