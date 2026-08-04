use std::cmp::Ordering;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::universal_io::{
    BufferedAppend, OpenOptions, Populate, StoredStruct, UniversalAppend, UniversalReadFs,
    UniversalWrite,
};

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::chunks::{chunk_name, chunk_open_options};
use crate::vector_storage::chunked_vectors::config::{
    ChunkedVectorsConfig, Status, ensure_config, ensure_status_file,
};
use crate::vector_storage::chunked_vectors::lifecycle::combine_flushers;

/// Append-only writer for chunked vectors storage.
pub struct ChunkedVectorsAppender<T, S: UniversalAppend + UniversalWrite> {
    chunks: Vec<BufferedAppend<S>>,
    directory: PathBuf,
    config: ChunkedVectorsConfig,
    status: StoredStruct<S, Status>,
    fs: S::Fs,
    _t: PhantomData<T>,
}

impl<T, S> ChunkedVectorsAppender<T, S>
where
    T: bytemuck::Pod + Send,
    S: UniversalAppend + UniversalWrite + 'static,
{
    /// Open a chunked-vectors directory for appending, creating it if missing.
    pub fn open(fs: S::Fs, directory: &Path, dim: usize) -> OperationResult<Self>
    where
        <S::Fs as UniversalReadFs>::OpenExtra: Clone,
    {
        fs_err::create_dir_all(directory)?;
        let status_path = ensure_status_file(&fs, directory)?;
        let mut status: StoredStruct<S, Status> = StoredStruct::open(
            &fs,
            status_path,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;
        let config = ensure_config::<T, _>(&fs, directory, dim, false)?;

        let vector_size_bytes = config.dim * size_of::<T>();
        let total_bytes_len = status.len * vector_size_bytes;

        // Reopen the chunks holding already-stored vectors to resume appending.
        // perf: opens (and mmaps) every chunk although only the tail one can
        // receive appends; a directory listing could validate the rest unopened
        let num_chunks = status.len.div_ceil(config.chunk_size_vectors);
        let mut chunks = Vec::with_capacity(num_chunks);
        let mut adopted = false;
        for chunk_id in 0..num_chunks {
            let expected_len = config
                .chunk_size_bytes
                .min(total_bytes_len.saturating_sub(chunk_id * config.chunk_size_bytes))
                as u64;
            let chunk = BufferedAppend::open_with_expected_len(
                &fs,
                chunk_name(directory, chunk_id),
                chunk_open_options(AdviceSetting::Global, Populate::No, true),
                Default::default(),
                expected_len,
            )?;

            let persisted_len = chunk.persisted_len();
            match persisted_len.cmp(&expected_len) {
                Ordering::Less => {
                    return Err(OperationError::inconsistent_storage(format!(
                        "vectors chunk is smaller than expected: expected len: {expected_len}, persisted len: {persisted_len}",
                    )));
                }
                Ordering::Equal => {}
                // Atomic-append backend: the extra tail is whole appends that
                // missed the last status flush — adopt them
                Ordering::Greater => {
                    let chunk_end_bytes =
                        chunk_id * config.chunk_size_bytes + persisted_len as usize;
                    if chunk_end_bytes > total_bytes_len {
                        assert!(chunk_end_bytes.is_multiple_of(vector_size_bytes));
                        status.len = chunk_end_bytes / vector_size_bytes;
                        adopted = true;
                    }
                }
            }
            chunks.push(chunk);
        }
        if adopted {
            status.flusher()()?;
        }

        Ok(Self {
            chunks,
            directory: directory.to_owned(),
            config,
            status,
            fs,
            _t: PhantomData,
        })
    }

    fn add_chunk(&mut self) -> OperationResult<()> {
        let chunk_file_path = chunk_name(&self.directory, self.chunks.len());
        let chunk = BufferedAppend::create(
            &self.fs,
            &chunk_file_path,
            chunk_open_options(AdviceSetting::Global, Populate::No, true),
            Default::default(),
        )?;

        self.chunks.push(chunk);

        Ok(())
    }

    #[inline]
    pub fn append_many(
        &mut self,
        vectors: &[T],
        count: usize,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let start_key = self.status.len;
        let (chunk_idx, chunk_offset) = self.config.chunk_slot(start_key, count, vectors.len())?;

        // Appending at the end of storage needs at most one new chunk
        if chunk_idx == self.chunks.len() {
            self.add_chunk()?;
        }

        let chunk = &mut self.chunks[chunk_idx];

        chunk.append((chunk_offset * size_of::<T>()) as u64, vectors)?;

        hw_counter
            .vector_io_write_counter()
            .incr_delta(size_of_val(vectors));

        self.status.len = start_key + count;

        Ok(())
    }

    pub fn append(
        &mut self,
        vector: &[T],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<VectorOffsetType> {
        let new_id = self.status.len;
        self.append_many(vector, 1, hw_counter)?;
        Ok(new_id)
    }

    pub fn flusher(&self) -> Flusher {
        let chunks_flushers = self.chunks.iter().map(|chunk| chunk.flusher());
        combine_flushers(chunks_flushers.collect(), self.status.flusher())
    }
}
