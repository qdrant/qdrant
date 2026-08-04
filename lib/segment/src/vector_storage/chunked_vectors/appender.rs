use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::universal_io::{
    BufferedAppend, OpenOptions, Populate, StoredStruct, UniversalAppend, UniversalWrite,
    UniversalWriteFileOps, read_whole_via,
};

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::chunked_vectors::chunks::chunk_name;
use crate::vector_storage::chunked_vectors::config::{ChunkedVectorsConfig, Status};

/// Append-only writer for chunked vectors storage.
pub struct ChunkedVectorsAppender<T, S: UniversalAppend + UniversalWrite> {
    chunks: Vec<BufferedAppend<S>>,
    directory: PathBuf,
    config: ChunkedVectorsConfig,
    status: StoredStruct<S, Status>,
    fs: S::Fs,
    _t: PhantomData<T>,
}

/// Options shared by the status and chunk files: write-only handles.
fn write_options() -> OpenOptions {
    OpenOptions {
        writeable: true,
        need_sequential: false,
        populate: Populate::No,
        advice: AdviceSetting::Global,
    }
}

impl<T, S> ChunkedVectorsAppender<T, S>
where
    T: bytemuck::Pod + Send,
    S: UniversalAppend + UniversalWrite + 'static,
{
    /// Open a chunked-vectors directory for appending, creating it if missing.
    pub fn open(fs: S::Fs, directory: &Path, dim: usize) -> OperationResult<Self> {
        fs_err::create_dir_all(directory)?;
        let status_path = ChunkedVectors::<T, S>::ensure_status_file(&fs, directory)?;
        let mut status: StoredStruct<S, Status> =
            StoredStruct::open(&fs, status_path, write_options(), Default::default())?;
        let config = ChunkedVectors::<T, S>::ensure_config(&fs, directory, dim, false)?;

        // Reopen the chunks holding already-stored vectors to resume appending.
        let num_chunks = status.len.div_ceil(config.chunk_size_vectors);
        let mut chunks = Vec::with_capacity(num_chunks);

        let total_bytes_len = status.len * config.dim * size_of::<T>();
        for chunk_id in 0..num_chunks {
            let chunk_path = chunk_name(directory, chunk_id);
            let chunk =
                BufferedAppend::open(&fs, &chunk_path, write_options(), Default::default())?;

            let expected_len = config
                .chunk_size_bytes
                .min(total_bytes_len.saturating_sub(chunk_id * config.chunk_size_bytes)) as u64;
            let persisted_len = chunk.persisted_len();

            // Validate chunk length
            match persisted_len.cmp(&expected_len) {
                std::cmp::Ordering::Less => {
                    return Err(OperationError::inconsistent_storage(format!(
                        "vectors chunk is smaller than expected: expected len: {}, persisted len: {}",
                        expected_len, persisted_len
                    )));
                }
                std::cmp::Ordering::Equal => {
                    // Ok
                    chunks.push(chunk)
                }
                std::cmp::Ordering::Greater => {
                    if S::APPEND_IS_ATOMIC {
                        // Adopt new length
                        let new_total_bytes_len =
                            chunk_id * config.chunk_size_bytes + persisted_len as usize;

                        if new_total_bytes_len > total_bytes_len {
                            assert!(
                                new_total_bytes_len.is_multiple_of(config.dim * size_of::<T>())
                            );
                            let new_vectors_len =
                                new_total_bytes_len / (config.dim * size_of::<T>());
                            status.len = new_vectors_len;
                            status.flusher()()?;
                        }

                        chunks.push(chunk)
                    } else {
                        // Truncate to prevent torn reads
                        drop(chunk);

                        let content =
                            read_whole_via(&fs, &chunk_path, |bytes| Ok(bytes.into_owned()))?;
                        fs.atomic_save(&chunk_path, &content[..expected_len as usize])?;

                        let chunk = BufferedAppend::open(
                            &fs,
                            &chunk_path,
                            write_options(),
                            Default::default(),
                        )?;

                        assert_eq!(expected_len as u64, chunk.persisted_len());

                        chunks.push(chunk)
                    }
                }
            }
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
            write_options(),
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
        assert_eq!(
            vectors.len(),
            count * self.config.dim,
            "Vector size mismatch"
        );

        let start_key = self.status.len;
        let chunk_idx = self.config.get_chunk_index(start_key);
        let chunk_offset = self.config.get_chunk_offset(start_key);

        // check if the vectors fit in the chunk
        if chunk_offset + vectors.len() > self.config.dim * self.config.chunk_size_vectors {
            return Err(OperationError::service_error(format!(
                "Vectors do not fit in the chunk. Chunk idx {chunk_idx}, chunk offset {chunk_offset}, vectors count {count}",
            )));
        }

        // Ensure capacity
        while chunk_idx >= self.chunks.len() {
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
        let status_flusher = self.status.flusher();
        let chunks_flushers: Vec<_> = self.chunks.iter().map(|chunk| chunk.flusher()).collect();

        Box::new(move || {
            for flusher in chunks_flushers {
                flusher()?;
            }
            status_flusher()?;
            Ok(())
        })
    }
}
