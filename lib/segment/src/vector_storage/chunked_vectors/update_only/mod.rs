#[cfg(test)]
mod tests;

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::universal_io::{
    OpenOptions, Populate, StoredStruct, UniversalAppend, UniversalReadFileOps, UniversalReadFs,
    UniversalWrite, UniversalWriteFileOps,
};

use crate::common::operation_error::OperationResult;
use crate::vector_storage::chunked_vectors::chunks::{chunk_name, list_chunk_files};
use crate::vector_storage::chunked_vectors::config::{
    ChunkedVectorsConfig, Status, ensure_config, status_file,
};

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

/// Options for the short-lived append-only handles
fn append_options() -> OpenOptions {
    OpenOptions {
        writeable: true,
        need_sequential: false,
        populate: Populate::No,
        advice: AdviceSetting::Global,
    }
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
        // An absent status file marks the first open
        if !fs.exists(&status_path)? {
            fs.create_dir(directory)?;
            fs.create(&status_path, size_of::<Status>())?;
        }
        let status: StoredStruct<S, Status> =
            StoredStruct::open(&fs, &status_path, append_options(), Default::default())?;
        let config = ensure_config::<T, _>(&fs, directory, dim, false)?;

        let writer = Self {
            directory: directory.to_owned(),
            config,
            status,
            fs,
            _t: PhantomData,
        };
        writer.ensure_chunk_lengths()?;
        Ok(writer)
    }

    /// Compare every chunk file's length against the stored vector count.
    ///
    /// Ensures every file is at the expected length by truncating or filling with zeroes.
    fn ensure_chunk_lengths(&self) -> OperationResult<()> {
        let total_bytes = self.status.len * self.config.dim * size_of::<T>();
        let num_chunks = self.status.len.div_ceil(self.config.chunk_size_vectors);

        let mut listed = list_chunk_files(&self.fs, &self.directory)?;

        for chunk_id in 0..num_chunks {
            let expected = self
                .config
                .chunk_size_bytes
                .min(total_bytes.saturating_sub(chunk_id * self.config.chunk_size_bytes))
                as u64;
            match listed.remove(&chunk_id) {
                Some(file_info) => {
                    match file_info.size.cmp(&expected) {
                        std::cmp::Ordering::Equal => {
                            // Ok
                        }
                        std::cmp::Ordering::Less => {
                            // fill with zeroes
                            log::warn!(
                                "Expected larger chunk, filling chunk {chunk_id} with zeroes"
                            );
                            let data = vec![0u8; (expected - file_info.size) as usize];
                            let mut file =
                                self.fs.open_append(&file_info.path, append_options())?;
                            file.append(file_info.size, &data)?;
                            file.flusher()()?;
                        }
                        std::cmp::Ordering::Greater => {
                            // truncate
                            log::warn!("Expected smaller chunk, truncating chunk {chunk_id}");
                            let file = self.fs.open_append(&file_info.path, append_options())?;
                            let content = file.read_whole::<u8>()?.into_owned();
                            drop(file);
                            self.fs
                                .atomic_save(&file_info.path, &content[..expected as usize])?;
                        }
                    }
                }
                None => {
                    // create and fill with zeroes
                    log::warn!(
                        "Expected non-existing chunk {chunk_id}, creating and filling with zeroes"
                    );
                    let mut file = self.open_chunk_for_append(chunk_id, true)?;
                    file.append(0, &vec![0u8; expected as usize])?;
                    file.flusher()()?;
                }
            }
        }

        // Files past the watermark hold no acknowledged data; a non-empty one
        // is an unacknowledged append from a crashed writer.
        for (chunk_id, file) in listed {
            if file.size > 0 {
                log::warn!(
                    "Chunk {chunk_id} past the stored vector count is not empty ({} bytes). Removing.",
                    file.size,
                );
                self.fs.remove(&file.path)?;
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
            let capacity = self.config.remaining_chunk_capacity(len);

            let batch: Vec<&[T]> = vectors.by_ref().take(capacity).collect();
            for vector in &batch {
                assert_eq!(vector.len(), self.config.dim, "Vector size mismatch");
            }
            let batch_bytes = batch.len() * self.config.dim * size_of::<T>();

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
        Ok(self.fs.open(&path, append_options(), Default::default())?)
    }
}
