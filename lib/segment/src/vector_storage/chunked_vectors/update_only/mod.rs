#[cfg(test)]
mod tests;

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::universal_io::{
    OpenOptions, Populate, UniversalAppend, UniversalReadFileOps, UniversalReadFs,
    UniversalWriteFileOps,
};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::chunks::{chunk_name, list_chunk_files};
use crate::vector_storage::chunked_vectors::config::{
    ChunkedVectorsConfig, Status, ensure_config, read_status_len, status_file,
};

/// Short-lived append-only writer for chunked vectors storage.
///
/// Holds no chunk handles: each [`append_many`](Self::append_many) opens the
/// touched chunks, appends, and persists the vector count, so a batch is
/// durable when it returns and there is nothing to flush.
#[derive(Debug)]
#[cfg_attr(not(test), expect(dead_code))]
pub struct UpdateOnlyChunkedVectors<T, S: UniversalAppend> {
    directory: PathBuf,
    config: ChunkedVectorsConfig,
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
    S: UniversalAppend + 'static,
{
    /// Open a chunked-vectors directory for appending, creating it if missing.
    pub fn open(fs: S::Fs, directory: &Path, dim: usize) -> OperationResult<Self> {
        let status_path = status_file(directory);
        // An absent status file marks the first open. Writing it eagerly keeps
        // the directory readable even if no batch ever lands.
        if !fs.exists(&status_path)? {
            fs.create_dir(directory)?;
            fs.atomic_save(&status_path, bytemuck::bytes_of(&Status { len: 0 }))?;
        }
        let config = ensure_config::<T, _>(&fs, directory, dim, false)?;

        Ok(Self {
            directory: directory.to_owned(),
            config,
            fs,
            _t: PhantomData,
        })
    }

    /// The vector count the directory records, read from storage every time so
    /// that it reflects what a previous writer left rather than anything this
    /// handle remembers.
    ///
    /// Needed where a storage's rows are not indexed by point slot — the
    /// multivector ones — so a batch knows where the row space ends.
    pub fn stored_len(&self) -> OperationResult<usize> {
        read_status_len(&self.fs, &status_file(&self.directory))
    }

    /// How many more vectors fit in the chunk that `key` falls in.
    ///
    /// A vector never straddles a chunk, so a caller placing a run of rows has
    /// to skip to the next chunk when the run does not fit in this one.
    pub fn remaining_chunk_keys(&self, key: usize) -> usize {
        self.config.remaining_chunk_capacity(key)
    }

    /// Replace the stored vector count.
    fn save_len(&self, len: usize) -> OperationResult<()> {
        self.fs.atomic_save(
            &status_file(&self.directory),
            bytemuck::bytes_of(&Status { len }),
        )?;
        Ok(())
    }

    /// Compare every chunk file's length against an external total length.
    ///
    /// Ensures every file is at the expected length by truncating or filling with zeroes.
    fn ensure_chunk_lengths(&self, target_len: usize) -> OperationResult<()> {
        let total_bytes = target_len * self.config.dim * size_of::<T>();
        let num_chunks = target_len.div_ceil(self.config.chunk_size_vectors);

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
                            let content = file.read_whole::<u8>()?;
                            let Some(truncated) = content.get(..expected as usize) else {
                                return Err(OperationError::service_error(format!(
                                    "Chunk {chunk_id} is {} bytes, shorter than the expected \
                                     truncation length {expected}",
                                    content.len(),
                                )));
                            };
                            let truncated = truncated.to_vec();
                            drop(file);
                            self.fs.atomic_save(&file_info.path, &truncated)?;
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

        // Files past the boundary hold no data this writer should serve
        for (chunk_id, file) in listed {
            log::warn!(
                "Chunk {chunk_id} past the target vector count ({} bytes). Removing.",
                file.size,
            );
            self.fs.remove(&file.path)?;
        }

        self.save_len(target_len)?;

        Ok(())
    }

    /// Append a batch of vectors at the end of the storage, one file append per
    /// touched chunk, then persist the new vector count.
    ///
    /// This method trusts the `start_key` to be the source of truth, so it will
    /// fill with zeroes or truncate chunks if necessary to make chunks' sizes
    /// match the argument
    // Takes &mut self to enforce the single-writer contract the appends rest on
    #[allow(clippy::needless_pass_by_ref_mut)]
    pub fn append_many<'a>(
        &mut self,
        start_key: VectorOffsetType,
        vectors: impl IntoIterator<Item = &'a [T]>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.ensure_chunk_lengths(start_key)?;

        let mut vectors = vectors.into_iter().peekable();
        let mut len = start_key;

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
        self.save_len(len)?;

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
