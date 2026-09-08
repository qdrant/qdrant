#[cfg(test)]
mod tests;

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::universal_io::{
    OpenOptions, Populate, UniversalAppend, UniversalFlush as _, UniversalRead as _,
    UniversalReadFs, UniversalWriteFileOps,
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
pub struct UpdateOnlyChunkedVectors<T> {
    directory: PathBuf,
    config: ChunkedVectorsConfig,
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

impl<T> UpdateOnlyChunkedVectors<T>
where
    T: bytemuck::Pod + Send,
{
    /// Open a chunked-vectors directory for appending, creating it if missing.
    pub fn open<Fs: UniversalReadFs + UniversalWriteFileOps>(
        fs: &Fs,
        directory: &Path,
        dim: usize,
    ) -> OperationResult<Self> {
        let status_path = status_file(directory);
        // An absent status file marks the first open. Writing it eagerly keeps
        // the directory readable even if no batch ever lands.
        if !fs.exists(&status_path)? {
            fs.create_dir(directory)?;
            fs.atomic_save(&status_path, bytemuck::bytes_of(&Status { len: 0 }))?;
        }
        let config = ensure_config::<T, _>(fs, directory, dim, false)?;

        Ok(Self {
            directory: directory.to_owned(),
            config,
            _t: PhantomData,
        })
    }

    /// The vector count the directory records, read from storage every time so
    /// that it reflects what a previous writer left rather than anything this
    /// handle remembers.
    ///
    /// Needed where a storage's rows are not indexed by point slot — the
    /// multivector ones — so a batch knows where the row space ends.
    pub fn stored_len<Fs: UniversalReadFs>(&self, fs: &Fs) -> OperationResult<usize> {
        Ok(read_status_len(fs, &status_file(&self.directory))?)
    }

    /// Replace the stored vector count.
    fn save_len<Fs: UniversalWriteFileOps>(&self, fs: &Fs, len: usize) -> OperationResult<()> {
        fs.atomic_save(
            &status_file(&self.directory),
            bytemuck::bytes_of(&Status { len }),
        )?;
        Ok(())
    }

    /// Compare every chunk file's length against an external total length.
    ///
    /// Ensures every file is at the expected length by truncating or filling with zeroes.
    fn ensure_chunk_lengths<Fs>(&self, fs: &Fs, target_len: usize) -> OperationResult<()>
    where
        Fs: UniversalReadFs<File: UniversalAppend> + UniversalWriteFileOps,
    {
        let total_bytes = target_len * self.config.dim * size_of::<T>();
        let num_chunks = target_len.div_ceil(self.config.chunk_size_vectors);

        let mut listed = list_chunk_files(fs, &self.directory)?;

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
                            let mut file = fs.open_append(&file_info.path, append_options())?;
                            file.append(file_info.size, &data)?;
                            file.flusher()()?;
                        }
                        std::cmp::Ordering::Greater => {
                            // truncate
                            log::warn!("Expected smaller chunk, truncating chunk {chunk_id}");
                            let file = fs.open_append(&file_info.path, append_options())?;
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
                            fs.atomic_save(&file_info.path, &truncated)?;
                        }
                    }
                }
                None => {
                    // create and fill with zeroes
                    log::warn!(
                        "Expected non-existing chunk {chunk_id}, creating and filling with zeroes"
                    );
                    let mut file = self.open_chunk_for_append(fs, chunk_id, true)?;
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
            fs.remove(&file.path)?;
        }

        self.save_len(fs, target_len)?;

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
    pub fn append_many<'a, I, Fs>(
        &mut self,
        fs: &Fs,
        start_key: VectorOffsetType,
        vectors: I,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>
    where
        I: IntoIterator<Item = &'a [T]>,
        I::IntoIter: ExactSizeIterator,
        Fs: UniversalReadFs<File: UniversalAppend> + UniversalWriteFileOps,
    {
        self.ensure_chunk_lengths(fs, start_key)?;

        let mut vectors = vectors.into_iter();
        let count = vectors.len();

        for part in self.config.split_run(start_key, count) {
            // The part an empty run resolves to: no batch to append, and
            // opening its chunk would create the file for nothing
            if part.count == 0 {
                continue;
            }

            let batch: Vec<&[T]> = vectors.by_ref().take(part.count).collect();
            for vector in &batch {
                assert_eq!(vector.len(), self.config.dim, "Vector size mismatch");
            }
            let batch_bytes = batch.len() * self.config.dim * size_of::<T>();

            let mut chunk =
                self.open_chunk_for_append(fs, part.chunk_idx, part.element_offset == 0)?;
            chunk.append_batch(
                (part.element_offset * size_of::<T>()) as u64,
                batch.iter().copied(),
            )?;
            // Flush in case of local backends.
            chunk.flusher()()?;

            hw_counter.vector_io_write_counter().incr_delta(batch_bytes);
        }

        // Persist the watermark only after the data landed
        self.save_len(fs, start_key + count)?;

        Ok(())
    }

    /// Open the chunk for appending; a `new` chunk is past the watermark, so
    /// it is created, truncating any leftover from a crashed writer.
    fn open_chunk_for_append<Fs>(
        &self,
        fs: &Fs,
        chunk_idx: usize,
        new: bool,
    ) -> OperationResult<Fs::File>
    where
        Fs: UniversalReadFs<File: UniversalAppend> + UniversalWriteFileOps,
    {
        let path = chunk_name(&self.directory, chunk_idx);
        if new {
            fs.create(&path, 0)?;
        }
        Ok(fs.open(&path, append_options(), Default::default())?)
    }
}
