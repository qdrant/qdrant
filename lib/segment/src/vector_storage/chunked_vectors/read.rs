use std::borrow::Cow;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};

use common::generic_consts::{AccessPattern, Random, Sequential};
use common::maybe_uninit::maybe_uninit_fill_from;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, Populate, ReadPipeline, ReadRange, TypedStorage, UniversalIoError, UniversalRead,
    UniversalReadFs, UserData, read_json_via, read_whole_via,
};
use num_traits::AsPrimitive;

use super::chunks::{chunk_name, preopen_chunks, read_chunks};
use super::config::{CONFIG_FILE_NAME, ChunkedVectorsConfig, STATUS_FILE_NAME};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::common::{PAGE_SIZE_BYTES, VECTOR_READ_BATCH_SIZE};
use crate::vector_storage::query_scorer::is_read_with_prefetch_efficient;
use crate::vector_storage::{VectorOffset, VectorOffsetType};

/// Read-only view over a chunked-vectors directory.
///
/// Holds the indexing logic (chunks, config, vector count) and exposes only the
/// read-side API. Status length is loaded from disk once at open time and is
/// not refreshed afterwards. Mutating storage uses [`super::ChunkedVectors`]
/// which wraps this and adds a writable status mmap.
#[derive(Debug)]
pub struct ChunkedVectorsRead<T: bytemuck::Pod + Send, S: UniversalRead> {
    pub(super) config: ChunkedVectorsConfig,
    /// Number of vectors currently stored. Snapshot for read-only mode; for
    /// [`super::ChunkedVectors`] this is kept in sync with the writable status
    /// mmap.
    pub(super) len: usize,
    pub(super) chunks: Vec<TypedStorage<S, T>>,
    pub(super) directory: PathBuf,
    /// Open-time chunk settings, reused by live-reload to open new chunks.
    pub(super) advice: AdviceSetting,
    pub(super) populate: Populate,
}

impl<T: bytemuck::Pod + Send, S: UniversalRead> ChunkedVectorsRead<T, S> {
    pub(super) fn config_file(directory: &Path) -> PathBuf {
        directory.join(CONFIG_FILE_NAME)
    }

    pub fn status_file(directory: &Path) -> PathBuf {
        directory.join(STATUS_FILE_NAME)
    }

    pub(super) fn load_config<Fs: UniversalReadFs>(
        fs: &Fs,
        config_file: &Path,
    ) -> OperationResult<Option<ChunkedVectorsConfig>> {
        match read_json_via::<Fs, ChunkedVectorsConfig>(fs, config_file) {
            Ok(config) => Ok(Some(config)),
            Err(UniversalIoError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Schedule background prefetch of every file [`Self::open`] will read.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        directory: &Path,
        advice: AdviceSetting,
        populate: Populate,
    ) -> OperationResult<()> {
        fs.schedule_prefetch(&Self::config_file(directory), None, None)?;
        fs.schedule_prefetch(&Self::status_file(directory), None, None)?;
        preopen_chunks(fs, directory, advice, populate)?;
        Ok(())
    }

    /// Open an existing chunked-vectors directory in read-only mode.
    ///
    /// Both `config.json` and `status.dat` must already exist; this function
    /// will not create them.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        directory: &Path,
        dim: usize,
        advice: AdviceSetting,
        populate: Populate,
    ) -> OperationResult<Self> {
        let config_file = Self::config_file(directory);
        let config = Self::load_config(fs, &config_file)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "Config file {} is missing",
                config_file.display(),
            ))
        })?;
        if config.dim != dim {
            return Err(OperationError::service_error(format!(
                "Wrong configuration in {}: expected {}, found {dim}",
                config_file.display(),
                config.dim,
            )));
        }

        let len = read_status_len(fs, &Self::status_file(directory))?;
        let chunks = read_chunks(fs, directory, advice, populate, false)?;

        Ok(Self {
            config,
            len,
            chunks,
            directory: directory.to_owned(),
            advice,
            populate,
        })
    }

    #[inline]
    pub(super) fn get_chunk_index(&self, key: usize) -> usize {
        key / self.config.chunk_size_vectors
    }

    /// Returns the byte offset of the vector in the chunk
    #[inline]
    pub(super) fn get_chunk_offset(&self, key: usize) -> usize {
        let chunk_vector_idx = key % self.config.chunk_size_vectors;
        chunk_vector_idx * self.config.dim
    }

    #[inline]
    pub fn max_vector_size_bytes(&self) -> usize {
        self.config.chunk_size_bytes
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn dim(&self) -> usize {
        self.config.dim
    }

    // returns how many vectors can be inserted starting from key
    pub fn get_remaining_chunk_keys(&self, start_key: VectorOffsetType) -> usize {
        let start_key = start_key.as_();
        let chunk_vector_idx = self.get_chunk_offset(start_key) / self.config.dim;
        self.config.chunk_size_vectors - chunk_vector_idx
    }

    #[inline]
    fn read_range(&self, offset: VectorOffsetType, count: usize) -> Option<(usize, ReadRange)> {
        if offset.checked_add(count)? > self.len {
            return None;
        }

        let chunk_idx = self.get_chunk_index(offset);
        if chunk_idx >= self.chunks.len() {
            return None;
        }

        let element_offset = self.get_chunk_offset(offset);
        let elements_length = count * self.config.dim;
        if element_offset + elements_length > self.config.chunk_size_vectors * self.config.dim {
            return None;
        }

        let range = ReadRange {
            byte_offset: (element_offset * size_of::<T>()) as u64,
            length: elements_length as u64,
        };

        Some((chunk_idx, range))
    }

    /// Returns `count` flattened vectors starting from `starting_key`.
    ///
    /// Returns `None` when:
    /// - chunk boundary is crossed
    /// - any section of `start_key..start_key + count` is out of bounds
    #[inline]
    fn get_many_impl(
        &self,
        start_key: VectorOffsetType,
        count: usize,
        force_sequential: bool,
    ) -> Option<Cow<'_, [T]>> {
        let (chunk_idx, range) = self.read_range(start_key, count)?;

        let chunk = &self.chunks[chunk_idx];

        let use_sequential =
            force_sequential || range.length as usize * size_of::<T>() > PAGE_SIZE_BYTES * 4;

        if use_sequential {
            chunk.read::<Sequential>(range).ok()
        } else {
            chunk.read::<Random>(range).ok()
        }
    }

    // non-linux implementation never returns Result::Err
    #[cfg_attr(not(target_os = "linux"), expect(clippy::unnecessary_wraps))]
    pub fn for_each_in_batch<F>(
        &self,
        keys: &[PointOffsetType],
        mut callback: F,
    ) -> OperationResult<()>
    where
        F: FnMut(usize, &[T]),
    {
        #[cfg(target_os = "linux")]
        if TypedStorage::<S, T>::kind() == common::universal_io::UniversalKind::IoUring {
            let point_offsets = keys
                .iter()
                .copied()
                .enumerate()
                .map(|(index, point_offset)| (index, point_offset, 1));

            return self.for_each_vector::<Random, _>(point_offsets, |idx, vectors| {
                callback(idx, vectors.as_ref());
                Ok(())
            });
        }

        // The `f` is most likely a scorer function. Fetching all vectors first, and then scoring
        // them is more cache friendly, than fetching and scoring in a single loop.

        let mut vectors_buffer = [const { MaybeUninit::uninit() }; VECTOR_READ_BATCH_SIZE];

        for (batch_idx, keys) in keys.chunks(VECTOR_READ_BATCH_SIZE).enumerate() {
            let force_sequential = is_read_with_prefetch_efficient(keys);

            let (vectors, _) = maybe_uninit_fill_from(
                &mut vectors_buffer,
                keys.iter().map(|&key| {
                    self.get_many_impl(key.offset(), 1, force_sequential)
                        .expect("vectors read")
                }),
            );

            let batch_offset = VECTOR_READ_BATCH_SIZE * batch_idx;

            for (vector_idx, vec) in vectors.iter().enumerate() {
                callback(batch_offset + vector_idx, vec.as_ref());
            }
        }

        Ok(())
    }

    /// Invoke `callback` for each flattened multi-vector at the given offsets.
    ///
    /// Drives the read pipeline directly across chunk files: refills it from the
    /// offsets, then drains completed reads.
    pub fn for_each_vector<P, U>(
        &self,
        mut offsets: impl Iterator<Item = (U, PointOffsetType, u32)>,
        mut callback: impl FnMut(U, Cow<'_, [T]>) -> OperationResult<()>,
    ) -> OperationResult<()>
    where
        P: AccessPattern,
        U: UserData,
    {
        // access pattern does not matter for io_uring
        let mut pipeline = S::ReadPipeline::<'_, U>::new()?;

        loop {
            while pipeline.can_schedule()
                && let Some((user_data, offset, count)) = offsets.next()
            {
                let (chunk_idx, range) = self
                    .read_range(offset as _, count as _)
                    .ok_or_else(|| OperationError::service_error("vector offset out of bounds"))?;
                let range = range.into_byte_range::<T>();
                pipeline.schedule::<P>(
                    user_data,
                    &self.chunks[chunk_idx].inner,
                    range,
                    align_of::<T>(),
                )?;
            }

            let Some((user_data, vector)) = pipeline.wait_bytemuck::<T>()? else {
                break;
            };
            callback(user_data, vector)?;
        }

        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = Vec::new();
        files.push(Self::config_file(&self.directory));
        files.push(Self::status_file(&self.directory));
        for chunk_idx in 0..self.chunks.len() {
            files.push(chunk_name(&self.directory, chunk_idx));
        }
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        vec![Self::config_file(&self.directory)] // TODO: Is config immutable?
    }

    #[inline]
    pub fn get<P: AccessPattern>(&self, key: VectorOffsetType) -> Option<Cow<'_, [T]>> {
        self.get_many_impl(key, 1, P::IS_SEQUENTIAL)
    }

    #[inline]
    pub fn get_many<P: AccessPattern>(
        &self,
        key: VectorOffsetType,
        count: usize,
    ) -> Option<Cow<'_, [T]>> {
        self.get_many_impl(key, count, P::IS_SEQUENTIAL)
    }

    pub fn is_on_disk(&self) -> bool {
        !self.config.populate.unwrap_or(false)
    }

    pub fn populate(&self) -> OperationResult<()> {
        for chunk in &self.chunks {
            chunk.populate()?;
        }
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            config: _,
            len: _,
            chunks,
            directory: _,
            advice: _,
            populate: _,
        } = self;
        for chunk in chunks {
            chunk.clear_ram_cache()?;
        }
        Ok(())
    }

    pub fn heap_size_bytes(&self) -> usize {
        let Self {
            config: _,
            len: _,
            chunks: _,
            directory: _,
            advice: _,
            populate: _,
        } = self;

        0
    }
}

pub(super) fn read_status_len<Fs: UniversalReadFs>(
    fs: &Fs,
    status_file: &Path,
) -> OperationResult<usize> {
    let needed = std::mem::size_of::<usize>();
    let len = read_whole_via(fs, status_file, |bytes| {
        let head = bytes.get(..needed).ok_or_else(|| {
            UniversalIoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "Status file {} is too short: {} < {needed}",
                    status_file.display(),
                    bytes.len(),
                ),
            ))
        })?;
        Ok(usize::from_ne_bytes(head.try_into().expect("size matches")))
    })?;
    Ok(len)
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::sorted_slice::SortedSlice;
    use common::universal_io::{MmapFile, MmapFs};
    use tempfile::Builder;

    use super::*;
    use crate::common::live_reload::LiveReload;
    use crate::vector_storage::chunked_vectors::ChunkedVectors;

    fn make_vec(seed: usize, dim: usize) -> Vec<f32> {
        (0..dim).map(|i| (seed * dim + i) as f32).collect()
    }

    /// A read-only view picks up writer-appended vectors after `live_reload`.
    #[test]
    fn live_reload_picks_up_appended_vectors() {
        const DIM: usize = 32;
        let dir = Builder::new().prefix("chunked_reload").tempdir().unwrap();
        let hw = HardwareCounterCell::disposable();

        let first: Vec<Vec<f32>> = (0..100).map(|s| make_vec(s, DIM)).collect();
        let second: Vec<Vec<f32>> = (100..250).map(|s| make_vec(s, DIM)).collect();

        let mut writer = ChunkedVectors::<f32, MmapFile>::open(
            MmapFs,
            dir.path(),
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        for vector in &first {
            writer.push(vector.as_slice(), &hw).unwrap();
        }
        writer.flusher()().unwrap();

        let mut reader = ChunkedVectorsRead::<f32, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.len(), first.len());

        // Append more through the writer, then reload the read-only view.
        for vector in &second {
            writer.push(vector.as_slice(), &hw).unwrap();
        }
        writer.flusher()().unwrap();

        let empty = SortedSlice::new(&[]).unwrap();
        reader.live_reload(&MmapFs, &empty, &empty, &hw).unwrap();

        assert_eq!(reader.len(), first.len() + second.len());
        let got = reader
            .get::<Random>(first.len() as VectorOffsetType)
            .unwrap();
        assert_eq!(got.as_ref(), second[0].as_slice());
    }

    /// `live_reload` opens only chunk files created since the last load, keeping
    /// the chunks it already holds (which still see vectors appended into them).
    #[test]
    fn live_reload_adopts_only_new_chunks() {
        const DIM: usize = 32; // 4096 vectors per test chunk
        let dir = Builder::new()
            .prefix("chunked_reload_grow")
            .tempdir()
            .unwrap();
        let hw = HardwareCounterCell::disposable();

        let mut writer = ChunkedVectors::<f32, MmapFile>::open(
            MmapFs,
            dir.path(),
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        for s in 0..4000 {
            writer.push(make_vec(s, DIM).as_slice(), &hw).unwrap();
        }
        writer.flusher()().unwrap();

        let mut reader = ChunkedVectorsRead::<f32, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.len(), 4000);
        assert_eq!(reader.chunks.len(), 1);

        for s in 4000..9000 {
            writer.push(make_vec(s, DIM).as_slice(), &hw).unwrap();
        }
        writer.flusher()().unwrap();

        let empty = SortedSlice::new(&[]).unwrap();
        reader.live_reload(&MmapFs, &empty, &empty, &hw).unwrap();

        assert_eq!(reader.len(), 9000);
        assert_eq!(reader.chunks.len(), 3, "two new chunk files adopted");

        // 4050 was appended into the already-open first chunk; 5000/8999 are new chunks.
        for offset in [3999, 4050, 5000, 8999] {
            assert_eq!(
                reader.get::<Random>(offset).unwrap().as_ref(),
                make_vec(offset, DIM).as_slice(),
            );
        }
    }
}
