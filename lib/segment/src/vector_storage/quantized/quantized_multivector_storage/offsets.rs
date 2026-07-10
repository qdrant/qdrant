use std::ops::DerefMut as _;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::mmap::{Advice, AdviceSetting, MmapFlusher, MmapSlice};
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, MmapFile, MmapFs, OpenOptions, Populate, ReadRange, TypedStorage, UniversalRead,
    UniversalReadFs, UniversalWrite,
};
use fs_err as fs;
use memmap2::MmapMut;

use super::{MultivectorOffset, MultivectorOffsetsStorage};
use crate::common::operation_error::OperationResult;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::{ChunkedVectors, ChunkedVectorsRead};

pub struct MultivectorOffsetsStorageRam {
    offsets: Vec<MultivectorOffset>,
    path: PathBuf,
}

impl MultivectorOffsetsStorageRam {
    pub fn create(
        path: &Path,
        offsets: impl Iterator<Item = MultivectorOffset>,
    ) -> OperationResult<Self> {
        let offsets: Vec<_> = offsets.collect();
        create_offsets_file_from_iter(path, offsets.len(), offsets.iter().cloned())?;
        Ok(MultivectorOffsetsStorageRam {
            path: path.to_path_buf(),
            offsets,
        })
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        let offsets_file = fs::OpenOptions::new().read(true).write(true).open(path)?;
        let offsets_mmap = unsafe { MmapMut::map_mut(&offsets_file) }?;
        let mut offsets_mmap_type =
            unsafe { MmapSlice::<MultivectorOffset>::try_from(offsets_mmap)? };
        Ok(MultivectorOffsetsStorageRam {
            offsets: offsets_mmap_type.deref_mut().iter().copied().collect(),
            path: path.to_path_buf(),
        })
    }

    /// Load all offsets into RAM through the provided [`UniversalRead`] filesystem,
    /// performing no writes.
    ///
    /// Read-only counterpart of [`Self::load`] used by read-only storages.
    pub fn open<S: UniversalRead>(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
    ) -> OperationResult<Self> {
        let offsets = TypedStorage::<S, MultivectorOffset>::new(fs.open(
            path,
            OpenOptions {
                writeable: false,
                need_sequential: true,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?);
        Ok(MultivectorOffsetsStorageRam {
            offsets: offsets.read_whole()?.into_owned(),
            path: path.to_path_buf(),
        })
    }
}

impl MultivectorOffsetsStorage for MultivectorOffsetsStorageRam {
    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset {
        self.offsets[idx as usize]
    }

    fn for_each_offset(
        &self,
        ids: &[PointOffsetType],
        mut callback: impl FnMut(usize, MultivectorOffset),
    ) -> OperationResult<()> {
        for (index, &id) in ids.iter().enumerate() {
            callback(index, self.get_offset(id));
        }
        Ok(())
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn upsert_offset(
        &mut self,
        id: PointOffsetType,
        offset: MultivectorOffset,
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        // Skip hardware counter increment because it's a RAM storage.
        if id as usize >= self.len() {
            self.offsets
                .resize(id as usize + 1, MultivectorOffset::default());
        }
        self.offsets[id as usize] = offset;
        Ok(())
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
        let Self { offsets, path: _ } = self;
        offsets.capacity() * std::mem::size_of::<MultivectorOffset>()
    }
}

#[derive(Debug)]
pub struct MultivectorOffsetsStorageMmap<S: UniversalRead = MmapFile> {
    offsets: TypedStorage<S, MultivectorOffset>,
    path: PathBuf,
}

impl MultivectorOffsetsStorageMmap<MmapFile> {
    pub fn create(
        path: &Path,
        offsets: impl Iterator<Item = MultivectorOffset>,
        count: usize,
    ) -> OperationResult<Self> {
        create_offsets_file_from_iter(path, count, offsets)?;
        MultivectorOffsetsStorageMmap::load(path)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        Self::open(&MmapFs, path)
    }
}

impl<S: UniversalRead> MultivectorOffsetsStorageMmap<S> {
    /// Open the offsets file read-only through the provided [`UniversalRead`] filesystem.
    ///
    /// Performs no writes, making this the entry point used by read-only storages.
    pub fn open(fs: &impl UniversalReadFs<File = S>, path: &Path) -> OperationResult<Self> {
        let offsets = TypedStorage::<S, MultivectorOffset>::new(fs.open(
            path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?);

        Ok(Self {
            offsets,
            path: path.to_path_buf(),
        })
    }

    pub fn populate(&self) -> OperationResult<()> {
        Ok(self.offsets.populate()?)
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        Ok(self.offsets.clear_ram_cache()?)
    }
}

impl<S: UniversalRead> MultivectorOffsetsStorage for MultivectorOffsetsStorageMmap<S> {
    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset {
        let offset = self
            .offsets
            .read::<Random>(ReadRange::one(
                u64::from(idx) * size_of::<MultivectorOffset>() as u64,
            ))
            .expect("multi-vector offset read");

        let [offset] = offset.as_ref() else {
            unreachable!("multi-vector offsets are stored as a single-element slice");
        };

        *offset
    }

    fn for_each_offset(
        &self,
        ids: &[PointOffsetType],
        mut callback: impl FnMut(usize, MultivectorOffset),
    ) -> OperationResult<()> {
        let ranges = ids.iter().copied().enumerate().map(|(idx, id)| {
            let offset = u64::from(id) * size_of::<MultivectorOffset>() as u64;
            (idx, ReadRange::one(offset))
        });

        self.offsets
            .read_batch::<Random, _>(ranges, |idx, offset| {
                let [offset] = offset else {
                    unreachable!("multi-vector offsets are stored as a single-element slice");
                };
                callback(idx, *offset);
                Ok(())
            })?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.offsets
            .len()
            .expect("multi-vector offsets length read") as _
    }

    fn upsert_offset(
        &mut self,
        _id: PointOffsetType,
        _offset: MultivectorOffset,
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Cannot upsert offset in mmap storage",
        ))
    }

    fn flusher(&self) -> MmapFlusher {
        // Mmap storage does not need a flusher, as it is non-appendable and already backed by a file.
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn heap_size_bytes(&self) -> usize {
        let Self {
            offsets: _,
            path: _,
        } = self;

        0
    }
}

/// Appendable (chunked) multivector offsets storage, generic over the [`UniversalWrite`]
/// backend `S`. Read-write counterpart of [`MultivectorOffsetsStorageChunkedRead`]; the
/// backend `fs` is supplied by the caller (defaulting to [`MmapFile`]).
pub struct MultivectorOffsetsStorageChunked<S: UniversalWrite + Send + 'static = MmapFile> {
    data: ChunkedVectors<MultivectorOffset, S>,
}

impl<S: UniversalWrite + Send + 'static> MultivectorOffsetsStorageChunked<S> {
    pub fn create(
        fs: S::Fs,
        path: &Path,
        offsets: impl Iterator<Item = MultivectorOffset>,
        in_ram: bool,
    ) -> OperationResult<Self> {
        let hw_counter = HardwareCounterCell::disposable();
        let mut offsets_storage = Self::load(fs, path, in_ram)?;
        for (id, offset) in offsets.enumerate() {
            offsets_storage.upsert_offset(id as PointOffsetType, offset, &hw_counter)?;
        }
        offsets_storage.flusher()()?;
        Ok(offsets_storage)
    }

    pub fn load(fs: S::Fs, path: &Path, in_ram: bool) -> OperationResult<Self> {
        let advice = if in_ram {
            AdviceSetting::from(Advice::Normal)
        } else {
            AdviceSetting::Global
        };
        let data = ChunkedVectors::open(fs, path, 1, advice, Populate::from(in_ram))?;
        Ok(Self { data })
    }

    pub fn populate(&self) -> OperationResult<()> {
        self.data.populate()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self { data } = self;
        data.clear_cache()
    }
}

impl<S: UniversalWrite + Send + 'static> MultivectorOffsetsStorage
    for MultivectorOffsetsStorageChunked<S>
{
    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset {
        self.data
            .get::<Random>(idx as VectorOffsetType)
            .and_then(|offsets| offsets.first().copied())
            .unwrap_or_default()
    }

    fn for_each_offset(
        &self,
        ids: &[PointOffsetType],
        mut callback: impl FnMut(usize, MultivectorOffset),
    ) -> OperationResult<()> {
        let point_offsets = ids
            .iter()
            .enumerate()
            .map(|(idx, &point_offset)| (idx, point_offset, 1));

        self.data
            .for_each_vector::<Random, _>(point_offsets, |idx, multi_offset| {
                let &[multi_offset] = multi_offset.as_ref() else {
                    unreachable!("multi-vector offsets are stored as vectors of length 1");
                };

                callback(idx, multi_offset);
                Ok(())
            })?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn flusher(&self) -> MmapFlusher {
        let flusher = ChunkedVectors::flusher(&self.data);
        Box::new(move || {
            flusher().map_err(|e| {
                std::io::Error::other(format!("Failed to flush multivector offsets storage: {e}"))
            })?;
            Ok(())
        })
    }

    fn upsert_offset(
        &mut self,
        id: PointOffsetType,
        offset: MultivectorOffset,
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        self.data
            .insert(id as VectorOffsetType, &[offset], hw_counter)
            .map_err(std::io::Error::other)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.data.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.data.immutable_files()
    }

    fn heap_size_bytes(&self) -> usize {
        let Self { data: _ } = self;
        0
    }
}

/// Read-only counterpart of [`MultivectorOffsetsStorageChunked`], generic over
/// the [`UniversalRead`] backend `S`.
pub struct MultivectorOffsetsStorageChunkedRead<S: UniversalRead> {
    pub(super) data: ChunkedVectorsRead<MultivectorOffset, S>,
}

impl<S: UniversalRead> MultivectorOffsetsStorageChunkedRead<S> {
    /// Schedule background prefetch of the files [`Self::open`] will read.
    pub fn preopen(fs: &impl CachedReadFs<File = S>, path: &Path) -> OperationResult<()> {
        ChunkedVectorsRead::<MultivectorOffset, S>::preopen(
            fs,
            path,
            AdviceSetting::Global,
            Populate::No,
        )
    }

    pub fn open(fs: &impl UniversalReadFs<File = S>, path: &Path) -> OperationResult<Self> {
        let data = ChunkedVectorsRead::open(
            fs,
            path,
            1,
            AdviceSetting::Global,
            Populate::No, // TODO(uio): consider `in_ram`?
        )?;
        Ok(Self { data })
    }

    pub fn populate(&self) -> OperationResult<()> {
        self.data.populate()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.data.clear_cache()
    }
}

impl<S: UniversalRead> MultivectorOffsetsStorage for MultivectorOffsetsStorageChunkedRead<S> {
    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset {
        self.data
            .get::<Random>(idx as VectorOffsetType)
            .and_then(|offsets| offsets.first().copied())
            .unwrap_or_default()
    }

    fn for_each_offset(
        &self,
        ids: &[PointOffsetType],
        mut callback: impl FnMut(usize, MultivectorOffset),
    ) -> OperationResult<()> {
        let point_offsets = ids
            .iter()
            .enumerate()
            .map(|(idx, &point_offset)| (idx, point_offset, 1));

        self.data
            .for_each_vector::<Random, _>(point_offsets, |idx, multi_offset| {
                let &[multi_offset] = multi_offset.as_ref() else {
                    unreachable!("multi-vector offsets are stored as vectors of length 1");
                };

                callback(idx, multi_offset);
                Ok(())
            })?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn upsert_offset(
        &mut self,
        _id: PointOffsetType,
        _offset: MultivectorOffset,
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Cannot upsert offset in read-only chunked storage",
        ))
    }

    fn flusher(&self) -> MmapFlusher {
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<PathBuf> {
        self.data.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.data.immutable_files()
    }

    fn heap_size_bytes(&self) -> usize {
        self.data.heap_size_bytes()
    }
}

fn create_offsets_file_from_iter(
    path: &Path,
    count: usize,
    iter: impl Iterator<Item = MultivectorOffset>,
) -> OperationResult<()> {
    path.parent()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Path must have a parent directory",
            )
        })
        .and_then(fs::create_dir_all)?;

    let offsets_file_size = count * std::mem::size_of::<MultivectorOffset>();
    let offsets_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        // Don't truncate because we explicitly set the length later
        .truncate(false)
        .open(path)?;
    offsets_file.set_len(offsets_file_size as u64)?;

    let offsets_mmap = unsafe { MmapMut::map_mut(&offsets_file) }?;
    let mut offsets_mmap_type = unsafe { MmapSlice::<MultivectorOffset>::try_from(offsets_mmap)? };
    let offsets_mut: &mut [MultivectorOffset] = offsets_mmap_type.deref_mut();
    for (dst, src) in offsets_mut.iter_mut().zip(iter) {
        *dst = src;
    }
    offsets_mmap_type.flusher()()?;
    Ok(())
}
