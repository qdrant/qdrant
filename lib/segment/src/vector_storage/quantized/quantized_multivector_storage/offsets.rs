use std::ops::DerefMut as _;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::mmap::{Advice, AdviceSetting, Flusher, MmapSlice};
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, MmapFile, MmapFs, OpenOptions, Populate, ReadRange, TypedStorage, UioResult,
    UniversalRead, UniversalReadFs, UniversalWrite,
};
use fs_err as fs;
use memmap2::MmapMut;

use super::{MultivectorOffset, MultivectorOffsetsStorage};
use crate::common::operation_error::OperationResult;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::chunked_vectors::read_only::ReadOnlyChunkedVectors;

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

    fn open_options(populate: Populate) -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: true,
            populate,
            advice: AdviceSetting::Global,
        }
    }

    /// Schedule background prefetch of the offsets file [`Self::open`] reads.
    ///
    /// The open reads the whole file, so the prefetch populates it.
    pub fn preopen(
        fs: &impl CachedReadFs,
        path: &Path,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        fs.schedule_open(
            path,
            Some(Self::open_options(Populate::PreferBackground)),
            None,
            hw_counter,
        );
        Ok(())
    }

    /// Load all offsets into RAM through the provided [`UniversalRead`] filesystem,
    /// performing no writes.
    ///
    /// Read-only counterpart of [`Self::load`] used by read-only storages.
    pub fn open<S: UniversalRead>(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        let offsets = TypedStorage::<S, MultivectorOffset>::new(fs.open(
            path,
            Self::open_options(Populate::No),
            Default::default(),
            hw_counter,
        )?);
        Ok(MultivectorOffsetsStorageRam {
            offsets: offsets.read_whole(hw_counter)?.into_owned(),
            path: path.to_path_buf(),
        })
    }
}

impl MultivectorOffsetsStorage for MultivectorOffsetsStorageRam {
    fn get_offset(
        &self,
        idx: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> MultivectorOffset {
        self.offsets[idx as usize]
    }

    fn for_each_offset(
        &self,
        ids: &[PointOffsetType],
        mut callback: impl FnMut(usize, MultivectorOffset),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        for (index, &id) in ids.iter().enumerate() {
            callback(index, self.get_offset(id, hw_counter));
        }
        Ok(())
    }

    fn len(&self, _hw_counter: &HardwareCounterCell) -> usize {
        self.offsets.len()
    }

    fn upsert_offset(
        &mut self,
        id: PointOffsetType,
        offset: MultivectorOffset,
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        // Skip hardware counter increment because it's a RAM storage.
        if id as usize >= self.len(hw_counter) {
            self.offsets
                .resize(id as usize + 1, MultivectorOffset::default());
        }
        self.offsets[id as usize] = offset;
        Ok(())
    }

    fn flusher(&self) -> Flusher {
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
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        create_offsets_file_from_iter(path, count, offsets)?;
        MultivectorOffsetsStorageMmap::load(path, hw_counter)
    }

    pub fn load(path: &Path, hw_counter: &HardwareCounterCell) -> OperationResult<Self> {
        Self::open(&MmapFs, path, hw_counter)
    }
}

impl<S: UniversalRead> MultivectorOffsetsStorageMmap<S> {
    fn open_options(populate: Populate) -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate,
            advice: AdviceSetting::Global,
        }
    }

    /// Schedule background prefetch of the offsets file [`Self::open`] reads.
    ///
    /// The offsets are read lazily; `populate` warms the parked handle for
    /// the `cached` memory placement.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        populate: Populate,
        hw_counter: &HardwareCounterCell,
    ) {
        fs.schedule_open(path, Some(Self::open_options(populate)), None, hw_counter);
    }

    /// Open the offsets file read-only through the provided [`UniversalRead`] filesystem.
    ///
    /// Performs no writes, making this the entry point used by read-only storages.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        let offsets = TypedStorage::<S, MultivectorOffset>::new(fs.open(
            path,
            Self::open_options(Populate::No),
            Default::default(),
            hw_counter,
        )?);

        Ok(Self {
            offsets,
            path: path.to_path_buf(),
        })
    }

    pub fn populate(&self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        Ok(self.offsets.populate(hw_counter)?)
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        Ok(self.offsets.clear_ram_cache()?)
    }
}

impl<S: UniversalRead> MultivectorOffsetsStorage for MultivectorOffsetsStorageMmap<S> {
    fn get_offset(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> MultivectorOffset {
        let offset = self
            .offsets
            .read(
                ReadRange::one(u64::from(idx) * size_of::<MultivectorOffset>() as u64),
                Random,
                hw_counter,
            )
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
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let ranges = ids.iter().copied().enumerate().map(|(idx, id)| {
            let offset = u64::from(id) * size_of::<MultivectorOffset>() as u64;
            (idx, ReadRange::one(offset))
        });

        self.offsets.read_batch(
            ranges,
            Random,
            |idx, offset| {
                let [offset] = offset else {
                    unreachable!("multi-vector offsets are stored as a single-element slice");
                };
                callback(idx, *offset);
                UioResult::Ok(())
            },
            hw_counter,
        )?;
        Ok(())
    }

    fn len(&self, hw_counter: &HardwareCounterCell) -> usize {
        self.offsets
            .len(hw_counter)
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

    fn flusher(&self) -> Flusher {
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
        let mut offsets_storage = Self::load(fs, path, in_ram, &hw_counter)?;
        for (id, offset) in offsets.enumerate() {
            offsets_storage.upsert_offset(id as PointOffsetType, offset, &hw_counter)?;
        }
        offsets_storage.flusher()()?;
        Ok(offsets_storage)
    }

    pub fn load(
        fs: S::Fs,
        path: &Path,
        in_ram: bool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        let advice = if in_ram {
            AdviceSetting::from(Advice::Normal)
        } else {
            AdviceSetting::Global
        };
        let data = ChunkedVectors::open(fs, path, 1, advice, Populate::from(in_ram), hw_counter)?;
        Ok(Self { data })
    }

    pub fn populate(&self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        self.data.populate(hw_counter)
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self { data } = self;
        data.clear_cache()
    }
}

impl<S: UniversalWrite + Send + 'static> MultivectorOffsetsStorage
    for MultivectorOffsetsStorageChunked<S>
{
    fn get_offset(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> MultivectorOffset {
        self.data
            .get::<Random>(idx as VectorOffsetType, hw_counter)
            .and_then(|offsets| offsets.first().copied())
            .unwrap_or_default()
    }

    fn for_each_offset(
        &self,
        ids: &[PointOffsetType],
        mut callback: impl FnMut(usize, MultivectorOffset),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let point_offsets = ids
            .iter()
            .enumerate()
            .map(|(idx, &point_offset)| (idx, point_offset, 1));

        self.data.for_each_vector::<Random, _>(
            point_offsets,
            |idx, multi_offset| {
                let &[multi_offset] = multi_offset.as_ref() else {
                    unreachable!("multi-vector offsets are stored as vectors of length 1");
                };

                callback(idx, multi_offset);
                Ok(())
            },
            hw_counter,
        )?;
        Ok(())
    }

    fn len(&self, _hw_counter: &HardwareCounterCell) -> usize {
        self.data.len()
    }

    fn flusher(&self) -> Flusher {
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
    pub(super) data: ReadOnlyChunkedVectors<MultivectorOffset, S>,
}

impl<S: UniversalRead> MultivectorOffsetsStorageChunkedRead<S> {
    /// Schedule background prefetch of the files [`Self::open`] will read.
    ///
    /// `populate` warms the parked chunks for the `cached` memory placement;
    /// the open itself always maps them lazily.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        populate: Populate,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        ReadOnlyChunkedVectors::<MultivectorOffset, S>::preopen(
            fs,
            path,
            AdviceSetting::Global,
            populate,
            hw_counter,
        )
    }

    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        let data = ReadOnlyChunkedVectors::open(
            fs,
            path,
            1,
            AdviceSetting::Global,
            Populate::No, // TODO(uio): consider `in_ram`?
            hw_counter,
        )?;
        Ok(Self { data })
    }

    pub fn populate(&self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        self.data.populate(hw_counter)
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.data.clear_cache()
    }
}

impl<S: UniversalRead> MultivectorOffsetsStorage for MultivectorOffsetsStorageChunkedRead<S> {
    fn get_offset(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> MultivectorOffset {
        self.data
            .get::<Random>(idx as VectorOffsetType, hw_counter)
            .and_then(|offsets| offsets.first().copied())
            .unwrap_or_default()
    }

    fn for_each_offset(
        &self,
        ids: &[PointOffsetType],
        mut callback: impl FnMut(usize, MultivectorOffset),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let point_offsets = ids
            .iter()
            .enumerate()
            .map(|(idx, &point_offset)| (idx, point_offset, 1));

        self.data.for_each_vector::<Random, _>(
            point_offsets,
            |idx, multi_offset| {
                let &[multi_offset] = multi_offset.as_ref() else {
                    unreachable!("multi-vector offsets are stored as vectors of length 1");
                };

                callback(idx, multi_offset);
                Ok(())
            },
            hw_counter,
        )?;
        Ok(())
    }

    fn len(&self, _hw_counter: &HardwareCounterCell) -> usize {
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

    fn flusher(&self) -> Flusher {
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
