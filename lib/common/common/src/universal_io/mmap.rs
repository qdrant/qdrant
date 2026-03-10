use std::borrow::Cow;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crate::mmap::{
    Advice, AdviceSetting, MULTI_MMAP_IS_SUPPORTED, MmapSlice, MmapSliceReadOnly, open_read_mmap,
    open_write_mmap,
};
use crate::universal_io::file_ops::UniversalReadFileOps;
use crate::universal_io::local_file_ops::local_list_files;
use crate::universal_io::{
    ElementOffset, ElementsRange, Flusher, OpenOptions, Result, UniversalIoError, UniversalRead,
    UniversalWrite,
};

/// Trait for mmap types that support read access to a slice of `T`.
///
/// Both [`MmapSlice<T>`] and [`MmapSliceReadOnly<T>`] satisfy this trait.
pub trait MmapReadAccess<T>: AsRef<[T]> + std::fmt::Debug {
    fn populate(&self) -> std::io::Result<()>;
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: Copy + 'static> MmapReadAccess<T> for MmapSlice<T> {
    fn populate(&self) -> std::io::Result<()> {
        MmapSlice::populate(self)
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

impl<T: Copy + 'static> MmapReadAccess<T> for MmapSliceReadOnly<T> {
    fn populate(&self) -> std::io::Result<()> {
        MmapSliceReadOnly::populate(self)
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

/// Memory-mapped universal I/O, generic over the primary mmap type.
///
/// `M` defaults to [`MmapSlice<T>`] (read-write) for backward compatibility.
/// Use [`MmapUniversalRo<T>`] for a read-only variant that opens with `Mmap` instead of `MmapMut`.
#[derive(Debug)]
pub struct MmapUniversal<T: Copy + 'static, M: MmapReadAccess<T> = MmapSlice<T>> {
    path: PathBuf,
    /// Main data mmap slice for read (and optionally write)
    ///
    /// Best suited for random reads.
    mmap: M,
    /// Read-only mmap slice best suited for sequential reads
    ///
    /// `None` on platforms that do not support multiple memory maps to the same file.
    mmap_seq: Option<MmapSliceReadOnly<T>>,
    _phantom: PhantomData<T>,
}

/// Read-write mmap universal (default, backward-compatible alias).
pub type MmapUniversalRw<T> = MmapUniversal<T, MmapSlice<T>>;

/// Read-only mmap universal.
pub type MmapUniversalRo<T> = MmapUniversal<T, MmapSliceReadOnly<T>>;

// --- Shared read logic ---

impl<T, M> MmapUniversal<T, M>
where
    T: Copy + 'static,
    M: MmapReadAccess<T>,
{
    fn as_seq_slice(&self) -> &[T] {
        self.mmap_seq
            .as_ref()
            .map(|m| m.as_ref())
            .unwrap_or(self.mmap.as_ref())
    }

    fn as_slice<const SEQUENTIAL: bool>(&self) -> &[T] {
        if SEQUENTIAL {
            self.as_seq_slice()
        } else {
            self.mmap.as_ref()
        }
    }

    fn read_impl<const SEQUENTIAL: bool>(&self, range: ElementsRange) -> Result<Cow<'_, [T]>> {
        let data_slice = self.as_slice::<SEQUENTIAL>();
        let start = range.start as usize;
        let end = start + range.length as usize;

        let data_range = data_slice
            .get(start..end)
            .ok_or(UniversalIoError::OutOfBounds {
                start: start as u64,
                end: end as u64,
                data_length: data_slice.len(),
            })?;

        Ok(Cow::Borrowed(data_range))
    }

    fn read_batch_impl<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = ElementsRange>,
        mut callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        let data_slice = self.as_slice::<SEQUENTIAL>();
        let data_length = data_slice.len();

        for (idx, range) in ranges.into_iter().enumerate() {
            let start = range.start as usize;
            let end = start + range.length as usize;

            let data_range = data_slice
                .get(start..end)
                .ok_or(UniversalIoError::OutOfBounds {
                    start: start as u64,
                    end: end as u64,
                    data_length,
                })?;

            callback(idx, data_range)?;
        }

        Ok(())
    }

    #[allow(clippy::unnecessary_wraps)]
    fn len_impl(&self) -> Result<u64> {
        Ok(self.mmap.len() as u64)
    }

    fn populate_impl(&self) -> Result<()> {
        if let Some(mmap_seq) = &self.mmap_seq {
            mmap_seq.populate()?;
        } else {
            self.mmap.populate()?;
        }
        Ok(())
    }

    fn clear_ram_cache_impl(&self) -> Result<()> {
        crate::fs::clear_disk_cache(&self.path)?;
        Ok(())
    }

    #[allow(clippy::unnecessary_wraps)]
    fn read_whole_impl(&self) -> Result<Cow<'_, [T]>> {
        Ok(Cow::Borrowed(self.mmap.as_ref()))
    }
}

// --- UniversalReadFileOps (generic over M) ---

impl<T, M> UniversalReadFileOps for MmapUniversal<T, M>
where
    T: 'static + Copy,
    M: MmapReadAccess<T>,
{
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        local_list_files(prefix_path)
    }

    fn exists(path: &Path) -> Result<bool> {
        fs_err::exists(path).map_err(Into::into)
    }
}

// --- UniversalRead for read-write variant ---

impl<T> UniversalRead<T> for MmapUniversal<T, MmapSlice<T>>
where
    T: Copy + 'static,
{
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self>
    where
        Self: Sized,
    {
        let OpenOptions {
            need_sequential,
            disk_parallel: _,
            populate,
            advice,
        } = options;

        let mmap_file = path.as_ref();
        let advice = advice.unwrap_or(AdviceSetting::Global);

        let mmap =
            open_write_mmap(mmap_file, advice, populate.unwrap_or_default()).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    UniversalIoError::NotFound {
                        path: mmap_file.to_path_buf(),
                    }
                } else {
                    e.into()
                }
            })?;
        let mmap = unsafe { MmapSlice::try_from(mmap) }?;

        let mmap_seq = if *MULTI_MMAP_IS_SUPPORTED && need_sequential {
            let mmap_seq =
                open_read_mmap(mmap_file, AdviceSetting::Advice(Advice::Sequential), false)?;
            Some(unsafe { MmapSliceReadOnly::try_from(mmap_seq) }?)
        } else {
            None
        };

        Ok(MmapUniversal {
            path: mmap_file.to_path_buf(),
            mmap,
            mmap_seq,
            _phantom: PhantomData,
        })
    }

    fn read<const SEQUENTIAL: bool>(&self, range: ElementsRange) -> Result<Cow<'_, [T]>> {
        self.read_impl::<SEQUENTIAL>(range)
    }

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = ElementsRange>,
        callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        self.read_batch_impl::<SEQUENTIAL>(ranges, callback)
    }

    fn len(&self) -> Result<u64> {
        self.len_impl()
    }

    fn populate(&self) -> Result<()> {
        self.populate_impl()
    }

    fn clear_ram_cache(&self) -> Result<()> {
        self.clear_ram_cache_impl()
    }

    fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        self.read_whole_impl()
    }
}

// --- UniversalRead for read-only variant ---

impl<T> UniversalRead<T> for MmapUniversal<T, MmapSliceReadOnly<T>>
where
    T: Copy + 'static,
{
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self>
    where
        Self: Sized,
    {
        let OpenOptions {
            need_sequential,
            disk_parallel: _,
            populate,
            advice,
        } = options;

        let mmap_file = path.as_ref();
        let advice = advice.unwrap_or(AdviceSetting::Global);

        let mmap =
            open_read_mmap(mmap_file, advice, populate.unwrap_or_default()).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    UniversalIoError::NotFound {
                        path: mmap_file.to_path_buf(),
                    }
                } else {
                    e.into()
                }
            })?;
        let mmap = unsafe { MmapSliceReadOnly::try_from(mmap) }?;

        let mmap_seq = if *MULTI_MMAP_IS_SUPPORTED && need_sequential {
            let mmap_seq =
                open_read_mmap(mmap_file, AdviceSetting::Advice(Advice::Sequential), false)?;
            Some(unsafe { MmapSliceReadOnly::try_from(mmap_seq) }?)
        } else {
            None
        };

        Ok(MmapUniversal {
            path: mmap_file.to_path_buf(),
            mmap,
            mmap_seq,
            _phantom: PhantomData,
        })
    }

    fn read<const SEQUENTIAL: bool>(&self, range: ElementsRange) -> Result<Cow<'_, [T]>> {
        self.read_impl::<SEQUENTIAL>(range)
    }

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = ElementsRange>,
        callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        self.read_batch_impl::<SEQUENTIAL>(ranges, callback)
    }

    fn len(&self) -> Result<u64> {
        self.len_impl()
    }

    fn populate(&self) -> Result<()> {
        self.populate_impl()
    }

    fn clear_ram_cache(&self) -> Result<()> {
        self.clear_ram_cache_impl()
    }

    fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        self.read_whole_impl()
    }
}

// --- UniversalWrite only for read-write variant ---

impl<T> UniversalWrite<T> for MmapUniversal<T, MmapSlice<T>>
where
    T: Copy + 'static,
{
    fn write(&mut self, offset: ElementOffset, data: &[T]) -> Result<()> {
        let mmap_slice: &mut [T] = &mut self.mmap;
        let data_length = mmap_slice.len();
        let start = offset as usize;
        let end = start + data.len();

        let target = mmap_slice
            .get_mut(start..end)
            .ok_or(UniversalIoError::OutOfBounds {
                start: offset,
                end: offset + data.len() as u64,
                data_length,
            })?;

        target.copy_from_slice(data);
        Ok(())
    }

    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ElementOffset, &'a [T])>,
    ) -> Result<()> {
        for (offset, data) in offset_data {
            self.write(offset, data)?;
        }
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let inner = self.mmap.flusher();
        Box::new(move || Ok(inner()?)) // Converts error type to UniversalIoError
    }
}
