use std::borrow::Cow;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crate::generic_consts::AccessPattern;
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
pub trait MmapAccess<T>: AsRef<[T]> + std::fmt::Debug + Sized {
    fn open_mmap(path: &Path, advice: AdviceSetting, populate: bool) -> Result<Self>;

    fn populate(&self) -> std::io::Result<()>;

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// MmapMut-backed slice
impl<T: Copy + 'static> MmapAccess<T> for MmapSlice<T> {
    fn open_mmap(path: &Path, advice: AdviceSetting, populate: bool) -> Result<Self> {
        let mmap = open_write_mmap(path, advice, populate)
            .map_err(|err| UniversalIoError::extract_not_found(err, path))?;
        Ok(unsafe { MmapSlice::try_from(mmap) }?)
    }

    fn populate(&self) -> std::io::Result<()> {
        MmapSlice::populate(self)
    }
}

// Mmap (read only) backed slice
impl<T: Copy + 'static> MmapAccess<T> for MmapSliceReadOnly<T> {
    fn open_mmap(path: &Path, advice: AdviceSetting, populate: bool) -> Result<Self> {
        let mmap = open_read_mmap(path, advice, populate)
            .map_err(|err| UniversalIoError::extract_not_found(err, path))?;
        Ok(unsafe { MmapSliceReadOnly::try_from(mmap) }?)
    }

    fn populate(&self) -> std::io::Result<()> {
        MmapSliceReadOnly::populate(self)
    }
}

/// Memory-mapped universal I/O, generic over the primary mmap type.
///
/// `M` defaults to [`MmapSlice<T>`] (read-write) for backward compatibility.
/// Use [`MmapUniversalRo<T>`] for a read-only variant that opens with `Mmap` instead of `MmapMut`.
#[derive(Debug)]
pub struct MmapUniversal<T: Copy + 'static, M: MmapAccess<T> = MmapSlice<T>> {
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

/// Read-write mmap universal (default).
pub type MmapUniversalRw<T> = MmapUniversal<T, MmapSlice<T>>;

/// Read-only mmap universal.
pub type MmapUniversalRo<T> = MmapUniversal<T, MmapSliceReadOnly<T>>;

impl<T, M> MmapUniversal<T, M>
where
    T: Copy + 'static,
    M: MmapAccess<T>,
{
    fn as_seq_slice(&self) -> &[T] {
        self.mmap_seq
            .as_ref()
            .map(|m| m.as_ref())
            .unwrap_or(self.mmap.as_ref())
    }

    fn as_slice<P: AccessPattern>(&self) -> &[T] {
        if P::IS_SEQUENTIAL {
            self.as_seq_slice()
        } else {
            self.mmap.as_ref()
        }
    }
}

impl<T, M> UniversalReadFileOps for MmapUniversal<T, M>
where
    T: 'static + Copy,
    M: MmapAccess<T>,
{
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        local_list_files(prefix_path)
    }

    fn exists(path: &Path) -> Result<bool> {
        fs_err::exists(path).map_err(Into::into)
    }
}

impl<T, M> UniversalRead<T> for MmapUniversal<T, M>
where
    T: Copy + 'static,
    M: MmapAccess<T>,
{
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let OpenOptions {
            need_sequential,
            disk_parallel: _,
            populate,
            advice,
        } = options;

        let mmap_file = path.as_ref();
        let advice = advice.unwrap_or(AdviceSetting::Global);

        let mmap = M::open_mmap(mmap_file, advice, populate.unwrap_or_default())?;

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

    fn read<P: AccessPattern>(&self, range: ElementsRange) -> Result<Cow<'_, [T]>> {
        let data_slice = self.as_slice::<P>();
        let start = range.start as usize;
        let end = start + range.length as usize;

        let data_range = data_slice
            .get(start..end)
            .ok_or(UniversalIoError::OutOfBounds {
                start: start as u64,
                end: end as u64,
                elements: data_slice.len(),
            })?;

        Ok(Cow::Borrowed(data_range))
    }

    fn read_batch<P: AccessPattern, E: From<UniversalIoError>>(
        &self,
        ranges: impl IntoIterator<Item = ElementsRange>,
        mut callback: impl FnMut(usize, &[T]) -> Result<(), E>,
    ) -> Result<(), E> {
        let data_slice = self.as_slice::<P>();
        let data_length = data_slice.len();

        for (idx, range) in ranges.into_iter().enumerate() {
            let start = range.start as usize;
            let end = start + range.length as usize;

            let data_range = data_slice
                .get(start..end)
                .ok_or(UniversalIoError::OutOfBounds {
                    start: start as u64,
                    end: end as u64,
                    elements: data_length,
                })?;

            callback(idx, data_range)?;
        }

        Ok(())
    }

    fn len(&self) -> Result<u64> {
        Ok(self.mmap.len() as u64)
    }

    fn populate(&self) -> Result<()> {
        if let Some(mmap_seq) = &self.mmap_seq {
            mmap_seq.populate()?;
        } else {
            self.mmap.populate()?;
        }
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        crate::fs::clear_disk_cache(&self.path)?;
        Ok(())
    }

    fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        Ok(Cow::Borrowed(self.mmap.as_ref()))
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
                start: start as u64,
                end: end as u64,
                elements: data_length,
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
