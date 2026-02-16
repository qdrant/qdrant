use std::borrow::Cow;
use std::io;
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};

use ahash::AHashMap;
use fs_err as fs;

use super::advice::{Advice, AdviceSetting};
use super::mmap_readonly::MmapSliceReadOnly;
use super::mmap_rw::{Error as MmapError, MmapFlusher, MmapSlice};
use super::ops::{
    MULTI_MMAP_IS_SUPPORTED, create_and_ensure_length, open_read_mmap, open_write_mmap,
};

const MMAP_CHUNKS_PATTERN_START: &str = "chunk_";
const MMAP_CHUNKS_PATTERN_END: &str = ".mmap";

/// Memory mapped chunk data, that can be read and written and also maintain sequential read-only view
#[derive(Debug)]
pub struct UniversalMmapChunk<T: Copy + Sized + 'static> {
    /// Main data mmap slice for read/write
    ///
    /// Best suited for random reads.
    mmap: MmapSlice<T>,
    /// Read-only mmap slice best suited for sequential reads
    ///
    /// `None` on platforms that do not support multiple memory maps to the same file.
    /// Use [`as_seq_slice`] utility function to access this mmap slice if available.
    _mmap_seq: Option<MmapSliceReadOnly<T>>,

    cached: Option<crate::disk_cache::CachedFile>,
}

pub enum MmapChunkView<'a, T: Copy + Sized + 'static> {
    Slice(&'a [T]),
    Box(Box<[T]>),
}

pub enum MmapChunkViewMut<'a, T: Copy + Sized + 'static> {
    Slice(&'a mut [T]),
    Box(Box<[T]>),
}

impl<T: Copy + Sized + 'static> MmapChunkView<'_, T> {
    pub fn as_slice(&self) -> &[T] {
        match self {
            MmapChunkView::Slice(slice) => slice,
            MmapChunkView::Box(boxed) => boxed.as_ref(),
        }
    }
}

impl<T: Copy + Sized + 'static> Default for MmapChunkView<'_, T> {
    fn default() -> Self {
        MmapChunkView::Slice(&[])
    }
}

impl<T: Copy + Sized + 'static> AsRef<[T]> for MmapChunkView<'_, T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T: Copy + Sized + 'static> Deref for MmapChunkView<'_, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<'a, T: Copy + Sized + 'static> From<MmapChunkView<'a, T>> for Cow<'a, [T]> {
    fn from(view: MmapChunkView<'a, T>) -> Self {
        match view {
            MmapChunkView::Slice(slice) => Cow::Borrowed(slice),
            MmapChunkView::Box(boxed) => Cow::Owned(boxed.into_vec()),
        }
    }
}

impl<T: Copy + Sized + 'static> UniversalMmapChunk<T> {
    pub fn get(&self, range: Range<usize>) -> MmapChunkView<'_, T> {
        if let Some(cached) = &self.cached {
            return get_cached(&cached, range);
        }
        MmapChunkView::Slice(&self.mmap[range])
    }

    pub fn get_seq(&self, range: Range<usize>) -> MmapChunkView<'_, T> {
        if let Some(cached) = &self.cached {
            return get_cached(&cached, range);
        }
        MmapChunkView::Slice(&self.as_seq_slice()[range])
    }

    pub fn write(&mut self, range: Range<usize>, data: &[T]) {
        self.mmap[range].copy_from_slice(data);
    }

    /// Helper to get a slice suited for sequential reads if available, otherwise use the main mmap
    fn as_seq_slice(&self) -> &[T] {
        #[expect(clippy::used_underscore_binding)]
        self._mmap_seq
            .as_ref()
            .map(|m| m.as_ref())
            .unwrap_or(self.mmap.as_ref())
    }

    pub fn flusher(&self) -> MmapFlusher {
        self.mmap.flusher()
    }

    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }

    pub fn populate(&self) -> io::Result<()> {
        #[expect(clippy::used_underscore_binding)]
        if let Some(mmap_seq) = &self._mmap_seq {
            mmap_seq.populate()?;
        }
        Ok(())
    }
}

/// Checks if the file name matches the pattern for mmap chunks
/// Return ID from the file name if it matches, None otherwise
fn check_mmap_file_name_pattern(file_name: &str) -> Option<usize> {
    file_name
        .strip_prefix(MMAP_CHUNKS_PATTERN_START)
        .and_then(|file_name| file_name.strip_suffix(MMAP_CHUNKS_PATTERN_END))
        .and_then(|file_name| file_name.parse::<usize>().ok())
}

pub fn read_mmaps<T: Copy + Sized>(
    directory: &Path,
    populate: bool,
    advice: AdviceSetting,
) -> Result<Vec<UniversalMmapChunk<T>>, MmapError> {
    let mut mmap_files: AHashMap<usize, _> = AHashMap::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let chunk_id = path
                .file_name()
                .and_then(|file_name| file_name.to_str())
                .and_then(check_mmap_file_name_pattern);

            if let Some(chunk_id) = chunk_id {
                mmap_files.insert(chunk_id, path);
            }
        }
    }

    let num_chunks = mmap_files.len();
    let mut result = Vec::with_capacity(num_chunks);
    for chunk_id in 0..num_chunks {
        let mmap_file = mmap_files.remove(&chunk_id).ok_or_else(|| {
            MmapError::MissingFile(format!(
                "Missing mmap chunk {chunk_id} in {}",
                directory.display(),
            ))
        })?;

        let mmap = open_write_mmap(&mmap_file, advice, populate)?;
        let mmap = unsafe { MmapSlice::try_from(mmap) }?;

        // Only open second mmap for sequential reads if supported
        let mmap_seq = if *MULTI_MMAP_IS_SUPPORTED {
            let mmap_seq =
                open_read_mmap(&mmap_file, AdviceSetting::Advice(Advice::Sequential), false)?;
            Some(unsafe { MmapSliceReadOnly::try_from(mmap_seq) }?)
        } else {
            None
        };

        let cached = crate::disk_cache::CacheController::global()
            .map(|controller| controller.open_file(&mmap_file))
            .transpose()?;

        result.push(UniversalMmapChunk {
            mmap,
            _mmap_seq: mmap_seq,
            cached,
        });
    }
    Ok(result)
}

pub fn chunk_name(directory: &Path, chunk_id: usize) -> PathBuf {
    directory.join(format!(
        "{MMAP_CHUNKS_PATTERN_START}{chunk_id}{MMAP_CHUNKS_PATTERN_END}",
    ))
}

pub fn create_chunk<T: Copy + Sized>(
    directory: &Path,
    chunk_id: usize,
    chunk_length_bytes: usize,
) -> Result<UniversalMmapChunk<T>, MmapError> {
    let chunk_file_path = chunk_name(directory, chunk_id);
    create_and_ensure_length(&chunk_file_path, chunk_length_bytes)?;
    let mmap = open_write_mmap(
        &chunk_file_path,
        AdviceSetting::Global,
        false, // don't populate newly created chunk, as it's empty and will be filled later
    )?;
    let mmap = unsafe { MmapSlice::try_from(mmap) }?;

    // Only open second mmap for sequential reads if supported
    let mmap_seq = if *MULTI_MMAP_IS_SUPPORTED {
        let mmap_seq = open_read_mmap(
            &chunk_file_path,
            AdviceSetting::Advice(Advice::Sequential),
            false,
        )?;
        Some(unsafe { MmapSliceReadOnly::try_from(mmap_seq) }?)
    } else {
        None
    };

    Ok(UniversalMmapChunk {
        mmap,
        _mmap_seq: mmap_seq,
        cached: None,
    })
}

pub fn get_cached<'a, T: Copy + Sized + 'static>(
    file: &'a crate::disk_cache::CachedFile,
    range: Range<usize>,
) -> MmapChunkView<'a, T> {
    let range = (range.start * size_of::<T>())..(range.end * size_of::<T>());
    // SAFETY: no
    match file.get_range(range) {
        Cow::Borrowed(b) => MmapChunkView::Slice(unsafe {
            std::slice::from_raw_parts(b.as_ptr() as *const T, b.len() / size_of::<T>())
        }),
        Cow::Owned(o) => MmapChunkView::Box({
            unsafe { crate::disk_cache::unsafe_transmute_zerocopy_vec(o) }.into_boxed_slice()
        }),
    }
}
